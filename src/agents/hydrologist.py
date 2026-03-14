"""Hydrologist Agent: data lineage from SQL, dbt, Airflow, and Python data flow."""

import re
from pathlib import Path
from typing import Any

from src.analyzers import (
    analyze_dbt_directory,
    analyze_sql_directory,
    analyze_sql_file,
)
from src.analyzers.dag_config_parser import parse_airflow_dag_python
from src.analyzers.python_data_flow import extract_python_data_flow
from src.graph import LineageGraph


_EXCLUDE_DIRS = {".git", "__pycache__", "node_modules", ".venv", "venv"}

_DBT_REF_RE = re.compile(r"\{\{\s*ref\s*\(\s*['\"](\w+)['\"]\s*\)\s*\}\}")


def _add_sql_lineage(lg: LineageGraph, repo_path: Path) -> None:
    """Add lineage from SQL files (and dbt models)."""
    for path in repo_path.rglob("*.sql"):
        if any(d in path.parts for d in _EXCLUDE_DIRS):
            continue
        try:
            source = path.read_bytes()
        except OSError:
            continue
        if not source.strip():
            continue
        rel = str(path.relative_to(repo_path)).replace("\\", "/")
        r = analyze_sql_file(path, source)
        if r["errors"] and not r["sources"]:
            continue
        lg.add_transformation(
            r["transform_id"],
            rel,
            r["transformation_type"],
            source_datasets=r["sources"],
            target_datasets=r["targets"],
            line_range=r.get("line_range"),
        )


def _add_dbt_lineage(lg: LineageGraph, repo_path: Path) -> None:
    """Add lineage from dbt schema.yml ref() relationships."""
    dbt_result = analyze_dbt_directory(repo_path)
    for model in dbt_result.get("models", []):
        name = model.get("name")
        refs = model.get("refs", [])
        if not name or not refs:
            continue
        transform_id = f"dbt:schema:{name}"
        lg.add_transformation(
            transform_id,
            "dbt schema",
            "dbt_model",
            source_datasets=refs,
            target_datasets=[name],
        )


def _add_dbt_ref_lineage(lg: LineageGraph, repo_path: Path) -> None:
    """Extract dbt ref() dependencies from SQL model files (Jinja-aware).

    Standard dbt SQL uses {{ ref('model') }} macros which sqlglot cannot parse.
    This function regex-scans .sql files to find those references and adds the
    correct staging→mart (and other cross-model) edges that the SQL parser misses.
    """
    for path in repo_path.rglob("*.sql"):
        if any(d in path.parts for d in _EXCLUDE_DIRS):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        refs = _DBT_REF_RE.findall(text)
        if not refs:
            continue
        rel = str(path.relative_to(repo_path)).replace("\\", "/")
        output_name = path.stem
        transform_id = f"{path}:{output_name}"
        # add_transformation is idempotent for node creation; adding edges here
        # supplements whatever the SQL parser already built.
        lg.add_transformation(
            transform_id,
            rel,
            "dbt_model",
            source_datasets=refs,
            target_datasets=[output_name],
        )


def _add_airflow_lineage(lg: LineageGraph, repo_path: Path) -> None:
    """Add task dependencies from Airflow DAG Python files as lineage (task flow)."""
    for path in repo_path.rglob("*.py"):
        if any(d in path.parts for d in _EXCLUDE_DIRS):
            continue
        # Heuristic: only analyze files in dags/ or named *dag*.py
        path_str = str(path).replace("\\", "/")
        if "dags" not in path_str and "dag" not in path.name.lower():
            continue
        try:
            source = path.read_bytes()
        except OSError:
            continue
        r = parse_airflow_dag_python(path, source)
        if not r["dag_id"] and not r["dependencies"]:
            continue
        dag_id = r["dag_id"] or path.stem
        for up, down in r["dependencies"]:
            # Treat as task-level flow: upstream_task -> downstream_task
            transform_id = f"airflow:{dag_id}:{up}->{down}"
            lg.add_transformation(
                transform_id,
                str(path.relative_to(repo_path)).replace("\\", "/"),
                "airflow_task",
                source_datasets=[up],
                target_datasets=[down],
            )


def _add_python_data_flow(lg: LineageGraph, repo_path: Path) -> None:
    """Add lineage from Python pandas/PySpark/SQLAlchemy read/write."""
    for path in repo_path.rglob("*.py"):
        if any(d in path.parts for d in _EXCLUDE_DIRS):
            continue
        try:
            source = path.read_bytes()
        except OSError:
            continue
        flows = extract_python_data_flow(path, source)
        if not flows:
            continue
        rel = str(path.relative_to(repo_path)).replace("\\", "/")
        reads = [f["dataset"] for f in flows if f["type"] == "read" and f["dataset"] != "dynamic"]
        writes = [f["dataset"] for f in flows if f["type"] == "write" and f["dataset"] != "dynamic"]
        if not reads and not writes:
            continue  # Skip if all refs are dynamic
        transform_id = f"python:{rel}:data_flow"
        lg.add_transformation(
            transform_id,
            rel,
            "python",
            source_datasets=reads,
            target_datasets=writes,
        )


def survey(
    repo_path: str | Path,
    *,
    output_dir: str | Path = ".cartography",
    include_sql: bool = True,
    include_dbt: bool = True,
    include_airflow: bool = True,
    include_python_flow: bool = True,
) -> LineageGraph:
    """
    Run the Hydrologist: build data lineage graph from SQL, dbt, Airflow, Python.

    Returns the LineageGraph and writes to output_dir/lineage_graph.json.
    """
    repo_path = Path(repo_path).resolve()
    output_dir = Path(output_dir)

    lg = LineageGraph()

    print(f"[Hydrologist] Building lineage graph for {repo_path} ...", flush=True)
    if include_sql:
        print("[Hydrologist] Extracting SQL lineage ...", flush=True)
        _add_sql_lineage(lg, repo_path)
    if include_dbt:
        print("[Hydrologist] Extracting dbt lineage ...", flush=True)
        _add_dbt_lineage(lg, repo_path)
        _add_dbt_ref_lineage(lg, repo_path)
    if include_airflow:
        print("[Hydrologist] Extracting Airflow lineage ...", flush=True)
        _add_airflow_lineage(lg, repo_path)
    if include_python_flow:
        print("[Hydrologist] Extracting Python data flow ...", flush=True)
        _add_python_data_flow(lg, repo_path)

    output_dir.mkdir(parents=True, exist_ok=True)
    lg.to_json(output_dir / "lineage_graph.json")

    G = lg.graph
    print(
        f"[Hydrologist] Done: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges -> "
        f"{output_dir / 'lineage_graph.json'}",
        flush=True,
    )
    return lg
