"""Orchestrator: wires all four agents in sequence, serializes to .cartography/.

Pipeline: Surveyor → Hydrologist → Semanticist → Archivist

Also supports:
  - Incremental update mode: re-analyze only files changed since last run.
  - Optional LLM stages (--llm flag in CLI): skips Semanticist/Archivist LLM calls
    if no LLM config is available or the user opts out.
"""

from __future__ import annotations

import json
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from src.agents import (
    TraceLogger,
    archivist_archive,
    hydrologist_survey,
    semanticist_analyse,
    surveyor_survey,
)


_GITHUB_URL_PATTERN = re.compile(
    r"^https?://(?:www\.)?github\.com/([^/]+)/([^/]+?)(?:\.git)?/?$",
    re.IGNORECASE,
)

_LAST_RUN_FILE = "last_run.json"


# ---------------------------------------------------------------------------
# Repo resolution
# ---------------------------------------------------------------------------

def _resolve_repo_path(repo_path: str | Path, clone_dir: Path | None = None) -> Path:
    """Resolve repo path. If GitHub URL, clone and return clone path."""
    repo_path = str(repo_path).strip()
    if repo_path.startswith(("http://", "https://")):
        match = _GITHUB_URL_PATTERN.match(repo_path)
        if match:
            owner, repo = match.group(1), match.group(2)
            if repo.endswith(".git"):
                repo = repo[:-4]
            target = clone_dir or Path(tempfile.mkdtemp(prefix="cartographer-"))
            target = target / f"{owner}-{repo}"
            if target.exists():
                return target.resolve()
            try:
                subprocess.run(
                    ["git", "clone", "--depth", "1", repo_path, str(target)],
                    capture_output=True,
                    check=True,
                    timeout=300,
                )
                return target.resolve()
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError) as e:
                raise ValueError(f"Failed to clone {repo_path}: {e}") from e
        raise ValueError(f"Unsupported GitHub URL format: {repo_path}")
    return Path(repo_path).resolve()


# ---------------------------------------------------------------------------
# Incremental update helpers
# ---------------------------------------------------------------------------

def _current_commit(repo_path: Path) -> str | None:
    """Return the current HEAD commit hash, or None if not a git repo."""
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_path), "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        pass
    return None


def _changed_files_since(repo_path: Path, since_commit: str) -> list[str]:
    """Return list of files changed between since_commit and HEAD."""
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_path), "diff", "--name-only", since_commit, "HEAD"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            return [f.strip() for f in result.stdout.splitlines() if f.strip()]
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        pass
    return []


def _load_last_run(output_dir: Path) -> dict[str, Any]:
    last_run_path = output_dir / _LAST_RUN_FILE
    if last_run_path.exists():
        try:
            with last_run_path.open() as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_last_run(output_dir: Path, commit: str | None, metadata: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    last_run_path = output_dir / _LAST_RUN_FILE
    data = {"commit": commit, **metadata}
    with last_run_path.open("w") as f:
        json.dump(data, f, indent=2)


# ---------------------------------------------------------------------------
# Public helpers (used by tests and CLI)
# ---------------------------------------------------------------------------

def _repo_slug(resolved: Path) -> str:
    """Derive a filesystem-safe subdirectory name from the resolved repo path.

    Examples:
        /home/user/jaffle_shop        → "jaffle_shop"
        /tmp/cartographer-xyz/dbt-labs-jaffle_shop → "dbt-labs-jaffle_shop"
    """
    import re
    name = resolved.name
    # Replace any non-alphanumeric chars (except hyphens/underscores/dots) with hyphens
    slug = re.sub(r"[^A-Za-z0-9._-]", "-", name).strip("-")
    return slug or "repo"


def repo_output_dir(base_output_dir: str | Path, repo_path: str | Path) -> Path:
    """Return the per-repo subdirectory inside base_output_dir.

    E.g. base=".cartography", repo="/home/user/jaffle_shop" → ".cartography/jaffle_shop"
    """
    resolved = Path(repo_path).resolve()
    return Path(base_output_dir) / _repo_slug(resolved)


def get_changed_files(repo_path: str | Path, output_dir: str | Path = ".cartography") -> list[str]:
    """Return files changed since the last cartographer run (for incremental mode).

    output_dir should be the per-repo directory (i.e. already includes the slug).
    Returns an empty list if no previous run exists or the repo has no git history.
    """
    resolved = Path(repo_path).resolve()
    out_dir = Path(output_dir)
    last_run = _load_last_run(out_dir)
    last_commit = last_run.get("commit")
    current = _current_commit(resolved)
    if last_commit and current and last_commit != current:
        return _changed_files_since(resolved, last_commit)
    return []


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run(
    repo_path: str | Path,
    *,
    output_dir: str | Path = ".cartography",
    days: int = 30,
    clone_dir: Path | None = None,
    run_llm: bool = False,
    config_path: str | Path | None = None,
    incremental: bool = False,
    include_sql: bool = True,
    include_dbt: bool = True,
    include_airflow: bool = True,
    include_python_flow: bool = True,
) -> dict[str, Any]:
    """Run the full analysis pipeline: Surveyor → Hydrologist → [Semanticist → Archivist].

    Args:
        repo_path: Local path or GitHub URL.
        output_dir: Where to write .cartography/ artifacts.
        days: Days for git velocity analysis.
        clone_dir: Clone directory for GitHub URLs.
        run_llm: If True, run Semanticist + Archivist LLM stages.
        config_path: Path to cartographer.toml for LLM config.
        incremental: If True, report files changed since last run.
        include_sql / include_dbt / include_airflow / include_python_flow:
            Toggle individual Hydrologist analyzers.

    Returns:
        Dict with module_graph, lineage_graph, semantic_results, archivist_paths,
        repo_path, output_dir, changed_files (incremental mode), commit.
    """
    from datetime import datetime, timezone

    resolved = _resolve_repo_path(repo_path, clone_dir)
    if not resolved.exists() or not resolved.is_dir():
        raise ValueError(f"Repository path does not exist or is not a directory: {resolved}")

    # Namespace output under a per-repo subdirectory so multiple repos don't
    # overwrite each other.  E.g. ".cartography/jaffle_shop/"
    output_dir = Path(output_dir) / _repo_slug(resolved)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Trace logger (shared across agents)
    trace_logger = TraceLogger(output_dir / "cartography_trace.jsonl")
    trace_logger.log(
        "Orchestrator", "pipeline_start",
        details={
            "repo_path": str(resolved),
            "output_dir": str(output_dir),
            "run_llm": run_llm,
            "incremental": incremental,
        },
    )

    # Incremental mode: detect changed files
    changed_files: list[str] = []
    current_commit = _current_commit(resolved)
    if incremental:
        last_run = _load_last_run(output_dir)
        last_commit = last_run.get("commit")
        if last_commit and current_commit and last_commit != current_commit:
            changed_files = _changed_files_since(resolved, last_commit)
            print(
                f"[Orchestrator] Incremental mode: {len(changed_files)} files changed since {last_commit[:8]}",
                flush=True,
            )
            trace_logger.log(
                "Orchestrator", "incremental_diff",
                details={"since_commit": last_commit, "changed_files": changed_files},
                source="git_analysis",
            )
        else:
            print("[Orchestrator] Incremental mode: no previous run found, running full analysis.", flush=True)

    # --- Stage 1: Surveyor ---
    trace_logger.log("Orchestrator", "stage_start", details={"stage": "Surveyor"})
    module_graph = surveyor_survey(resolved, output_dir=output_dir, days=days)
    trace_logger.log(
        "Surveyor", "survey_complete",
        details={
            "modules": module_graph.graph.number_of_nodes(),
            "edges": module_graph.graph.number_of_edges(),
        },
    )

    # --- Stage 2: Hydrologist ---
    trace_logger.log("Orchestrator", "stage_start", details={"stage": "Hydrologist"})
    lineage_graph = hydrologist_survey(
        resolved,
        output_dir=output_dir,
        include_sql=include_sql,
        include_dbt=include_dbt,
        include_airflow=include_airflow,
        include_python_flow=include_python_flow,
    )
    trace_logger.log(
        "Hydrologist", "survey_complete",
        details={
            "lineage_nodes": lineage_graph.graph.number_of_nodes(),
            "lineage_edges": lineage_graph.graph.number_of_edges(),
        },
    )

    semantic_results: dict[str, Any] = {}
    archivist_paths: dict[str, str] = {}

    # --- Stage 3: Semanticist (LLM) ---
    if run_llm:
        from src.llm_config import load_config
        llm_config = load_config(config_path)

        trace_logger.log("Orchestrator", "stage_start", details={"stage": "Semanticist"})
        semantic_results = semanticist_analyse(
            resolved,
            module_graph,
            lineage_graph,
            output_dir=output_dir,
            config=llm_config,
        )
        trace_logger.log(
            "Semanticist", "analyse_complete",
            details={
                "purpose_statements": len(semantic_results.get("purpose_statements", {})),
                "drift_count": sum(
                    1 for d in semantic_results.get("doc_drift", {}).values() if d.get("has_drift")
                ),
            },
            source="llm_inference",
        )

        # --- Stage 4: Archivist ---
        trace_logger.log("Orchestrator", "stage_start", details={"stage": "Archivist"})
        archivist_paths = archivist_archive(
            module_graph,
            lineage_graph,
            semantic_results=semantic_results,
            output_dir=output_dir,
            config=llm_config,
            trace_logger=trace_logger,
        )
    else:
        # Still run the Archivist to produce CODEBASE.md (LLM overview paragraph skipped)
        trace_logger.log("Orchestrator", "stage_start", details={"stage": "Archivist (no-LLM)"})
        archivist_paths = archivist_archive(
            module_graph,
            lineage_graph,
            semantic_results=None,
            output_dir=output_dir,
            config=None,
            trace_logger=trace_logger,
        )

    # Save last-run metadata for incremental mode
    _save_last_run(
        output_dir,
        current_commit,
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "repo_path": str(resolved),
            "modules": module_graph.graph.number_of_nodes(),
            "lineage_nodes": lineage_graph.graph.number_of_nodes(),
        },
    )

    trace_logger.log(
        "Orchestrator", "pipeline_complete",
        details={"output_dir": str(output_dir), "commit": current_commit},
    )

    return {
        "module_graph": module_graph,
        "lineage_graph": lineage_graph,
        "semantic_results": semantic_results,
        "archivist_paths": archivist_paths,
        "repo_path": str(resolved),
        "output_dir": str(output_dir),
        "changed_files": changed_files,
        "commit": current_commit,
    }
