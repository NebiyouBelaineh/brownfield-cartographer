"""Parse Airflow DAG definitions and dbt schema/project YAML for pipeline topology."""

import re
from pathlib import Path
from typing import Any

import yaml
from tree_sitter import Node

from src.analyzers.tree_sitter_analyzer import LanguageRouter

# Re-export for convenience
_LANG_ROUTER = LanguageRouter()


def _get_text(node: Node, source: bytes) -> str:
    return source[node.start_byte : node.end_byte].decode("utf-8", errors="replace")


def _find_child(node: Node, *types: str) -> Node | None:
    for c in node.children:
        if c.type in types:
            return c
    return None


def _walk(node: Node, source: bytes, visitor: callable) -> None:
    visitor(node, source)
    for c in node.children:
        _walk(c, source, visitor)


# ---------------------------------------------------------------------------
# dbt schema.yml / models YAML
# ---------------------------------------------------------------------------


def parse_dbt_schema_yml(content: str | bytes) -> dict[str, Any]:
    """
    Parse dbt schema.yml or models YAML. Extracts models, sources, and ref relationships.

    Returns dict with:
        - models: list of {name, columns, refs}
        - sources: list of {name, database, schema, tables}
        - errors: list of parse errors
    """
    result: dict[str, Any] = {"models": [], "sources": [], "errors": []}
    if isinstance(content, bytes):
        content = content.decode("utf-8", errors="replace")

    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as e:
        result["errors"].append(str(e))
        return result

    if not isinstance(data, dict):
        return result

    # Models
    if "models" in data:
        models_data = data["models"]
        if isinstance(models_data, list):
            for m in models_data:
                if isinstance(m, dict) and "name" in m:
                    refs = _extract_refs_from_model(m)
                    result["models"].append({
                        "name": m["name"],
                        "columns": m.get("columns", []),
                        "refs": refs,
                    })
        elif isinstance(models_data, dict):
            # Nested: models: project: model_name: {...}
            for project_models in models_data.values():
                if isinstance(project_models, dict):
                    for model_name, model_config in project_models.items():
                        if isinstance(model_config, dict) and "name" not in model_config:
                            refs = _extract_refs_from_model(model_config)
                            result["models"].append({
                                "name": model_name,
                                "columns": model_config.get("columns", []),
                                "refs": refs,
                            })

    # Sources
    if "sources" in data:
        sources_data = data["sources"]
        if isinstance(sources_data, list):
            for s in sources_data:
                if isinstance(s, dict) and "name" in s:
                    tables = []
                    for t in s.get("tables", []):
                        if isinstance(t, dict) and "name" in t:
                            tables.append(t["name"])
                    result["sources"].append({
                        "name": s["name"],
                        "database": s.get("database"),
                        "schema": s.get("schema", s["name"]),
                        "tables": tables,
                    })

    return result


def _extract_refs_from_model(model: dict) -> list[str]:
    """Extract ref('x') references from relationship tests in columns."""
    refs: list[str] = []
    for col in model.get("columns", []):
        if not isinstance(col, dict):
            continue
        for test in col.get("tests", []):
            if isinstance(test, dict) and "relationships" in test:
                rel = test["relationships"]
                if isinstance(rel, dict) and "to" in rel:
                    to = rel["to"]
                    if isinstance(to, str) and "ref(" in to:
                        # ref('customers') or ref("customers")
                        match = re.search(r"ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)", to)
                        if match:
                            refs.append(match.group(1))
    return refs


def parse_dbt_project_yml(content: str | bytes) -> dict[str, Any]:
    """
    Parse dbt_project.yml. Extracts project name, model paths, and model config.

    Returns dict with: name, model-paths, models (nested config), errors.
    """
    result: dict[str, Any] = {"name": None, "model_paths": [], "models": {}, "errors": []}
    if isinstance(content, bytes):
        content = content.decode("utf-8", errors="replace")

    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as e:
        result["errors"].append(str(e))
        return result

    if not isinstance(data, dict):
        return result

    result["name"] = data.get("name")
    result["model_paths"] = data.get("model-paths", ["models"])
    result["models"] = data.get("models", {})

    return result


# ---------------------------------------------------------------------------
# Airflow DAG Python
# ---------------------------------------------------------------------------


def parse_airflow_dag_python(
    path: str | Path,
    source: str | bytes,
    router: LanguageRouter | None = None,
) -> dict[str, Any]:
    """
    Parse an Airflow DAG Python file. Extracts dag_id, task IDs, and dependencies.

    Returns dict with:
        - dag_id: str or None
        - tasks: list of task identifiers (variable names)
        - dependencies: list of (upstream, downstream) pairs
        - source_file: path
        - errors: list
    """
    path = Path(path)
    router = router or _LANG_ROUTER
    if isinstance(source, bytes):
        source_bytes = source
        source_str = source.decode("utf-8", errors="replace")
    else:
        source_str = source
        source_bytes = source.encode("utf-8")

    result: dict[str, Any] = {
        "dag_id": None,
        "tasks": [],
        "dependencies": [],
        "source_file": str(path),
        "errors": [],
    }

    if not router.supports(path) or Path(path).suffix.lower() != ".py":
        return result

    tree = router.parse(path, source_bytes)
    if tree is None:
        return result

    task_names: set[str] = set()
    dag_ids: list[str] = []

    def visit(node: Node, src: bytes) -> None:
        nonlocal dag_ids, task_names

        # DAG(..., dag_id="x") or DAG(dag_id='x')
        if node.type == "call":
            children = node.children
            first_id = next((c for c in children if c.type == "identifier"), None)
            is_dag = first_id and _get_text(first_id, src) == "DAG"
            for c in children:
                if c.type == "argument_list":
                    for k in c.children:
                        if k.type == "keyword_argument":
                            arg_name = _find_child(k, "identifier")
                            if arg_name and _get_text(arg_name, src) == "dag_id" and is_dag:
                                val = _find_child(k, "string")
                                if val:
                                    dag_ids.append(_get_text(val, src).strip("'\""))
                                break
                            if arg_name and _get_text(arg_name, src) == "task_id":
                                val = _find_child(k, "string")
                                if val:
                                    task_names.add(_get_text(val, src).strip("'\""))
            return

        # Binary op >> : left >> right (can be nested: t1 >> t2 >> t3)
        if node.type == "binary_operator":
            children = node.children
            if len(children) >= 3:
                op_idx = 1
                while op_idx < len(children) and children[op_idx].type not in ("operator", ">>"):
                    op_idx += 1
                if op_idx < len(children) and ">>" in _get_text(children[op_idx], src):
                    left = children[0]
                    right = children[op_idx + 1] if op_idx + 1 < len(children) else None
                    if right is not None:
                        up = _last_task_in_expr(left, src)
                        down = _first_task_in_expr(right, src)
                        if up and down:
                            result["dependencies"].append((up, down))

    def _task_name(n: Node, src: bytes) -> str | None:
        if n.type == "identifier":
            return _get_text(n, src)
        if n.type == "attribute":
            return _task_name(n.children[0], src)
        if n.type == "parenthesized_expression" and len(n.children) > 1:
            return _task_name(n.children[1], src)
        return None

    def _last_task_in_expr(n: Node, src: bytes) -> str | None:
        """Rightmost task in t1 >> t2 >> t3 (returns t3 for the whole expr, t2 for t1>>t2)."""
        if n.type == "binary_operator" and len(n.children) >= 3:
            op_idx = 1
            while op_idx < len(n.children) and ">>" not in _get_text(n.children[op_idx], src):
                op_idx += 1
            if op_idx + 1 < len(n.children):
                return _last_task_in_expr(n.children[op_idx + 1], src)
            return _last_task_in_expr(n.children[0], src)
        return _task_name(n, src)

    def _first_task_in_expr(n: Node, src: bytes) -> str | None:
        """Leftmost task in t1 >> t2 >> t3 (returns t1)."""
        if n.type == "binary_operator" and len(n.children) >= 1:
            return _first_task_in_expr(n.children[0], src)
        return _task_name(n, src)

    _walk(tree.root_node, source_bytes, visit)

    result["dag_id"] = dag_ids[0] if dag_ids else None
    # Collect task names from dependencies
    for u, d in result["dependencies"]:
        task_names.add(u)
        task_names.add(d)
    result["tasks"] = list(task_names)

    return result


def analyze_dbt_directory(directory: str | Path) -> dict[str, Any]:
    """
    Scan a dbt project directory for schema/project YAML and extract pipeline topology.

    Returns merged result from all schema yml files + dbt_project.yml.
    """
    directory = Path(directory)
    if not directory.is_dir():
        return {"models": [], "sources": [], "project": {}, "errors": []}

    models: list[dict] = []
    sources: list[dict] = []
    project: dict = {}
    errors: list[str] = []

    # dbt_project.yml
    project_path = directory / "dbt_project.yml"
    if project_path.exists():
        try:
            p = parse_dbt_project_yml(project_path.read_bytes())
            project = {k: v for k, v in p.items() if k != "errors"}
            errors.extend(p.get("errors", []))
        except Exception as e:
            errors.append(str(e))

    # Schema yml files (models/, etc.)
    for path in directory.rglob("*.yml"):
        if path.name.startswith("."):
            continue
        try:
            content = path.read_bytes()
            r = parse_dbt_schema_yml(content)
            models.extend(r.get("models", []))
            sources.extend(r.get("sources", []))
            errors.extend(r.get("errors", []))
        except Exception:
            pass

    for path in directory.rglob("*.yaml"):
        if path.name.startswith("."):
            continue
        try:
            content = path.read_bytes()
            r = parse_dbt_schema_yml(content)
            models.extend(r.get("models", []))
            sources.extend(r.get("sources", []))
            errors.extend(r.get("errors", []))
        except Exception:
            pass

    return {
        "models": models,
        "sources": sources,
        "project": project,
        "errors": errors,
    }


def analyze_airflow_dag_file(path: str | Path, source: str | bytes) -> dict[str, Any]:
    """Convenience: parse a single Airflow DAG Python file."""
    return parse_airflow_dag_python(path, source)
