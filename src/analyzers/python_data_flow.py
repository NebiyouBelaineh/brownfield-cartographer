"""Python data flow analyzer: pandas, PySpark, SQLAlchemy read/write patterns."""

from pathlib import Path
from typing import Any

from tree_sitter import Node, Tree

from src.analyzers.tree_sitter_analyzer import LanguageRouter

# (method_name, direction) -> "read" | "write"
_PANDAS_READ = {"read_csv", "read_parquet", "read_sql", "read_excel", "read_json", "read_hdf"}
_PANDAS_WRITE = {"to_csv", "to_parquet", "to_sql", "to_excel", "to_json", "to_hdf"}
_PYSPARK_READ = {"csv", "parquet", "table", "jdbc", "json", "orc"}
_PYSPARK_WRITE = {"save", "saveAsTable", "insertInto"}


def _get_text(node: Node, source: bytes) -> str:
    return source[node.start_byte : node.end_byte].decode("utf-8", errors="replace")


def _find_child(node: Node, *types: str) -> Node | None:
    for c in node.children:
        if c.type in types:
            return c
    return None


def _extract_string_arg(call_node: Node, source: bytes) -> str | None:
    """Extract first string argument from a call. Returns None for dynamic refs."""
    for c in call_node.children:
        if c.type == "argument_list":
            for arg in c.children:
                if arg.type == "string":
                    s = _get_text(arg, source).strip("'\"")
                    return s if s else None
                if arg.type == "keyword_argument":
                    continue
                if arg.type in ("identifier", "attribute", "call"):
                    return None  # Dynamic
            break
    return None


def _get_call_name(node: Node, source: bytes) -> str | None:
    """Get the name of the function being called (e.g. read_csv, to_csv)."""
    if node.type != "call":
        return None
    for c in node.children:
        if c.type == "attribute":
            return _get_text(c, source).split(".")[-1]
        if c.type == "identifier":
            return _get_text(c, source)
    return None


def _walk_calls(node: Node, source: bytes, visitor: Any) -> None:
    if node.type == "call":
        visitor(node, source)
    for c in node.children:
        _walk_calls(c, source, visitor)


def extract_python_data_flow(path: str | Path, source: bytes) -> list[dict[str, Any]]:
    """
    Extract data read/write patterns from Python source.
    Returns list of {type: 'read'|'write', dataset: str, method: str, line: int}.
    dataset is "dynamic" if path cannot be statically determined.
    """
    path = Path(path)
    if path.suffix.lower() != ".py":
        return []

    router = LanguageRouter()
    tree = router.parse(path, source)
    if tree is None:
        return []

    results: list[dict[str, Any]] = []

    def visit(call: Node, src: bytes) -> None:
        name = _get_call_name(call, src)
        if not name:
            return
        line = call.start_point[0] + 1 if call.start_point else 0

        if name in _PANDAS_READ:
            dataset = _extract_string_arg(call, src) or "dynamic"
            results.append({"type": "read", "dataset": dataset, "method": name, "line": line})
        elif name in _PANDAS_WRITE:
            dataset = _extract_string_arg(call, src) or "dynamic"
            results.append({"type": "write", "dataset": dataset, "method": name, "line": line})
        elif name in _PYSPARK_READ:
            # spark.read.csv("path") - the call is on .csv(...)
            dataset = _extract_string_arg(call, src) or "dynamic"
            results.append({"type": "read", "dataset": dataset, "method": f"spark.read.{name}", "line": line})
        elif name in _PYSPARK_WRITE:
            dataset = "dynamic"  # write path often in option
            results.append({"type": "write", "dataset": dataset, "method": name, "line": line})
        elif name == "execute":
            # SQLAlchemy: execute(text("SELECT...")) - table names in SQL, hard to extract
            results.append({"type": "read", "dataset": "dynamic", "method": "execute", "line": line})

    _walk_calls(tree.root_node, source, visit)
    return results
