"""Python data flow analyzer: pandas, PySpark, SQLAlchemy read/write patterns."""

import logging
from pathlib import Path
from typing import Any

from tree_sitter import Node, Tree

from src.analyzers.tree_sitter_analyzer import LanguageRouter

logger = logging.getLogger(__name__)

# (method_name, direction) -> "read" | "write"
_PANDAS_READ = {"read_csv", "read_parquet", "read_sql", "read_excel", "read_json", "read_hdf"}
_PANDAS_WRITE = {"to_csv", "to_parquet", "to_sql", "to_excel", "to_json", "to_hdf"}
_PYSPARK_READ = {"csv", "parquet", "table", "jdbc", "json", "orc"}
_PYSPARK_WRITE = {"save", "saveAsTable", "insertInto"}

# SQLAlchemy patterns
_SQLALCHEMY_CONNECTION = {"create_engine", "create_async_engine"}
_SQLALCHEMY_POSITIONAL_READ = {"read_sql_table", "read_sql_query"}


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


def _make_entry(
    type_: str,
    dataset: str,
    method: str,
    line: int,
    path: Path,
) -> dict[str, Any]:
    """Build a data-flow record, adding a dynamic_ref entry when dataset is unresolved."""
    entry: dict[str, Any] = {"type": type_, "dataset": dataset, "method": method, "line": line}
    if dataset == "dynamic":
        entry["dynamic_ref"] = {"file": str(path), "line": line, "call": method}
        logger.warning(
            "[python_data_flow] Unresolved (dynamic) reference at %s:%d in call '%s'",
            path, line, method,
        )
    return entry


def extract_python_data_flow(path: str | Path, source: bytes) -> list[dict[str, Any]]:
    """
    Extract data read/write patterns from Python source.
    Returns list of {type: 'read'|'write', dataset: str, method: str, line: int}.
    dataset is "dynamic" if path cannot be statically determined;
    those entries also carry a "dynamic_ref" key with file/line/call details.
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
            results.append(_make_entry("read", dataset, name, line, path))
        elif name in _PANDAS_WRITE:
            dataset = _extract_string_arg(call, src) or "dynamic"
            results.append(_make_entry("write", dataset, name, line, path))
        elif name in _PYSPARK_READ:
            dataset = _extract_string_arg(call, src) or "dynamic"
            results.append(_make_entry("read", dataset, f"spark.read.{name}", line, path))
        elif name in _PYSPARK_WRITE:
            # write path is often set via .option() chains — always dynamic
            results.append(_make_entry("write", "dynamic", name, line, path))
        elif name in _SQLALCHEMY_CONNECTION:
            # create_engine("dialect://...") — first arg is the connection URL
            dataset = _extract_string_arg(call, src) or "dynamic"
            results.append(_make_entry("connection", dataset, name, line, path))
        elif name in _SQLALCHEMY_POSITIONAL_READ:
            # pd.read_sql_table("table", con) / pd.read_sql_query("SELECT...", con)
            dataset = _extract_string_arg(call, src) or "dynamic"
            results.append(_make_entry("read", dataset, name, line, path))
        elif name == "execute":
            # SQLAlchemy session/engine.execute — SQL body is dynamic
            results.append(_make_entry("read", "dynamic", "execute", line, path))

    _walk_calls(tree.root_node, source, visit)
    return results
