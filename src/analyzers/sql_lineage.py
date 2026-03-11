"""SQL lineage extraction using sqlglot. Parses .sql and dbt model files for table dependencies."""

from pathlib import Path
from typing import Any

import sqlglot
from sqlglot import exp

from src.analyzers.tree_sitter_analyzer import ts_fallback_extract_sql_tables

# Dialects supported for parsing
SUPPORTED_DIALECTS = (
    "postgres",
    "bigquery",
    "snowflake",
    "duckdb",
    "spark",
    "mysql",
    "tsql",  # SQL Server / T-SQL
)


def _get_table_name(table: exp.Table) -> str:
    """Get fully qualified table name (schema.table or just table)."""
    parts = []
    if table.catalog:
        parts.append(table.catalog)
    if table.db:
        parts.append(table.db)
    if table.this:
        name = table.this.name if hasattr(table.this, "name") else str(table.this)
        parts.append(name)
    return ".".join(parts) if parts else ""


def _tables_in_expression(node: exp.Expression) -> set[str]:
    """Extract all table references from an expression (SELECT, CTE, etc.)."""
    tables = set()
    for t in node.find_all(exp.Table):
        name = _get_table_name(t)
        if name:
            tables.add(name)
    return tables


def _cte_dependencies(select: exp.Select) -> dict[str, set[str]]:
    """
    Extract CTE name -> source tables for each CTE in the WITH clause.
    Returns dict mapping cte_alias to set of source table names.
    """
    deps: dict[str, set[str]] = {}
    with_clause = select.args.get("with_")
    if not with_clause:
        return deps

    for cte in with_clause.expressions:
        if not isinstance(cte, exp.CTE):
            continue
        alias = cte.alias
        if alias is None:
            continue
        # Alias can be str or TableAlias
        if isinstance(alias, str):
            alias_name = alias
        elif hasattr(alias, "this") and hasattr(alias.this, "name"):
            alias_name = alias.this.name
        else:
            alias_name = str(alias)
        # Sources: tables in the CTE's SELECT (exclude other CTEs defined in same WITH)
        cte_tables = _tables_in_expression(cte.this)
        other_cte_names = set()
        for e in with_clause.expressions:
            if e == cte:
                continue
            a = e.alias
            if isinstance(a, str):
                other_cte_names.add(a)
            elif a and hasattr(a, "this") and hasattr(a.this, "name"):
                other_cte_names.add(a.this.name)
        sources = cte_tables - other_cte_names
        deps[alias_name] = sources
    return deps


def _main_query_sources(select: exp.Select) -> set[str]:
    """Extract source tables from the main SELECT (after WITH)."""
    # The main from_ and joins
    return _tables_in_expression(select)


def extract_table_dependencies(
    sql: str,
    dialect: str = "postgres",
) -> dict[str, Any]:
    """
    Extract table dependencies from a SQL string.

    Returns dict with:
        - sources: set of source table names (from FROM, JOIN, CTE bodies)
        - targets: set of output names (CTEs that are only consumed within query, or inferred)
        - cte_map: dict of CTE alias -> source tables
        - errors: list of parse errors
    """
    result: dict[str, Any] = {
        "sources": set(),
        "targets": set(),
        "cte_map": {},
        "errors": [],
    }

    try:
        parsed = sqlglot.parse_one(sql, dialect=dialect)
    except Exception as e:
        result["errors"].append(str(e))
        # Fallback: use tree-sitter to extract table names from malformed SQL
        raw = sql.encode() if isinstance(sql, str) else sql
        fallback_tables = ts_fallback_extract_sql_tables(raw)
        result["sources"] = fallback_tables
        result["_fallback"] = True
        return result

    if not isinstance(parsed, exp.Select):
        result["errors"].append("Expected SELECT statement")
        return result

    # CTE dependencies
    cte_deps = _cte_dependencies(parsed)
    result["cte_map"] = {k: list(v) for k, v in cte_deps.items()}

    # All sources: from main query + from each CTE
    all_sources: set[str] = set()
    main_sources = _main_query_sources(parsed)
    all_sources.update(main_sources)
    for sources in cte_deps.values():
        all_sources.update(sources)

    # Resolve: CTEs defined in this query are intermediate, not external sources
    cte_names = set(cte_deps.keys())
    external_sources = all_sources - cte_names
    result["sources"] = external_sources

    # Targets: not derivable from SQL alone; caller provides via output_name (e.g. dbt model name)
    result["targets"] = set()

    return result


def analyze_sql_file(
    path: str | Path,
    source: str | bytes,
    output_name: str | None = None,
    dialect: str = "postgres",
) -> dict[str, Any]:
    """
    Analyze a SQL file and extract lineage.

    Args:
        path: File path (used for output_name inference if not provided)
        source: SQL content (str or bytes)
        output_name: Override for output table name (e.g. dbt model name from filename)
        dialect: SQL dialect for parsing

    Returns:
        Dict with sources, targets, transform_id, source_file, line_range, errors.
        Suitable for feeding into LineageGraph.add_transformation().
    """
    path = Path(path)
    if isinstance(source, bytes):
        source = source.decode("utf-8", errors="replace")

    total_lines = len(source.splitlines()) or 1
    line_range = (1, total_lines)

    deps = extract_table_dependencies(source, dialect=dialect)

    # Infer output name: from filename stem for dbt models (e.g. orders.sql -> orders)
    if output_name is None:
        output_name = path.stem

    transform_id = f"{path}:{output_name}"

    return {
        "source_file": str(path),
        "transform_id": transform_id,
        "output_name": output_name,
        "sources": list(deps["sources"]),
        "targets": [output_name],  # This SQL file produces output_name (e.g. dbt model)
        "cte_map": deps.get("cte_map", {}),
        "line_range": line_range,
        "errors": deps.get("errors", []),
        "transformation_type": "sql",
    }


def parse_sql_file(
    path: str | Path,
    source: str | bytes,
    dialect: str = "postgres",
) -> exp.Expression | None:
    """
    Parse a SQL file. Returns the parsed expression or None on failure.
    """
    if isinstance(source, bytes):
        source = source.decode("utf-8", errors="replace")
    try:
        return sqlglot.parse_one(source, dialect=dialect)
    except Exception:
        return None


def analyze_sql_directory(
    directory: str | Path,
    dialect: str = "postgres",
) -> list[dict[str, Any]]:
    """
    Scan a directory for .sql files and extract lineage from each.
    Recursively descends into subdirectories (e.g. dbt models/).
    Returns list of analyze_sql_file results.
    """
    directory = Path(directory)
    if not directory.is_dir():
        return []

    results: list[dict[str, Any]] = []
    for path in directory.rglob("*.sql"):
        if path.is_file():
            try:
                source = path.read_bytes()
                if not source.strip():
                    continue
                r = analyze_sql_file(path, source, dialect=dialect)
                results.append(r)
            except Exception:
                pass  # Skip unreadable files
    return results
