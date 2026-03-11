"""Tests for src/analyzers/sql_lineage.py."""

import tempfile
from pathlib import Path

import pytest

from src.analyzers.sql_lineage import (
    analyze_sql_directory,
    analyze_sql_file,
    extract_table_dependencies,
    parse_sql_file,
)


# ---------------------------------------------------------------------------
# extract_table_dependencies
# ---------------------------------------------------------------------------


class TestExtractTableDependencies:
    def test_simple_select(self):
        sql = "SELECT * FROM orders"
        result = extract_table_dependencies(sql)
        assert "orders" in result["sources"]
        assert result["errors"] == []

    def test_select_with_join(self):
        sql = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id"
        result = extract_table_dependencies(sql)
        assert "orders" in result["sources"]
        assert "customers" in result["sources"]

    def test_cte_sources(self):
        sql = """
        WITH ranked AS (
            SELECT * FROM raw_orders
        )
        SELECT * FROM ranked
        """
        result = extract_table_dependencies(sql)
        # raw_orders is an external source; ranked is internal CTE
        assert "raw_orders" in result["sources"]
        assert "ranked" not in result["sources"]

    def test_cte_map_populated(self):
        sql = """
        WITH cte AS (SELECT id FROM base_table)
        SELECT * FROM cte
        """
        result = extract_table_dependencies(sql)
        assert "cte" in result["cte_map"]
        assert "base_table" in result["cte_map"]["cte"]

    def test_multiple_ctes(self):
        sql = """
        WITH a AS (SELECT * FROM src1),
             b AS (SELECT * FROM src2)
        SELECT * FROM a JOIN b ON a.id = b.id
        """
        result = extract_table_dependencies(sql)
        assert "src1" in result["sources"]
        assert "src2" in result["sources"]
        assert "a" not in result["sources"]
        assert "b" not in result["sources"]

    def test_targets_empty_by_default(self):
        sql = "SELECT * FROM foo"
        result = extract_table_dependencies(sql)
        assert result["targets"] == set()

    def test_non_select_returns_error(self):
        sql = "INSERT INTO foo VALUES (1, 2)"
        result = extract_table_dependencies(sql)
        assert len(result["errors"]) > 0

    def test_invalid_sql_returns_error(self):
        sql = "NOT VALID SQL AT ALL $$$$"
        result = extract_table_dependencies(sql)
        assert len(result["errors"]) > 0

    def test_schema_qualified_table(self):
        sql = "SELECT * FROM public.orders"
        result = extract_table_dependencies(sql)
        assert any("orders" in s for s in result["sources"])

    def test_bigquery_dialect(self):
        sql = "SELECT * FROM `project.dataset.table`"
        # bigquery dialect - may parse differently but should not crash
        result = extract_table_dependencies(sql, dialect="bigquery")
        assert isinstance(result["sources"], set)
        assert isinstance(result["errors"], list)

    def test_empty_sql_returns_error(self):
        result = extract_table_dependencies("")
        assert len(result["errors"]) > 0


# ---------------------------------------------------------------------------
# analyze_sql_file
# ---------------------------------------------------------------------------


class TestAnalyzeSqlFile:
    def test_basic_file_analysis(self):
        sql = "SELECT * FROM raw_orders"
        result = analyze_sql_file("models/orders.sql", sql)
        assert result["transformation_type"] == "sql"
        assert result["source_file"] == "models/orders.sql"
        assert "raw_orders" in result["sources"]

    def test_output_name_inferred_from_stem(self):
        sql = "SELECT * FROM raw_orders"
        result = analyze_sql_file("models/clean_orders.sql", sql)
        assert result["output_name"] == "clean_orders"
        assert "clean_orders" in result["targets"]

    def test_output_name_override(self):
        sql = "SELECT * FROM raw_orders"
        result = analyze_sql_file("models/orders.sql", sql, output_name="my_model")
        assert result["output_name"] == "my_model"
        assert "my_model" in result["targets"]

    def test_transform_id_includes_path_and_name(self):
        sql = "SELECT * FROM raw_orders"
        result = analyze_sql_file("models/orders.sql", sql)
        assert "models/orders.sql" in result["transform_id"]
        assert "orders" in result["transform_id"]

    def test_bytes_source(self):
        sql = b"SELECT * FROM orders"
        result = analyze_sql_file("q.sql", sql)
        assert "orders" in result["sources"]

    def test_invalid_sql_produces_errors(self):
        result = analyze_sql_file("bad.sql", "NOT VALID SQL $$$$")
        assert len(result["errors"]) > 0

    def test_cte_not_in_sources(self):
        sql = """
        WITH cte AS (SELECT * FROM raw)
        SELECT * FROM cte
        """
        result = analyze_sql_file("model.sql", sql)
        assert "raw" in result["sources"]
        assert "cte" not in result["sources"]


# ---------------------------------------------------------------------------
# parse_sql_file
# ---------------------------------------------------------------------------


class TestParseSqlFile:
    def test_returns_expression_for_valid_sql(self):
        expr = parse_sql_file("x.sql", "SELECT 1")
        assert expr is not None

    def test_returns_none_for_invalid_sql(self):
        expr = parse_sql_file("x.sql", "$$$ NOT SQL $$$")
        assert expr is None

    def test_bytes_input(self):
        expr = parse_sql_file("x.sql", b"SELECT 1 FROM foo")
        assert expr is not None


# ---------------------------------------------------------------------------
# analyze_sql_directory
# ---------------------------------------------------------------------------


class TestAnalyzeSqlDirectory:
    def test_nonexistent_directory(self):
        result = analyze_sql_directory("/does/not/exist")
        assert result == []

    def test_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = analyze_sql_directory(tmpdir)
            assert result == []

    def test_scans_sql_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "orders.sql").write_text("SELECT * FROM raw_orders")
            (Path(tmpdir) / "customers.sql").write_text("SELECT * FROM raw_customers")
            results = analyze_sql_directory(tmpdir)
            assert len(results) == 2
            output_names = {r["output_name"] for r in results}
            assert "orders" in output_names
            assert "customers" in output_names

    def test_skips_empty_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "empty.sql").write_text("   ")
            results = analyze_sql_directory(tmpdir)
            assert results == []

    def test_recursive_scan(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            subdir = Path(tmpdir) / "models" / "staging"
            subdir.mkdir(parents=True)
            (subdir / "stg_orders.sql").write_text("SELECT * FROM raw.orders")
            results = analyze_sql_directory(tmpdir)
            assert len(results) == 1
            assert results[0]["output_name"] == "stg_orders"
