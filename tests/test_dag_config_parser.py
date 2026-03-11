"""Tests for src/analyzers/dag_config_parser.py."""

import tempfile
from pathlib import Path

import pytest

from src.analyzers.dag_config_parser import (
    analyze_airflow_dag_file,
    analyze_dbt_directory,
    parse_airflow_dag_python,
    parse_dbt_project_yml,
    parse_dbt_schema_yml,
)


# ---------------------------------------------------------------------------
# parse_dbt_schema_yml
# ---------------------------------------------------------------------------


class TestParseDbtSchemaYml:
    def test_basic_model(self):
        content = """
version: 2
models:
  - name: orders
    columns:
      - name: id
"""
        result = parse_dbt_schema_yml(content)
        assert len(result["models"]) == 1
        assert result["models"][0]["name"] == "orders"
        assert result["errors"] == []

    def test_multiple_models(self):
        content = """
version: 2
models:
  - name: orders
  - name: customers
"""
        result = parse_dbt_schema_yml(content)
        names = [m["name"] for m in result["models"]]
        assert "orders" in names
        assert "customers" in names

    def test_sources_parsed(self):
        content = """
version: 2
sources:
  - name: raw
    database: analytics
    schema: raw_data
    tables:
      - name: orders
      - name: customers
"""
        result = parse_dbt_schema_yml(content)
        assert len(result["sources"]) == 1
        src = result["sources"][0]
        assert src["name"] == "raw"
        assert src["database"] == "analytics"
        assert "orders" in src["tables"]
        assert "customers" in src["tables"]

    def test_ref_relationship_extracted(self):
        content = """
version: 2
models:
  - name: orders
    columns:
      - name: customer_id
        tests:
          - relationships:
              to: ref('customers')
              field: id
"""
        result = parse_dbt_schema_yml(content)
        model = result["models"][0]
        assert "customers" in model["refs"]

    def test_bytes_input(self):
        content = b"version: 2\nmodels:\n  - name: test_model\n"
        result = parse_dbt_schema_yml(content)
        assert result["models"][0]["name"] == "test_model"

    def test_empty_content(self):
        result = parse_dbt_schema_yml("")
        assert result["models"] == []
        assert result["sources"] == []

    def test_invalid_yaml_error(self):
        result = parse_dbt_schema_yml("{{ invalid: yaml: }: }")
        assert len(result["errors"]) > 0

    def test_no_models_key(self):
        content = "version: 2\nname: my_project\n"
        result = parse_dbt_schema_yml(content)
        assert result["models"] == []

    def test_columns_list_preserved(self):
        content = """
version: 2
models:
  - name: orders
    columns:
      - name: id
      - name: total
"""
        result = parse_dbt_schema_yml(content)
        assert len(result["models"][0]["columns"]) == 2

    def test_source_default_schema(self):
        content = """
version: 2
sources:
  - name: raw
    tables:
      - name: orders
"""
        result = parse_dbt_schema_yml(content)
        # schema defaults to name if not specified
        assert result["sources"][0]["schema"] == "raw"


# ---------------------------------------------------------------------------
# parse_dbt_project_yml
# ---------------------------------------------------------------------------


class TestParseDbtProjectYml:
    def test_basic_project(self):
        content = """
name: my_project
version: '1.0'
model-paths: ['models']
"""
        result = parse_dbt_project_yml(content)
        assert result["name"] == "my_project"
        assert result["model_paths"] == ["models"]
        assert result["errors"] == []

    def test_default_model_paths(self):
        content = "name: project\n"
        result = parse_dbt_project_yml(content)
        assert result["model_paths"] == ["models"]

    def test_models_config_extracted(self):
        content = """
name: project
models:
  project:
    +materialized: table
"""
        result = parse_dbt_project_yml(content)
        assert isinstance(result["models"], dict)

    def test_bytes_input(self):
        content = b"name: my_project\n"
        result = parse_dbt_project_yml(content)
        assert result["name"] == "my_project"

    def test_invalid_yaml(self):
        result = parse_dbt_project_yml("{bad: yaml: }: }")
        assert len(result["errors"]) > 0

    def test_empty_content(self):
        result = parse_dbt_project_yml("")
        assert result["name"] is None


# ---------------------------------------------------------------------------
# parse_airflow_dag_python
# ---------------------------------------------------------------------------


class TestParseAirflowDagPython:
    def _dag(self, code: str) -> dict:
        return parse_airflow_dag_python("my_dag.py", code)

    def test_dag_id_extracted(self):
        code = """
from airflow import DAG
dag = DAG(dag_id='my_pipeline')
"""
        result = self._dag(code)
        assert result["dag_id"] == "my_pipeline"

    def test_no_dag_id(self):
        code = "x = 1\n"
        result = self._dag(code)
        assert result["dag_id"] is None

    def test_task_ids_extracted(self):
        code = """
from airflow.operators.python import PythonOperator
t1 = PythonOperator(task_id='extract')
t2 = PythonOperator(task_id='transform')
"""
        result = self._dag(code)
        assert "extract" in result["tasks"]
        assert "transform" in result["tasks"]

    def test_dependencies_extracted(self):
        code = """
from airflow import DAG
dag = DAG(dag_id='test_dag')
t1 >> t2
"""
        result = self._dag(code)
        assert ("t1", "t2") in result["dependencies"]

    def test_chain_dependency(self):
        code = """
t1 >> t2 >> t3
"""
        result = self._dag(code)
        deps = result["dependencies"]
        # Should have t1->t2 and t2->t3
        assert len(deps) >= 1

    def test_source_file_in_result(self):
        result = self._dag("x = 1")
        assert result["source_file"] == "my_dag.py"

    def test_non_py_file_returns_empty(self):
        result = parse_airflow_dag_python("dag.yaml", "key: value")
        assert result["dag_id"] is None
        assert result["tasks"] == []

    def test_bytes_source(self):
        code = b"from airflow import DAG\ndag = DAG(dag_id='test')\n"
        result = parse_airflow_dag_python("dag.py", code)
        assert result["dag_id"] == "test"

    def test_empty_source(self):
        result = self._dag("")
        assert result["dag_id"] is None
        assert result["tasks"] == []
        assert result["dependencies"] == []


# ---------------------------------------------------------------------------
# analyze_dbt_directory
# ---------------------------------------------------------------------------


class TestAnalyzeDbtDirectory:
    def test_nonexistent_directory(self):
        result = analyze_dbt_directory("/does/not/exist")
        assert result["models"] == []
        assert result["sources"] == []

    def test_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = analyze_dbt_directory(tmpdir)
            assert result["models"] == []

    def test_reads_schema_yml(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            content = "version: 2\nmodels:\n  - name: orders\n"
            (Path(tmpdir) / "schema.yml").write_text(content)
            result = analyze_dbt_directory(tmpdir)
            names = [m["name"] for m in result["models"]]
            assert "orders" in names

    def test_reads_dbt_project_yml(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            (Path(tmpdir) / "dbt_project.yml").write_text("name: my_dbt_project\n")
            result = analyze_dbt_directory(tmpdir)
            assert result["project"].get("name") == "my_dbt_project"

    def test_reads_yaml_extension(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            content = "version: 2\nmodels:\n  - name: customers\n"
            (Path(tmpdir) / "models.yaml").write_text(content)
            result = analyze_dbt_directory(tmpdir)
            names = [m["name"] for m in result["models"]]
            assert "customers" in names

    def test_recursive_scan(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            models_dir = Path(tmpdir) / "models" / "staging"
            models_dir.mkdir(parents=True)
            content = "version: 2\nmodels:\n  - name: stg_orders\n"
            (models_dir / "schema.yml").write_text(content)
            result = analyze_dbt_directory(tmpdir)
            names = [m["name"] for m in result["models"]]
            assert "stg_orders" in names

    def test_skips_hidden_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            content = "version: 2\nmodels:\n  - name: hidden_model\n"
            (Path(tmpdir) / ".hidden.yml").write_text(content)
            result = analyze_dbt_directory(tmpdir)
            names = [m["name"] for m in result["models"]]
            assert "hidden_model" not in names


# ---------------------------------------------------------------------------
# analyze_airflow_dag_file (convenience wrapper)
# ---------------------------------------------------------------------------


def test_analyze_airflow_dag_file_wrapper():
    code = "from airflow import DAG\ndag = DAG(dag_id='wrapped_test')\n"
    result = analyze_airflow_dag_file("dag.py", code)
    assert result["dag_id"] == "wrapped_test"
