"""Tests for src/analyzers/python_data_flow.py."""

import pytest

from src.analyzers.python_data_flow import extract_python_data_flow


def _run(code: str, filename: str = "test_file.py") -> list[dict]:
    return extract_python_data_flow(filename, code.encode("utf-8"))


# ---------------------------------------------------------------------------
# Non-Python files
# ---------------------------------------------------------------------------


def test_non_python_file_returns_empty():
    result = extract_python_data_flow("script.sh", b"echo hello")
    assert result == []


def test_yaml_file_returns_empty():
    result = extract_python_data_flow("config.yaml", b"key: value")
    assert result == []


# ---------------------------------------------------------------------------
# Pandas reads
# ---------------------------------------------------------------------------


def test_read_csv_static_path():
    code = "import pandas as pd\ndf = pd.read_csv('data/orders.csv')"
    results = _run(code)
    reads = [r for r in results if r["type"] == "read"]
    assert len(reads) == 1
    assert reads[0]["method"] == "read_csv"
    assert reads[0]["dataset"] == "data/orders.csv"


def test_read_parquet_static_path():
    code = "df = pd.read_parquet('s3://bucket/events.parquet')"
    results = _run(code)
    assert any(r["method"] == "read_parquet" and r["type"] == "read" for r in results)


def test_read_json_static():
    code = "df = pd.read_json('data.json')"
    results = _run(code)
    assert any(r["method"] == "read_json" for r in results)


def test_read_excel_static():
    code = "df = pd.read_excel('report.xlsx')"
    results = _run(code)
    assert any(r["method"] == "read_excel" for r in results)


def test_read_sql_static():
    code = "df = pd.read_sql('SELECT * FROM orders', con)"
    results = _run(code)
    reads = [r for r in results if r["method"] == "read_sql"]
    assert len(reads) == 1
    assert reads[0]["type"] == "read"


def test_read_dynamic_path():
    code = "df = pd.read_csv(path_variable)"
    results = _run(code)
    reads = [r for r in results if r["method"] == "read_csv"]
    assert reads[0]["dataset"] == "dynamic"


# ---------------------------------------------------------------------------
# Pandas writes
# ---------------------------------------------------------------------------


def test_to_csv_static():
    code = "df.to_csv('output/orders.csv')"
    results = _run(code)
    writes = [r for r in results if r["type"] == "write"]
    assert len(writes) == 1
    assert writes[0]["method"] == "to_csv"
    assert writes[0]["dataset"] == "output/orders.csv"


def test_to_parquet_static():
    code = "df.to_parquet('output.parquet')"
    results = _run(code)
    assert any(r["method"] == "to_parquet" and r["type"] == "write" for r in results)


def test_to_sql_static():
    code = "df.to_sql('orders', con)"
    results = _run(code)
    writes = [r for r in results if r["method"] == "to_sql"]
    assert writes[0]["type"] == "write"


def test_to_json_static():
    code = "df.to_json('output.json')"
    results = _run(code)
    assert any(r["method"] == "to_json" and r["type"] == "write" for r in results)


# ---------------------------------------------------------------------------
# PySpark patterns
# ---------------------------------------------------------------------------


def test_pyspark_read_csv():
    code = "df = spark.read.csv('hdfs://data/events')"
    results = _run(code)
    reads = [r for r in results if r["type"] == "read"]
    assert any("spark.read.csv" in r["method"] for r in reads)


def test_pyspark_read_parquet():
    code = "df = spark.read.parquet('s3://bucket/data')"
    results = _run(code)
    assert any("spark.read.parquet" in r.get("method", "") for r in results)


def test_pyspark_write_save():
    code = "df.write.save('/output/path')"
    results = _run(code)
    writes = [r for r in results if r["type"] == "write"]
    assert any(r["method"] == "save" for r in writes)


def test_pyspark_write_save_as_table():
    code = "df.write.saveAsTable('my_table')"
    results = _run(code)
    writes = [r for r in results if r["type"] == "write"]
    assert any(r["method"] == "saveAsTable" for r in writes)


# ---------------------------------------------------------------------------
# SQLAlchemy execute
# ---------------------------------------------------------------------------


def test_sqlalchemy_execute():
    code = "conn.execute(text('SELECT * FROM orders'))"
    results = _run(code)
    assert any(r["method"] == "execute" for r in results)
    assert any(r["dataset"] == "dynamic" for r in results)


# ---------------------------------------------------------------------------
# Line numbers
# ---------------------------------------------------------------------------


def test_line_number_reported():
    code = "import pandas as pd\n\ndf = pd.read_csv('data.csv')"
    results = _run(code)
    reads = [r for r in results if r["method"] == "read_csv"]
    assert reads[0]["line"] == 3


# ---------------------------------------------------------------------------
# Multiple operations
# ---------------------------------------------------------------------------


def test_multiple_operations():
    code = """
import pandas as pd
df1 = pd.read_csv('input.csv')
df2 = pd.read_parquet('other.parquet')
df1.to_csv('output.csv')
"""
    results = _run(code)
    reads = [r for r in results if r["type"] == "read"]
    writes = [r for r in results if r["type"] == "write"]
    assert len(reads) == 2
    assert len(writes) == 1


def test_empty_file_returns_empty():
    results = _run("")
    assert results == []
