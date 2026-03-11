"""Tests for src/models/edges.py and src/models/nodes.py."""

import pytest
from pydantic import ValidationError

from src.models.edges import (
    CallsEdge,
    ConfiguresEdge,
    ConsumesEdge,
    EdgeType,
    ImportsEdge,
    ProducesEdge,
    edge_attrs,
)
from src.models.nodes import DatasetNode, FunctionNode, ModuleNode, TransformationNode


# ---------------------------------------------------------------------------
# EdgeType enum
# ---------------------------------------------------------------------------


def test_edge_type_values():
    assert EdgeType.IMPORTS == "IMPORTS"
    assert EdgeType.PRODUCES == "PRODUCES"
    assert EdgeType.CONSUMES == "CONSUMES"
    assert EdgeType.CALLS == "CALLS"
    assert EdgeType.CONFIGURES == "CONFIGURES"


def test_edge_type_is_str_enum():
    assert isinstance(EdgeType.IMPORTS, str)


# ---------------------------------------------------------------------------
# Edge models
# ---------------------------------------------------------------------------


def test_imports_edge_defaults():
    e = ImportsEdge()
    assert e.edge_type == EdgeType.IMPORTS
    assert e.weight == 1


def test_imports_edge_custom_weight():
    e = ImportsEdge(weight=5)
    assert e.weight == 5


def test_imports_edge_frozen_type():
    e = ImportsEdge()
    with pytest.raises(Exception):
        e.edge_type = EdgeType.CALLS


def test_produces_edge_default():
    e = ProducesEdge()
    assert e.edge_type == EdgeType.PRODUCES


def test_consumes_edge_default():
    e = ConsumesEdge()
    assert e.edge_type == EdgeType.CONSUMES


def test_calls_edge_defaults():
    e = CallsEdge()
    assert e.edge_type == EdgeType.CALLS
    assert e.call_count is None


def test_calls_edge_with_count():
    e = CallsEdge(call_count=3)
    assert e.call_count == 3


def test_configures_edge_default():
    e = ConfiguresEdge()
    assert e.edge_type == EdgeType.CONFIGURES


# ---------------------------------------------------------------------------
# edge_attrs helper
# ---------------------------------------------------------------------------


def test_edge_attrs_base():
    attrs = edge_attrs(EdgeType.IMPORTS)
    assert attrs["edge_type"] == "IMPORTS"


def test_edge_attrs_with_kwargs():
    attrs = edge_attrs(EdgeType.CALLS, call_count=7, extra="x")
    assert attrs["edge_type"] == "CALLS"
    assert attrs["call_count"] == 7
    assert attrs["extra"] == "x"


def test_edge_attrs_produces():
    attrs = edge_attrs(EdgeType.PRODUCES)
    assert attrs["edge_type"] == "PRODUCES"
    assert len(attrs) == 1


# ---------------------------------------------------------------------------
# Node models
# ---------------------------------------------------------------------------


def test_module_node_required_fields():
    node = ModuleNode(path="src/foo.py", language="python")
    assert node.path == "src/foo.py"
    assert node.language == "python"
    assert node.complexity_score == 0.0
    assert node.is_dead_code_candidate is False
    assert node.purpose_statement is None


def test_module_node_full_construction():
    node = ModuleNode(
        path="src/bar.py",
        language="python",
        complexity_score=42.0,
        purpose_statement="Handles ETL",
        domain_cluster="data",
        change_velocity_30d=5,
        is_dead_code_candidate=True,
        last_modified="2024-01-01T00:00:00",
    )
    assert node.complexity_score == 42.0
    assert node.domain_cluster == "data"
    assert node.change_velocity_30d == 5
    assert node.is_dead_code_candidate is True


def test_module_node_missing_path_raises():
    with pytest.raises(ValidationError):
        ModuleNode(language="python")  # type: ignore[call-arg]


def test_function_node_defaults():
    fn = FunctionNode(qualified_name="src.foo.bar", parent_module="src/foo.py")
    assert fn.qualified_name == "src.foo.bar"
    assert fn.parent_module == "src/foo.py"
    assert fn.is_public_api is True
    assert fn.signature is None
    assert fn.line_range is None


def test_function_node_full():
    fn = FunctionNode(
        qualified_name="src.foo._helper",
        parent_module="src/foo.py",
        signature="(x: int) -> str",
        is_public_api=False,
        line_range=(10, 20),
    )
    assert fn.is_public_api is False
    assert fn.line_range == (10, 20)


def test_dataset_node_defaults():
    ds = DatasetNode(name="orders")
    assert ds.name == "orders"
    assert ds.storage_type == "table"
    assert ds.extra == {}


def test_dataset_node_custom_storage():
    ds = DatasetNode(name="events.parquet", storage_type="file")
    assert ds.storage_type == "file"


def test_transformation_node_defaults():
    t = TransformationNode(
        transform_id="models/orders.sql:orders",
        source_file="models/orders.sql",
        transformation_type="sql",
    )
    assert t.source_datasets == []
    assert t.target_datasets == []


def test_transformation_node_full():
    t = TransformationNode(
        transform_id="etl.py:load",
        source_file="etl.py",
        transformation_type="python",
        source_datasets=["raw_orders"],
        target_datasets=["clean_orders"],
    )
    assert "raw_orders" in t.source_datasets
    assert "clean_orders" in t.target_datasets


# ---------------------------------------------------------------------------
# Field validators
# ---------------------------------------------------------------------------


class TestModuleNodeValidators:
    def test_language_normalized_to_lowercase(self):
        node = ModuleNode(path="foo.py", language="Python")
        assert node.language == "python"

    def test_language_mixed_case(self):
        node = ModuleNode(path="foo.ts", language="TypeScript")
        assert node.language == "typescript"

    def test_negative_complexity_raises(self):
        with pytest.raises(ValidationError):
            ModuleNode(path="foo.py", language="python", complexity_score=-1.0)

    def test_zero_complexity_ok(self):
        node = ModuleNode(path="foo.py", language="python", complexity_score=0.0)
        assert node.complexity_score == 0.0

    def test_negative_velocity_raises(self):
        with pytest.raises(ValidationError):
            ModuleNode(path="foo.py", language="python", change_velocity_30d=-1)

    def test_zero_velocity_ok(self):
        node = ModuleNode(path="foo.py", language="python", change_velocity_30d=0)
        assert node.change_velocity_30d == 0

    def test_none_velocity_ok(self):
        node = ModuleNode(path="foo.py", language="python", change_velocity_30d=None)
        assert node.change_velocity_30d is None


class TestFunctionNodeValidators:
    def test_valid_line_range_ok(self):
        fn = FunctionNode(qualified_name="m.f", parent_module="m.py", line_range=(1, 10))
        assert fn.line_range == (1, 10)

    def test_equal_start_end_ok(self):
        fn = FunctionNode(qualified_name="m.f", parent_module="m.py", line_range=(5, 5))
        assert fn.line_range == (5, 5)

    def test_inverted_line_range_raises(self):
        with pytest.raises(ValidationError):
            FunctionNode(qualified_name="m.f", parent_module="m.py", line_range=(10, 5))

    def test_none_line_range_ok(self):
        fn = FunctionNode(qualified_name="m.f", parent_module="m.py")
        assert fn.line_range is None


class TestDatasetNodeValidators:
    def test_valid_storage_types(self):
        for st in ("table", "file", "stream", "api", "task"):
            ds = DatasetNode(name="x", storage_type=st)
            assert ds.storage_type == st

    def test_invalid_storage_type_raises(self):
        with pytest.raises(ValidationError):
            DatasetNode(name="x", storage_type="database")


class TestTransformationNodeValidators:
    def test_valid_transformation_types(self):
        for tt in ("sql", "dbt_model", "airflow_task", "python", "dbt"):
            t = TransformationNode(transform_id="x", source_file="f", transformation_type=tt)
            assert t.transformation_type == tt

    def test_invalid_transformation_type_raises(self):
        with pytest.raises(ValidationError):
            TransformationNode(transform_id="x", source_file="f", transformation_type="spark")
