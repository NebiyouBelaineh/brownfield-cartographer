"""Tests for src/graph/knowledge_graph.py — ModuleGraph and LineageGraph."""

import json
import tempfile
from pathlib import Path

import pytest

from src.graph.knowledge_graph import LineageGraph, ModuleGraph
from src.models.edges import EdgeType


# ---------------------------------------------------------------------------
# ModuleGraph
# ---------------------------------------------------------------------------


class TestModuleGraph:
    def test_empty_graph(self):
        mg = ModuleGraph()
        assert mg.graph.number_of_nodes() == 0
        assert mg.graph.number_of_edges() == 0

    def test_add_module(self):
        mg = ModuleGraph()
        mg.add_module("src/foo.py", "python")
        assert mg.graph.has_node("src/foo.py")
        data = mg.graph.nodes["src/foo.py"]
        assert data["language"] == "python"
        assert data["node_type"] == "module"

    def test_add_module_idempotent(self):
        mg = ModuleGraph()
        mg.add_module("src/foo.py", "python")
        mg.add_module("src/foo.py", "python", extra_attr=True)  # second call should be ignored
        assert mg.graph.number_of_nodes() == 1

    def test_add_module_with_extra_attrs(self):
        mg = ModuleGraph()
        mg.add_module("src/foo.py", "python", change_velocity_30d=3)
        assert mg.graph.nodes["src/foo.py"]["change_velocity_30d"] == 3

    def test_add_function(self):
        mg = ModuleGraph()
        mg.add_function("src.foo.bar", "src/foo.py")
        assert mg.graph.has_node("src.foo.bar")
        data = mg.graph.nodes["src.foo.bar"]
        assert data["node_type"] == "function"
        assert data["parent_module"] == "src/foo.py"

    def test_add_function_idempotent(self):
        mg = ModuleGraph()
        mg.add_function("src.foo.bar", "src/foo.py")
        mg.add_function("src.foo.bar", "src/foo.py")
        assert mg.graph.number_of_nodes() == 1

    def test_add_import_edge(self):
        mg = ModuleGraph()
        mg.add_module("src/a.py", "python")
        mg.add_module("src/b.py", "python")
        mg.add_import("src/a.py", "src/b.py")
        assert mg.graph.has_edge("src/a.py", "src/b.py")
        attrs = mg.graph["src/a.py"]["src/b.py"]
        assert attrs["edge_type"] == EdgeType.IMPORTS.value

    def test_add_import_default_weight(self):
        mg = ModuleGraph()
        mg.add_import("a.py", "b.py")
        assert mg.graph["a.py"]["b.py"]["weight"] == 1

    def test_add_import_custom_weight(self):
        mg = ModuleGraph()
        mg.add_import("a.py", "b.py", weight=5)
        assert mg.graph["a.py"]["b.py"]["weight"] == 5

    def test_add_call_edge(self):
        mg = ModuleGraph()
        mg.add_call("src.foo.main", "src.bar.helper")
        assert mg.graph.has_edge("src.foo.main", "src.bar.helper")
        attrs = mg.graph["src.foo.main"]["src.bar.helper"]
        assert attrs["edge_type"] == EdgeType.CALLS.value

    def test_add_call_with_count(self):
        mg = ModuleGraph()
        mg.add_call("fn_a", "fn_b", call_count=3)
        assert mg.graph["fn_a"]["fn_b"]["call_count"] == 3

    def test_add_call_without_count_no_key(self):
        mg = ModuleGraph()
        mg.add_call("fn_a", "fn_b")
        assert "call_count" not in mg.graph["fn_a"]["fn_b"]

    def test_to_dict_structure(self):
        mg = ModuleGraph()
        mg.add_module("src/a.py", "python")
        mg.add_module("src/b.py", "python")
        mg.add_import("src/a.py", "src/b.py")
        d = mg.to_dict()
        assert "nodes" in d
        assert "links" in d or "edges" in d  # networkx uses "links"

    def test_to_json_roundtrip(self):
        mg = ModuleGraph()
        mg.add_module("src/a.py", "python")
        mg.add_import("src/a.py", "src/b.py")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "module_graph.json"
            mg.to_json(path)
            assert path.exists()
            mg2 = ModuleGraph.from_json(path)
            assert mg2.graph.has_node("src/a.py")
            assert mg2.graph.has_edge("src/a.py", "src/b.py")

    def test_to_json_creates_parent_dirs(self):
        mg = ModuleGraph()
        mg.add_module("x.py", "python")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "nested" / "dir" / "graph.json"
            mg.to_json(path)
            assert path.exists()

    def test_to_dict_is_json_serializable(self):
        mg = ModuleGraph()
        mg.add_module("src/a.py", "python", complexity=10.5)
        d = mg.to_dict()
        # Should not raise
        json.dumps(d)


# ---------------------------------------------------------------------------
# LineageGraph
# ---------------------------------------------------------------------------


class TestLineageGraph:
    def test_empty_graph(self):
        lg = LineageGraph()
        assert lg.graph.number_of_nodes() == 0

    def test_add_dataset(self):
        lg = LineageGraph()
        lg.add_dataset("orders", "table")
        assert lg.graph.has_node("orders")
        data = lg.graph.nodes["orders"]
        assert data["storage_type"] == "table"
        assert data["node_type"] == "dataset"

    def test_add_dataset_default_storage_type(self):
        lg = LineageGraph()
        lg.add_dataset("events")
        assert lg.graph.nodes["events"]["storage_type"] == "table"

    def test_add_dataset_idempotent(self):
        lg = LineageGraph()
        lg.add_dataset("orders")
        lg.add_dataset("orders")
        assert lg.graph.number_of_nodes() == 1

    def test_add_transformation_creates_node(self):
        lg = LineageGraph()
        lg.add_transformation(
            transform_id="models/orders.sql:orders",
            source_file="models/orders.sql",
            transformation_type="sql",
            source_datasets=["raw_orders"],
            target_datasets=["orders"],
        )
        assert lg.graph.has_node("models/orders.sql:orders")
        data = lg.graph.nodes["models/orders.sql:orders"]
        assert data["transformation_type"] == "sql"
        assert data["node_type"] == "transformation"

    def test_add_transformation_creates_datasets(self):
        lg = LineageGraph()
        lg.add_transformation(
            "t1", "etl.py", "python",
            source_datasets=["raw"],
            target_datasets=["clean"],
        )
        assert lg.graph.has_node("raw")
        assert lg.graph.has_node("clean")

    def test_add_transformation_produces_edges(self):
        lg = LineageGraph()
        lg.add_transformation(
            "t1", "etl.py", "python",
            source_datasets=["raw"],
            target_datasets=["clean"],
        )
        # PRODUCES: t1 → clean
        assert lg.graph.has_edge("t1", "clean")
        assert lg.graph["t1"]["clean"]["edge_type"] == EdgeType.PRODUCES.value

    def test_add_transformation_consumes_edges(self):
        lg = LineageGraph()
        lg.add_transformation(
            "t1", "etl.py", "python",
            source_datasets=["raw"],
            target_datasets=["clean"],
        )
        # CONSUMES: raw → t1
        assert lg.graph.has_edge("raw", "t1")
        assert lg.graph["raw"]["t1"]["edge_type"] == EdgeType.CONSUMES.value

    def test_add_transformation_no_sources(self):
        lg = LineageGraph()
        lg.add_transformation("t1", "etl.py", "python", target_datasets=["output"])
        assert lg.graph.has_node("t1")
        assert lg.graph.has_node("output")
        assert lg.graph.has_edge("t1", "output")

    def test_add_transformation_no_targets(self):
        lg = LineageGraph()
        lg.add_transformation("t1", "etl.py", "python", source_datasets=["input"])
        assert lg.graph.has_edge("input", "t1")

    def test_blast_radius_direct(self):
        lg = LineageGraph()
        lg.add_transformation("t1", "f.py", "sql", source_datasets=["A"], target_datasets=["B"])
        lg.add_transformation("t2", "f.py", "sql", source_datasets=["B"], target_datasets=["C"])
        # A → t1 → B → t2 → C
        radius = lg.blast_radius("A")
        assert "t1" in radius
        assert "B" in radius
        assert "t2" in radius
        assert "C" in radius

    def test_blast_radius_nonexistent_node(self):
        lg = LineageGraph()
        assert lg.blast_radius("ghost") == set()

    def test_blast_radius_leaf_node(self):
        lg = LineageGraph()
        lg.add_dataset("sink")
        assert lg.blast_radius("sink") == set()

    def test_find_sources(self):
        lg = LineageGraph()
        lg.add_transformation("t1", "f.py", "sql", source_datasets=["A"], target_datasets=["B"])
        sources = lg.find_sources()
        assert "A" in sources
        assert "t1" not in sources  # t1 has incoming from A

    def test_find_sinks(self):
        lg = LineageGraph()
        lg.add_transformation("t1", "f.py", "sql", source_datasets=["A"], target_datasets=["B"])
        sinks = lg.find_sinks()
        assert "B" in sinks

    def test_find_sources_empty_graph(self):
        lg = LineageGraph()
        assert lg.find_sources() == []

    def test_to_json_roundtrip(self):
        lg = LineageGraph()
        lg.add_dataset("orders", "table")
        lg.add_transformation("t1", "etl.py", "python", source_datasets=["orders"], target_datasets=["clean_orders"])
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "lineage_graph.json"
            lg.to_json(path)
            assert path.exists()
            lg2 = LineageGraph.from_json(path)
            assert lg2.graph.has_node("orders")
            assert lg2.graph.has_node("t1")

    def test_to_dict_is_json_serializable(self):
        lg = LineageGraph()
        lg.add_dataset("ds", "file")
        d = lg.to_dict()
        json.dumps(d)

    def test_chain_lineage(self):
        """A → t1 → B → t2 → C: sources=[A], sinks=[C]."""
        lg = LineageGraph()
        lg.add_transformation("t1", "f.py", "sql", source_datasets=["A"], target_datasets=["B"])
        lg.add_transformation("t2", "f.py", "sql", source_datasets=["B"], target_datasets=["C"])
        assert "A" in lg.find_sources()
        assert "C" in lg.find_sinks()
