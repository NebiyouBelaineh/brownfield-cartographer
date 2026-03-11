"""NetworkX wrapper for module graph and lineage graph with serialization."""

import json
from pathlib import Path
from typing import Any

import networkx as nx

from src.models import EdgeType, edge_attrs


def _to_json_safe(obj: Any) -> Any:
    """Convert object to JSON-serializable form."""
    if isinstance(obj, dict):
        return {k: _to_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_json_safe(x) for x in obj]
    if hasattr(obj, "model_dump"):
        return _to_json_safe(obj.model_dump())
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return str(obj)


def _node_link_format(G: nx.DiGraph) -> dict[str, Any]:
    """Convert DiGraph to node-link format for JSON serialization."""
    data = nx.node_link_data(G)
    return _to_json_safe(data)


def _from_node_link(data: dict[str, Any]) -> nx.DiGraph:
    """Build DiGraph from node-link format."""
    return nx.node_link_graph(data, directed=True)


class ModuleGraph:
    """NetworkX DiGraph for module import structure and call graph."""

    def __init__(self) -> None:
        self._G: nx.DiGraph = nx.DiGraph()

    @property
    def graph(self) -> nx.DiGraph:
        return self._G

    def add_module(
        self,
        path: str,
        language: str,
        **attrs: Any,
    ) -> None:
        """Add a module node. Path is the node id."""
        if not self._G.has_node(path):
            self._G.add_node(path, path=path, language=language, node_type="module", **attrs)

    def add_function(
        self,
        qualified_name: str,
        parent_module: str,
        **attrs: Any,
    ) -> None:
        """Add a function node."""
        if not self._G.has_node(qualified_name):
            self._G.add_node(
                qualified_name,
                qualified_name=qualified_name,
                parent_module=parent_module,
                node_type="function",
                **attrs,
            )

    def add_import(self, source_module: str, target_module: str, weight: int = 1) -> None:
        """Add IMPORTS edge from source to target module."""
        self._G.add_edge(
            source_module,
            target_module,
            **edge_attrs(EdgeType.IMPORTS, weight=weight),
        )

    def add_call(self, caller: str, callee: str, call_count: int | None = None) -> None:
        """Add CALLS edge from caller to callee function."""
        attrs = edge_attrs(EdgeType.CALLS)
        if call_count is not None:
            attrs["call_count"] = call_count
        self._G.add_edge(caller, callee, **attrs)

    def to_dict(self) -> dict[str, Any]:
        """Export to node-link format (JSON-serializable)."""
        return _node_link_format(self._G)

    def to_json(self, path: Path | str) -> None:
        """Write graph to JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def from_json(cls, path: Path | str) -> "ModuleGraph":
        """Load graph from JSON file."""
        path = Path(path)
        with path.open() as f:
            data = json.load(f)
        mg = cls()
        mg._G = _from_node_link(data)
        return mg


class LineageGraph:
    """NetworkX DiGraph for data lineage (datasets and transformations)."""

    def __init__(self) -> None:
        self._G: nx.DiGraph = nx.DiGraph()

    @property
    def graph(self) -> nx.DiGraph:
        return self._G

    def add_dataset(
        self, name: str, storage_type: str = "table", **attrs: Any
    ) -> None:
        """Add a dataset node. storage_type: table|file|stream|api|task."""
        if not self._G.has_node(name):
            self._G.add_node(name, name=name, storage_type=storage_type, node_type="dataset", **attrs)

    def add_transformation(
        self,
        transform_id: str,
        source_file: str,
        transformation_type: str,
        source_datasets: list[str] | None = None,
        target_datasets: list[str] | None = None,
        **attrs: Any,
    ) -> None:
        """Add a transformation node. Links to datasets via PRODUCES/CONSUMES."""
        if not self._G.has_node(transform_id):
            self._G.add_node(
                transform_id,
                transform_id=transform_id,
                source_file=source_file,
                transformation_type=transformation_type,
                node_type="transformation",
                source_datasets=source_datasets or [],
                target_datasets=target_datasets or [],
                **attrs,
            )
        # Add PRODUCES edges to target datasets
        for target in target_datasets or []:
            self.add_dataset(target, "table")  # default storage_type
            self._G.add_edge(transform_id, target, **edge_attrs(EdgeType.PRODUCES))
        # Add CONSUMES edges from source datasets
        for source in source_datasets or []:
            self.add_dataset(source, "table")
            self._G.add_edge(source, transform_id, **edge_attrs(EdgeType.CONSUMES))

    def blast_radius(self, node: str) -> set[str]:
        """Return all nodes downstream of node (BFS)."""
        if not self._G.has_node(node):
            return set()
        return set(nx.descendants(self._G, node))

    def find_sources(self) -> list[str]:
        """Nodes with in-degree 0 (entry points)."""
        return [n for n in self._G.nodes() if self._G.in_degree(n) == 0]

    def find_sinks(self) -> list[str]:
        """Nodes with out-degree 0 (exit points)."""
        return [n for n in self._G.nodes() if self._G.out_degree(n) == 0]

    def to_dict(self) -> dict[str, Any]:
        """Export to node-link format (JSON-serializable)."""
        return _node_link_format(self._G)

    def to_json(self, path: Path | str) -> None:
        """Write graph to JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def from_json(cls, path: Path | str) -> "LineageGraph":
        """Load graph from JSON file."""
        path = Path(path)
        with path.open() as f:
            data = json.load(f)
        lg = cls()
        lg._G = _from_node_link(data)
        return lg
