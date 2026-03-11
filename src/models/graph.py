"""Graph types for module graph and lineage graph serialization."""

from typing import Any

from pydantic import BaseModel, Field

from .edges import EdgeType


class ModuleGraphSchema(BaseModel):
    """Serialization schema for the module import graph.

    Nodes: ModuleNode, FunctionNode. Edges: IMPORTS, CALLS.
    """

    nodes: list[dict[str, Any]] = Field(
        default_factory=list,
        description="List of node attributes (path, language, etc.)",
    )
    edges: list[dict[str, Any]] = Field(
        default_factory=list,
        description="List of (source, target, attrs) as dicts",
    )
    edge_types: list[EdgeType] = Field(
        default=[EdgeType.IMPORTS, EdgeType.CALLS],
        description="Edge types used in this graph",
    )


class LineageGraphSchema(BaseModel):
    """Serialization schema for the data lineage graph.

    Nodes: DatasetNode, TransformationNode. Edges: PRODUCES, CONSUMES.
    """

    nodes: list[dict[str, Any]] = Field(
        default_factory=list,
        description="List of node attributes (name, storage_type, etc.)",
    )
    edges: list[dict[str, Any]] = Field(
        default_factory=list,
        description="List of (source, target, attrs) as dicts",
    )
    edge_types: list[EdgeType] = Field(
        default=[EdgeType.PRODUCES, EdgeType.CONSUMES],
        description="Edge types used in this graph",
    )
