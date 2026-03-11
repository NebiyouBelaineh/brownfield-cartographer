"""Edge types and schemas for the knowledge graph."""

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class EdgeType(str, Enum):
    """Canonical edge types in the knowledge graph."""

    IMPORTS = "IMPORTS"  # source_module → target_module
    PRODUCES = "PRODUCES"  # transformation → dataset
    CONSUMES = "CONSUMES"  # transformation → dataset (upstream)
    CALLS = "CALLS"  # function → function
    CONFIGURES = "CONFIGURES"  # config_file → module/pipeline


class ImportsEdge(BaseModel):
    """IMPORTS: source_module → target_module. Weight = import_count."""

    edge_type: EdgeType = Field(default=EdgeType.IMPORTS, frozen=True)
    weight: int = Field(default=1, description="Import count")


class ProducesEdge(BaseModel):
    """PRODUCES: transformation → dataset. Captures data lineage."""

    edge_type: EdgeType = Field(default=EdgeType.PRODUCES, frozen=True)


class ConsumesEdge(BaseModel):
    """CONSUMES: transformation → dataset. Captures upstream dependencies."""

    edge_type: EdgeType = Field(default=EdgeType.CONSUMES, frozen=True)


class CallsEdge(BaseModel):
    """CALLS: function → function. For call graph analysis."""

    edge_type: EdgeType = Field(default=EdgeType.CALLS, frozen=True)
    call_count: Optional[int] = Field(default=None, description="Number of call sites")


class ConfiguresEdge(BaseModel):
    """CONFIGURES: config_file → module/pipeline. YAML/ENV relationship."""

    edge_type: EdgeType = Field(default=EdgeType.CONFIGURES, frozen=True)


def edge_attrs(edge_type: EdgeType, **kwargs: Any) -> dict[str, Any]:
    """Build edge attributes dict for NetworkX from edge type and kwargs."""
    base: dict[str, Any] = {"edge_type": edge_type.value}
    base.update(kwargs)
    return base
