"""Pydantic node schemas for the knowledge graph."""

from typing import Any, Optional

from pydantic import BaseModel, Field


class ModuleNode(BaseModel):
    """A source file (module) in the module graph."""

    path: str = Field(description="Relative path from repo root")
    language: str = Field(description="Detected language (python, yaml, javascript, etc.)")
    complexity_score: float = Field(default=0.0, description="Approximate complexity (line count)")
    purpose_statement: Optional[str] = Field(default=None, description="Human-readable summary")
    domain_cluster: Optional[str] = Field(default=None, description="Semantic domain cluster")
    change_velocity_30d: Optional[int] = Field(default=None, description="Commits in last 30 days")
    is_dead_code_candidate: bool = Field(default=False)
    last_modified: Optional[str] = Field(default=None, description="ISO datetime string")
    extra: dict[str, Any] = Field(default_factory=dict, description="Additional attributes")


class FunctionNode(BaseModel):
    """A function or method in the module graph."""

    qualified_name: str = Field(description="Fully qualified name (module.function)")
    parent_module: str = Field(description="Module path that defines this function")
    signature: Optional[str] = Field(default=None, description="Function signature text")
    is_public_api: bool = Field(default=True)
    line_range: Optional[tuple[int, int]] = Field(default=None, description="(start_line, end_line)")


class DatasetNode(BaseModel):
    """A dataset (table, file, stream) in the lineage graph."""

    name: str = Field(description="Dataset identifier (table name, file path, etc.)")
    storage_type: str = Field(default="table", description="table|file|stream|api|task")
    extra: dict[str, Any] = Field(default_factory=dict)


class TransformationNode(BaseModel):
    """A transformation (SQL, dbt model, Python ETL) in the lineage graph."""

    transform_id: str = Field(description="Unique identifier for this transformation")
    source_file: str = Field(description="Source file path that defines this transformation")
    transformation_type: str = Field(description="sql|dbt|airflow|python")
    source_datasets: list[str] = Field(default_factory=list, description="Input dataset names")
    target_datasets: list[str] = Field(default_factory=list, description="Output dataset names")
    extra: dict[str, Any] = Field(default_factory=dict)
