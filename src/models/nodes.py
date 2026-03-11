"""Pydantic node schemas for the knowledge graph."""

from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


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

    @field_validator("language")
    @classmethod
    def _normalize_language(cls, v: str) -> str:
        return v.lower()

    @field_validator("complexity_score")
    @classmethod
    def _non_negative_complexity(cls, v: float) -> float:
        if v < 0:
            raise ValueError("complexity_score must be >= 0")
        return v

    @field_validator("change_velocity_30d")
    @classmethod
    def _non_negative_velocity(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v < 0:
            raise ValueError("change_velocity_30d must be >= 0")
        return v


class FunctionNode(BaseModel):
    """A function or method in the module graph."""

    qualified_name: str = Field(description="Fully qualified name (module.function)")
    parent_module: str = Field(description="Module path that defines this function")
    signature: Optional[str] = Field(default=None, description="Function signature text")
    is_public_api: bool = Field(default=True)
    line_range: Optional[tuple[int, int]] = Field(default=None, description="(start_line, end_line)")

    @field_validator("line_range")
    @classmethod
    def _valid_line_range(cls, v: Optional[tuple[int, int]]) -> Optional[tuple[int, int]]:
        if v is not None and v[0] > v[1]:
            raise ValueError(f"line_range start ({v[0]}) must be <= end ({v[1]})")
        return v


_VALID_STORAGE_TYPES = {"table", "file", "stream", "api", "task"}


class DatasetNode(BaseModel):
    """A dataset (table, file, stream) in the lineage graph."""

    name: str = Field(description="Dataset identifier (table name, file path, etc.)")
    storage_type: str = Field(default="table", description="table|file|stream|api|task")
    extra: dict[str, Any] = Field(default_factory=dict)

    @field_validator("storage_type")
    @classmethod
    def _valid_storage_type(cls, v: str) -> str:
        if v not in _VALID_STORAGE_TYPES:
            raise ValueError(f"storage_type must be one of {_VALID_STORAGE_TYPES}, got {v!r}")
        return v


_VALID_TRANSFORMATION_TYPES = {"sql", "dbt_model", "airflow_task", "python", "dbt"}


class TransformationNode(BaseModel):
    """A transformation (SQL, dbt model, Python ETL) in the lineage graph."""

    transform_id: str = Field(description="Unique identifier for this transformation")
    source_file: str = Field(description="Source file path that defines this transformation")
    transformation_type: str = Field(description="sql|dbt_model|airflow_task|python")
    source_datasets: list[str] = Field(default_factory=list, description="Input dataset names")
    target_datasets: list[str] = Field(default_factory=list, description="Output dataset names")
    extra: dict[str, Any] = Field(default_factory=dict)

    @field_validator("transformation_type")
    @classmethod
    def _valid_transformation_type(cls, v: str) -> str:
        if v not in _VALID_TRANSFORMATION_TYPES:
            raise ValueError(f"transformation_type must be one of {_VALID_TRANSFORMATION_TYPES}, got {v!r}")
        return v
