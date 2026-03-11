"""Static analysis components: tree-sitter, SQL lineage, DAG config parsing."""

from .python_data_flow import extract_python_data_flow
from .dag_config_parser import (
    analyze_airflow_dag_file,
    analyze_dbt_directory,
    parse_airflow_dag_python,
    parse_dbt_project_yml,
    parse_dbt_schema_yml,
)
from .sql_lineage import (
    SUPPORTED_DIALECTS,
    analyze_sql_directory,
    analyze_sql_file,
    extract_table_dependencies,
)
from .tree_sitter_analyzer import (
    LanguageRouter,
    analyze_module,
    extract_module_info,
)

__all__ = [
    "LanguageRouter",
    "analyze_module",
    "extract_module_info",
    "SUPPORTED_DIALECTS",
    "analyze_sql_file",
    "analyze_sql_directory",
    "extract_table_dependencies",
    "parse_dbt_schema_yml",
    "parse_dbt_project_yml",
    "parse_airflow_dag_python",
    "analyze_dbt_directory",
    "analyze_airflow_dag_file",
    "extract_python_data_flow",
]
