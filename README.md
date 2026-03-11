# brownfield-cartographer

A multi-agent codebase intelligence system that rapidly maps complex, undocumented codebases —
producing a queryable knowledge graph of architecture, data flows, and module structure.

Given a local path or GitHub URL, Brownfield Cartographer runs two agents in sequence:

- **Surveyor**: builds a module import graph, computes PageRank for architectural hubs, identifies
  circular dependencies (SCCs), dead code candidates, and git velocity per file.
- **Hydrologist**: traces data lineage from SQL files, dbt models (`schema.yml` + `ref()`
  relationships), Airflow DAG task dependencies (`>>`), and Python pandas/PySpark read-write
  patterns.

Artifacts are written to `.cartography/` as JSON files ready for inspection or programmatic querying.

---

## Installation

Requires Python 3.10+ and [uv](https://github.com/astral-sh/uv).

```bash
git clone https://github.com/NebiyouBelaineh/brownfield-cartographer
cd brownfield-cartographer
uv sync
```

---

## Usage

### Analyze a local repository

```bash
uv run cartographer analyze /path/to/repo
```

### Analyze a GitHub repository (clones automatically)

```bash
uv run cartographer analyze https://github.com/apache/airflow
```

### Options

```
cartographer analyze <repo_path> [OPTIONS]

Arguments:
  repo_path            Local directory path or GitHub URL

Options:
  -o, --output DIR     Output directory for artifacts (default: .cartography)
  --days N             Days of git history for velocity analysis (default: 30)
  --clone-dir DIR      Directory to clone remote repos into (default: temp dir)
  --no-sql             Skip SQL lineage analysis
  --no-dbt             Skip dbt schema.yml lineage analysis
  --no-airflow         Skip Airflow DAG task dependency analysis
  --no-python-flow     Skip Python pandas/PySpark data flow analysis
```

### Example output

```
Analysis complete.
  Repo:          /home/user/airflow
  Output:        .cartography
  Module graph:  8017 nodes, 12242 edges
  Lineage graph: 430 nodes, 461 edges
```

---

## Output artifacts

All artifacts are written to the `--output` directory (default: `.cartography/`).

| File | Contents |
|------|----------|
| `module_graph.json` | NetworkX node-link format. Nodes: source files (Python, JS/TS, YAML). Node attributes: `language`, `pagerank`, `change_velocity_30d`, `in_cycle`, `is_dead_code_candidate`. Edges: `IMPORTS`. |
| `lineage_graph.json` | NetworkX node-link format. Nodes: datasets and transformations. Edges: `PRODUCES`, `CONSUMES`. Captures SQL table dependencies, dbt model lineage, Airflow task flows, and Python data I/O. |

Both files use the `"edges"` key (NetworkX 3.x format) for edge data.

### Programmatic use

```python
from src.graph import ModuleGraph, LineageGraph

mg = ModuleGraph.from_json(".cartography/module_graph.json")
lg = LineageGraph.from_json(".cartography/lineage_graph.json")

# Entry-point and terminal datasets
print(lg.find_sources())
print(lg.find_sinks())

# Blast radius: everything downstream of a node
print(lg.blast_radius("some_task_or_dataset"))
```

---

## Testing

Install dev dependencies first:

```bash
uv sync --extra dev
```

Run all tests:

```bash
uv run pytest tests/ -v
```

Run a specific test file:

```bash
uv run pytest tests/test_sql_lineage.py -v
```

Filter by keyword:

```bash
uv run pytest tests/ -k "cte"
```

### Test coverage

| File | Module tested |
|------|--------------|
| `tests/test_models_edges.py` | `src/models/edges.py`, `src/models/nodes.py` — edge types, node schemas, `edge_attrs()` |
| `tests/test_knowledge_graph.py` | `src/graph/knowledge_graph.py` — `ModuleGraph`, `LineageGraph`, blast radius, serialization |
| `tests/test_sql_lineage.py` | `src/analyzers/sql_lineage.py` — CTE resolution, JOIN parsing, multi-dialect, directory scanning |
| `tests/test_python_data_flow.py` | `src/analyzers/python_data_flow.py` — pandas, PySpark, SQLAlchemy read/write detection |
| `tests/test_dag_config_parser.py` | `src/analyzers/dag_config_parser.py` — dbt schema/project YAML, Airflow DAG task/dependency extraction |
| `tests/test_tree_sitter_analyzer.py` | `src/analyzers/tree_sitter_analyzer.py` — `LanguageRouter`, Python import/function/class extraction, `analyze_module` |

---

## Dependencies

All dependencies are locked in `uv.lock`. No LLM API keys or external services are required
for the Surveyor and Hydrologist agents.

| Package | Purpose |
|---------|---------|
| `tree-sitter` + grammars | AST parsing for Python, JS/TS, YAML |
| `sqlglot` | SQL parsing and table dependency extraction (20+ dialects) |
| `networkx` | Graph data structure, PageRank, SCC algorithms |
| `pydantic` | Schema validation for graph nodes and edges |
| `pyyaml` | YAML parsing for dbt and Airflow configs |
| `numpy` | Required by NetworkX for PageRank computation |
