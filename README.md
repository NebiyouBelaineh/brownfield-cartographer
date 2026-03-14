# brownfield-cartographer

A multi-agent codebase intelligence system that rapidly maps complex, undocumented codebases —
producing a queryable knowledge graph of architecture, data flows, and module structure.

Given a local path or GitHub URL, Brownfield Cartographer runs a four-agent pipeline:

- **Surveyor**: builds a module import graph, computes PageRank for architectural hubs, identifies
  circular dependencies (SCCs), dead code candidates, and git velocity per file.
- **Hydrologist**: traces data lineage from SQL files, dbt models (`schema.yml` + `ref()`
  relationships), Airflow DAG task dependencies (`>>`), and Python pandas/PySpark read-write
  patterns.
- **Semanticist** *(optional, requires LLM)*: generates per-module purpose statements, detects
  doc drift (docstring vs. actual code divergence), and clusters modules into architectural
  domains using KMeans on embeddings. Automatically escalates to a cloud model for large repos.
- **Archivist** *(optional, requires LLM)*: synthesizes results into living artifacts — a
  `CODEBASE.md` context file ready for AI agent injection, a `onboarding_brief.md` with FDE
  Day-One answers, and a `cartography_trace.jsonl` audit log.

Artifacts are written to `.cartography/<repo-name>/` as JSON and Markdown files ready for
inspection or programmatic querying.

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

### Analyze with LLM stages (Semanticist + Archivist)

```bash
uv run cartographer analyze /path/to/repo --llm
```

### Analyze a GitHub repository (clones automatically)

```bash
uv run cartographer analyze https://github.com/apache/airflow --llm
```

### Query the knowledge graph interactively

```bash
uv run cartographer query /path/to/repo
```

### Ask a single question (non-interactive)

```bash
uv run cartographer query /path/to/repo -q "What modules handle authentication?"
```

### Options

```
cartographer analyze <repo_path> [OPTIONS]

Arguments:
  repo_path            Local directory path or GitHub URL

Options:
  -o, --output DIR     Output directory for artifacts (default: .cartography)
  --config FILE        Path to cartographer.toml (LLM provider config)
  --days N             Days of git history for velocity analysis (default: 30)
  --clone-dir DIR      Directory to clone remote repos into (default: temp dir)
  --llm                Enable LLM stages: Semanticist + Archivist
  --incremental        Report files changed since the last run
  --no-sql             Skip SQL lineage analysis
  --no-dbt             Skip dbt schema.yml lineage analysis
  --no-airflow         Skip Airflow DAG task dependency analysis
  --no-python-flow     Skip Python pandas/PySpark data flow analysis

cartographer query <repo_path> [OPTIONS]

Options:
  -o, --output DIR     Directory containing .cartography/ artifacts (default: .cartography)
  -q, --question STR   Single question (non-interactive). Omit to start REPL.
  --config FILE        Path to cartographer.toml
```

### Example output

```
Analysis complete.
  Repo:          /home/user/airflow
  Output:        .cartography
  Commit:        a1b2c3d
  Module graph:  8017 nodes, 12242 edges
  Lineage graph: 430 nodes, 461 edges
  Purpose stmts: 512
  Doc drift:     23 modules
  CODEBASE.md:   .cartography/airflow/CODEBASE.md
  Onboarding:    .cartography/airflow/onboarding_brief.md
  Trace log:     .cartography/airflow/cartography_trace.jsonl
```

---

## LLM Configuration

LLM settings are read from `cartographer.toml` (auto-detected in the current directory or `~/.cartographer.toml`) or environment variables. API keys are always read from environment — never stored in config files.

### Ollama (local, default)

```toml
[llm]
provider = "ollama"
model = "qwen2.5-coder:7b"
base_url = "http://localhost:11434"
embedding_model = "nomic-embed-text"
```

### OpenAI

```toml
[llm]
provider = "openai"
model = "gpt-4o-mini"
# OPENAI_API_KEY read from environment
```

### Anthropic

```toml
[llm]
provider = "anthropic"
model = "claude-3-5-haiku-20241022"
# ANTHROPIC_API_KEY read from environment
```

### Tiered models (bulk vs. synthesis)

For cost-efficient analysis of large repos, configure a cheap model for per-module calls and an
expensive cloud model for high-quality synthesis:

```toml
[llm]
provider = "ollama"
model = "qwen2.5-coder:7b"
base_url = "http://localhost:11434"
cheap_model = "qwen2.5-coder:7b"
cloud_model = "claude-3-5-sonnet-20241022"   # escalated to for synthesis / large repos
large_repo_threshold = 100                    # Python module count above which cloud model is used
```

### Environment variable overrides

```
CARTOGRAPHER_LLM_PROVIDER
CARTOGRAPHER_LLM_MODEL
CARTOGRAPHER_LLM_BASE_URL
CARTOGRAPHER_LLM_CHEAP_MODEL
CARTOGRAPHER_LLM_CLOUD_MODEL
CARTOGRAPHER_EMBEDDING_MODEL
CARTOGRAPHER_LARGE_REPO_THRESHOLD
CARTOGRAPHER_CONFIG   # path to cartographer.toml
```

---

## Output artifacts

All artifacts are written to `<output>/<repo-name>/` (default: `.cartography/<repo-name>/`).

| File | Contents |
|------|----------|
| `module_graph.json` | NetworkX node-link format. Nodes: source files. Attributes: `language`, `pagerank`, `change_velocity_30d`, `in_cycle`, `is_dead_code_candidate`. Edges: `IMPORTS`. |
| `lineage_graph.json` | NetworkX node-link format. Nodes: datasets and transformations. Edges: `PRODUCES`, `CONSUMES`. Covers SQL, dbt, Airflow, and Python data I/O. |
| `embeddings.json` | Per-module semantic embeddings used by the Navigator for similarity search. |
| `last_run.json` | Metadata from the most recent analysis run (commit SHA, timestamps). |
| `CODEBASE.md` | LLM-generated architecture overview, ready for injection into AI coding agents. *(requires --llm)* |
| `onboarding_brief.md` | Five FDE Day-One answers with evidence from the codebase. *(requires --llm)* |
| `cartography_trace.jsonl` | Append-only JSONL audit log of every agent action (agent, action, confidence, evidence source). *(requires --llm)* |

Both graph files use the `"edges"` key (NetworkX 3.x format).

### Programmatic use

```python
from src.graph import ModuleGraph, LineageGraph

mg = ModuleGraph.from_json(".cartography/myrepo/module_graph.json")
lg = LineageGraph.from_json(".cartography/myrepo/lineage_graph.json")

# Entry-point and terminal datasets
print(lg.find_sources())
print(lg.find_sinks())

# Blast radius: everything downstream of a node
print(lg.blast_radius("some_task_or_dataset"))
```

---

## Navigator tools

The `query` command exposes four tools over the knowledge graph:

| Tool | Description |
|------|-------------|
| `find_implementation(concept)` | Semantic similarity search over module purpose statements |
| `trace_lineage(dataset, direction)` | Graph traversal upstream or downstream of a dataset |
| `blast_radius(module_path)` | All downstream dependents of a module |
| `explain_module(path)` | LLM-generated explanation of a specific module |

---

## Observability

Navigator tool calls are traced with [LangSmith](https://smith.langchain.com/) when
`LANGCHAIN_API_KEY` and `LANGCHAIN_TRACING_V2=true` are set in the environment.

```bash
export LANGCHAIN_API_KEY=your-key
export LANGCHAIN_TRACING_V2=true
export LANGCHAIN_PROJECT=brownfield-cartographer
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

All dependencies are locked in `uv.lock`.

| Package | Purpose |
|---------|---------|
| `tree-sitter` + grammars | AST parsing for Python, JS/TS, YAML |
| `sqlglot` | SQL parsing and table dependency extraction (20+ dialects) |
| `networkx` | Graph data structure, PageRank, SCC algorithms |
| `pydantic` | Schema validation for graph nodes and edges |
| `pyyaml` | YAML parsing for dbt and Airflow configs |
| `numpy` | Required by NetworkX for PageRank computation |
| `litellm` | Unified LLM provider interface (Ollama, OpenAI, Anthropic, OpenRouter, …) |
| `scikit-learn` | KMeans clustering for domain detection (Semanticist) |
| `langsmith` | LLM call tracing and observability (Navigator) |
