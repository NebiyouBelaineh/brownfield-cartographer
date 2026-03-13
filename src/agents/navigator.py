"""Navigator Agent: tool-calling query interface over the knowledge graph.

Implements four tools:
  find_implementation(concept)        — semantic search over purpose statements
  trace_lineage(dataset, direction)   — graph traversal (upstream/downstream)
  blast_radius(module_path)           — all downstream dependents of a module
  explain_module(path)                — LLM explanation of a specific module

Uses a lightweight tool-calling loop via litellm — compatible with Ollama
and any cloud provider that supports function/tool calling.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import networkx as nx

from src.graph import LineageGraph, ModuleGraph
from src.llm_config import LLMConfig, chat_completion, load_config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Embedding helpers
# ---------------------------------------------------------------------------

def _load_embeddings(output_dir: Path) -> dict[str, list[float]]:
    """Load cached embeddings from output_dir/embeddings.json."""
    embeddings_path = output_dir / "embeddings.json"
    if embeddings_path.exists():
        try:
            return json.loads(embeddings_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("[Navigator] Could not load embeddings: %s", exc)
    return {}


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity. Returns 0.0 on error or zero vectors."""
    try:
        import numpy as np
        va = np.array(a, dtype=float)
        vb = np.array(b, dtype=float)
        denom = float(np.linalg.norm(va) * np.linalg.norm(vb))
        if denom == 0.0:
            return 0.0
        return float(np.dot(va, vb) / denom)
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Tool implementations (pure graph / static analysis — no LLM required)
# ---------------------------------------------------------------------------

def find_implementation(
    concept: str,
    module_graph: ModuleGraph,
    *,
    top_n: int = 5,
    embeddings: dict[str, list[float]] | None = None,
    config: LLMConfig | None = None,
) -> list[dict[str, Any]]:
    """Find modules that implement a given concept.

    Uses vector similarity search over purpose-statement embeddings when available
    (requires embeddings dict and a config to embed the query). Falls back to
    keyword scoring over purpose statements when embeddings are unavailable.

    Returns up to top_n results with file path and evidence source.
    """
    G = module_graph.graph

    # --- Vector similarity path ---
    if embeddings:
        query_vec: list[float] | None = None
        if config is not None:
            try:
                import litellm  # noqa: PLC0415
                model = config.model
                if config.provider == "ollama" and not model.startswith("ollama/"):
                    model = f"ollama/{model}"
                resp = litellm.embedding(
                    model=model,
                    input=[concept],
                    api_base=config.base_url if config.provider == "ollama" else None,
                )
                data = resp.data if hasattr(resp, "data") else resp["data"]
                raw = data[0].embedding if hasattr(data[0], "embedding") else data[0]["embedding"]
                query_vec = [float(x) for x in raw]
            except Exception as exc:
                logger.warning("[Navigator] Query embedding failed, using keyword search: %s", exc)

        if query_vec is not None:
            sim_scores: list[tuple[float, str, dict[str, Any]]] = []
            for node in G.nodes():
                if G.nodes[node].get("node_type") != "module":
                    continue
                if node in embeddings:
                    sim = _cosine_similarity(query_vec, embeddings[node])
                    if sim > 0.0:
                        sim_scores.append((sim, node, G.nodes[node]))
            sim_scores.sort(key=lambda x: x[0], reverse=True)
            results = []
            for sim, path, attrs in sim_scores[:top_n]:
                results.append({
                    "path": path,
                    "score": round(sim, 4),
                    "purpose_statement": attrs.get("purpose_statement", "(no statement)"),
                    "domain": attrs.get("domain_cluster", "unknown"),
                    "language": attrs.get("language", "?"),
                    "evidence_source": "embedding_similarity",
                })
            if results:
                return results

    # --- Keyword scoring fallback ---
    concept_lower = concept.lower()
    keywords = [w for w in concept_lower.split() if len(w) >= 2]

    kw_scores: list[tuple[float, str, dict[str, Any]]] = []
    for node in G.nodes():
        if G.nodes[node].get("node_type") != "module":
            continue
        attrs = G.nodes[node]
        text = " ".join(filter(None, [
            attrs.get("purpose_statement", ""),
            node,
            attrs.get("domain_cluster", ""),
        ])).lower()

        score = sum(1.0 for kw in keywords if kw in text)
        if concept_lower in node.lower():
            score += 3.0
        if score > 0:
            kw_scores.append((score, node, attrs))

    kw_scores.sort(key=lambda x: x[0], reverse=True)
    results = []
    for score, path, attrs in kw_scores[:top_n]:
        results.append({
            "path": path,
            "score": score,
            "purpose_statement": attrs.get("purpose_statement", "(no statement)"),
            "domain": attrs.get("domain_cluster", "unknown"),
            "language": attrs.get("language", "?"),
            "evidence_source": "keyword_search",
        })
    return results


def trace_lineage(
    dataset: str,
    direction: str,
    lineage_graph: LineageGraph,
) -> dict[str, Any]:
    """Trace upstream or downstream lineage for a dataset.

    Args:
        dataset: Dataset/table name to trace.
        direction: "upstream" (ancestors) or "downstream" (descendants).

    Returns:
        Dict with nodes, edges, and path summary.
    """
    G = lineage_graph.graph
    direction = direction.lower().strip()

    if not G.has_node(dataset):
        # Fuzzy match
        matches = [n for n in G.nodes() if dataset.lower() in n.lower()]
        if matches:
            dataset = matches[0]
        else:
            return {
                "dataset": dataset,
                "direction": direction,
                "nodes": [],
                "edges": [],
                "error": f"Dataset '{dataset}' not found in lineage graph.",
            }

    if direction == "upstream":
        related = nx.ancestors(G, dataset)
    else:
        related = nx.descendants(G, dataset)

    # Build sub-graph
    subgraph_nodes = list(related | {dataset})
    edges = []
    for u, v, data in G.edges(data=True):
        if u in subgraph_nodes and v in subgraph_nodes:
            edges.append({
                "from": u,
                "to": v,
                "type": data.get("edge_type", "?"),
                "source_file": G.nodes.get(u, {}).get("source_file"),
                "line_range": data.get("line_range"),
            })

    node_details = []
    for n in subgraph_nodes:
        node_details.append({
            "name": n,
            "node_type": G.nodes[n].get("node_type", "?"),
            "source_file": G.nodes[n].get("source_file"),
            "transformation_type": G.nodes[n].get("transformation_type"),
        })

    return {
        "dataset": dataset,
        "direction": direction,
        "node_count": len(subgraph_nodes),
        "nodes": node_details,
        "edges": edges,
        "evidence_source": "static_analysis (graph traversal)",
    }


def blast_radius(
    module_path: str,
    module_graph: ModuleGraph,
    lineage_graph: LineageGraph,
) -> dict[str, Any]:
    """Return all downstream dependents of a module/dataset.

    Checks both the module import graph and the lineage graph.
    """
    G_mod = module_graph.graph
    G_lin = lineage_graph.graph

    module_deps: list[str] = []
    if G_mod.has_node(module_path):
        # Import graph edges go importer → imported (A → B means A depends on B).
        # Blast radius of B = everything that depends on B = ancestors of B.
        module_deps = list(nx.ancestors(G_mod, module_path))
    else:
        # Partial match in module graph
        matches = [n for n in G_mod.nodes() if module_path.lower() in n.lower()]
        if matches:
            module_path_resolved = matches[0]
            module_deps = list(nx.ancestors(G_mod, module_path_resolved))
        else:
            module_path_resolved = module_path

    lineage_deps: list[str] = []
    if G_lin.has_node(module_path):
        lineage_deps = list(nx.descendants(G_lin, module_path))

    return {
        "module": module_path,
        "module_graph_dependents": sorted(module_deps),
        "lineage_graph_dependents": sorted(lineage_deps),
        "total_impact": len(set(module_deps) | set(lineage_deps)),
        "evidence_source": "static_analysis (BFS from node)",
    }


def explain_module(
    path: str,
    repo_path: str | Path,
    module_graph: ModuleGraph,
    *,
    config: LLMConfig | None = None,
) -> dict[str, Any]:
    """LLM explanation of a specific module with file:line evidence."""
    G = module_graph.graph
    repo_path = Path(repo_path)

    # Find the module in the graph (exact or fuzzy)
    if not G.has_node(path):
        matches = [n for n in G.nodes() if path.lower() in n.lower()]
        if matches:
            path = matches[0]

    attrs = G.nodes.get(path, {})
    existing_purpose = attrs.get("purpose_statement")

    abs_path = repo_path / path
    try:
        code = abs_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        code = ""

    if existing_purpose:
        # Already have a purpose statement — augment with structural facts
        explanation = existing_purpose
        confidence = "high"
        source = "static_analysis + llm_inference"
    elif code and config:
        from src.agents.semanticist import generate_purpose_statement
        explanation = generate_purpose_statement(path, code, config=config)
        confidence = "medium"
        source = "llm_inference"
    else:
        explanation = f"No code found at {path} — cannot explain."
        confidence = "low"
        source = "static_analysis"

    # Structural facts from graph
    importers = list(G.predecessors(path)) if G.has_node(path) else []
    imports = list(G.successors(path)) if G.has_node(path) else []

    return {
        "path": path,
        "language": attrs.get("language", "?"),
        "domain": attrs.get("domain_cluster", "unknown"),
        "explanation": explanation,
        "imports": imports[:10],
        "imported_by": importers[:10],
        "complexity_score": attrs.get("complexity_score", 0),
        "change_velocity_30d": attrs.get("change_velocity_30d", 0),
        "is_dead_code_candidate": attrs.get("is_dead_code_candidate", False),
        "confidence": confidence,
        "evidence_source": source,
    }


# ---------------------------------------------------------------------------
# Tool definitions for LLM function-calling
# ---------------------------------------------------------------------------

_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "find_implementation",
            "description": "Find modules that implement a given concept or feature.",
            "parameters": {
                "type": "object",
                "properties": {
                    "concept": {
                        "type": "string",
                        "description": "The concept or feature to search for (e.g. 'revenue calculation', 'user auth').",
                    }
                },
                "required": ["concept"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "trace_lineage",
            "description": "Trace upstream or downstream data lineage for a dataset or table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "dataset": {
                        "type": "string",
                        "description": "Name of the dataset or table to trace.",
                    },
                    "direction": {
                        "type": "string",
                        "enum": ["upstream", "downstream"],
                        "description": "'upstream' to find sources, 'downstream' to find dependents.",
                    },
                },
                "required": ["dataset", "direction"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "blast_radius",
            "description": "Find all downstream dependents of a module — what breaks if it changes.",
            "parameters": {
                "type": "object",
                "properties": {
                    "module_path": {
                        "type": "string",
                        "description": "Relative path to the module (e.g. 'src/transforms/revenue.py').",
                    }
                },
                "required": ["module_path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "explain_module",
            "description": "Explain what a specific module does and how it fits in the system.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Relative path to the module.",
                    }
                },
                "required": ["path"],
            },
        },
    },
]


# ---------------------------------------------------------------------------
# Navigator agent — tool-calling loop
# ---------------------------------------------------------------------------

class Navigator:
    """Interactive query agent over the knowledge graph.

    Dispatches user questions to the four tools, then asks the LLM to
    synthesise a final answer with evidence citations.
    """

    def __init__(
        self,
        module_graph: ModuleGraph,
        lineage_graph: LineageGraph,
        repo_path: str | Path,
        *,
        config: LLMConfig | None = None,
        config_path: str | Path | None = None,
        output_dir: str | Path | None = None,
    ) -> None:
        self.module_graph = module_graph
        self.lineage_graph = lineage_graph
        self.repo_path = Path(repo_path)
        self.config = config or load_config(config_path)
        # Load pre-computed embeddings for vector similarity search
        self._embeddings: dict[str, list[float]] = (
            _load_embeddings(Path(output_dir)) if output_dir is not None else {}
        )

    def _dispatch_tool(self, name: str, args: dict[str, Any]) -> Any:
        if name == "find_implementation":
            return find_implementation(
                args["concept"],
                self.module_graph,
                embeddings=self._embeddings or None,
                config=self.config,
            )
        if name == "trace_lineage":
            return trace_lineage(args["dataset"], args["direction"], self.lineage_graph)
        if name == "blast_radius":
            return blast_radius(args["module_path"], self.module_graph, self.lineage_graph)
        if name == "explain_module":
            return explain_module(
                args["path"], self.repo_path, self.module_graph, config=self.config
            )
        return {"error": f"Unknown tool: {name}"}

    def query(self, user_question: str, *, max_tool_rounds: int = 3) -> str:
        """Process a user question, calling tools as needed, and return a final answer.

        Uses a tool-calling loop:
        1. Send question + tool definitions to LLM.
        2. If LLM requests tool calls, execute them and feed results back.
        3. Repeat up to max_tool_rounds.
        4. Return the final synthesised answer.

        Falls back to direct tool dispatch if the LLM doesn't support tool calling.
        """
        import litellm

        messages: list[dict[str, Any]] = [
            {
                "role": "system",
                "content": (
                    "You are the Navigator — an intelligent query interface for a codebase knowledge graph. "
                    "Answer questions about the codebase architecture and data lineage by calling the available tools. "
                    "Always cite evidence: include file paths, line ranges, or dataset names in your answers. "
                    "After calling tools, synthesise a clear, direct answer."
                ),
            },
            {"role": "user", "content": user_question},
        ]

        kw = {**self.config.litellm_kwargs}

        for _round in range(max_tool_rounds):
            try:
                response = litellm.completion(
                    messages=messages,
                    tools=_TOOLS,
                    tool_choice="auto",
                    **kw,
                )
            except Exception:
                # If tool calling unsupported (e.g. some Ollama models), fall back to
                # direct synthesis without tools
                return self._fallback_answer(user_question)

            msg = response.choices[0].message
            tool_calls = getattr(msg, "tool_calls", None) or []

            if not tool_calls:
                # Final answer
                return msg.content or "(no response)"

            # Execute tool calls
            messages.append(msg.model_dump() if hasattr(msg, "model_dump") else dict(msg))
            for tc in tool_calls:
                fn_name = tc.function.name
                try:
                    fn_args = json.loads(tc.function.arguments)
                except (json.JSONDecodeError, AttributeError):
                    fn_args = {}

                tool_result = self._dispatch_tool(fn_name, fn_args)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps(tool_result, default=str),
                })

        # If we exhausted tool rounds, ask for a final answer
        messages.append({
            "role": "user",
            "content": "Please synthesise a final answer based on the tool results above.",
        })
        try:
            response = litellm.completion(messages=messages, **kw)
            return response.choices[0].message.content or "(no response)"
        except Exception as exc:
            return f"[Navigator error: {exc}]"

    def _fallback_answer(self, question: str) -> str:
        """Answer without tool calling — do keyword-based tool dispatch then LLM synthesis."""
        question_lower = question.lower()

        tool_results: list[dict[str, Any]] = []

        # Heuristic routing based on question content
        if any(w in question_lower for w in ["where", "find", "implement", "logic", "code for", "which file"]):
            concept = question.replace("where is", "").replace("find", "").replace("?", "").strip()
            tool_results.append({
                "tool": "find_implementation",
                "result": find_implementation(
                    concept, self.module_graph,
                    embeddings=self._embeddings or None,
                    config=self.config,
                ),
            })

        if any(w in question_lower for w in ["upstream", "downstream", "lineage", "source", "feeds", "produces"]):
            # Try to extract a dataset name (simple heuristic: quoted strings or after "of"/"for")
            import re
            m = re.search(r'"([^"]+)"|\'([^\']+)\'|(?:of|for|table|dataset)\s+(\S+)', question, re.IGNORECASE)
            dataset = (m.group(1) or m.group(2) or m.group(3)).strip(".,") if m else question.split()[-1]
            direction = "upstream" if "upstream" in question_lower or "source" in question_lower else "downstream"
            tool_results.append({"tool": "trace_lineage", "result": trace_lineage(dataset, direction, self.lineage_graph)})

        if any(w in question_lower for w in ["blast radius", "break", "impact", "depend", "change"]):
            import re
            m = re.search(r'"([^"]+)"|\'([^\']+)\'|(?:module|file|path)\s+(\S+)', question, re.IGNORECASE)
            if m:
                path = (m.group(1) or m.group(2) or m.group(3)).strip(".,")
                tool_results.append({"tool": "blast_radius", "result": blast_radius(path, self.module_graph, self.lineage_graph)})

        if any(w in question_lower for w in ["explain", "what does", "describe", "tell me about"]):
            import re
            m = re.search(r'"([^"]+)"|\'([^\']+)\'|(?:module|file)\s+(\S+)', question, re.IGNORECASE)
            if m:
                path = (m.group(1) or m.group(2) or m.group(3)).strip(".,")
                tool_results.append({"tool": "explain_module", "result": explain_module(path, self.repo_path, self.module_graph, config=self.config)})

        context = json.dumps(tool_results, indent=2, default=str) if tool_results else "(no tool results)"
        messages = [
            {
                "role": "system",
                "content": (
                    "You are the Navigator for a codebase knowledge graph. "
                    "Answer the user's question using the tool results provided. "
                    "Always cite file paths and evidence."
                ),
            },
            {
                "role": "user",
                "content": f"Question: {question}\n\nTool results:\n{context}",
            },
        ]
        return chat_completion(messages, config=self.config, max_tokens=512)

    def interactive(self) -> None:
        """Start an interactive REPL for querying the knowledge graph."""
        print("\nBrownfield Cartographer — Navigator (type 'exit' to quit)\n")
        print("Ask questions about the codebase architecture or data lineage.\n")
        print("Examples:")
        print('  "Where is the revenue calculation logic?"')
        print('  "What upstream sources feed the daily_active_users table?"')
        print('  "What breaks if I change src/transforms/revenue.py?"')
        print('  "Explain what src/ingestion/kafka_consumer.py does"\n')

        while True:
            try:
                user_input = input("Navigator> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nExiting.")
                break

            if not user_input:
                continue
            if user_input.lower() in {"exit", "quit", "q"}:
                print("Exiting.")
                break

            print("\nThinking ...\n")
            answer = self.query(user_input)
            print(f"{answer}\n")
