"""Semanticist Agent: LLM-powered purpose extraction, doc drift detection, domain clustering.

All LLM calls go through src.llm_config.chat_completion so the same code works
with Ollama (local) and any cloud provider (OpenAI, Anthropic, OpenRouter …).
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

from src.graph import LineageGraph, ModuleGraph
from src.llm_config import LLMConfig, TokenBudget, chat_completion, chat_completion_tiered, load_config

logger = logging.getLogger(__name__)

try:
    import numpy as np
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score
    _SKLEARN_AVAILABLE = True
except ImportError:
    _SKLEARN_AVAILABLE = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MAX_CODE_CHARS = 6_000  # truncate large files before sending to LLM


def _truncate_code(code: str, max_chars: int = _MAX_CODE_CHARS) -> str:
    if len(code) <= max_chars:
        return code
    half = max_chars // 2
    return code[:half] + "\n\n... [truncated for brevity] ...\n\n" + code[-half:]


def _read_file_safe(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _extract_docstring(code: str) -> str:
    """Extract the first module-level docstring from Python source."""
    match = re.match(r'\s*(?:\'\'\'(.*?)\'\'\'|"""(.*?)""")', code, re.DOTALL)
    if match:
        return (match.group(1) or match.group(2)).strip()
    return ""


# ---------------------------------------------------------------------------
# Purpose statement generation
# ---------------------------------------------------------------------------

def generate_purpose_statement(
    module_path: str,
    code: str,
    *,
    config: LLMConfig | None = None,
    budget: TokenBudget | None = None,
) -> str:
    """Generate a 2-3 sentence purpose statement for a module from its code.

    Grounds the statement in the implementation, not the docstring.
    Uses the cheap model tier (bulk operation).
    """
    code_snippet = _truncate_code(code)
    messages = [
        {
            "role": "system",
            "content": (
                "You are a senior software engineer analysing an unfamiliar codebase. "
                "Your task: given source code, write a concise 2-3 sentence purpose statement "
                "that describes WHAT the module does and WHY it exists in business terms. "
                "Do NOT reference the docstring or comments — derive the answer from the implementation. "
                "Be specific. Mention the key operations, data structures, or algorithms used."
            ),
        },
        {
            "role": "user",
            "content": (
                f"File: {module_path}\n\n"
                f"```\n{code_snippet}\n```\n\n"
                "Write the purpose statement (2-3 sentences, no bullet points, plain prose)."
            ),
        },
    ]
    try:
        return chat_completion(messages, config=config, budget=budget, max_tokens=256).strip()
    except Exception as exc:
        return f"[LLM error: {exc}]"


# ---------------------------------------------------------------------------
# Documentation drift detection
# ---------------------------------------------------------------------------

def detect_doc_drift(
    module_path: str,
    purpose_statement: str,
    docstring: str,
    *,
    config: LLMConfig | None = None,
    budget: TokenBudget | None = None,
) -> dict[str, Any]:
    """Compare generated purpose vs existing docstring. Flag contradictions.

    Returns:
        {
            "has_drift": bool,
            "drift_summary": str | None,   # None when no drift
            "confidence": "high" | "low",
        }
    """
    if not docstring:
        return {"has_drift": False, "drift_summary": None, "confidence": "high"}

    messages = [
        {
            "role": "system",
            "content": (
                "You compare a module's actual behaviour (derived from code analysis) "
                "against its documented description. "
                "Reply in this exact format:\n"
                "DRIFT: YES or NO\n"
                "SUMMARY: one sentence explaining the discrepancy (or 'None' if no drift)\n"
                "CONFIDENCE: HIGH or LOW"
            ),
        },
        {
            "role": "user",
            "content": (
                f"File: {module_path}\n\n"
                f"ACTUAL PURPOSE (derived from code):\n{purpose_statement}\n\n"
                f"DOCUMENTED PURPOSE (existing docstring):\n{docstring}\n\n"
                "Does the docstring contradict or significantly misrepresent the actual purpose?"
            ),
        },
    ]
    try:
        raw = chat_completion(messages, config=config, budget=budget, max_tokens=128).strip()
    except Exception as exc:
        return {"has_drift": False, "drift_summary": f"[LLM error: {exc}]", "confidence": "low"}

    has_drift = bool(re.search(r"DRIFT:\s*YES", raw, re.IGNORECASE))
    summary_match = re.search(r"SUMMARY:\s*(.+)", raw, re.IGNORECASE)
    conf_match = re.search(r"CONFIDENCE:\s*(HIGH|LOW)", raw, re.IGNORECASE)

    summary = summary_match.group(1).strip() if summary_match else None
    if summary and summary.lower() == "none":
        summary = None

    return {
        "has_drift": has_drift,
        "drift_summary": summary if has_drift else None,
        "confidence": (conf_match.group(1).lower() if conf_match else "low"),
    }


# ---------------------------------------------------------------------------
# Domain clustering
# ---------------------------------------------------------------------------

_DEFAULT_DOMAINS = [
    "ingestion",
    "transformation",
    "serving",
    "monitoring",
    "configuration",
    "utilities",
    "testing",
    "unknown",
]


def assign_domain(
    module_path: str,
    purpose_statement: str,
    *,
    domains: list[str] | None = None,
    config: LLMConfig | None = None,
    budget: TokenBudget | None = None,
) -> str:
    """Classify a module into one of the standard FDE domain buckets (LLM fallback)."""
    domain_list = domains or _DEFAULT_DOMAINS
    domain_str = ", ".join(domain_list)

    messages = [
        {
            "role": "system",
            "content": (
                f"Classify the module into exactly one of these domains: {domain_str}. "
                "Reply with ONLY the domain name, nothing else."
            ),
        },
        {
            "role": "user",
            "content": (
                f"File: {module_path}\n"
                f"Purpose: {purpose_statement}"
            ),
        },
    ]
    try:
        raw = chat_completion(messages, config=config, budget=budget, max_tokens=16).strip().lower()
        for d in domain_list:
            if d.lower() in raw:
                return d.lower()
        return "unknown"
    except Exception:
        return "unknown"


def cluster_into_domains(
    modules: list[dict[str, Any]],
    *,
    domains: list[str] | None = None,
    config: LLMConfig | None = None,
    budget: TokenBudget | None = None,
) -> dict[str, str]:
    """Assign every module to a domain bucket via per-module LLM classification (fallback).

    Prefer embed_and_cluster() for embedding-based clustering when sklearn is available.
    """
    result: dict[str, str] = {}
    for m in modules:
        path = m.get("path", "")
        purpose = m.get("purpose_statement") or ""
        if not purpose or purpose.startswith("[LLM error"):
            result[path] = "unknown"
        else:
            result[path] = assign_domain(path, purpose, domains=domains, config=config, budget=budget)
    return result


# ---------------------------------------------------------------------------
# Embedding-based clustering
# ---------------------------------------------------------------------------

def generate_embeddings(
    texts: list[str],
    *,
    config: LLMConfig,
    embedding_model: str | None = None,
) -> list[list[float]] | None:
    """Generate embeddings via litellm.embedding() (works with Ollama + cloud providers).

    Args:
        texts: List of strings to embed.
        config: LLM config (provider/base_url used for routing).
        embedding_model: Override model name (e.g. "nomic-embed-text" for Ollama).
                         Falls back to config.model if None.

    Returns:
        List of float vectors, or None if embedding fails.
    """
    import litellm  # noqa: PLC0415

    model = embedding_model or config.model
    if config.provider == "ollama" and not model.startswith("ollama/"):
        model = f"ollama/{model}"

    try:
        response = litellm.embedding(
            model=model,
            input=texts,
            api_base=config.base_url if config.provider == "ollama" else None,
        )
        data = response.data if hasattr(response, "data") else response["data"]
        return [
            [float(x) for x in (item.embedding if hasattr(item, "embedding") else item["embedding"])]
            for item in data
        ]
    except Exception as exc:
        logger.warning("[Semanticist] Embedding generation failed: %s", exc)
        return None


def _select_k(n_modules: int, embeddings: list[list[float]]) -> int:
    """Auto-select k for k-means using silhouette score, or fall back to formula."""
    default_k = min(8, max(2, n_modules // 5))
    if not _SKLEARN_AVAILABLE or n_modules < 4:
        return default_k
    X = np.array(embeddings)
    best_k, best_score = default_k, -1.0
    for k in range(2, min(default_k + 1, n_modules)):
        km = KMeans(n_clusters=k, random_state=42, n_init="auto")
        labels = km.fit_predict(X)
        if len(set(labels)) < 2:
            continue
        score = silhouette_score(X, labels)
        if score > best_score:
            best_score, best_k = score, k
    return best_k


def _label_cluster(
    purpose_samples: list[str],
    *,
    config: LLMConfig,
    budget: TokenBudget | None = None,
) -> str:
    """Ask the LLM (expensive tier) to give a human-readable label to a cluster."""
    sample_text = "\n".join(f"- {p}" for p in purpose_samples[:5])
    messages = [
        {
            "role": "system",
            "content": (
                "You name architectural domain clusters for a software engineering team. "
                "Given a list of module purpose statements that belong to the same cluster, "
                "reply with ONLY a single short domain label (2-4 words, lowercase, no punctuation). "
                "Examples: 'data ingestion', 'api serving', 'configuration management'."
            ),
        },
        {
            "role": "user",
            "content": f"Cluster members:\n{sample_text}\n\nDomain label:",
        },
    ]
    try:
        return chat_completion_tiered(
            messages, tier="expensive", config=config, budget=budget, max_tokens=16
        ).strip().lower()
    except Exception:
        return "unknown"


def embed_and_cluster(
    modules: list[dict[str, Any]],
    *,
    config: LLMConfig,
    output_dir: Path,
    embedding_model: str | None = None,
    budget: TokenBudget | None = None,
) -> dict[str, str]:
    """Cluster modules into domains using embeddings + k-means + LLM-labeled clusters.

    Falls back to cluster_into_domains() (per-module LLM classification) if
    sklearn is unavailable or embedding generation fails.

    Embeddings are cached to output_dir/embeddings.json for reuse across runs.

    Args:
        modules: List of {"path": str, "purpose_statement": str, ...} dicts.
        config: LLM config.
        output_dir: Per-repo .cartography directory for caching embeddings.
        embedding_model: Override embedding model (e.g. "nomic-embed-text").
        budget: TokenBudget accumulator.

    Returns:
        Dict mapping module path → domain label string.
    """
    if not _SKLEARN_AVAILABLE:
        logger.info("[Semanticist] sklearn unavailable — falling back to LLM classification.")
        return cluster_into_domains(modules, config=config, budget=budget)

    # Collect modules that have a usable purpose statement
    purpose_map: dict[str, str] = {
        m["path"]: m.get("purpose_statement", "")
        for m in modules
        if m.get("purpose_statement") and not m.get("purpose_statement", "").startswith("[LLM error")
    }

    if len(purpose_map) < 2:
        return cluster_into_domains(modules, config=config, budget=budget)

    paths = list(purpose_map.keys())
    texts = list(purpose_map.values())

    # Load cached embeddings
    embeddings_path = output_dir / "embeddings.json"
    cached: dict[str, list[float]] = {}
    if embeddings_path.exists():
        try:
            cached = json.loads(embeddings_path.read_text(encoding="utf-8"))
        except Exception:
            cached = {}

    missing_paths = [p for p in paths if p not in cached]
    missing_texts = [purpose_map[p] for p in missing_paths]

    if missing_texts:
        new_vecs = generate_embeddings(missing_texts, config=config, embedding_model=embedding_model)
        if new_vecs is None:
            logger.warning("[Semanticist] Embedding failed — falling back to LLM classification.")
            return cluster_into_domains(modules, config=config, budget=budget)
        for p, vec in zip(missing_paths, new_vecs):
            cached[p] = vec
        output_dir.mkdir(parents=True, exist_ok=True)
        embeddings_path.write_text(json.dumps(cached), encoding="utf-8")
        logger.info("[Semanticist] Embeddings cached to %s", embeddings_path)

    vecs = [cached[p] for p in paths]
    k = _select_k(len(paths), vecs)

    X = np.array(vecs)
    km = KMeans(n_clusters=k, random_state=42, n_init="auto")
    raw_labels = km.fit_predict(X)

    # Group paths by cluster id
    clusters: dict[int, list[str]] = {}
    for path, label in zip(paths, raw_labels):
        clusters.setdefault(int(label), []).append(path)

    # Generate a meaningful label for each cluster via LLM (expensive tier)
    centroids = km.cluster_centers_
    cluster_labels: dict[int, str] = {}
    for cluster_id, member_paths in clusters.items():
        centroid = centroids[cluster_id]
        member_vecs = np.array([cached[p] for p in member_paths])
        dists = np.linalg.norm(member_vecs - centroid, axis=1)
        sorted_members = [member_paths[i] for i in np.argsort(dists)][:5]
        purposes = [purpose_map[p] for p in sorted_members]
        cluster_labels[cluster_id] = _label_cluster(purposes, config=config, budget=budget)
        logger.info(
            "[Semanticist] Cluster %d (%d modules) → '%s'",
            cluster_id, len(member_paths), cluster_labels[cluster_id],
        )

    result: dict[str, str] = {}
    for path, cluster_id in zip(paths, raw_labels):
        result[path] = cluster_labels[int(cluster_id)]

    # Modules without a purpose statement default to "unknown"
    for m in modules:
        if m["path"] not in result:
            result[m["path"]] = "unknown"

    return result


# ---------------------------------------------------------------------------
# Day-One question answering
# ---------------------------------------------------------------------------

_DAY_ONE_QUESTIONS = [
    "What is the primary data ingestion path?",
    "What are the 3-5 most critical output datasets/endpoints?",
    "What is the blast radius if the most critical module fails?",
    "Where is the business logic concentrated vs distributed?",
    "What has changed most frequently in the last 90 days (high-velocity files)?",
]


def answer_day_one_questions(
    module_graph: ModuleGraph,
    lineage_graph: LineageGraph,
    *,
    config: LLMConfig | None = None,
    budget: TokenBudget | None = None,
    top_n_modules: int = 20,
) -> dict[str, str]:
    """Synthesise the Five FDE Day-One Answers from graph data.

    Returns dict mapping question → answer (with evidence citations).
    """
    G_mod = module_graph.graph
    G_lin = lineage_graph.graph

    # Build a compact architectural context from graph data
    # Top modules by PageRank (architectural hubs)
    pageranks = {
        n: G_mod.nodes[n].get("pagerank", 0.0)
        for n in G_mod.nodes()
        if G_mod.nodes[n].get("node_type") == "module"
    }
    top_modules = sorted(pageranks, key=lambda n: pageranks[n], reverse=True)[:top_n_modules]

    # High-velocity files
    velocity = {
        n: G_mod.nodes[n].get("change_velocity_30d", 0)
        for n in G_mod.nodes()
        if G_mod.nodes[n].get("node_type") == "module"
    }
    hot_files = sorted(velocity, key=lambda n: velocity[n], reverse=True)[:10]

    # Circular deps
    import networkx as nx
    sccs = [list(c) for c in nx.strongly_connected_components(G_mod) if len(c) > 1]
    circular_deps = sccs[:5]

    # Lineage sources and sinks
    sources = lineage_graph.find_sources()[:10]
    sinks = lineage_graph.find_sinks()[:10]

    # Module purpose index (for modules that have one)
    purpose_index = "\n".join(
        f"- {n}: {G_mod.nodes[n].get('purpose_statement', '(no purpose statement)')}"
        for n in top_modules
        if G_mod.nodes[n].get("purpose_statement")
    )

    context = f"""ARCHITECTURAL CONTEXT
====================
Top modules by PageRank (architectural hubs):
{chr(10).join(f'  {i+1}. {m} (pagerank={pageranks.get(m, 0):.4f})' for i, m in enumerate(top_modules))}

High-velocity files (most commits in last 30d):
{chr(10).join(f'  - {f} ({velocity.get(f, 0)} commits)' for f in hot_files)}

Circular dependencies (potential debt):
{chr(10).join(f'  - {c}' for c in circular_deps) or '  None detected'}

Data lineage sources (entry points, in-degree=0):
{chr(10).join(f'  - {s}' for s in sources) or '  None detected'}

Data lineage sinks (outputs, out-degree=0):
{chr(10).join(f'  - {s}' for s in sinks) or '  None detected'}

Module purpose index:
{purpose_index or '  (no purpose statements generated yet)'}
"""

    questions_block = "\n".join(f"{i+1}. {q}" for i, q in enumerate(_DAY_ONE_QUESTIONS))

    messages = [
        {
            "role": "system",
            "content": (
                "You are a senior forward-deployed engineer (FDE) who has just analysed "
                "an unfamiliar production codebase. Answer the five Day-One questions below "
                "using ONLY the provided architectural context as evidence. "
                "For each answer, cite at least one specific file path or dataset name. "
                "If the context does not contain enough information, say so explicitly."
            ),
        },
        {
            "role": "user",
            "content": (
                f"{context}\n\n"
                f"Please answer these five questions:\n{questions_block}\n\n"
                "Format: Q1: [answer]\nQ2: [answer]\n… etc."
            ),
        },
    ]
    try:
        raw = chat_completion_tiered(
            messages, tier="expensive", config=config, budget=budget, max_tokens=1024
        ).strip()
    except Exception as exc:
        raw = f"[LLM error: {exc}]"

    # Parse Q1..Q5 from the response
    answers: dict[str, str] = {}
    for i, question in enumerate(_DAY_ONE_QUESTIONS, start=1):
        pattern = rf"Q{i}[:\.]?\s*(.+?)(?=Q{i+1}[:\.]|$)"
        match = re.search(pattern, raw, re.DOTALL | re.IGNORECASE)
        answers[question] = match.group(1).strip() if match else raw
    return answers


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def analyse(
    repo_path: str | Path,
    module_graph: ModuleGraph,
    lineage_graph: LineageGraph,
    *,
    output_dir: str | Path = ".cartography",
    config: LLMConfig | None = None,
    config_path: str | Path | None = None,
    skip_purpose: bool = False,
    embedding_model: str | None = None,
) -> dict[str, Any]:
    """Run the full Semanticist pipeline.

    1. Generate purpose statements for all Python modules (cheap model tier).
    2. Detect documentation drift.
    3. Cluster modules into domains via embeddings + k-means + LLM-labeled clusters.
    4. Answer the Five FDE Day-One Questions (expensive model tier).

    Returns a dict with keys:
        purpose_statements: {path: str}
        doc_drift: {path: drift_dict}
        domain_clusters: {path: domain}
        day_one_answers: {question: answer}
        token_budget: {prompt_tokens, completion_tokens, total_tokens}
    """
    repo_path = Path(repo_path).resolve()
    output_dir = Path(output_dir)

    if config is None:
        config = load_config(config_path)

    budget = TokenBudget()

    G = module_graph.graph
    python_modules = [
        n for n in G.nodes()
        if G.nodes[n].get("node_type") == "module"
        and G.nodes[n].get("language") == "python"
    ]

    purpose_statements: dict[str, str] = {}
    doc_drift: dict[str, dict[str, Any]] = {}

    if not skip_purpose:
        total = len(python_modules)
        print(f"[Semanticist] Generating purpose statements for {total} Python modules ...", flush=True)
        for i, mod_path in enumerate(python_modules, start=1):
            abs_path = repo_path / mod_path
            code = _read_file_safe(abs_path)
            if not code.strip():
                continue

            purpose = generate_purpose_statement(mod_path, code, config=config, budget=budget)
            purpose_statements[mod_path] = purpose

            # Update the graph node in place
            if G.has_node(mod_path):
                G.nodes[mod_path]["purpose_statement"] = purpose

            # Doc drift
            docstring = _extract_docstring(code)
            drift = detect_doc_drift(mod_path, purpose, docstring, config=config, budget=budget)
            doc_drift[mod_path] = drift
            if drift["has_drift"]:
                if G.has_node(mod_path):
                    G.nodes[mod_path]["doc_drift"] = drift["drift_summary"]

            if i % 10 == 0:
                print(f"[Semanticist] {i}/{total} modules processed ...", flush=True)

    print("[Semanticist] Clustering modules into domains (embedding-based) ...", flush=True)
    modules_for_clustering = [
        {"path": p, "purpose_statement": purpose_statements.get(p) or G.nodes[p].get("purpose_statement", "")}
        for p in python_modules
    ]
    domain_clusters = embed_and_cluster(
        modules_for_clustering,
        config=config,
        output_dir=output_dir,
        embedding_model=embedding_model,
        budget=budget,
    )
    for mod_path, domain in domain_clusters.items():
        if G.has_node(mod_path):
            G.nodes[mod_path]["domain_cluster"] = domain

    print("[Semanticist] Answering Five FDE Day-One Questions (expensive tier) ...", flush=True)
    day_one_answers = answer_day_one_questions(
        module_graph, lineage_graph, config=config, budget=budget
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    print(
        f"[Semanticist] Done. Drift detected in "
        f"{sum(1 for d in doc_drift.values() if d['has_drift'])} modules. "
        f"Tokens used: {budget.total_tokens}",
        flush=True,
    )

    return {
        "purpose_statements": purpose_statements,
        "doc_drift": doc_drift,
        "domain_clusters": domain_clusters,
        "day_one_answers": day_one_answers,
        "token_budget": budget.as_dict(),
    }
