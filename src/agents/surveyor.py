"""Surveyor Agent: static structure analysis, module graph, git velocity, dead code."""

import subprocess
from pathlib import Path
from typing import Any

import networkx as nx

from src.analyzers import LanguageRouter, extract_module_info
from src.graph import ModuleGraph


# File extensions to analyze
ANALYZABLE_EXTENSIONS = {".py", ".yaml", ".yml", ".js", ".jsx", ".ts", ".tsx"}


def _resolve_import_simple(
    repo_root: Path,
    current_file: Path,
    module_name: str,
    all_paths: set[str],
) -> str | None:
    """Resolve import to a path in all_paths. Returns None if external/unresolved."""
    repo_root = Path(repo_root).resolve()
    current_dir = Path(current_file).parent
    try:
        current_rel = current_dir.relative_to(repo_root)
    except ValueError:
        current_rel = Path(".")

    candidates: list[str] = []
    if module_name.startswith("."):
        dots = len(module_name) - len(module_name.lstrip("."))
        rest = module_name.lstrip(".").split(".") if module_name.strip(".") else []
        base_parts = list(current_rel.parts)[: max(0, len(current_rel.parts) - dots)]
        base = Path(*base_parts) if base_parts else Path(".")
        if rest:
            candidates.append(str((base / rest[0]).with_suffix(".py")).replace("\\", "/"))
            candidates.append(str(base / rest[0] / "__init__.py").replace("\\", "/"))
        else:
            candidates.append(str(base / "__init__.py").replace("\\", "/"))
    else:
        parts = module_name.split(".")
        candidates.append(f"{parts[0]}.py")
        candidates.append(f"{parts[0]}/__init__.py")
        if len(parts) > 1:
            candidates.append(f"{'/'.join(parts)}.py")
            candidates.append(f"{'/'.join(parts)}/__init__.py")

    for c in candidates:
        if c in all_paths:
            return c
    for p in all_paths:
        for c in candidates:
            if p == c or p.endswith("/" + c):
                return p
    return None


def extract_git_velocity(repo_path: str | Path, days: int = 30) -> dict[str, int]:
    """
    Compute change frequency per file using git log --follow.
    Returns dict mapping file path (relative to repo) to commit count.
    """
    repo_path = Path(repo_path)
    if not (repo_path / ".git").exists():
        return {}

    result: dict[str, int] = {}
    try:
        cmd = [
            "git",
            "-C",
            str(repo_path),
            "log",
            f"--since={days} days ago",
            "--name-only",
            "--pretty=format:",
        ]
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if out.returncode != 0:
            return {}

        for line in out.stdout.splitlines():
            line = line.strip()
            if line:
                result[line.replace("\\", "/")] = result.get(line.replace("\\", "/"), 0) + 1
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        pass

    return result


def survey(
    repo_path: str | Path,
    *,
    output_dir: str | Path = ".cartography",
    days: int = 30,
    exclude_dirs: set[str] | None = None,
) -> ModuleGraph:
    """
    Run the Surveyor: build module graph, PageRank, SCCs, dead code, git velocity.

    Returns the ModuleGraph and writes to output_dir/module_graph.json.
    """
    repo_path = Path(repo_path).resolve()
    output_dir = Path(output_dir)
    exclude_dirs = exclude_dirs or {".git", "__pycache__", "node_modules", ".venv", "venv"}

    mg = ModuleGraph()
    router = LanguageRouter()

    # Collect analyzable files
    all_paths: set[str] = set()
    files_to_analyze: list[Path] = []
    for ext in ANALYZABLE_EXTENSIONS:
        for p in repo_path.rglob(f"*{ext}"):
            if p.is_file() and not any(d in p.parts for d in exclude_dirs):
                try:
                    rel = p.relative_to(repo_path)
                    s = str(rel).replace("\\", "/")
                    all_paths.add(s)
                    files_to_analyze.append(p)
                except ValueError:
                    pass

    # Git velocity
    velocity = extract_git_velocity(repo_path, days=days)

    # Analyze each file and build graph
    for p in files_to_analyze:
        try:
            source = p.read_bytes()
        except OSError:
            continue

        rel = str(p.relative_to(repo_path)).replace("\\", "/")
        if not router.supports(p):
            continue

        info = extract_module_info(p, source, router)
        if info["module_node"] is None:
            continue

        # Add module node
        attrs: dict[str, Any] = {"change_velocity_30d": velocity.get(rel, 0)}
        mg.add_module(rel, info["language"], **attrs)

        # Add import edges (Python only for now - has structured imports)
        if info["language"] == "python":
            for mod, name in info["imports"]:
                target = _resolve_import_simple(repo_path, p, mod, all_paths)
                if target and target != rel:
                    mg.add_import(rel, target, weight=1)

    # PageRank for architectural hubs (requires numpy/scipy; skip if unavailable)
    G = mg.graph
    if G.number_of_nodes() > 0:
        try:
            pagerank = nx.pagerank(G, max_iter=100)
            for n, score in pagerank.items():
                if G.has_node(n):
                    G.nodes[n]["pagerank"] = round(score, 6)
        except (nx.PowerIterationFailedConvergence, ZeroDivisionError, ModuleNotFoundError):
            pass

    # Strongly connected components (circular dependencies)
    try:
        sccs = list(nx.strongly_connected_components(G))
        cycles = [list(c) for c in sccs if len(c) > 1]
        for n in G.nodes():
            G.nodes[n]["in_cycle"] = any(n in c for c in cycles)
    except nx.NetworkXError:
        pass

    # Dead code: modules with no incoming import edges (or only from themselves)
    for n in G.nodes():
        in_edges = list(G.predecessors(n))
        preds = [p for p in in_edges if p != n]
        is_dead = len(preds) == 0 and G.out_degree(n) > 0
        if G.has_node(n):
            G.nodes[n]["is_dead_code_candidate"] = is_dead

    # Serialize
    output_dir.mkdir(parents=True, exist_ok=True)
    mg.to_json(output_dir / "module_graph.json")

    return mg
