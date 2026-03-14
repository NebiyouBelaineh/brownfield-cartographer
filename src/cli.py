"""CLI entry point for the Brownfield Cartographer.

Subcommands:
  analyze  — run full analysis pipeline (Surveyor + Hydrologist + optional LLM stages)
  query    — interactive Navigator query mode over existing .cartography/ artifacts
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    """Entry point for the cartographer CLI."""
    from src.llm_config import _load_dotenv
    _load_dotenv()

    parser = argparse.ArgumentParser(
        prog="cartographer",
        description="Brownfield Cartographer — map codebase architecture and data lineage.",
    )
    parser.add_argument(
        "--config",
        default=None,
        metavar="FILE",
        help="Path to cartographer.toml (LLM provider config). Default: auto-detect.",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # ------------------------------------------------------------------ analyze
    analyze_parser = subparsers.add_parser(
        "analyze",
        help="Run full analysis on a repository (Surveyor + Hydrologist + optional LLM).",
    )
    analyze_parser.add_argument(
        "repo_path",
        help="Repository path (local directory or GitHub URL, e.g. https://github.com/dbt-labs/jaffle_shop)",
    )
    analyze_parser.add_argument(
        "-o", "--output",
        default=".cartography",
        help="Output directory for artifacts (default: .cartography)",
    )
    analyze_parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Days for git velocity analysis (default: 30)",
    )
    analyze_parser.add_argument(
        "--clone-dir",
        help="When repo_path is a GitHub URL, clone to this directory (default: temp dir)",
    )
    analyze_parser.add_argument(
        "--llm",
        action="store_true",
        help=(
            "Enable LLM stages: Semanticist (purpose statements, doc drift, domain clustering, "
            "Day-One Q&A) and Archivist (AI-generated CODEBASE.md overview). "
            "Requires a configured LLM provider in cartographer.toml or env vars."
        ),
    )
    analyze_parser.add_argument(
        "--incremental",
        action="store_true",
        help="Report files changed since the last run (still re-analyzes the full graph).",
    )
    analyze_parser.add_argument(
        "--no-sql",
        action="store_true",
        help="Disable SQL lineage analysis",
    )
    analyze_parser.add_argument(
        "--no-dbt",
        action="store_true",
        help="Disable dbt schema lineage analysis",
    )
    analyze_parser.add_argument(
        "--no-airflow",
        action="store_true",
        help="Disable Airflow DAG task dependency analysis",
    )
    analyze_parser.add_argument(
        "--no-python-flow",
        action="store_true",
        help="Disable Python pandas/PySpark data flow analysis",
    )

    # ------------------------------------------------------------------ query
    query_parser = subparsers.add_parser(
        "query",
        help="Interactive Navigator query mode over existing .cartography/ artifacts.",
    )
    query_parser.add_argument(
        "repo_path",
        help="Repository path (must match the path used during analyze).",
    )
    query_parser.add_argument(
        "-o", "--output",
        default=".cartography",
        help="Directory containing .cartography/ artifacts (default: .cartography)",
    )
    query_parser.add_argument(
        "-q", "--question",
        default=None,
        help="Single question to answer (non-interactive). If omitted, starts interactive REPL.",
    )

    args = parser.parse_args()

    if args.command == "analyze":
        return _cmd_analyze(args)
    if args.command == "query":
        return _cmd_query(args)
    if args.command is None:
        parser.print_help()
        return 0
    return 1


def _cmd_analyze(args: argparse.Namespace) -> int:
    """Execute the analyze command."""
    from src.orchestrator import run

    clone_dir = Path(args.clone_dir) if args.clone_dir else None
    config_path = Path(args.config) if args.config else None

    try:
        result = run(
            args.repo_path,
            output_dir=args.output,
            days=args.days,
            clone_dir=clone_dir,
            run_llm=args.llm,
            config_path=config_path,
            incremental=args.incremental,
            include_sql=not args.no_sql,
            include_dbt=not args.no_dbt,
            include_airflow=not args.no_airflow,
            include_python_flow=not args.no_python_flow,
        )
        mg = result["module_graph"]
        lg = result["lineage_graph"]
        changed = result.get("changed_files", [])

        print("Analysis complete.")
        print(f"  Repo:          {result['repo_path']}")
        print(f"  Output:        {result['output_dir']}")
        print(f"  Commit:        {result.get('commit') or '(not a git repo)'}")
        print(f"  Module graph:  {mg.graph.number_of_nodes()} nodes, {mg.graph.number_of_edges()} edges")
        print(f"  Lineage graph: {lg.graph.number_of_nodes()} nodes, {lg.graph.number_of_edges()} edges")

        if args.incremental and changed:
            print(f"  Changed files: {len(changed)}")
            for f in changed[:10]:
                print(f"    - {f}")
            if len(changed) > 10:
                print(f"    … and {len(changed) - 10} more")

        paths = result.get("archivist_paths", {})
        if paths.get("codebase_md_path"):
            print(f"  CODEBASE.md:   {paths['codebase_md_path']}")
        if paths.get("onboarding_brief_path"):
            print(f"  Onboarding:    {paths['onboarding_brief_path']}")
        if paths.get("trace_path"):
            print(f"  Trace log:     {paths['trace_path']}")

        semantic = result.get("semantic_results", {})
        if semantic:
            purposes = len(semantic.get("purpose_statements", {}))
            drifts = sum(1 for d in semantic.get("doc_drift", {}).values() if d.get("has_drift"))
            print(f"  Purpose stmts: {purposes}")
            print(f"  Doc drift:     {drifts} modules")

        return 0
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130


def _cmd_query(args: argparse.Namespace) -> int:
    """Execute the query command — interactive or single-question Navigator mode."""
    from src.agents.navigator import Navigator
    from src.graph import LineageGraph, ModuleGraph
    from src.orchestrator import repo_output_dir

    # Mirror the per-repo subdirectory logic used by `analyze`
    output_dir = repo_output_dir(args.output, args.repo_path)
    module_graph_path = output_dir / "module_graph.json"
    lineage_graph_path = output_dir / "lineage_graph.json"

    if not module_graph_path.exists():
        print(
            f"Error: {module_graph_path} not found. Run 'cartographer analyze {args.repo_path}' first.",
            file=sys.stderr,
        )
        return 1

    try:
        module_graph = ModuleGraph.from_json(module_graph_path)
    except Exception as e:
        print(f"Error loading module graph: {e}", file=sys.stderr)
        return 1

    if lineage_graph_path.exists():
        try:
            lineage_graph = LineageGraph.from_json(lineage_graph_path)
        except Exception:
            lineage_graph = LineageGraph()
    else:
        lineage_graph = LineageGraph()

    config_path = Path(args.config) if args.config else None

    navigator = Navigator(
        module_graph,
        lineage_graph,
        args.repo_path,
        config_path=config_path,
        output_dir=output_dir,
    )

    if args.question:
        answer = navigator.query(args.question)
        print(answer)
        return 0

    navigator.interactive()
    return 0


if __name__ == "__main__":
    sys.exit(main())
