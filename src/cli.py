"""CLI entry point: cartographer analyze <repo_path>."""

import argparse
import sys

from src.orchestrator import run


def main() -> int:
    """Entry point for the cartographer CLI."""
    parser = argparse.ArgumentParser(
        prog="cartographer",
        description="Brownfield Cartographer — map codebase architecture and data lineage.",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Run full analysis on a repository")
    analyze_parser.add_argument(
        "repo_path",
        help="Repository path (local directory or GitHub URL, e.g. https://github.com/apache/airflow)",
    )
    analyze_parser.add_argument(
        "-o",
        "--output",
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

    args = parser.parse_args()

    if args.command == "analyze":
        return _cmd_analyze(args)
    if args.command is None:
        parser.print_help()
        return 0
    return 1


def _cmd_analyze(args: argparse.Namespace) -> int:
    """Execute the analyze command."""
    from pathlib import Path

    clone_dir = Path(args.clone_dir) if args.clone_dir else None

    try:
        result = run(
            args.repo_path,
            output_dir=args.output,
            days=args.days,
            clone_dir=clone_dir,
            include_sql=not args.no_sql,
            include_dbt=not args.no_dbt,
            include_airflow=not args.no_airflow,
            include_python_flow=not args.no_python_flow,
        )
        mg = result["module_graph"]
        lg = result["lineage_graph"]
        print("Analysis complete.")
        print(f"  Repo:          {result['repo_path']}")
        print(f"  Output:        {result['output_dir']}")
        print(f"  Module graph:  {mg.graph.number_of_nodes()} nodes, {mg.graph.number_of_edges()} edges")
        print(f"  Lineage graph: {lg.graph.number_of_nodes()} nodes, {lg.graph.number_of_edges()} edges")
        return 0
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
