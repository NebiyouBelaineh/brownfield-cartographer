"""Orchestrator: wires Surveyor and Hydrologist in sequence, serializes to .cartography/."""

import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from src.agents import hydrologist_survey, surveyor_survey


_GITHUB_URL_PATTERN = re.compile(
    r"^https?://(?:www\.)?github\.com/([^/]+)/([^/]+?)(?:\.git)?/?$",
    re.IGNORECASE,
)


def _resolve_repo_path(repo_path: str | Path, clone_dir: Path | None = None) -> Path:
    """
    Resolve repo path. If GitHub URL, clone and return clone path.
    Returns the path to the repository root.
    """
    repo_path = str(repo_path).strip()
    if repo_path.startswith(("http://", "https://")):
        match = _GITHUB_URL_PATTERN.match(repo_path)
        if match:
            owner, repo = match.group(1), match.group(2)
            if repo.endswith(".git"):
                repo = repo[:-4]
            target = clone_dir or Path(tempfile.mkdtemp(prefix="cartographer-"))
            target = target / f"{owner}-{repo}"
            if target.exists():
                # Already cloned; could pull, but for analysis we use as-is
                return target.resolve()
            try:
                subprocess.run(
                    ["git", "clone", "--depth", "1", repo_path, str(target)],
                    capture_output=True,
                    check=True,
                    timeout=300,
                )
                return target.resolve()
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError) as e:
                raise ValueError(f"Failed to clone {repo_path}: {e}") from e
        raise ValueError(f"Unsupported GitHub URL format: {repo_path}")
    return Path(repo_path).resolve()


def run(
    repo_path: str | Path,
    *,
    output_dir: str | Path = ".cartography",
    days: int = 30,
    clone_dir: Path | None = None,
    **hydrologist_kw: Any,
) -> dict[str, Any]:
    """
    Run the full analysis pipeline: Surveyor → Hydrologist.

    Args:
        repo_path: Local path or GitHub URL (e.g. https://github.com/apache/airflow)
        output_dir: Where to write .cartography/ artifacts (default: .cartography)
        days: Days for git velocity (default: 30)
        clone_dir: If repo_path is GitHub URL, clone to this dir. Default: temp dir.
        **hydrologist_kw: Passed to hydrologist_survey (include_sql, include_dbt, etc.)

    Returns:
        Dict with module_graph, lineage_graph, repo_path, output_dir.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    resolved = _resolve_repo_path(repo_path, clone_dir)
    if not resolved.exists() or not resolved.is_dir():
        raise ValueError(f"Repository path does not exist or is not a directory: {resolved}")

    # Surveyor first (module graph)
    module_graph = surveyor_survey(resolved, output_dir=output_dir, days=days)

    # Hydrologist second (lineage graph)
    lineage_graph = hydrologist_survey(
        resolved,
        output_dir=output_dir,
        **{k: v for k, v in hydrologist_kw.items() if k in {"include_sql", "include_dbt", "include_airflow", "include_python_flow"}},
    )

    return {
        "module_graph": module_graph,
        "lineage_graph": lineage_graph,
        "repo_path": str(resolved),
        "output_dir": str(output_dir),
    }
