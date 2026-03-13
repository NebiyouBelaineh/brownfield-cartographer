"""Tests for src/orchestrator.py — full pipeline and incremental update mode."""

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.orchestrator import (
    _changed_files_since,
    _current_commit,
    _load_last_run,
    _repo_slug,
    _resolve_repo_path,
    _save_last_run,
    get_changed_files,
    repo_output_dir,
    run,
)


# ---------------------------------------------------------------------------
# _resolve_repo_path
# ---------------------------------------------------------------------------

class TestResolveRepoPath:
    def test_local_path_resolved(self, tmp_path):
        result = _resolve_repo_path(tmp_path)
        assert result == tmp_path.resolve()

    def test_invalid_local_path_still_resolves(self, tmp_path):
        # Resolution doesn't check existence — that happens in run()
        result = _resolve_repo_path(tmp_path / "nonexistent")
        assert isinstance(result, Path)

    def test_github_url_triggers_clone(self, tmp_path):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            # Pre-create the target dir so we skip the clone
            target = tmp_path / "owner-repo"
            target.mkdir()
            result = _resolve_repo_path("https://github.com/owner/repo", clone_dir=tmp_path)
        assert result == target.resolve()

    def test_invalid_url_raises(self):
        with pytest.raises(ValueError, match="Unsupported GitHub URL"):
            _resolve_repo_path("https://gitlab.com/owner/repo")

    def test_github_url_strips_git_suffix(self, tmp_path):
        target = tmp_path / "owner-repo"
        target.mkdir()
        result = _resolve_repo_path("https://github.com/owner/repo.git", clone_dir=tmp_path)
        assert result == target.resolve()


# ---------------------------------------------------------------------------
# Incremental helpers
# ---------------------------------------------------------------------------

class TestCurrentCommit:
    def test_returns_hash_for_git_repo(self, tmp_path):
        # Init a real git repo
        subprocess.run(["git", "init", str(tmp_path)], capture_output=True)
        subprocess.run(["git", "-C", str(tmp_path), "config", "user.email", "test@test.com"], capture_output=True)
        subprocess.run(["git", "-C", str(tmp_path), "config", "user.name", "Test"], capture_output=True)
        (tmp_path / "file.txt").write_text("hello")
        subprocess.run(["git", "-C", str(tmp_path), "add", "."], capture_output=True)
        subprocess.run(["git", "-C", str(tmp_path), "commit", "-m", "init"], capture_output=True)

        commit = _current_commit(tmp_path)
        assert commit is not None
        assert len(commit) == 40

    def test_returns_none_for_non_git_dir(self, tmp_path):
        commit = _current_commit(tmp_path)
        assert commit is None


class TestRepoSlug:
    def test_plain_name(self, tmp_path):
        repo = tmp_path / "jaffle_shop"
        repo.mkdir()
        assert _repo_slug(repo) == "jaffle_shop"

    def test_hyphenated_name(self, tmp_path):
        repo = tmp_path / "dbt-labs-jaffle_shop"
        repo.mkdir()
        assert _repo_slug(repo) == "dbt-labs-jaffle_shop"

    def test_repo_output_dir(self, tmp_path):
        repo = tmp_path / "my_repo"
        repo.mkdir()
        result = repo_output_dir(tmp_path / ".cartography", repo)
        assert result == tmp_path / ".cartography" / "my_repo"

    def test_two_repos_get_different_dirs(self, tmp_path):
        repo_a = tmp_path / "repo_a"
        repo_b = tmp_path / "repo_b"
        repo_a.mkdir()
        repo_b.mkdir()
        dir_a = repo_output_dir(tmp_path / ".cartography", repo_a)
        dir_b = repo_output_dir(tmp_path / ".cartography", repo_b)
        assert dir_a != dir_b


class TestChangedFilesSince:
    def test_returns_empty_list_on_error(self, tmp_path):
        # Non-git dir: subprocess will fail
        result = _changed_files_since(tmp_path, "abc123")
        assert result == []


class TestLastRunPersistence:
    def test_save_and_load(self, tmp_path):
        _save_last_run(tmp_path, "abc123", {"modules": 10})
        data = _load_last_run(tmp_path)
        assert data["commit"] == "abc123"
        assert data["modules"] == 10

    def test_load_returns_empty_if_missing(self, tmp_path):
        data = _load_last_run(tmp_path)
        assert data == {}

    def test_load_returns_empty_on_corrupt_file(self, tmp_path):
        (tmp_path / "last_run.json").write_text("not valid json {{}")
        data = _load_last_run(tmp_path)
        assert data == {}


class TestGetChangedFiles:
    def test_empty_when_no_previous_run(self, tmp_path):
        result = get_changed_files(tmp_path, tmp_path)
        assert result == []

    def test_empty_when_same_commit(self, tmp_path):
        _save_last_run(tmp_path, "abc123", {})
        with patch("src.orchestrator._current_commit", return_value="abc123"):
            result = get_changed_files(tmp_path, tmp_path)
        assert result == []

    def test_returns_changed_files_when_different_commit(self, tmp_path):
        _save_last_run(tmp_path, "old123", {})
        with patch("src.orchestrator._current_commit", return_value="new456"):
            with patch("src.orchestrator._changed_files_since", return_value=["src/foo.py", "src/bar.py"]):
                result = get_changed_files(tmp_path, tmp_path)
        assert "src/foo.py" in result
        assert "src/bar.py" in result


# ---------------------------------------------------------------------------
# run() — full pipeline
# ---------------------------------------------------------------------------

class TestRun:
    def _make_repo(self, tmp_path):
        """Create a minimal Python repo for analysis."""
        src = tmp_path / "src"
        src.mkdir()
        (src / "app.py").write_text("import os\n\ndef main(): pass\n")
        (src / "utils.py").write_text("def helper(): return 1\n")
        return tmp_path

    def test_run_returns_expected_keys(self, tmp_path):
        repo = self._make_repo(tmp_path)
        out = tmp_path / ".cartography"
        result = run(repo, output_dir=out, run_llm=False)
        assert "module_graph" in result
        assert "lineage_graph" in result
        assert "semantic_results" in result
        assert "archivist_paths" in result
        assert "repo_path" in result
        assert "output_dir" in result
        assert "changed_files" in result

    def test_run_writes_module_graph_json(self, tmp_path):
        repo = self._make_repo(tmp_path)
        out = tmp_path / ".cartography"
        with patch("src.agents.archivist.chat_completion", return_value="Overview."):
            run(repo, output_dir=out, run_llm=False)
        actual_out = repo_output_dir(out, repo)
        assert (actual_out / "module_graph.json").exists()

    def test_run_writes_lineage_graph_json(self, tmp_path):
        repo = self._make_repo(tmp_path)
        out = tmp_path / ".cartography"
        with patch("src.agents.archivist.chat_completion", return_value="Overview."):
            run(repo, output_dir=out, run_llm=False)
        actual_out = repo_output_dir(out, repo)
        assert (actual_out / "lineage_graph.json").exists()

    def test_run_writes_codebase_md(self, tmp_path):
        repo = self._make_repo(tmp_path)
        out = tmp_path / ".cartography"
        with patch("src.agents.archivist.chat_completion", return_value="Overview."):
            run(repo, output_dir=out, run_llm=False)
        actual_out = repo_output_dir(out, repo)
        assert (actual_out / "CODEBASE.md").exists()

    def test_run_writes_trace_log(self, tmp_path):
        repo = self._make_repo(tmp_path)
        out = tmp_path / ".cartography"
        with patch("src.agents.archivist.chat_completion", return_value="Overview."):
            run(repo, output_dir=out, run_llm=False)
        actual_out = repo_output_dir(out, repo)
        assert (actual_out / "cartography_trace.jsonl").exists()
        lines = (actual_out / "cartography_trace.jsonl").read_text().strip().split("\n")
        entries = [json.loads(l) for l in lines if l.strip()]
        assert len(entries) > 0

    def test_run_saves_last_run_json(self, tmp_path):
        repo = self._make_repo(tmp_path)
        out = tmp_path / ".cartography"
        with patch("src.agents.archivist.chat_completion", return_value="Overview."):
            run(repo, output_dir=out, run_llm=False)
        actual_out = repo_output_dir(out, repo)
        assert (actual_out / "last_run.json").exists()
        data = json.loads((actual_out / "last_run.json").read_text())
        assert "timestamp" in data

    def test_run_incremental_reports_changed_files(self, tmp_path):
        repo = self._make_repo(tmp_path)
        out = tmp_path / ".cartography"
        # Pre-seed last_run.json in the per-repo subdir
        actual_out = repo_output_dir(out, repo)
        _save_last_run(actual_out, "old_commit_abc", {})

        with (
            patch("src.orchestrator._current_commit", return_value="new_commit_xyz"),
            patch("src.orchestrator._changed_files_since", return_value=["src/app.py"]),
            patch("src.agents.archivist.chat_completion", return_value="Overview."),
        ):
            result = run(repo, output_dir=out, run_llm=False, incremental=True)

        assert "src/app.py" in result["changed_files"]

    def test_run_with_llm_calls_semanticist(self, tmp_path):
        repo = self._make_repo(tmp_path)
        out = tmp_path / ".cartography"

        with (
            patch("src.agents.semanticist.chat_completion", return_value="Purpose statement."),
            patch("src.agents.archivist.chat_completion", return_value="Overview."),
            patch("src.agents.semanticist.detect_doc_drift", return_value={"has_drift": False, "drift_summary": None, "confidence": "high"}),
            patch("src.agents.semanticist.assign_domain", return_value="serving"),
            patch("src.agents.semanticist.answer_day_one_questions", return_value={"q": "a"}),
        ):
            result = run(repo, output_dir=out, run_llm=True)

        assert result["semantic_results"] != {}

    def test_run_raises_for_nonexistent_repo(self, tmp_path):
        with pytest.raises(ValueError, match="does not exist"):
            run(tmp_path / "nonexistent_repo", output_dir=tmp_path / ".out", run_llm=False)
