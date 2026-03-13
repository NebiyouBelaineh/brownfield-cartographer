"""Tests for src/agents/archivist.py — CODEBASE.md, onboarding brief, trace logger."""

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from src.agents.archivist import (
    TraceLogger,
    archive,
    generate_codebase_md,
    generate_onboarding_brief,
    _top_modules_by_pagerank,
    _high_velocity_files,
    _circular_deps,
    _doc_drift_modules,
)
from src.graph import LineageGraph, ModuleGraph
from src.llm_config import LLMConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def ollama_config():
    return LLMConfig(provider="ollama", model="qwen2.5-coder:7b")


@pytest.fixture
def simple_graphs():
    mg = ModuleGraph()
    mg.add_module("src/ingest.py", "python", pagerank=0.4, change_velocity_30d=10)
    mg.add_module("src/transform.py", "python", pagerank=0.3, change_velocity_30d=5)
    mg.add_module("src/serve.py", "python", pagerank=0.1, change_velocity_30d=1)
    mg.add_import("src/transform.py", "src/ingest.py")
    mg.add_import("src/serve.py", "src/transform.py")

    lg = LineageGraph()
    lg.add_transformation("t1", "src/ingest.py", "python", source_datasets=["raw"], target_datasets=["clean"])
    lg.add_transformation("t2", "src/transform.py", "python", source_datasets=["clean"], target_datasets=["output"])

    return mg, lg


# ---------------------------------------------------------------------------
# TraceLogger
# ---------------------------------------------------------------------------

class TestTraceLogger:
    def test_creates_file_and_appends(self, tmp_path):
        log_path = tmp_path / "trace.jsonl"
        logger = TraceLogger(log_path)
        logger.log("Archivist", "test_action", details={"key": "value"})
        logger.log("Archivist", "second_action")

        lines = log_path.read_text().strip().split("\n")
        assert len(lines) == 2

        entry = json.loads(lines[0])
        assert entry["agent"] == "Archivist"
        assert entry["action"] == "test_action"
        assert entry["details"]["key"] == "value"
        assert "timestamp" in entry

    def test_default_fields(self, tmp_path):
        log_path = tmp_path / "trace.jsonl"
        logger = TraceLogger(log_path)
        logger.log("Agent", "action")

        entry = json.loads(log_path.read_text().strip())
        assert entry["confidence"] == "high"
        assert entry["evidence_source"] == "static_analysis"

    def test_creates_parent_dirs(self, tmp_path):
        log_path = tmp_path / "nested" / "deep" / "trace.jsonl"
        logger = TraceLogger(log_path)
        logger.log("X", "y")
        assert log_path.exists()


# ---------------------------------------------------------------------------
# Graph helper functions
# ---------------------------------------------------------------------------

class TestGraphHelpers:
    def test_top_modules_by_pagerank(self, simple_graphs):
        mg, _ = simple_graphs
        top = _top_modules_by_pagerank(mg, n=2)
        assert len(top) == 2
        assert top[0]["path"] == "src/ingest.py"

    def test_high_velocity_files(self, simple_graphs):
        mg, _ = simple_graphs
        hot = _high_velocity_files(mg, n=2)
        assert hot[0]["path"] == "src/ingest.py"
        assert hot[0]["commits"] == 10

    def test_high_velocity_excludes_zero(self, simple_graphs):
        mg, _ = simple_graphs
        mg.add_module("src/dead.py", "python", change_velocity_30d=0)
        hot = _high_velocity_files(mg)
        paths = [h["path"] for h in hot]
        assert "src/dead.py" not in paths

    def test_circular_deps_detected(self):
        mg = ModuleGraph()
        mg.add_module("a.py", "python")
        mg.add_module("b.py", "python")
        mg.add_import("a.py", "b.py")
        mg.add_import("b.py", "a.py")
        cycles = _circular_deps(mg)
        assert len(cycles) >= 1
        cycle_flat = [n for c in cycles for n in c]
        assert "a.py" in cycle_flat

    def test_doc_drift_modules(self):
        mg = ModuleGraph()
        mg.add_module("src/drift.py", "python")
        mg.graph.nodes["src/drift.py"]["doc_drift"] = "Docstring says CSV but code uses Parquet."
        drifts = _doc_drift_modules(mg)
        assert len(drifts) == 1
        assert drifts[0]["path"] == "src/drift.py"


# ---------------------------------------------------------------------------
# generate_codebase_md
# ---------------------------------------------------------------------------

class TestGenerateCodebaseMd:
    def test_contains_required_sections(self, simple_graphs, ollama_config):
        mg, lg = simple_graphs
        with patch("src.agents.archivist.chat_completion", return_value="This is the architecture overview."):
            md = generate_codebase_md(mg, lg, config=ollama_config)

        assert "# CODEBASE.md" in md
        assert "## Architecture Overview" in md
        assert "## Critical Path" in md
        assert "## Data Sources" in md
        assert "## Domain Architecture Map" in md
        assert "## Known Debt" in md
        assert "## High-Velocity Files" in md

    def test_contains_module_paths(self, simple_graphs, ollama_config):
        mg, lg = simple_graphs
        with patch("src.agents.archivist.chat_completion", return_value="Overview."):
            md = generate_codebase_md(mg, lg, config=ollama_config)
        assert "src/ingest.py" in md

    def test_llm_error_in_overview_is_included(self, simple_graphs, ollama_config):
        mg, lg = simple_graphs
        with patch("src.agents.archivist.chat_completion", side_effect=Exception("timeout")):
            md = generate_codebase_md(mg, lg, config=ollama_config)
        assert "[LLM error" in md

    def test_sources_and_sinks_listed(self, simple_graphs, ollama_config):
        mg, lg = simple_graphs
        with patch("src.agents.archivist.chat_completion", return_value="Overview."):
            md = generate_codebase_md(mg, lg, config=ollama_config)
        assert "raw" in md  # source dataset
        assert "output" in md  # sink dataset


# ---------------------------------------------------------------------------
# generate_onboarding_brief
# ---------------------------------------------------------------------------

class TestGenerateOnboardingBrief:
    def test_contains_five_questions(self, simple_graphs):
        mg, lg = simple_graphs
        answers = {
            "What is the primary data ingestion path?": "Via src/ingest.py",
            "What are the 3-5 most critical output datasets?": "output table",
            "What is the blast radius if the most critical module fails?": "Everything",
            "Where is the business logic concentrated?": "src/transform.py",
            "What has changed most frequently?": "src/ingest.py",
        }
        brief = generate_onboarding_brief(answers, mg, lg)
        assert "Q1:" in brief
        assert "Q5:" in brief

    def test_contains_module_paths(self, simple_graphs):
        mg, lg = simple_graphs
        brief = generate_onboarding_brief({"q": "a"}, mg, lg)
        assert "src/ingest.py" in brief

    def test_lists_sources_and_sinks(self, simple_graphs):
        mg, lg = simple_graphs
        brief = generate_onboarding_brief({}, mg, lg)
        assert "raw" in brief
        assert "output" in brief


# ---------------------------------------------------------------------------
# archive (full integration, mocked LLM)
# ---------------------------------------------------------------------------

class TestArchive:
    def test_writes_all_artifacts(self, simple_graphs, tmp_path, ollama_config):
        mg, lg = simple_graphs
        with patch("src.agents.archivist.chat_completion", return_value="Architecture overview."):
            paths = archive(mg, lg, output_dir=tmp_path, config=ollama_config)

        assert Path(paths["codebase_md_path"]).exists()
        assert Path(paths["onboarding_brief_path"]).exists()
        assert Path(paths["trace_path"]).exists()

    def test_codebase_md_has_content(self, simple_graphs, tmp_path, ollama_config):
        mg, lg = simple_graphs
        with patch("src.agents.archivist.chat_completion", return_value="Architecture overview."):
            paths = archive(mg, lg, output_dir=tmp_path, config=ollama_config)
        content = Path(paths["codebase_md_path"]).read_text()
        assert len(content) > 100

    def test_trace_log_has_entries(self, simple_graphs, tmp_path, ollama_config):
        mg, lg = simple_graphs
        with patch("src.agents.archivist.chat_completion", return_value="Overview."):
            paths = archive(mg, lg, output_dir=tmp_path, config=ollama_config)

        lines = Path(paths["trace_path"]).read_text().strip().split("\n")
        entries = [json.loads(l) for l in lines if l.strip()]
        actions = [e["action"] for e in entries]
        assert "generate_codebase_md" in actions
        assert "archive_complete" in actions

    def test_archive_with_semantic_results(self, simple_graphs, tmp_path, ollama_config):
        mg, lg = simple_graphs
        semantic = {
            "day_one_answers": {"Q1": "Via ingest.py", "Q2": "output table"},
            "purpose_statements": {},
        }
        with patch("src.agents.archivist.chat_completion", return_value="Overview."):
            paths = archive(mg, lg, semantic_results=semantic, output_dir=tmp_path, config=ollama_config)

        brief = Path(paths["onboarding_brief_path"]).read_text()
        assert "Via ingest.py" in brief

    def test_archive_no_llm_config(self, simple_graphs, tmp_path):
        mg, lg = simple_graphs
        # When config=None, archivist falls back but still tries LLM for overview
        with patch("src.agents.archivist.chat_completion", return_value="Fallback overview."):
            paths = archive(mg, lg, output_dir=tmp_path, config=None)
        assert Path(paths["codebase_md_path"]).exists()
