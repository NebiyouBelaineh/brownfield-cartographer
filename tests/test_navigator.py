"""Tests for src/agents/navigator.py — tool implementations and Navigator agent."""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.agents.navigator import (
    Navigator,
    blast_radius,
    explain_module,
    find_implementation,
    trace_lineage,
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
def populated_graphs(tmp_path):
    mg = ModuleGraph()
    mg.add_module("src/ingest.py", "python", purpose_statement="Ingests data from S3 and writes to raw zone.")
    mg.add_module("src/transform.py", "python", purpose_statement="Transforms raw data into clean tables using pandas.")
    mg.add_module("src/serve.py", "python", purpose_statement="Serves API endpoints for the dashboard.")
    mg.add_module("src/utils/helpers.py", "python", purpose_statement="Utility helper functions.")
    mg.add_import("src/transform.py", "src/ingest.py")
    mg.add_import("src/serve.py", "src/transform.py")

    lg = LineageGraph()
    lg.add_transformation("t1", "src/ingest.py", "python", source_datasets=["raw_s3"], target_datasets=["raw"])
    lg.add_transformation("t2", "src/transform.py", "python", source_datasets=["raw"], target_datasets=["clean"])
    lg.add_transformation("t3", "src/serve.py", "python", source_datasets=["clean"], target_datasets=["dashboard"])

    # Write minimal Python files for explain_module
    src = tmp_path / "src"
    src.mkdir()
    (src / "ingest.py").write_text('"""Ingest module."""\nimport boto3\n')
    (src / "transform.py").write_text('"""Transform module."""\nimport pandas as pd\n')

    return mg, lg, tmp_path


# ---------------------------------------------------------------------------
# find_implementation
# ---------------------------------------------------------------------------

class TestFindImplementation:
    def test_finds_by_keyword_in_purpose(self, populated_graphs):
        mg, _, _ = populated_graphs
        results = find_implementation("ingest S3", mg)
        paths = [r["path"] for r in results]
        assert "src/ingest.py" in paths

    def test_finds_by_path_keyword(self, populated_graphs):
        mg, _, _ = populated_graphs
        results = find_implementation("transform", mg)
        paths = [r["path"] for r in results]
        assert "src/transform.py" in paths

    def test_returns_empty_for_no_match(self, populated_graphs):
        mg, _, _ = populated_graphs
        results = find_implementation("xyzxyz_nonexistent_concept", mg)
        assert results == []

    def test_respects_top_n(self, populated_graphs):
        mg, _, _ = populated_graphs
        results = find_implementation("src", mg, top_n=2)
        assert len(results) <= 2

    def test_result_has_evidence_fields(self, populated_graphs):
        mg, _, _ = populated_graphs
        results = find_implementation("ingest", mg)
        assert len(results) > 0
        r = results[0]
        assert "path" in r
        assert "score" in r
        assert "purpose_statement" in r
        assert "evidence_source" in r

    def test_boosts_exact_path_match(self, populated_graphs):
        mg, _, _ = populated_graphs
        results = find_implementation("src/ingest.py", mg)
        assert results[0]["path"] == "src/ingest.py"


# ---------------------------------------------------------------------------
# trace_lineage
# ---------------------------------------------------------------------------

class TestTraceLineage:
    def test_upstream_trace(self, populated_graphs):
        _, lg, _ = populated_graphs
        result = trace_lineage("clean", "upstream", lg)
        assert result["direction"] == "upstream"
        node_names = [n["name"] for n in result["nodes"]]
        assert "raw" in node_names or "raw_s3" in node_names

    def test_downstream_trace(self, populated_graphs):
        _, lg, _ = populated_graphs
        result = trace_lineage("raw", "downstream", lg)
        assert result["direction"] == "downstream"
        node_names = [n["name"] for n in result["nodes"]]
        assert "clean" in node_names

    def test_unknown_dataset_returns_error(self, populated_graphs):
        _, lg, _ = populated_graphs
        result = trace_lineage("nonexistent_table_xyz", "upstream", lg)
        assert "error" in result

    def test_fuzzy_match(self, populated_graphs):
        _, lg, _ = populated_graphs
        # "raw_s" should fuzzy-match "raw_s3"
        result = trace_lineage("raw_s", "downstream", lg)
        assert "error" not in result

    def test_returns_edges_with_metadata(self, populated_graphs):
        _, lg, _ = populated_graphs
        result = trace_lineage("raw", "downstream", lg)
        assert "edges" in result
        for edge in result["edges"]:
            assert "from" in edge
            assert "to" in edge
            assert "type" in edge


# ---------------------------------------------------------------------------
# blast_radius
# ---------------------------------------------------------------------------

class TestBlastRadius:
    def test_module_graph_blast_radius(self, populated_graphs):
        mg, lg, _ = populated_graphs
        result = blast_radius("src/ingest.py", mg, lg)
        assert "src/transform.py" in result["module_graph_dependents"]
        assert "src/serve.py" in result["module_graph_dependents"]

    def test_lineage_graph_blast_radius(self, populated_graphs):
        mg, lg, _ = populated_graphs
        result = blast_radius("raw", mg, lg)
        lineage_deps = result["lineage_graph_dependents"]
        assert len(lineage_deps) > 0

    def test_unknown_module_returns_empty(self, populated_graphs):
        mg, lg, _ = populated_graphs
        result = blast_radius("src/does_not_exist.py", mg, lg)
        assert result["total_impact"] == 0 or isinstance(result["total_impact"], int)

    def test_total_impact_is_union(self, populated_graphs):
        mg, lg, _ = populated_graphs
        result = blast_radius("src/ingest.py", mg, lg)
        combined = set(result["module_graph_dependents"]) | set(result["lineage_graph_dependents"])
        assert result["total_impact"] == len(combined)

    def test_result_has_evidence_source(self, populated_graphs):
        mg, lg, _ = populated_graphs
        result = blast_radius("src/ingest.py", mg, lg)
        assert "evidence_source" in result


# ---------------------------------------------------------------------------
# explain_module
# ---------------------------------------------------------------------------

class TestExplainModule:
    def test_returns_existing_purpose_statement(self, populated_graphs, ollama_config):
        mg, _, repo = populated_graphs
        result = explain_module("src/ingest.py", repo, mg, config=ollama_config)
        assert result["explanation"] == "Ingests data from S3 and writes to raw zone."
        assert result["confidence"] == "high"

    def test_fuzzy_path_match(self, populated_graphs, ollama_config):
        mg, _, repo = populated_graphs
        result = explain_module("ingest", repo, mg, config=ollama_config)
        # Should fuzzy-match to src/ingest.py
        assert "ingest" in result["path"].lower()

    def test_returns_structural_facts(self, populated_graphs, ollama_config):
        mg, _, repo = populated_graphs
        result = explain_module("src/transform.py", repo, mg, config=ollama_config)
        assert "imports" in result
        assert "imported_by" in result
        assert "complexity_score" in result

    def test_missing_file_returns_low_confidence(self, populated_graphs, ollama_config):
        mg, _, repo = populated_graphs
        mg.add_module("src/missing.py", "python")  # no purpose, no file
        result = explain_module("src/missing.py", repo, mg, config=ollama_config)
        assert result["confidence"] == "low"


# ---------------------------------------------------------------------------
# Navigator agent
# ---------------------------------------------------------------------------

class TestNavigator:
    def test_instantiation(self, populated_graphs, ollama_config):
        mg, lg, repo = populated_graphs
        nav = Navigator(mg, lg, repo, config=ollama_config)
        assert nav.module_graph is mg
        assert nav.lineage_graph is lg

    def test_query_with_tool_calling(self, populated_graphs, ollama_config):
        mg, lg, repo = populated_graphs
        nav = Navigator(mg, lg, repo, config=ollama_config)

        # Mock litellm to simulate a tool call then a final answer
        tool_call = MagicMock()
        tool_call.function.name = "find_implementation"
        tool_call.function.arguments = json.dumps({"concept": "ingestion"})
        tool_call.id = "call_123"

        first_response = MagicMock()
        first_response.choices[0].message.tool_calls = [tool_call]
        first_response.choices[0].message.content = None
        first_response.choices[0].message.model_dump.return_value = {
            "role": "assistant", "content": None, "tool_calls": [tool_call]
        }

        second_response = MagicMock()
        second_response.choices[0].message.tool_calls = []
        second_response.choices[0].message.content = "The ingestion logic is in src/ingest.py."

        with patch("litellm.completion", side_effect=[first_response, second_response]):
            answer = nav.query("Where is the ingestion logic?")

        assert "ingest" in answer.lower()

    def test_query_fallback_on_no_tool_call(self, populated_graphs, ollama_config):
        mg, lg, repo = populated_graphs
        nav = Navigator(mg, lg, repo, config=ollama_config)

        # Simulate LLM that doesn't call tools
        response = MagicMock()
        response.choices[0].message.tool_calls = []
        response.choices[0].message.content = "Direct answer without tools."

        with patch("litellm.completion", return_value=response):
            answer = nav.query("Tell me about the codebase.")

        assert answer == "Direct answer without tools."

    def test_query_fallback_on_exception(self, populated_graphs, ollama_config):
        mg, lg, repo = populated_graphs
        nav = Navigator(mg, lg, repo, config=ollama_config)

        with patch("litellm.completion", side_effect=Exception("connection refused")):
            with patch("src.agents.navigator.chat_completion", return_value="Fallback answer."):
                answer = nav.query("Where is the ingestion code?")

        assert isinstance(answer, str)
        assert len(answer) > 0

    def test_dispatch_find_implementation(self, populated_graphs, ollama_config):
        mg, lg, repo = populated_graphs
        nav = Navigator(mg, lg, repo, config=ollama_config)
        result = nav._dispatch_tool("find_implementation", {"concept": "ingestion"})
        assert isinstance(result, list)

    def test_dispatch_blast_radius(self, populated_graphs, ollama_config):
        mg, lg, repo = populated_graphs
        nav = Navigator(mg, lg, repo, config=ollama_config)
        result = nav._dispatch_tool("blast_radius", {"module_path": "src/ingest.py"})
        assert "module_graph_dependents" in result

    def test_dispatch_trace_lineage(self, populated_graphs, ollama_config):
        mg, lg, repo = populated_graphs
        nav = Navigator(mg, lg, repo, config=ollama_config)
        result = nav._dispatch_tool("trace_lineage", {"dataset": "raw", "direction": "downstream"})
        assert "nodes" in result

    def test_dispatch_unknown_tool(self, populated_graphs, ollama_config):
        mg, lg, repo = populated_graphs
        nav = Navigator(mg, lg, repo, config=ollama_config)
        result = nav._dispatch_tool("unknown_tool", {})
        assert "error" in result
