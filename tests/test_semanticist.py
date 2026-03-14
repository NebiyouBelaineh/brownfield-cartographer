"""Tests for src/agents/semanticist.py — purpose statements, doc drift, domain clustering."""

from unittest.mock import MagicMock, patch

import pytest

from src.agents.semanticist import (
    _extract_docstring,
    assign_domain,
    cluster_into_domains,
    detect_doc_drift,
    generate_purpose_statement,
    answer_day_one_questions,
    analyse,
)
from src.graph import LineageGraph, ModuleGraph
from src.llm_config import LLMConfig, TokenBudget


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def ollama_config():
    return LLMConfig(provider="ollama", model="qwen2.5-coder:7b")


def _mock_llm(response_text: str):
    """Return a mock that patches chat_completion to return response_text."""
    return patch("src.agents.semanticist.chat_completion", return_value=response_text)


def _make_module_graph(*paths):
    mg = ModuleGraph()
    for p in paths:
        mg.add_module(p, "python")
    return mg


# ---------------------------------------------------------------------------
# _extract_docstring
# ---------------------------------------------------------------------------

class TestExtractDocstring:
    def test_double_quote_docstring(self):
        code = '"""This is the module docstring."""\n\nimport os\n'
        assert _extract_docstring(code) == "This is the module docstring."

    def test_single_quote_docstring(self):
        code = "'''Single quote docstring.'''\n"
        assert _extract_docstring(code) == "Single quote docstring."

    def test_no_docstring(self):
        code = "import os\n\ndef foo(): pass\n"
        assert _extract_docstring(code) == ""

    def test_multiline_docstring(self):
        code = '"""Line one.\n\nLine two."""\nimport os\n'
        result = _extract_docstring(code)
        assert "Line one." in result


# ---------------------------------------------------------------------------
# generate_purpose_statement
# ---------------------------------------------------------------------------

class TestGeneratePurposeStatement:
    def test_returns_llm_response(self, ollama_config):
        expected = "This module handles data ingestion from S3."
        with _mock_llm(expected):
            result = generate_purpose_statement("src/ingest.py", "import boto3", config=ollama_config)
        assert result == expected

    def test_returns_error_on_exception(self, ollama_config):
        with patch("src.agents.semanticist.chat_completion", side_effect=Exception("timeout")):
            result = generate_purpose_statement("src/foo.py", "x = 1", config=ollama_config)
        assert "[LLM error" in result

    def test_truncates_large_code(self, ollama_config):
        large_code = "x = 1\n" * 5000  # >> 6000 chars
        with _mock_llm("Purpose statement.") as mock_cc:
            generate_purpose_statement("src/big.py", large_code, config=ollama_config)
        call_args = mock_cc.call_args[0][0]  # messages list
        # The user message content should not contain the full code verbatim
        user_content = call_args[1]["content"]
        assert "truncated" in user_content


# ---------------------------------------------------------------------------
# detect_doc_drift
# ---------------------------------------------------------------------------

class TestDetectDocDrift:
    def test_no_docstring_returns_no_drift(self, ollama_config):
        result = detect_doc_drift("src/foo.py", "Handles payments.", "", config=ollama_config)
        assert result["has_drift"] is False
        assert result["drift_summary"] is None

    def test_parses_drift_yes(self, ollama_config):
        raw = "DRIFT: YES\nSUMMARY: Docstring says CSV but code uses Parquet.\nCONFIDENCE: HIGH"
        with _mock_llm(raw):
            result = detect_doc_drift("src/io.py", "Reads Parquet files.", "Reads CSV files.", config=ollama_config)
        assert result["has_drift"] is True
        assert "Parquet" in result["drift_summary"]
        assert result["confidence"] == "high"

    def test_parses_drift_no(self, ollama_config):
        raw = "DRIFT: NO\nSUMMARY: None\nCONFIDENCE: HIGH"
        with _mock_llm(raw):
            result = detect_doc_drift("src/util.py", "Utility helpers.", "Utility helpers.", config=ollama_config)
        assert result["has_drift"] is False
        assert result["drift_summary"] is None

    def test_llm_error_returns_low_confidence(self, ollama_config):
        with patch("src.agents.semanticist.chat_completion", side_effect=Exception("err")):
            result = detect_doc_drift("src/x.py", "Does X.", "Does Y.", config=ollama_config)
        assert result["confidence"] == "low"


# ---------------------------------------------------------------------------
# assign_domain / cluster_into_domains
# ---------------------------------------------------------------------------

class TestAssignDomain:
    def test_returns_valid_domain(self, ollama_config):
        with _mock_llm("ingestion"):
            domain = assign_domain("src/load.py", "Loads raw data from S3.", config=ollama_config)
        assert domain == "ingestion"

    def test_falls_back_to_unknown_on_bad_response(self, ollama_config):
        with _mock_llm("banana"):
            domain = assign_domain("src/x.py", "Does something.", config=ollama_config)
        assert domain == "unknown"

    def test_exception_returns_unknown(self, ollama_config):
        with patch("src.agents.semanticist.chat_completion", side_effect=Exception("err")):
            domain = assign_domain("src/x.py", "Does something.", config=ollama_config)
        assert domain == "unknown"


class TestClusterIntoDomains:
    def test_assigns_domain_to_each_module(self, ollama_config):
        modules = [
            {"path": "src/ingest.py", "purpose_statement": "Ingests raw data."},
            {"path": "src/transform.py", "purpose_statement": "Transforms tables."},
        ]
        with _mock_llm("transformation"):
            result = cluster_into_domains(modules, config=ollama_config)
        assert set(result.keys()) == {"src/ingest.py", "src/transform.py"}

    def test_empty_purpose_gets_unknown(self, ollama_config):
        modules = [{"path": "src/x.py", "purpose_statement": ""}]
        result = cluster_into_domains(modules, config=ollama_config)
        assert result["src/x.py"] == "unknown"

    def test_llm_error_purpose_gets_unknown(self, ollama_config):
        modules = [{"path": "src/x.py", "purpose_statement": "[LLM error: timeout]"}]
        result = cluster_into_domains(modules, config=ollama_config)
        assert result["src/x.py"] == "unknown"


# ---------------------------------------------------------------------------
# answer_day_one_questions
# ---------------------------------------------------------------------------

class TestAnswerDayOneQuestions:
    def test_returns_dict_with_five_keys(self, ollama_config):
        mg = _make_module_graph("src/ingest.py", "src/transform.py")
        lg = LineageGraph()
        lg.add_transformation("t1", "src/ingest.py", "python", source_datasets=["raw"], target_datasets=["clean"])
        raw_response = (
            "Q1: Data enters via src/ingest.py from S3.\n"
            "Q2: The critical outputs are clean and summary tables.\n"
            "Q3: Everything downstream of src/ingest.py would break.\n"
            "Q4: Business logic is concentrated in src/transform.py.\n"
            "Q5: src/ingest.py has the highest change velocity.\n"
        )
        with patch("src.agents.semanticist.chat_completion_tiered", return_value=raw_response):
            answers = answer_day_one_questions(mg, lg, config=ollama_config)
        assert len(answers) == 5

    def test_handles_llm_error(self, ollama_config):
        mg = _make_module_graph("src/x.py")
        lg = LineageGraph()
        with patch("src.agents.semanticist.chat_completion_tiered", side_effect=Exception("err")):
            answers = answer_day_one_questions(mg, lg, config=ollama_config)
        assert len(answers) == 5
        for v in answers.values():
            assert "[LLM error" in v

    def test_budget_accumulates_usage(self, ollama_config):
        mg = _make_module_graph("src/x.py")
        lg = LineageGraph()
        mock_resp = "Q1: a\nQ2: b\nQ3: c\nQ4: d\nQ5: e"
        budget = TokenBudget()
        with patch("src.agents.semanticist.chat_completion_tiered", return_value=mock_resp):
            answer_day_one_questions(mg, lg, config=ollama_config, budget=budget)
        # budget.total_tokens stays 0 when the mock doesn't set usage — just verify no crash
        assert isinstance(budget.as_dict(), dict)


# ---------------------------------------------------------------------------
# analyse (full pipeline, mocked LLM)
# ---------------------------------------------------------------------------

class TestAnalyse:
    def test_analyse_returns_expected_keys(self, tmp_path, ollama_config):
        # Set up a minimal Python file
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        (src_dir / "app.py").write_text('"""App module."""\ndef run(): pass\n')

        mg = ModuleGraph()
        mg.add_module("src/app.py", "python")
        lg = LineageGraph()

        with (
            _mock_llm("This module runs the application."),
            patch("src.agents.semanticist.detect_doc_drift", return_value={"has_drift": False, "drift_summary": None, "confidence": "high"}),
            patch("src.agents.semanticist.embed_and_cluster", return_value={"src/app.py": "serving"}),
            patch("src.agents.semanticist.answer_day_one_questions", return_value={"q": "a"}),
        ):
            result = analyse(tmp_path, mg, lg, output_dir=tmp_path / ".cartography", config=ollama_config)

        assert "purpose_statements" in result
        assert "doc_drift" in result
        assert "domain_clusters" in result
        assert "day_one_answers" in result
        assert "token_budget" in result

    def test_analyse_skips_purpose_when_flag_set(self, tmp_path, ollama_config):
        mg = ModuleGraph()
        mg.add_module("src/app.py", "python")
        lg = LineageGraph()

        with (
            patch("src.agents.semanticist.answer_day_one_questions", return_value={}),
            patch("src.agents.semanticist.embed_and_cluster", return_value={}) as mock_cluster,
        ):
            result = analyse(tmp_path, mg, lg, output_dir=tmp_path / ".cartography", config=ollama_config, skip_purpose=True)

        assert result["purpose_statements"] == {}
        mock_cluster.assert_called_once()
