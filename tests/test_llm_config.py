"""Tests for src/llm_config.py — LLM provider configuration."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.llm_config import LLMConfig, TokenBudget, load_config, chat_completion, chat_completion_tiered


class TestLLMConfig:
    def test_defaults(self):
        cfg = LLMConfig()
        assert cfg.provider == "ollama"
        assert cfg.model == "qwen2.5-coder:7b"
        assert cfg.base_url == "http://localhost:11434"

    def test_litellm_model_ollama_prefix(self):
        cfg = LLMConfig(provider="ollama", model="qwen2.5-coder:7b")
        assert cfg.litellm_model == "ollama/qwen2.5-coder:7b"

    def test_litellm_model_ollama_already_prefixed(self):
        cfg = LLMConfig(provider="ollama", model="ollama/qwen2.5-coder:7b")
        assert cfg.litellm_model == "ollama/qwen2.5-coder:7b"

    def test_litellm_model_openai(self):
        cfg = LLMConfig(provider="openai", model="gpt-4o-mini")
        assert cfg.litellm_model == "gpt-4o-mini"

    def test_litellm_model_anthropic(self):
        cfg = LLMConfig(provider="anthropic", model="claude-3-5-haiku-20241022")
        assert cfg.litellm_model == "claude-3-5-haiku-20241022"

    def test_litellm_kwargs_ollama(self):
        cfg = LLMConfig(provider="ollama", model="qwen2.5-coder:7b", base_url="http://localhost:11434")
        kw = cfg.litellm_kwargs
        assert kw["model"] == "ollama/qwen2.5-coder:7b"
        assert kw["api_base"] == "http://localhost:11434"

    def test_litellm_kwargs_openai_no_base_url(self):
        cfg = LLMConfig(provider="openai", model="gpt-4o-mini")
        kw = cfg.litellm_kwargs
        assert kw["model"] == "gpt-4o-mini"
        assert "api_base" not in kw

    def test_litellm_kwargs_includes_extra(self):
        cfg = LLMConfig(provider="ollama", model="llama3", extra={"temperature": 0.1})
        kw = cfg.litellm_kwargs
        assert kw["temperature"] == 0.1


class TestLoadConfig:
    def test_defaults_when_no_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("CARTOGRAPHER_CONFIG", raising=False)
        monkeypatch.delenv("CARTOGRAPHER_LLM_PROVIDER", raising=False)
        monkeypatch.delenv("CARTOGRAPHER_LLM_MODEL", raising=False)
        monkeypatch.delenv("CARTOGRAPHER_LLM_BASE_URL", raising=False)
        cfg = load_config()
        assert cfg.provider == "ollama"
        assert cfg.model == "qwen2.5-coder:7b"

    def test_load_from_toml_file(self, tmp_path):
        toml_content = b'[llm]\nprovider = "openai"\nmodel = "gpt-4o-mini"\n'
        config_file = tmp_path / "cartographer.toml"
        config_file.write_bytes(toml_content)
        cfg = load_config(config_path=config_file)
        assert cfg.provider == "openai"
        assert cfg.model == "gpt-4o-mini"

    def test_load_base_url_from_toml(self, tmp_path):
        toml_content = b'[llm]\nprovider = "ollama"\nmodel = "llama3"\nbase_url = "http://remote:11434"\n'
        config_file = tmp_path / "cartographer.toml"
        config_file.write_bytes(toml_content)
        cfg = load_config(config_path=config_file)
        assert cfg.base_url == "http://remote:11434"

    def test_env_var_overrides_toml(self, tmp_path, monkeypatch):
        toml_content = b'[llm]\nprovider = "ollama"\nmodel = "qwen2.5-coder:7b"\n'
        config_file = tmp_path / "cartographer.toml"
        config_file.write_bytes(toml_content)
        monkeypatch.setenv("CARTOGRAPHER_LLM_PROVIDER", "openai")
        monkeypatch.setenv("CARTOGRAPHER_LLM_MODEL", "gpt-4o")
        cfg = load_config(config_path=config_file)
        assert cfg.provider == "openai"
        assert cfg.model == "gpt-4o"

    def test_env_var_base_url_override(self, tmp_path, monkeypatch):
        toml_content = b'[llm]\nprovider = "ollama"\nmodel = "llama3"\n'
        config_file = tmp_path / "cartographer.toml"
        config_file.write_bytes(toml_content)
        monkeypatch.setenv("CARTOGRAPHER_LLM_BASE_URL", "http://gpu-server:11434")
        cfg = load_config(config_path=config_file)
        assert cfg.base_url == "http://gpu-server:11434"

    def test_extra_keys_forwarded(self, tmp_path):
        toml_content = b'[llm]\nprovider = "ollama"\nmodel = "qwen2.5-coder:7b"\ntemperature = 0.3\n'
        config_file = tmp_path / "cartographer.toml"
        config_file.write_bytes(toml_content)
        cfg = load_config(config_path=config_file)
        assert cfg.extra.get("temperature") == pytest.approx(0.3)

    def test_cartographer_config_env_var(self, tmp_path, monkeypatch):
        toml_content = b'[llm]\nprovider = "anthropic"\nmodel = "claude-3-5-haiku-20241022"\n'
        config_file = tmp_path / "custom.toml"
        config_file.write_bytes(toml_content)
        monkeypatch.setenv("CARTOGRAPHER_CONFIG", str(config_file))
        cfg = load_config()
        assert cfg.provider == "anthropic"


class TestChatCompletion:
    def test_calls_litellm_completion(self):
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "Hello from LLM"

        with patch("litellm.completion", return_value=mock_response) as mock_completion:
            cfg = LLMConfig(provider="ollama", model="qwen2.5-coder:7b")
            result = chat_completion([{"role": "user", "content": "Hi"}], config=cfg)

        assert result == "Hello from LLM"
        mock_completion.assert_called_once()
        call_kwargs = mock_completion.call_args[1]
        assert call_kwargs["model"] == "ollama/qwen2.5-coder:7b"
        assert call_kwargs["api_base"] == "http://localhost:11434"

    def test_passes_override_kwargs(self):
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "ok"

        with patch("litellm.completion", return_value=mock_response) as mock_completion:
            cfg = LLMConfig(provider="ollama", model="llama3")
            chat_completion([{"role": "user", "content": "test"}], config=cfg, max_tokens=128)

        call_kwargs = mock_completion.call_args[1]
        assert call_kwargs.get("max_tokens") == 128

    def test_budget_records_usage(self):
        mock_usage = MagicMock()
        mock_usage.prompt_tokens = 10
        mock_usage.completion_tokens = 20
        mock_usage.total_tokens = 30

        mock_response = MagicMock()
        mock_response.choices[0].message.content = "result"
        mock_response.usage = mock_usage

        budget = TokenBudget()
        with patch("litellm.completion", return_value=mock_response):
            cfg = LLMConfig(provider="ollama", model="qwen2.5-coder:7b")
            chat_completion([{"role": "user", "content": "hi"}], config=cfg, budget=budget)

        assert budget.prompt_tokens == 10
        assert budget.completion_tokens == 20
        assert budget.total_tokens == 30


class TestTokenBudget:
    def test_defaults_zero(self):
        b = TokenBudget()
        assert b.prompt_tokens == 0
        assert b.completion_tokens == 0
        assert b.total_tokens == 0

    def test_record_accumulates(self):
        b = TokenBudget()
        usage1 = MagicMock(prompt_tokens=5, completion_tokens=10, total_tokens=15)
        usage2 = MagicMock(prompt_tokens=3, completion_tokens=7, total_tokens=10)
        b.record(usage1)
        b.record(usage2)
        assert b.prompt_tokens == 8
        assert b.completion_tokens == 17
        assert b.total_tokens == 25

    def test_record_none_is_safe(self):
        b = TokenBudget()
        b.record(None)
        assert b.total_tokens == 0

    def test_as_dict(self):
        b = TokenBudget(prompt_tokens=1, completion_tokens=2, total_tokens=3)
        d = b.as_dict()
        assert d == {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}


class TestLLMConfigTiering:
    def test_cheap_model_defaults_to_primary(self):
        cfg = LLMConfig(provider="ollama", model="qwen2.5-coder:7b")
        assert cfg.cheap_litellm_model == "ollama/qwen2.5-coder:7b"

    def test_cloud_model_defaults_to_primary(self):
        cfg = LLMConfig(provider="ollama", model="qwen2.5-coder:7b")
        assert cfg.expensive_litellm_model == "ollama/qwen2.5-coder:7b"

    def test_cheap_model_override(self):
        cfg = LLMConfig(provider="ollama", model="llama3", cheap_model="qwen2.5-coder:3b")
        assert cfg.cheap_litellm_model == "ollama/qwen2.5-coder:3b"
        assert cfg.expensive_litellm_model == "ollama/llama3"

    def test_tiered_call_uses_cheap_model(self):
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "cheap answer"
        cfg = LLMConfig(provider="ollama", model="big-model", cheap_model="small-model")

        with patch("litellm.completion", return_value=mock_response) as mock_completion:
            result = chat_completion_tiered(
                [{"role": "user", "content": "hi"}], tier="cheap", config=cfg
            )

        assert result == "cheap answer"
        call_kwargs = mock_completion.call_args[1]
        assert call_kwargs["model"] == "ollama/small-model"

    def test_tiered_call_uses_cloud_model(self):
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "cloud answer"
        cfg = LLMConfig(provider="ollama", model="small-model", cloud_model="big-model")

        with patch("litellm.completion", return_value=mock_response) as mock_completion:
            result = chat_completion_tiered(
                [{"role": "user", "content": "hi"}], tier="expensive", config=cfg
            )

        assert result == "cloud answer"
        call_kwargs = mock_completion.call_args[1]
        assert call_kwargs["model"] == "big-model"
        assert "api_base" not in call_kwargs

    def test_load_config_reads_tiered_models(self, tmp_path, monkeypatch):
        toml_content = (
            b'[llm]\nprovider = "ollama"\nmodel = "llama3"\n'
            b'cheap_model = "qwen2.5-coder:3b"\ncloud_model = "qwen2.5-coder:32b"\n'
        )
        config_file = tmp_path / "cartographer.toml"
        config_file.write_bytes(toml_content)
        monkeypatch.delenv("CARTOGRAPHER_LLM_CHEAP_MODEL", raising=False)
        monkeypatch.delenv("CARTOGRAPHER_LLM_CLOUD_MODEL", raising=False)
        cfg = load_config(config_path=config_file)
        assert cfg.cheap_model == "qwen2.5-coder:3b"
        assert cfg.cloud_model == "qwen2.5-coder:32b"
