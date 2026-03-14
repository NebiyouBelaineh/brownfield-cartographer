"""LLM provider configuration for the Brownfield Cartographer.

Supports any provider backed by litellm (Ollama, OpenAI, Anthropic, OpenRouter, etc.).
API keys are always read from environment variables — never stored in config files.

Config file (cartographer.toml) example:

    [llm]
    provider = "ollama"
    model = "qwen2.5-coder:7b"
    base_url = "http://localhost:11434"

    [llm]
    provider = "openai"
    model = "gpt-4o-mini"
    # OPENAI_API_KEY read from environment

    [llm]
    provider = "anthropic"
    model = "claude-3-5-haiku-20241022"
    # ANTHROPIC_API_KEY read from environment
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

try:
    import tomllib  # Python 3.11+
except ImportError:
    try:
        import tomli as tomllib  # type: ignore[no-reattr]
    except ImportError:
        tomllib = None  # type: ignore[assignment]


_DEFAULT_CONFIG_PATHS = [
    Path("cartographer.toml"),
    Path.home() / ".cartographer.toml",
]

_DEFAULT_ENV_PATHS = [Path(".env"), Path.home() / ".env"]


def _load_dotenv() -> None:
    """Load .env file into os.environ if python-dotenv is available, else parse manually."""
    for env_path in _DEFAULT_ENV_PATHS:
        if not env_path.exists():
            continue
        try:
            from dotenv import load_dotenv  # type: ignore[import]
            load_dotenv(env_path, override=False)
        except ImportError:
            # Manual minimal parser: KEY=VALUE lines, ignores comments and blank lines
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
        break

# litellm model prefix for ollama
_OLLAMA_PREFIX = "ollama/"


@dataclass
class TokenBudget:
    """Accumulates token usage across multiple litellm calls."""

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

    def record(self, usage: Any) -> None:
        """Record usage from a litellm response.usage object. Safe if usage is None."""
        if usage is None:
            return
        self.prompt_tokens += getattr(usage, "prompt_tokens", 0) or 0
        self.completion_tokens += getattr(usage, "completion_tokens", 0) or 0
        self.total_tokens += getattr(usage, "total_tokens", 0) or 0

    def as_dict(self) -> dict[str, int]:
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
        }


@dataclass
class LLMConfig:
    """Resolved LLM provider configuration."""

    provider: str = "ollama"
    model: str = "qwen2.5-coder:7b"
    base_url: str = "http://localhost:11434"
    # Extra kwargs forwarded to litellm.completion (e.g. temperature, max_tokens)
    extra: dict[str, Any] = field(default_factory=dict)
    # Model tiering: cheap for bulk calls, expensive for synthesis.
    # Default None means "use the primary model" for both tiers.
    cheap_model: str | None = None
    cloud_model: str | None = None
    # Module count above which the expensive model is used instead of local Ollama.
    large_repo_threshold: int = 100
    # Dedicated embedding model (e.g. "nomic-embed-text" for Ollama).
    # Falls back to primary model if None, which only works if that model supports embeddings.
    embedding_model: str | None = None

    def _to_litellm_model(self, model_name: str) -> str:
        if self.provider == "ollama" and not model_name.startswith(_OLLAMA_PREFIX):
            return f"{_OLLAMA_PREFIX}{model_name}"
        return model_name

    @property
    def litellm_model(self) -> str:
        """Return the primary model string in litellm format."""
        return self._to_litellm_model(self.model)

    @property
    def cheap_litellm_model(self) -> str:
        """Model to use for bulk/cheap calls (defaults to primary model)."""
        return self._to_litellm_model(self.cheap_model or self.model)

    @property
    def expensive_litellm_model(self) -> str:
        """Model to use for high-quality synthesis calls (defaults to primary model).

        Cloud models (cloud_model field) are passed as-is so litellm routes them
        to the correct provider (Anthropic, OpenAI, etc.) without the ollama/ prefix.
        """
        if self.cloud_model:
            return self.cloud_model
        return self._to_litellm_model(self.model)

    @property
    def litellm_kwargs(self) -> dict[str, Any]:
        """Return kwargs suitable for litellm.completion(**kwargs)."""
        kw: dict[str, Any] = {"model": self.litellm_model}
        if self.provider == "ollama" and self.base_url:
            kw["api_base"] = self.base_url
        kw.update(self.extra)
        return kw


def load_config(config_path: str | Path | None = None) -> LLMConfig:
    """Load LLM config from a TOML file, environment variables, or defaults.

    Resolution order (highest priority first):
    1. Explicit config_path argument
    2. CARTOGRAPHER_CONFIG env var
    3. ./cartographer.toml
    4. ~/.cartographer.toml
    5. Built-in defaults (ollama / qwen2.5-coder:7b)

    Environment variable overrides (applied after file load):
        CARTOGRAPHER_LLM_PROVIDER   — e.g. "openai"
        CARTOGRAPHER_LLM_MODEL      — e.g. "gpt-4o-mini"
        CARTOGRAPHER_LLM_BASE_URL   — e.g. "http://localhost:11434"
    """
    cfg = LLMConfig()

    # Find config file
    candidates: list[Path] = []
    if config_path:
        candidates.append(Path(config_path))
    env_path = os.environ.get("CARTOGRAPHER_CONFIG")
    if env_path:
        candidates.append(Path(env_path))
    candidates.extend(_DEFAULT_CONFIG_PATHS)

    toml_data: dict[str, Any] = {}
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            if tomllib is None:
                break  # Can't parse TOML without the library; fall through to env/defaults
            try:
                with candidate.open("rb") as f:
                    toml_data = tomllib.load(f)
                break
            except Exception:
                pass

    llm_section: dict[str, Any] = toml_data.get("llm", {})
    if llm_section.get("provider"):
        cfg.provider = llm_section["provider"]
    if llm_section.get("model"):
        cfg.model = llm_section["model"]
    if llm_section.get("base_url"):
        cfg.base_url = llm_section["base_url"]
    if llm_section.get("cheap_model"):
        cfg.cheap_model = llm_section["cheap_model"]
    if llm_section.get("cloud_model"):
        cfg.cloud_model = llm_section["cloud_model"]
    if llm_section.get("large_repo_threshold") is not None:
        cfg.large_repo_threshold = int(llm_section["large_repo_threshold"])
    # Any extra keys in [llm] are forwarded as litellm kwargs
    if llm_section.get("embedding_model"):
        cfg.embedding_model = llm_section["embedding_model"]
    known = {"provider", "model", "base_url", "cheap_model", "cloud_model", "large_repo_threshold", "embedding_model"}
    cfg.extra = {k: v for k, v in llm_section.items() if k not in known}

    # Environment variable overrides
    if os.environ.get("CARTOGRAPHER_LLM_PROVIDER"):
        cfg.provider = os.environ["CARTOGRAPHER_LLM_PROVIDER"]
    if os.environ.get("CARTOGRAPHER_LLM_MODEL"):
        cfg.model = os.environ["CARTOGRAPHER_LLM_MODEL"]
    if os.environ.get("CARTOGRAPHER_LLM_BASE_URL"):
        cfg.base_url = os.environ["CARTOGRAPHER_LLM_BASE_URL"]
    if os.environ.get("CARTOGRAPHER_LLM_CHEAP_MODEL"):
        cfg.cheap_model = os.environ["CARTOGRAPHER_LLM_CHEAP_MODEL"]
    if os.environ.get("CARTOGRAPHER_LLM_CLOUD_MODEL"):
        cfg.cloud_model = os.environ["CARTOGRAPHER_LLM_CLOUD_MODEL"]
    if os.environ.get("CARTOGRAPHER_EMBEDDING_MODEL"):
        cfg.embedding_model = os.environ["CARTOGRAPHER_EMBEDDING_MODEL"]
    if os.environ.get("CARTOGRAPHER_LARGE_REPO_THRESHOLD"):
        try:
            cfg.large_repo_threshold = int(os.environ["CARTOGRAPHER_LARGE_REPO_THRESHOLD"])
        except ValueError:
            pass

    return cfg


def chat_completion(
    messages: list[dict[str, str]],
    config: LLMConfig | None = None,
    config_path: str | Path | None = None,
    *,
    budget: "TokenBudget | None" = None,
    **override_kwargs: Any,
) -> str:
    """Call the configured LLM and return the response text.

    Args:
        messages: List of {"role": ..., "content": ...} dicts.
        config: Pre-built LLMConfig. If None, loads from config_path or defaults.
        config_path: Path to cartographer.toml to load config from.
        budget: Optional TokenBudget to accumulate token usage into.
        **override_kwargs: Extra kwargs forwarded to litellm.completion (e.g. max_tokens).

    Returns:
        The assistant message content as a string.
    """
    import litellm  # noqa: PLC0415

    if config is None:
        config = load_config(config_path)

    kw = {**config.litellm_kwargs, **override_kwargs}
    response = litellm.completion(messages=messages, **kw)
    if budget is not None:
        budget.record(getattr(response, "usage", None))
    return response.choices[0].message.content or ""


def chat_completion_tiered(
    messages: list[dict[str, str]],
    tier: Literal["cheap", "expensive"] = "cheap",
    config: LLMConfig | None = None,
    config_path: str | Path | None = None,
    *,
    budget: "TokenBudget | None" = None,
    **override_kwargs: Any,
) -> str:
    """Call the LLM using the cheap or expensive model tier.

    For local Ollama deployments both tiers default to the same model (cost is
    negligible). Deployments can set cheap_model / cloud_model in
    cartographer.toml to route bulk vs synthesis calls to different models.

    Args:
        messages: Chat messages.
        tier: "cheap" for bulk/per-module calls; "expensive" for synthesis.
        config: Pre-built LLMConfig.
        config_path: Path to cartographer.toml.
        budget: Optional TokenBudget to accumulate usage.
        **override_kwargs: Extra kwargs forwarded to litellm.completion.
    """
    import litellm  # noqa: PLC0415

    if config is None:
        config = load_config(config_path)

    model_str = config.cheap_litellm_model if tier == "cheap" else config.expensive_litellm_model
    kw: dict[str, Any] = {"model": model_str}
    # Add Ollama api_base only when the selected model is actually routed through Ollama.
    # Cloud models (tier="expensive" with cloud_model set) bypass Ollama entirely.
    using_cloud = tier == "expensive" and config.cloud_model
    if config.provider == "ollama" and config.base_url and not using_cloud:
        kw["api_base"] = config.base_url
    kw.update(config.extra)
    kw.update(override_kwargs)

    response = litellm.completion(messages=messages, **kw)
    if budget is not None:
        budget.record(getattr(response, "usage", None))
    return response.choices[0].message.content or ""
