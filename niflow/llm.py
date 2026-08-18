"""Pluggable LLM connection for niflow's plain-English features.

No LLM configured means the features that need one are simply off — nothing
else in niflow changes. Settings are read from the real environment first,
then ``.niflow.env``, then a plain ``.env`` in the CWD (where API keys
usually live; git-ignored):

    GOOGLE_API_KEY       The go-to: with just this set, niflow talks to
                         Gemini through its OpenAI-compatible endpoint using
                         the cheapest model (NIFLOW_LLM_MODEL overrides).
                         GEMINI_API_KEY works too.
    NIFLOW_LLM_URL       Explicit endpoint base URL — beats the Gemini
                         default. Two protocols are spoken:
                         - OpenAI-compatible chat completions: Ollama
                           (http://localhost:11434/v1), LM Studio, vLLM,
                           OpenAI itself, most corporate gateways.
                         - Anthropic: https://api.anthropic.com
    NIFLOW_LLM_MODEL     Model name (e.g. llama3.1, gpt-4o, claude-sonnet-5).
    NIFLOW_LLM_KEY       API key, if the endpoint wants one (Ollama doesn't).
    NIFLOW_LLM_PROVIDER  "openai" or "anthropic". Optional — guessed from
                         the URL ("anthropic" anywhere in it wins).

Use :func:`llm_config` to test availability and :func:`complete` to run one
system+user exchange; :class:`LLMUnavailable` carries setup instructions.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from niflow.config import _find_config_file, _parse_env_file
from niflow.utils import get_logger

logger = get_logger()

_DOTENV = Path(".env")  # generic project env file — where API keys usually live

_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai"
# Rolling alias for the newest flash-lite — Google's cheapest text tier.
# (Pinned models get retired for new API users; the alias doesn't.)
_GEMINI_DEFAULT_MODEL = "gemini-flash-lite-latest"

_HOWTO = (
    "no LLM endpoint is configured. Easiest: put GOOGLE_API_KEY=... in .env "
    "(git-ignored) and niflow uses Gemini's cheapest model. Or set an "
    "explicit endpoint, e.g. for a local Ollama:\n"
    "    NIFLOW_LLM_URL=http://localhost:11434/v1\n"
    "    NIFLOW_LLM_MODEL=llama3.1\n"
    "or for Anthropic:\n"
    "    NIFLOW_LLM_URL=https://api.anthropic.com\n"
    "    NIFLOW_LLM_MODEL=claude-sonnet-5\n"
    "    NIFLOW_LLM_KEY=sk-ant-..."
)


class LLMUnavailable(RuntimeError):
    """Raised when an LLM feature is used but no endpoint is configured."""

    def __init__(self, message: str = _HOWTO):
        super().__init__(message)


@dataclass
class LLMConfig:
    url: str
    model: str
    key: Optional[str] = None
    provider: str = "openai"  # or "anthropic"


def _setting(key: str) -> Optional[str]:
    value = os.getenv(key)
    if value is None:
        path = _find_config_file(None)
        if path is not None:
            value = _parse_env_file(path).get(key)
    if value is None and _DOTENV.is_file():
        value = _parse_env_file(_DOTENV).get(key)
    return value or None


def llm_config() -> Optional[LLMConfig]:
    """The configured LLM connection, or ``None`` when the feature is off."""
    url = _setting("NIFLOW_LLM_URL")
    if not url:
        google = _setting("GOOGLE_API_KEY") or _setting("GEMINI_API_KEY")
        if google:
            # The go-to: a Google key alone means Gemini via its
            # OpenAI-compatible endpoint, cheapest model unless overridden.
            return LLMConfig(
                url=_GEMINI_URL,
                model=_setting("NIFLOW_LLM_MODEL") or _GEMINI_DEFAULT_MODEL,
                key=_setting("NIFLOW_LLM_KEY") or google,
                provider="openai",
            )
        return None
    model = _setting("NIFLOW_LLM_MODEL")
    if not model:
        logger.warning("NIFLOW_LLM_URL is set but NIFLOW_LLM_MODEL is not — LLM off")
        return None
    provider = (_setting("NIFLOW_LLM_PROVIDER") or "").lower()
    if provider not in ("openai", "anthropic"):
        provider = "anthropic" if "anthropic" in url.lower() else "openai"
    return LLMConfig(url=url.rstrip("/"), model=model,
                     key=_setting("NIFLOW_LLM_KEY"), provider=provider)


def complete(system: str, prompt: str, config: Optional[LLMConfig] = None,
             timeout: float = 300.0) -> str:
    """One system+user exchange against the configured endpoint -> reply text.

    Raises :class:`LLMUnavailable` when nothing is configured and
    ``RuntimeError`` with the endpoint's own message on API errors.
    """
    import requests

    config = config or llm_config()
    if config is None:
        raise LLMUnavailable()

    if config.provider == "anthropic":
        resp = requests.post(
            f"{config.url}/v1/messages",
            headers={"x-api-key": config.key or "", "anthropic-version": "2023-06-01"},
            json={"model": config.model, "max_tokens": 4096, "system": system,
                  "messages": [{"role": "user", "content": prompt}]},
            timeout=timeout,
        )
        _raise_for(resp, config)
        return "".join(
            block.get("text", "") for block in resp.json().get("content", [])
        ).strip()

    headers = {"Authorization": f"Bearer {config.key}"} if config.key else {}
    resp = requests.post(
        f"{config.url}/chat/completions",
        headers=headers,
        json={"model": config.model, "temperature": 0.2,
              "messages": [{"role": "system", "content": system},
                           {"role": "user", "content": prompt}]},
        timeout=timeout,
    )
    _raise_for(resp, config)
    return (resp.json()["choices"][0]["message"]["content"] or "").strip()


def _raise_for(resp, config: LLMConfig) -> None:
    if resp.status_code >= 400:
        detail = resp.text[:500]
        raise RuntimeError(
            f"LLM endpoint {config.url} ({config.model}) returned "
            f"{resp.status_code}: {detail}"
        )
