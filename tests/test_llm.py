"""niflow.llm — config resolution and both wire protocols, no network."""
from __future__ import annotations

import json

import pytest

import niflow.llm as llm


@pytest.fixture()
def clean(monkeypatch, tmp_path):
    for key in ("NIFLOW_LLM_URL", "NIFLOW_LLM_MODEL", "NIFLOW_LLM_KEY",
                "NIFLOW_LLM_PROVIDER", "GOOGLE_API_KEY", "GEMINI_API_KEY"):
        monkeypatch.delenv(key, raising=False)
    # A developer's real ~/.niflow.env and ./.env must not leak into tests.
    monkeypatch.setattr(llm, "_find_config_file", lambda explicit=None: None)
    monkeypatch.setattr(llm, "_DOTENV", tmp_path / ".env")
    return monkeypatch


def test_unconfigured_is_off(clean):
    assert llm.llm_config() is None
    with pytest.raises(llm.LLMUnavailable, match="NIFLOW_LLM_URL"):
        llm.complete("sys", "user")


def test_url_without_model_is_off(clean):
    clean.setenv("NIFLOW_LLM_URL", "http://localhost:11434/v1")
    assert llm.llm_config() is None


def test_openai_is_the_default_provider(clean):
    clean.setenv("NIFLOW_LLM_URL", "http://localhost:11434/v1/")
    clean.setenv("NIFLOW_LLM_MODEL", "llama3.1")
    config = llm.llm_config()
    assert config.provider == "openai"
    assert config.url == "http://localhost:11434/v1"  # trailing slash dropped
    assert config.model == "llama3.1" and config.key is None


def test_google_key_alone_defaults_to_cheapest_gemini(clean):
    clean.setenv("GOOGLE_API_KEY", "AIza-test")
    config = llm.llm_config()
    assert config.provider == "openai"
    assert config.url == "https://generativelanguage.googleapis.com/v1beta/openai"
    assert config.model == "gemini-flash-lite-latest"
    assert config.key == "AIza-test"
    # NIFLOW_LLM_MODEL upgrades the model without touching the endpoint.
    clean.setenv("NIFLOW_LLM_MODEL", "gemini-2.5-pro")
    assert llm.llm_config().model == "gemini-2.5-pro"


def test_google_key_is_read_from_dotenv_file(clean):
    llm._DOTENV.write_text("GOOGLE_API_KEY=from-dotenv\n")
    assert llm.llm_config().key == "from-dotenv"


def test_explicit_url_beats_the_gemini_default(clean):
    clean.setenv("GOOGLE_API_KEY", "AIza-test")
    clean.setenv("NIFLOW_LLM_URL", "http://localhost:11434/v1")
    clean.setenv("NIFLOW_LLM_MODEL", "llama3.1")
    config = llm.llm_config()
    assert config.url == "http://localhost:11434/v1"
    assert config.model == "llama3.1"


def test_anthropic_guessed_from_url_and_provider_override_wins(clean):
    clean.setenv("NIFLOW_LLM_URL", "https://api.anthropic.com")
    clean.setenv("NIFLOW_LLM_MODEL", "claude-sonnet-5")
    assert llm.llm_config().provider == "anthropic"
    clean.setenv("NIFLOW_LLM_PROVIDER", "openai")
    assert llm.llm_config().provider == "openai"


class _Resp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status
        self.text = json.dumps(payload)

    def json(self):
        return self._payload


def test_complete_openai_protocol(monkeypatch):
    calls = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        calls.update(url=url, headers=headers, body=json)
        return _Resp({"choices": [{"message": {"content": " hi there "}}]})

    monkeypatch.setattr("requests.post", fake_post)
    config = llm.LLMConfig(url="http://x/v1", model="m", key="k")
    assert llm.complete("sys", "user", config=config) == "hi there"
    assert calls["url"] == "http://x/v1/chat/completions"
    assert calls["headers"]["Authorization"] == "Bearer k"
    assert calls["body"]["messages"] == [
        {"role": "system", "content": "sys"},
        {"role": "user", "content": "user"},
    ]


def test_complete_anthropic_protocol(monkeypatch):
    calls = {}

    def fake_post(url, headers=None, json=None, timeout=None):
        calls.update(url=url, headers=headers, body=json)
        return _Resp({"content": [{"type": "text", "text": "a"},
                                  {"type": "text", "text": "b"}]})

    monkeypatch.setattr("requests.post", fake_post)
    config = llm.LLMConfig(url="https://api.anthropic.com", model="m",
                           key="sk", provider="anthropic")
    assert llm.complete("sys", "user", config=config) == "ab"
    assert calls["url"] == "https://api.anthropic.com/v1/messages"
    assert calls["headers"]["x-api-key"] == "sk"
    assert calls["body"]["system"] == "sys"
    assert calls["body"]["messages"] == [{"role": "user", "content": "user"}]


def test_endpoint_errors_surface_with_status(monkeypatch):
    monkeypatch.setattr("requests.post",
                        lambda *a, **k: _Resp({"error": "nope"}, status=401))
    config = llm.LLMConfig(url="http://x/v1", model="m")
    with pytest.raises(RuntimeError, match="401"):
        llm.complete("sys", "user", config=config)
