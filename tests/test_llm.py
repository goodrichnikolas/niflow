"""niflow.llm — config resolution and both wire protocols, no network."""
from __future__ import annotations

import json

import pytest

import niflow.llm as llm


@pytest.fixture()
def clean(monkeypatch, tmp_path):
    for key in ("NIFLOW_LLM_URL", "NIFLOW_LLM_MODEL", "NIFLOW_LLM_KEY",
                "NIFLOW_LLM_PROVIDER", "NIFLOW_LLM_CLAUDE_BIN",
                "NIFLOW_LLM_CLAUDE_CODE", "GOOGLE_API_KEY", "GEMINI_API_KEY"):
        monkeypatch.delenv(key, raising=False)
    # A developer's real ~/.niflow.env and ./.env must not leak into tests.
    monkeypatch.setattr(llm, "_find_config_file", lambda explicit=None: None)
    monkeypatch.setattr(llm, "_DOTENV", tmp_path / ".env")
    # Nor a real `claude` on PATH — auto-detection is opt-in per test, so CI
    # (and a dev laptop that happens to have Claude Code) behave the same.
    monkeypatch.setattr(llm.shutil, "which", lambda binary: None)
    return monkeypatch


@pytest.fixture()
def with_claude(clean):
    """As `clean`, but with a `claude` binary pretending to be on PATH."""
    clean.setattr(llm.shutil, "which",
                  lambda binary: f"/usr/bin/{binary}" if "claude" in binary else None)
    return clean


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


# ------------------------------------------------- claude-code provider

# Selection precedence, most explicit first: NIFLOW_LLM_PROVIDER, then an
# explicit URL, then a Google key, then an auto-detected `claude` binary.
# Anything the user configured on purpose has to beat auto-detection —
# niflow must never quietly start spending Claude Code tokens instead.


def test_claude_code_is_autodetected_when_nothing_else_is_configured(with_claude):
    config = llm.llm_config()
    assert config.provider == "claude-code"
    assert config.binary == "/usr/bin/claude"
    assert config.url == "" and config.key is None  # no endpoint, no key
    assert config.describe() == "Claude Code (local CLI)"


def test_claude_code_autodetection_can_be_switched_off(with_claude):
    with_claude.setenv("NIFLOW_LLM_CLAUDE_CODE", "0")
    assert llm.llm_config() is None
    with_claude.setenv("NIFLOW_LLM_CLAUDE_CODE", "1")
    assert llm.llm_config().provider == "claude-code"


def test_configured_backends_beat_autodetected_claude_code(with_claude):
    with_claude.setenv("NIFLOW_LLM_URL", "http://localhost:11434/v1")
    with_claude.setenv("NIFLOW_LLM_MODEL", "llama3.1")
    assert llm.llm_config().provider == "openai"
    with_claude.delenv("NIFLOW_LLM_URL")
    with_claude.delenv("NIFLOW_LLM_MODEL")
    with_claude.setenv("GOOGLE_API_KEY", "AIza-test")
    assert llm.llm_config().url.startswith("https://generativelanguage")


def test_explicit_claude_code_provider_beats_everything(with_claude):
    with_claude.setenv("GOOGLE_API_KEY", "AIza-test")
    with_claude.setenv("NIFLOW_LLM_URL", "http://localhost:11434/v1")
    with_claude.setenv("NIFLOW_LLM_PROVIDER", "claude-code")
    assert llm.llm_config().provider == "claude-code"


def test_explicit_claude_code_without_a_binary_is_off(clean):
    clean.setenv("NIFLOW_LLM_PROVIDER", "claude-code")
    assert llm.llm_config() is None  # and logs a warning, rather than pretending


def test_claude_binary_path_override(clean, tmp_path):
    fake = tmp_path / "claude"
    fake.write_text("#!/bin/sh\n")
    clean.setenv("NIFLOW_LLM_CLAUDE_BIN", str(fake))
    assert llm.llm_config().binary == str(fake)  # found off-PATH, as a file


def test_model_env_selects_the_claude_code_model(with_claude):
    with_claude.setenv("NIFLOW_LLM_MODEL", "sonnet")
    config = llm.llm_config()
    assert config.model == "sonnet"
    assert config.describe() == "Claude Code (local CLI) — sonnet"
    assert llm.claude_code_argv(config, "sys")[-2:] == ["--model", "sonnet"]


def test_claude_code_argv_is_headless_and_toolless():
    config = llm.LLMConfig(model="claude-code", provider="claude-code",
                           binary="/usr/bin/claude")
    argv = llm.claude_code_argv(config, "be terse")
    assert argv[0] == "/usr/bin/claude"
    assert "--print" in argv                       # one shot, never interactive
    assert argv[argv.index("--output-format") + 1] == "json"
    assert argv[argv.index("--system-prompt") + 1] == "be terse"
    assert argv[argv.index("--tools") + 1] == ""   # no tools -> no permission prompt
    assert "--strict-mcp-config" in argv           # ignore niflow's own .mcp.json
    assert "--model" not in argv                   # the CLI's own default stands


class _Proc:
    def __init__(self, stdout="", stderr="", returncode=0):
        self.stdout, self.stderr, self.returncode = stdout, stderr, returncode


def _payload(result, is_error=False):
    return json.dumps({"type": "result", "is_error": is_error, "result": result})


def test_complete_claude_code_runs_the_cli(monkeypatch):
    calls = {}

    def fake_run(argv, input=None, capture_output=None, text=None, timeout=None):
        calls.update(argv=argv, input=input, timeout=timeout)
        return _Proc(stdout=_payload(" the answer \n"))

    monkeypatch.setattr("subprocess.run", fake_run)
    config = llm.LLMConfig(model="claude-code", provider="claude-code",
                           binary="/usr/bin/claude")
    assert llm.complete("sys", "user", config=config, timeout=42) == "the answer"
    assert calls["argv"][0] == "/usr/bin/claude"
    assert calls["input"] == "user"   # prompt on stdin, which is then closed
    assert calls["timeout"] == 42


def test_complete_claude_code_plain_text_output(monkeypatch):
    # --output-format text (or a future envelope change) still yields the text.
    monkeypatch.setattr("subprocess.run",
                        lambda *a, **k: _Proc(stdout=" plain \n"))
    config = llm.LLMConfig(provider="claude-code", binary="claude")
    assert llm.complete("sys", "user", config=config) == "plain"


def test_complete_claude_code_missing_binary(monkeypatch):
    def boom(*a, **k):
        raise FileNotFoundError(2, "No such file or directory")

    monkeypatch.setattr("subprocess.run", boom)
    config = llm.LLMConfig(provider="claude-code", binary="/nope/claude")
    with pytest.raises(RuntimeError, match="not found"):
        llm.complete("sys", "user", config=config)


def test_complete_claude_code_nonzero_exit(monkeypatch):
    monkeypatch.setattr(
        "subprocess.run",
        lambda *a, **k: _Proc(stdout="", stderr="Credit balance is too low",
                              returncode=1))
    config = llm.LLMConfig(provider="claude-code", binary="claude")
    with pytest.raises(RuntimeError, match="exited 1: Credit balance"):
        llm.complete("sys", "user", config=config)


def test_complete_claude_code_reported_error_with_zero_exit(monkeypatch):
    # The CLI can report failure inside the JSON envelope and still exit 0.
    monkeypatch.setattr(
        "subprocess.run",
        lambda *a, **k: _Proc(stdout=_payload("model not found", is_error=True)))
    config = llm.LLMConfig(provider="claude-code", binary="claude")
    with pytest.raises(RuntimeError, match="model not found"):
        llm.complete("sys", "user", config=config)


def test_complete_claude_code_login_prompt_says_so(monkeypatch):
    monkeypatch.setattr(
        "subprocess.run",
        lambda *a, **k: _Proc(stderr="Invalid API key · Please run /login",
                              returncode=1))
    config = llm.LLMConfig(provider="claude-code", binary="claude")
    with pytest.raises(RuntimeError, match="not logged in"):
        llm.complete("sys", "user", config=config)


def test_complete_claude_code_empty_output(monkeypatch):
    monkeypatch.setattr("subprocess.run", lambda *a, **k: _Proc(stdout="  "))
    config = llm.LLMConfig(provider="claude-code", binary="claude")
    with pytest.raises(RuntimeError, match="no text"):
        llm.complete("sys", "user", config=config)


def test_complete_claude_code_timeout_is_fatal_not_a_hang(monkeypatch):
    import subprocess

    def slow(*a, **k):
        raise subprocess.TimeoutExpired(cmd="claude", timeout=k.get("timeout"))

    monkeypatch.setattr("subprocess.run", slow)
    config = llm.LLMConfig(provider="claude-code", binary="claude")
    with pytest.raises(RuntimeError, match="within 5s"):
        llm.complete("sys", "user", config=config, timeout=5)
