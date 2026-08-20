"""Pluggable LLM connection for niflow's plain-English features.

No LLM configured means the features that need one are simply off — nothing
else in niflow changes. Settings are read from the real environment first,
then ``.niflow.env``, then a plain ``.env`` in the CWD (where API keys
usually live; git-ignored):

    NIFLOW_LLM_PROVIDER  "claude-code", "openai" or "anthropic". Optional —
                         "claude-code" is the only one worth setting by hand
                         (the other two are guessed from the URL).
    GOOGLE_API_KEY       The go-to when you have a key: with just this set,
                         niflow talks to Gemini through its OpenAI-compatible
                         endpoint using the cheapest model (NIFLOW_LLM_MODEL
                         overrides). GEMINI_API_KEY works too.
    NIFLOW_LLM_URL       Explicit endpoint base URL — beats the Gemini
                         default. Two protocols are spoken:
                         - OpenAI-compatible chat completions: Ollama
                           (http://localhost:11434/v1), LM Studio, vLLM,
                           OpenAI itself, most corporate gateways.
                         - Anthropic: https://api.anthropic.com
    NIFLOW_LLM_MODEL     Model name (e.g. llama3.1, gpt-4o, claude-sonnet-5,
                         or a Claude Code alias like "sonnet"/"opus").
    NIFLOW_LLM_KEY       API key, if the endpoint wants one (Ollama doesn't).
    NIFLOW_LLM_CLAUDE_BIN  Path to the `claude` binary, when it isn't on PATH.
    NIFLOW_LLM_CLAUDE_CODE  Set to 0/false/off to stop niflow auto-detecting a
                         local `claude` and keep the feature off instead.

**No API key anywhere?** If the Claude Code CLI (`claude`) is installed and
logged in, that is enough: niflow shells out to it for one headless
completion per call. It is picked up automatically when nothing else is
configured, or pinned with ``NIFLOW_LLM_PROVIDER=claude-code``. Resolution
order is deliberate — an explicit provider, then an explicit URL, then a
Google key, and only then the auto-detected CLI, so niflow never quietly
spends Claude Code tokens on a machine that was configured for something
else.

Use :func:`llm_config` to test availability and :func:`complete` to run one
system+user exchange; :class:`LLMUnavailable` carries setup instructions.
"""
from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from niflow.config import _find_config_file, _parse_env_file
from niflow.utils import get_logger

logger = get_logger()

_DOTENV = Path(".env")  # generic project env file — where API keys usually live

_GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/openai"
# Rolling alias for the newest flash-lite — Google's cheapest text tier.
# (Pinned models get retired for new API users; the alias doesn't.)
_GEMINI_DEFAULT_MODEL = "gemini-flash-lite-latest"

_CLAUDE_CODE = "claude-code"
_CLAUDE_CODE_BIN = "claude"
_CLAUDE_CODE_LABEL = "Claude Code (local CLI)"
# Sentinel model name: "whatever the installed CLI is already set to". We only
# pass --model when the user actually named one, so work machines pinned to a
# particular model by policy keep that model.
_CLAUDE_CODE_DEFAULT_MODEL = "claude-code"

_HOWTO = (
    "no LLM is configured. With Claude Code installed and logged in, nothing "
    "else is needed — niflow shells out to the `claude` CLI (it is picked up "
    "automatically, or pin it with NIFLOW_LLM_PROVIDER=claude-code). "
    "Otherwise, with an API key: put GOOGLE_API_KEY=... in .env (git-ignored) "
    "and niflow uses Gemini's cheapest model. Or set an explicit endpoint, "
    "e.g. for a local Ollama:\n"
    "    NIFLOW_LLM_URL=http://localhost:11434/v1\n"
    "    NIFLOW_LLM_MODEL=llama3.1\n"
    "or for the Anthropic API:\n"
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
    """A resolved LLM connection.

    ``url``/``key`` are empty for providers that aren't an HTTP endpoint at
    all (``claude-code`` drives a local binary instead — ``binary``), so read
    :meth:`describe` rather than ``url`` when showing this to a human.
    """

    url: str = ""
    model: str = ""
    key: Optional[str] = None
    provider: str = "openai"  # "openai", "anthropic", or "claude-code"
    binary: Optional[str] = None  # claude-code: the CLI to run

    def describe(self) -> str:
        """One-line "where do explanations come from" for status displays."""
        if self.provider == _CLAUDE_CODE:
            if self.model and self.model != _CLAUDE_CODE_DEFAULT_MODEL:
                return f"{_CLAUDE_CODE_LABEL} — {self.model}"
            return _CLAUDE_CODE_LABEL
        return f"{self.url} ({self.model})"


def _setting(key: str) -> Optional[str]:
    value = os.getenv(key)
    if value is None:
        path = _find_config_file(None)
        if path is not None:
            value = _parse_env_file(path).get(key)
    if value is None and _DOTENV.is_file():
        value = _parse_env_file(_DOTENV).get(key)
    return value or None


def _truthy(value: Optional[str], default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() not in ("0", "false", "off", "no", "")


def _claude_code_config() -> Optional[LLMConfig]:
    """The local Claude Code CLI as an LLM, or ``None`` when it isn't there."""
    binary = _setting("NIFLOW_LLM_CLAUDE_BIN") or _CLAUDE_CODE_BIN
    resolved = shutil.which(binary) or (binary if os.path.isfile(binary) else None)
    if resolved is None:
        return None
    return LLMConfig(
        model=_setting("NIFLOW_LLM_MODEL") or _CLAUDE_CODE_DEFAULT_MODEL,
        provider=_CLAUDE_CODE,
        binary=resolved,
    )


def llm_config() -> Optional[LLMConfig]:
    """The configured LLM connection, or ``None`` when the feature is off."""
    provider = (_setting("NIFLOW_LLM_PROVIDER") or "").lower().replace("_", "-")
    if provider in (_CLAUDE_CODE, "claudecode", "claude code"):
        # Pinned by hand: complain loudly rather than silently falling back,
        # so a typo'd binary path doesn't look like "the feature is off".
        config = _claude_code_config()
        if config is None:
            logger.warning(
                "NIFLOW_LLM_PROVIDER=claude-code but no `claude` binary was "
                "found on PATH (set NIFLOW_LLM_CLAUDE_BIN) — LLM off"
            )
        return config

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
        # Last resort: an installed, logged-in Claude Code needs no keys, so
        # the feature "just works" on a work laptop. NIFLOW_LLM_CLAUDE_CODE=0
        # opts out for anyone who'd rather have the feature stay off.
        if _truthy(_setting("NIFLOW_LLM_CLAUDE_CODE")):
            return _claude_code_config()
        return None
    model = _setting("NIFLOW_LLM_MODEL")
    if not model:
        logger.warning("NIFLOW_LLM_URL is set but NIFLOW_LLM_MODEL is not — LLM off")
        return None
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
    config = config or llm_config()
    if config is None:
        raise LLMUnavailable()

    if config.provider == _CLAUDE_CODE:
        return _complete_claude_code(system, prompt, config, timeout)

    import requests

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


def claude_code_argv(config: LLMConfig, system: str) -> List[str]:
    """The headless `claude` command line for one completion (prompt on stdin).

    Every flag earns its place:

    ``--print``            one shot, no interactive session, exit when done.
    ``--output-format json``  a parseable envelope: ``result`` holds the text
                           and ``is_error`` catches failures the CLI reports
                           with exit status 0 (an API 404, say).
    ``--system-prompt``    replaces Claude Code's coding-agent system prompt
                           with ours — we want a text generator, not an agent.
    ``--tools ""``         no tools at all, so nothing can ask for permission
                           (a permission prompt in a pipe is a hang) and no
                           file reads sneak into the answer.
    ``--strict-mcp-config``  with no ``--mcp-config``, this ignores every MCP
                           server on the machine — including niflow's own
                           .mcp.json, which would otherwise be loaded here and
                           point Claude Code back at NiFi. Also saves startup.
    ``--no-session-persistence``  don't litter ~/.claude with transcripts of
                           machine-generated one-shots.
    ``--model``            only when the user named one; otherwise the CLI's
                           own default (possibly pinned by org policy) stands.
    """
    argv = [config.binary or _CLAUDE_CODE_BIN, "--print",
            "--output-format", "json",
            "--system-prompt", system,
            "--tools", "",
            "--strict-mcp-config",
            "--no-session-persistence"]
    if config.model and config.model != _CLAUDE_CODE_DEFAULT_MODEL:
        argv += ["--model", config.model]
    return argv


def _complete_claude_code(system: str, prompt: str, config: LLMConfig,
                          timeout: float) -> str:
    """Drive the local Claude Code CLI for one completion.

    The prompt goes in on stdin, which is then closed: that is what stops the
    CLI blocking on a TTY if it ever wants input (an expired login, say). The
    timeout is the backstop for the same failure mode.
    """
    import json
    import subprocess

    argv = claude_code_argv(config, system)
    try:
        proc = subprocess.run(
            argv, input=prompt, capture_output=True, text=True, timeout=timeout,
        )
    except FileNotFoundError:
        raise RuntimeError(
            f"Claude Code CLI not found ({argv[0]!r}). Install it and log in "
            "(`claude` then /login), or set NIFLOW_LLM_CLAUDE_BIN to its path."
        ) from None
    except subprocess.TimeoutExpired:
        raise RuntimeError(
            f"Claude Code CLI ({argv[0]}) produced no answer within "
            f"{timeout:.0f}s and was killed."
        ) from None

    stdout, stderr = (proc.stdout or "").strip(), (proc.stderr or "").strip()
    payload = {}
    if stdout.startswith("{"):
        try:
            payload = json.loads(stdout)
        except ValueError:
            payload = {}
    text = (payload.get("result") or "").strip() if payload else stdout
    failed = proc.returncode != 0 or bool(payload.get("is_error"))

    if failed or not text:
        detail = (text or stderr or stdout or "no output")[:500]
        if _looks_like_auth_trouble(detail):
            raise RuntimeError(
                f"Claude Code CLI ({argv[0]}) is not logged in: {detail}. "
                "Run `claude` once and /login, then retry."
            )
        if not failed:
            raise RuntimeError(
                f"Claude Code CLI ({argv[0]}) returned no text: {detail}"
            )
        raise RuntimeError(
            f"Claude Code CLI ({argv[0]}) exited {proc.returncode}: {detail}"
        )
    return text


def _looks_like_auth_trouble(detail: str) -> bool:
    """Spot the "you need to log in" family of CLI failures for a clear error."""
    low = detail.lower()
    return any(marker in low for marker in (
        "/login", "not logged in", "log in", "invalid api key",
        "authentication", "unauthorized", "oauth token",
    ))


def _raise_for(resp, config: LLMConfig) -> None:
    if resp.status_code >= 400:
        detail = resp.text[:500]
        raise RuntimeError(
            f"LLM endpoint {config.url} ({config.model}) returned "
            f"{resp.status_code}: {detail}"
        )
