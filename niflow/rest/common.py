"""Shared pieces of the REST client: error type, constants, secrets/params helpers."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Iterator, Optional, Union

from niflow.core import Flow, ParameterContext, ProcessGroup
from niflow.utils import get_logger

logger = get_logger()

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)

_POLL_TIMEOUT_S = 120

_POLL_INTERVAL_S = 0.5

class NiFiApiError(RuntimeError):
    """A NiFi REST call failed; carries the status code and response body."""

    def __init__(self, status: int, message: str):
        super().__init__(f"NiFi API error {status}: {message}")
        self.status = status


def _iter_contexts(flow: Flow) -> Iterator[ParameterContext]:
    """Unique parameter contexts in the tree, in first-seen order."""
    seen: set = set()

    def visit(pg: ProcessGroup) -> Iterator[ParameterContext]:
        ctx = pg.parameter_context
        if ctx is not None and id(ctx) not in seen:
            seen.add(id(ctx))
            yield ctx
        for child in pg.process_groups:
            yield from visit(child)

    yield from visit(flow)

def _strip_version_control(contents: dict) -> None:
    """Recursively remove registry coordinates so a copy is fully detached."""
    contents.pop("versionedFlowCoordinates", None)
    for child in contents.get("processGroups") or []:
        _strip_version_control(child)

def _load_env_overlay(env: Optional[str]) -> Dict[str, str]:
    """Load ``.niflow-params.<env>.env`` (empty dict when no env is selected)."""
    if not env:
        return {}
    path = Path(f".niflow-params.{env}.env")
    if not path.exists():
        raise FileNotFoundError(
            f"No parameter overlay for environment {env!r}: {path} not found"
        )
    return _load_secrets(path)

def _load_secrets(secrets: Union[None, dict, str, Path]) -> Dict[str, str]:
    """Normalise the secrets argument to a flat dict.

    Accepts a dict (used as-is), or a path to an env-style file with
    ``param=value`` / ``Context::param=value`` lines (``#`` comments allowed).
    Defaults to ``.niflow-secrets.env`` in the CWD when present.
    """
    if isinstance(secrets, dict):
        return dict(secrets)
    path = Path(secrets) if secrets else Path(".niflow-secrets.env")
    if not path.exists():
        if secrets:
            raise FileNotFoundError(f"Secrets file not found: {path}")
        return {}
    out: Dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        out[key.strip()] = value.strip()
    return out
