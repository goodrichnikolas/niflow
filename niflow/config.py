"""NiFi connection settings and login.

Configuration is resolved in three layers, weakest first:

1. Built-in defaults (the local Docker dev instance).
2. A ``.niflow.env`` config file — env-style ``KEY=VALUE`` lines. Looked up
   at the path in ``$NIFLOW_CONFIG``, else ``./.niflow.env``, else
   ``~/.niflow.env``. Fill this out once per environment (see
   ``docs/work-nifi-setup.md``) and every niflow command connects the same
   way — CLI, web helper, and library alike.
3. Real environment variables, which always win.

Recognised keys (file or environment):

    NIFLOW_NIFI_HOST         https://host:8443/nifi-api  (must include /nifi-api)
    NIFLOW_NIFI_USER         username for token login
    NIFLOW_NIFI_PASSWORD     password for token login (empty = anonymous)
    NIFLOW_NIFI_CLIENT_CERT  path to a PEM client certificate -> mTLS auth
    NIFLOW_NIFI_CLIENT_KEY   path to the PEM private key (omit if the cert
                             file contains both)
    NIFLOW_NIFI_CA_BUNDLE    path to the CA/server cert PEM to trust
    NIFLOW_NIFI_VERIFY_SSL   true/false (ignored when CA_BUNDLE is set)

When a client certificate is configured it IS the identity: token login is
skipped entirely and the username/password are ignored.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Optional

from pydantic import BaseModel

from niflow.utils import get_logger

logger = get_logger()

# Config-file key -> NiFiConfig field.
_KEYS = {
    "NIFLOW_NIFI_HOST": "host",
    "NIFLOW_NIFI_USER": "username",
    "NIFLOW_NIFI_PASSWORD": "password",
    "NIFLOW_NIFI_CLIENT_CERT": "client_cert",
    "NIFLOW_NIFI_CLIENT_KEY": "client_key",
    "NIFLOW_NIFI_CA_BUNDLE": "ca_bundle",
    "NIFLOW_NIFI_VERIFY_SSL": "verify_ssl",
}
_BOOL_FIELDS = {"verify_ssl"}


class NiFiConfig(BaseModel):
    """How to reach and authenticate to a NiFi instance."""

    # Host URL must include the /nifi-api base path.
    host: str = "https://localhost:8443/nifi-api"
    username: str = "admin"
    password: str = "adminpassword123"
    # mTLS: PEM client certificate (and key, if not bundled into the cert
    # file). When set, this is the identity — no token login happens.
    client_cert: Optional[str] = None
    client_key: Optional[str] = None
    # Trust: a CA/server-cert PEM path beats the verify_ssl boolean.
    ca_bundle: Optional[str] = None
    # Self-signed dev cert -> no verification by default.
    verify_ssl: bool = False
    suppress_ssl_warnings: bool = True

    @property
    def auth_mode(self) -> str:
        """``"cert"``, ``"password"``, or ``"anonymous"``."""
        if self.client_cert:
            return "cert"
        if self.password:
            return "password"
        return "anonymous"

    @classmethod
    def from_env(cls, config_file: Optional[str] = None) -> "NiFiConfig":
        """Resolve config: defaults < config file < real environment.

        ``config_file`` overrides the file search order ($NIFLOW_CONFIG,
        ./.niflow.env, ~/.niflow.env — first hit wins).
        """
        data: Dict[str, object] = {}

        path = _find_config_file(config_file)
        if path is not None:
            for key, raw in _parse_env_file(path).items():
                field = _KEYS.get(key)
                if field is None:
                    logger.warning("%s: unknown key %r ignored", path, key)
                    continue
                data[field] = _coerce(field, raw)
            logger.info("Loaded connection config from %s", path)

        for key, field in _KEYS.items():
            raw = os.getenv(key)
            if raw is not None:
                data[field] = _coerce(field, raw)

        return cls(**data)


def _coerce(field: str, raw: str) -> object:
    if field in _BOOL_FIELDS:
        return raw.lower() in ("1", "true", "yes")
    return raw


def _find_config_file(explicit: Optional[str]) -> Optional[Path]:
    candidates = []
    if explicit:
        candidates.append(Path(explicit))
    elif env_path := os.getenv("NIFLOW_CONFIG"):
        candidates.append(Path(env_path))
    else:
        candidates.append(Path.cwd() / ".niflow.env")
        candidates.append(Path.home() / ".niflow.env")
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    if explicit or os.getenv("NIFLOW_CONFIG"):
        logger.warning("Config file %s not found; using defaults + environment", candidates[0])
    return None


def _parse_env_file(path: Path) -> Dict[str, str]:
    """Parse ``KEY=VALUE`` lines; blanks and ``#`` comments ignored.

    Values may be quoted; an empty value is meaningful (e.g. an empty
    password means anonymous access).
    """
    out: Dict[str, str] = {}
    for lineno, line in enumerate(path.read_text().splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            logger.warning("%s:%d: not KEY=VALUE, ignored: %r", path, lineno, line)
            continue
        key, _, value = line.partition("=")
        out[key.strip()] = value.strip().strip("'\"")
    return out


def connect(config: Optional[NiFiConfig] = None) -> NiFiConfig:
    """Configure nipyapi and log in to NiFi single-user auth.

    Raises a clear ``RuntimeError`` if the session can't be established.
    """
    import nipyapi

    config = config or NiFiConfig.from_env()

    nipyapi.config.nifi_config.host = config.host
    nipyapi.config.nifi_config.verify_ssl = config.verify_ssl
    if config.suppress_ssl_warnings:
        nipyapi.security.set_ssl_warning_suppression(True)

    logger.info("Connecting to NiFi at %s as %r", config.host, config.username)
    try:
        nipyapi.utils.set_endpoint(
            config.host,
            ssl=config.host.lower().startswith("https"),
            login=True,
            username=config.username,
            password=config.password,
        )
    except Exception as exc:  # surface a useful message instead of a raw API error
        raise RuntimeError(
            f"Failed to connect/login to NiFi at {config.host} as {config.username!r}: {exc}\n"
            "Check that NiFi is running (make nifi-up), the host/port are correct, "
            "and the credentials match SINGLE_USER_CREDENTIALS_* in docker-compose.yml."
        ) from exc

    if not nipyapi.security.get_service_access_status("nifi", bool_response=True):
        raise RuntimeError(
            f"Connected to {config.host} but the session is not authenticated. "
            "Verify the username/password."
        )
    logger.info("Authenticated to NiFi.")
    return config
