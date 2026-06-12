"""NiFi connection settings and login.

Single-user auth over HTTPS (NiFi 2.x): NiFlow points nipyapi at the endpoint,
disables SSL verification for the self-signed dev cert, and performs a token
login. Override any field via env vars (see :meth:`NiFiConfig.from_env`).
"""
from __future__ import annotations

import os
from typing import Optional

from pydantic import BaseModel

from niflow.utils import get_logger

logger = get_logger()


class NiFiConfig(BaseModel):
    """How to reach and authenticate to a NiFi instance."""

    # Host URL must include the /nifi-api base path (nipyapi does not add it).
    host: str = "https://localhost:8443/nifi-api"
    username: str = "admin"
    password: str = "adminpassword123"
    # Self-signed dev cert -> no verification by default.
    verify_ssl: bool = False
    suppress_ssl_warnings: bool = True

    @classmethod
    def from_env(cls) -> "NiFiConfig":
        """Build config from ``NIFLOW_NIFI_HOST`` / ``_USER`` / ``_PASSWORD``.

        Unset vars fall back to the local-Docker defaults above.
        """
        data = {}
        if host := os.getenv("NIFLOW_NIFI_HOST"):
            data["host"] = host
        if user := os.getenv("NIFLOW_NIFI_USER"):
            data["username"] = user
        if password := os.getenv("NIFLOW_NIFI_PASSWORD"):
            data["password"] = password
        if (verify := os.getenv("NIFLOW_NIFI_VERIFY_SSL")) is not None:
            data["verify_ssl"] = verify.lower() in ("1", "true", "yes")
        return cls(**data)


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
