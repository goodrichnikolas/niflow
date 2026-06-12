"""Shared pytest fixtures: NiFi reachability gate for integration tests."""
import os
import socket
from urllib.parse import urlparse

import pytest

from niflow import NiFiConfig


def _nifi_reachable(config: NiFiConfig) -> bool:
    parsed = urlparse(config.host)
    host = parsed.hostname or "localhost"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        with socket.create_connection((host, port), timeout=3):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def nifi_client():
    """A logged-in :class:`NiFiClient`, or skip if no NiFi is reachable.

    Unlike the ``nifi`` fixture this never touches nipyapi, so it works
    against NiFi 1.x (e.g. ``NIFLOW_NIFI_HOST=https://localhost:8444/nifi-api``)
    as well as 2.x.
    """
    config = NiFiConfig.from_env()
    if not _nifi_reachable(config):
        pytest.skip(f"NiFi not reachable at {config.host} (start it with 'make nifi-up')")
    from niflow.client import NiFiClient

    client = NiFiClient(config)
    client.login()
    return client


@pytest.fixture(scope="session")
def nifi():
    """Connect to a live NiFi, or skip the test if one isn't reachable.

    Session-scoped: ``connect()`` sets global nipyapi state (endpoint + auth),
    so a single per-session login is sufficient and avoids paying the login
    cost on every test. Function-scoped fixtures may still request this one.
    """
    config = NiFiConfig.from_env()
    if not _nifi_reachable(config):
        pytest.skip(f"NiFi not reachable at {config.host} (start it with 'make nifi-up')")
    from niflow.config import connect

    connect(config)
    return config
