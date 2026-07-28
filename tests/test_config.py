"""Config-file loading and cert-auth plumbing (no NiFi, no network)."""
from __future__ import annotations

import pytest

from niflow.client import NiFiClient
from niflow.config import NiFiConfig


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    """Isolate from the developer's real env/config files."""
    for key in ("NIFLOW_CONFIG", "NIFLOW_NIFI_HOST", "NIFLOW_NIFI_USER",
                "NIFLOW_NIFI_PASSWORD", "NIFLOW_NIFI_CLIENT_CERT",
                "NIFLOW_NIFI_CLIENT_KEY", "NIFLOW_NIFI_CA_BUNDLE",
                "NIFLOW_NIFI_VERIFY_SSL"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(tmp_path)  # no stray ./.niflow.env
    monkeypatch.setenv("HOME", str(tmp_path))  # no stray ~/.niflow.env


def _write(tmp_path, name, text):
    path = tmp_path / name
    path.write_text(text)
    return str(path)


def test_defaults_without_any_config():
    config = NiFiConfig.from_env()
    assert config.host == "https://localhost:8443/nifi-api"
    assert config.auth_mode == "password"


def test_config_file_via_niflow_config_env(tmp_path, monkeypatch):
    path = _write(tmp_path, "work.env", (
        "# work NiFi\n"
        "NIFLOW_NIFI_HOST=https://work-nifi:8443/nifi-api\n"
        "NIFLOW_NIFI_USER=nikolas\n"
        "NIFLOW_NIFI_PASSWORD='hunter2'\n"
        "NIFLOW_NIFI_VERIFY_SSL=true\n"
    ))
    monkeypatch.setenv("NIFLOW_CONFIG", path)
    config = NiFiConfig.from_env()
    assert config.host == "https://work-nifi:8443/nifi-api"
    assert config.username == "nikolas"
    assert config.password == "hunter2"  # quotes stripped
    assert config.verify_ssl is True


def test_cwd_niflow_env_is_found(tmp_path):
    _write(tmp_path, ".niflow.env", "NIFLOW_NIFI_HOST=https://cwd:8443/nifi-api\n")
    assert NiFiConfig.from_env().host == "https://cwd:8443/nifi-api"


def test_real_environment_beats_config_file(tmp_path, monkeypatch):
    path = _write(tmp_path, "work.env", "NIFLOW_NIFI_USER=from-file\n")
    monkeypatch.setenv("NIFLOW_CONFIG", path)
    monkeypatch.setenv("NIFLOW_NIFI_USER", "from-env")
    assert NiFiConfig.from_env().username == "from-env"


def test_empty_password_in_file_means_anonymous(tmp_path, monkeypatch):
    path = _write(tmp_path, "anon.env", "NIFLOW_NIFI_PASSWORD=\n")
    monkeypatch.setenv("NIFLOW_CONFIG", path)
    config = NiFiConfig.from_env()
    assert config.password == "" and config.auth_mode == "anonymous"


def test_cert_config_wins_auth_mode(tmp_path, monkeypatch):
    cert = _write(tmp_path, "me.pem", "dummy")
    path = _write(tmp_path, "cert.env", (
        f"NIFLOW_NIFI_CLIENT_CERT={cert}\n"
        "NIFLOW_NIFI_PASSWORD=ignored\n"
    ))
    monkeypatch.setenv("NIFLOW_CONFIG", path)
    assert NiFiConfig.from_env().auth_mode == "cert"


def test_explicit_config_file_argument(tmp_path):
    path = _write(tmp_path, "explicit.env", "NIFLOW_NIFI_HOST=https://x:1/nifi-api\n")
    assert NiFiConfig.from_env(path).host == "https://x:1/nifi-api"


# --- client plumbing --------------------------------------------------------
def test_client_session_gets_cert_and_ca(tmp_path):
    cert = _write(tmp_path, "me.pem", "dummy")
    key = _write(tmp_path, "me.key", "dummy")
    ca = _write(tmp_path, "ca.pem", "dummy")
    client = NiFiClient(NiFiConfig(
        client_cert=cert, client_key=key, ca_bundle=ca,
    ))
    assert client.session.cert == (cert, key)
    assert client.session.verify == ca


def test_client_cert_without_key_passed_alone(tmp_path):
    cert = _write(tmp_path, "combined.pem", "dummy")
    client = NiFiClient(NiFiConfig(client_cert=cert))
    assert client.session.cert == cert


class _RecordingSession:
    def __init__(self):
        self.calls = []

    def request(self, method, url, **kw):
        self.calls.append((method, url, kw))

        class R:
            status_code = 200
            text = "{}"

            def json(self):
                return {"about": {"version": "1.24.0"}}

        return R()


def test_cert_auth_skips_token_login(tmp_path):
    cert = _write(tmp_path, "me.pem", "dummy")
    session = _RecordingSession()
    client = NiFiClient(
        NiFiConfig(client_cert=cert, password="should-be-ignored"), session=session
    )
    client.version()
    assert all("/access/token" not in url for _, url, _kw in session.calls)
    # And no bearer header was attached.
    assert all(
        "Authorization" not in kw.get("headers", {}) for *_ignore, kw in session.calls
    )
