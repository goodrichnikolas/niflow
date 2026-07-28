"""`niflow doctor` diagnostics against scripted fake sessions."""
from __future__ import annotations

import json

import pytest
import requests

from niflow.config import NiFiConfig
from niflow.doctor import FAIL, OK, WARN, format_checks, run_checks


class FakeResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body or {}
        self.text = json.dumps(self._body)

    def json(self):
        return self._body


class FakeSession:
    """Route by path; raise per-path exceptions when configured."""

    def __init__(self, routes=None, raises=None, supports_login=True):
        self.routes = routes or {}
        self.raises = raises or {}
        self.supports_login = supports_login
        self.verify = False
        self.cert = None

    def request(self, method, url, **kw):
        path = url.split("/nifi-api", 1)[-1]
        for fragment, exc in self.raises.items():
            if fragment in path:
                raise exc
        if "/access/config" in path:
            return FakeResponse(200, {"config": {"supportsLogin": self.supports_login}})
        if "/access/token" in path:
            return _token_response()
        if "/flow/about" in path:
            return FakeResponse(200, {"about": {"version": "1.24.0"}})
        if "/flow/current-user" in path:
            return FakeResponse(200, {"identity": "CN=nikolas"})
        if "/flow/process-groups/root" in path:
            return FakeResponse(200, {"processGroupFlow": {"id": "root-id", "flow": {"processGroups": []}}})
        return self.routes.get(path, FakeResponse(200, {}))


def _token_response():
    r = FakeResponse(201)
    r.text = "tok-abc"
    return r


def _by_title(checks):
    return {c.title: c for c in checks}


def test_healthy_password_server_all_ok():
    checks = run_checks(NiFiConfig(password="pw"), session=FakeSession())
    assert all(c.status != FAIL for c in checks)
    by = _by_title(checks)
    assert "1.24.0" in by["authentication"].detail
    assert "CN=nikolas" in by["identity"].detail
    assert "All good" in format_checks(checks)


def test_unreachable_host_fails_with_hint():
    session = FakeSession(raises={"/access/config": requests.exceptions.ConnectionError("refused")})
    checks = run_checks(NiFiConfig(), session=session)
    by = _by_title(checks)
    assert by["reachability"].status == FAIL
    assert "podman ps" in by["reachability"].detail


def test_tls_trust_failure_suggests_ca_bundle():
    session = FakeSession(
        raises={"/access/config": requests.exceptions.SSLError("self-signed certificate")}
    )
    checks = run_checks(NiFiConfig(), session=session)
    by = _by_title(checks)
    assert by["TLS trust"].status == FAIL
    assert "NIFLOW_NIFI_CA_BUNDLE" in by["TLS trust"].detail


def test_server_demanding_client_cert_is_identified():
    session = FakeSession(
        raises={"/access/config": requests.exceptions.SSLError(
            "sslv3 alert handshake failure: certificate required")}
    )
    checks = run_checks(NiFiConfig(), session=session)
    by = _by_title(checks)
    assert by["TLS handshake"].status == FAIL
    assert "NIFLOW_NIFI_CLIENT_CERT" in by["TLS handshake"].detail


def test_password_config_against_cert_only_server_fails_clearly():
    session = FakeSession(supports_login=False)
    checks = run_checks(NiFiConfig(password="pw"), session=session)
    by = _by_title(checks)
    assert by["auth mismatch"].status == FAIL
    assert "client certificate" in by["auth mismatch"].detail


def test_missing_cert_file_fails_before_any_network(tmp_path):
    checks = run_checks(NiFiConfig(client_cert=str(tmp_path / "nope.pem")))
    by = _by_title(checks)
    assert by["client certificate"].status == FAIL


def test_host_without_nifi_api_warns():
    checks = run_checks(
        NiFiConfig(host="https://work:8443", password="pw"), session=FakeSession()
    )
    assert _by_title(checks)["host path"].status == WARN


def test_doctor_cli_exit_codes(monkeypatch, capsys):
    from niflow import cli as cli_mod
    from niflow.doctor import Check

    monkeypatch.setattr(
        "niflow.doctor.run_checks", lambda config: [Check(OK, "configuration", "x")]
    )
    assert cli_mod.main(["doctor"]) == 0
    assert "configuration" in capsys.readouterr().out

    monkeypatch.setattr(
        "niflow.doctor.run_checks", lambda config: [Check(FAIL, "reachability", "down")]
    )
    assert cli_mod.main(["doctor"]) == 1
