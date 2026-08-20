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

    def __init__(self, routes=None, raises=None, supports_login=True, verify=False):
        self.routes = routes or {}
        self.raises = raises or {}
        self.supports_login = supports_login
        self.verify = verify
        self.cert = None
        self.calls = []  # (method, url, kwargs)

    def request(self, method, url, **kw):
        self.calls.append((method, url, kw))
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


def test_catalog_check_matches_and_mismatches():
    from niflow.doctor import OK, WARN, _catalog_check
    from niflow.processors.catalog import CATALOG_META

    ok = _catalog_check(CATALOG_META["nifi_version"])
    assert ok.status == OK and "matches" in ok.detail
    warn = _catalog_check("0.0.0-other")
    assert warn.status == WARN and "make catalog" in warn.detail


# --- TLS trust material (T5) ------------------------------------------------
def test_trust_material_names_the_ca_bundle(tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("dummy")
    session = FakeSession(verify=str(ca))
    checks = run_checks(NiFiConfig(password="pw", ca_bundle=str(ca)), session=session)
    by = _by_title(checks)
    assert by["trust material"].status == OK
    assert str(ca) in by["trust material"].detail


def test_trust_material_warns_when_verification_is_off():
    checks = run_checks(NiFiConfig(password="pw"), session=FakeSession())
    by = _by_title(checks)
    assert by["trust material"].status == WARN
    assert "NIFLOW_NIFI_CA_BUNDLE" in by["trust material"].detail


def test_environment_ca_bundle_is_reported(monkeypatch):
    """The diagnostic that explains the work machine: requests reads these."""
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/etc/corp/corp-ca.pem")
    checks = run_checks(NiFiConfig(password="pw"), session=FakeSession())
    by = _by_title(checks)
    assert by["trust environment"].status == WARN
    assert "REQUESTS_CA_BUNDLE=/etc/corp/corp-ca.pem" in by["trust environment"].detail
    # ...and it tells you what to do about it when niflow has no bundle of its own.
    assert "NIFLOW_NIFI_CA_BUNDLE" in by["trust environment"].detail


def test_no_environment_check_when_nothing_is_set(monkeypatch):
    for name in ("REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE"):
        monkeypatch.delenv(name, raising=False)
    checks = run_checks(NiFiConfig(password="pw"), session=FakeSession())
    assert "trust environment" not in _by_title(checks)


def test_reachability_probe_pins_verify(tmp_path):
    """The /access/config probe goes through the client, with our trust material."""
    ca = tmp_path / "ca.pem"
    ca.write_text("dummy")
    session = FakeSession(verify=str(ca))
    run_checks(NiFiConfig(password="pw", ca_bundle=str(ca)), session=session)
    probes = [kw for _m, url, kw in session.calls if "/access/config" in url]
    assert probes and all(kw["verify"] == str(ca) for kw in probes)
