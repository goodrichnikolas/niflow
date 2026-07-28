"""Connection diagnostician: figure out how to talk to a NiFi and prove it.

``run_checks`` walks the layers in order — configuration sanity, TLS
reachability, what auth the server supports, whether the configured
credentials actually work, and what identity NiFi sees — and returns a list
of results with actionable next steps. Exposed as ``niflow doctor``.

Built for the "unknown work server" situation: point NIFLOW_NIFI_HOST at
it, run the doctor, and the failure messages tell you what to put in
``.niflow.env`` next (see docs/work-nifi-setup.md for the full playbook).
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from niflow.config import NiFiConfig
from niflow.utils import get_logger

logger = get_logger()

OK, WARN, FAIL = "ok", "warn", "fail"


@dataclass
class Check:
    status: str  # ok | warn | fail
    title: str
    detail: str = ""


def run_checks(config: Optional[NiFiConfig] = None, session=None) -> List[Check]:
    """Run all diagnostics; never raises. ``session`` is injectable for tests."""
    from niflow.client import NiFiClient

    config = config or NiFiConfig.from_env()
    checks: List[Check] = []

    # --- 1. configuration sanity -----------------------------------------
    checks.append(Check(OK, "configuration", (
        f"host={config.host}  auth={config.auth_mode}"
        + (f"  user={config.username!r}" if config.auth_mode == "password" else "")
    )))
    if not config.host.rstrip("/").endswith("/nifi-api"):
        checks.append(Check(WARN, "host path", (
            f"{config.host!r} does not end in /nifi-api — NiFi's REST base is "
            "https://host:port/nifi-api; append it unless you know otherwise"
        )))
    for label, path in (("client certificate", config.client_cert),
                        ("client key", config.client_key),
                        ("CA bundle", config.ca_bundle)):
        if path and not Path(path).is_file():
            checks.append(Check(FAIL, label, f"file not found: {path}"))
    if any(c.status == FAIL for c in checks):
        return checks

    client = NiFiClient(config, session=session)

    # --- 2. reachability + TLS trust --------------------------------------
    try:
        import requests.exceptions as rex
    except ImportError:  # pragma: no cover
        rex = None

    try:
        resp = client.session.request(
            "GET", f"{client.base}/access/config", timeout=10
        )
    except Exception as exc:
        text = str(exc)
        if "certificate required" in text.lower() or "alert" in text.lower() and "handshake" in text.lower():
            checks.append(Check(FAIL, "TLS handshake", (
                "the server REQUIRES a client certificate (mTLS). Get a PEM "
                "cert+key issued for you and set NIFLOW_NIFI_CLIENT_CERT / "
                "NIFLOW_NIFI_CLIENT_KEY"
            )))
        elif rex is not None and isinstance(exc, rex.SSLError):
            checks.append(Check(FAIL, "TLS trust", (
                "could not verify the server certificate. Export the server/CA "
                "cert and set NIFLOW_NIFI_CA_BUNDLE=<pem>, or (test only) "
                f"NIFLOW_NIFI_VERIFY_SSL=false. Underlying error: {text[:200]}"
            )))
        else:
            checks.append(Check(FAIL, "reachability", (
                f"cannot reach {client.base}: {text[:200]}. Check host/port, "
                "that the container is running (podman ps), and any proxy/VPN"
            )))
        return checks
    checks.append(Check(OK, "reachability", f"{client.base} answered (HTTP {resp.status_code})"))

    # --- 3. what auth does the server support? ----------------------------
    supports_login: Optional[bool] = None
    try:
        supports_login = bool(resp.json().get("config", {}).get("supportsLogin"))
    except Exception:
        pass
    if supports_login is True:
        checks.append(Check(OK, "server auth", (
            "server supports username/password login (single-user, LDAP, or "
            "Kerberos behind /access/token)"
        )))
        if config.auth_mode == "cert":
            checks.append(Check(WARN, "auth mismatch", (
                "you configured a client certificate but the server offers "
                "password login — the cert may still work if authorized, but "
                "username/password is the likely intended path"
            )))
    elif supports_login is False:
        checks.append(Check(OK, "server auth", (
            "server does NOT offer password login — identity comes from a "
            "client certificate (or an SSO proxy in front)"
        )))
        if config.auth_mode == "password":
            checks.append(Check(FAIL, "auth mismatch", (
                "you configured username/password but the server has no login "
                "endpoint. You need a client certificate: set "
                "NIFLOW_NIFI_CLIENT_CERT / NIFLOW_NIFI_CLIENT_KEY"
            )))
            return checks

    # --- 4. do the configured credentials actually work? -------------------
    try:
        about = client._get_json("/flow/about").get("about", {})
        live_version = about.get("version", "?")
        checks.append(Check(OK, "authentication", (
            f"authenticated via {config.auth_mode}; NiFi version {live_version}"
        )))
        checks.append(_catalog_check(live_version))
    except Exception as exc:
        text = str(exc)
        hint = {
            "cert": ("the certificate was presented but rejected/unauthorized. "
                     "Its DN may need to be added to NiFi's users/policies "
                     "(or the identity-mapping rules differ)"),
            "password": "wrong username/password, or the account lacks access",
            "anonymous": "the server requires authentication — set "
                         "NIFLOW_NIFI_PASSWORD or NIFLOW_NIFI_CLIENT_CERT",
        }[config.auth_mode]
        checks.append(Check(FAIL, "authentication", f"{hint}. Error: {text[:200]}"))
        return checks

    # --- 5. who am I, and can I see the canvas? ----------------------------
    try:
        identity = client._get_json("/flow/current-user").get("identity", "?")
        checks.append(Check(OK, "identity", f"NiFi sees you as {identity!r}"))
    except Exception as exc:
        checks.append(Check(WARN, "identity", f"/flow/current-user failed: {str(exc)[:120]}"))
    try:
        client.root_id()
        groups = sum(1 for _ in client.walk_groups())
        checks.append(Check(OK, "canvas access", f"root canvas readable ({groups} process group(s))"))
    except Exception as exc:
        checks.append(Check(FAIL, "canvas access", (
            "authenticated but cannot read the canvas — your account likely "
            f"lacks 'view the user interface'/component policies: {str(exc)[:160]}"
        )))
    return checks


def _catalog_check(live_version: str) -> Check:
    """Compare the generated catalog's provenance stamp to the live server.

    The catalog (factories + the validation rulebook) is harvested FROM a
    NiFi instance; against a different version it can miss types or carry
    stale rules — and it already went silently stale once. WARN, not FAIL:
    everything works, validation is just only as good as its rulebook.
    """
    try:
        from niflow.processors.catalog import CATALOG_META
    except ImportError:
        return Check(WARN, "catalog", (
            "the processor catalog has no provenance stamp (generated before "
            "stamping existed) — regenerate against this server: make catalog"
        ))
    cat_version = CATALOG_META.get("nifi_version", "?")
    if cat_version != live_version:
        return Check(WARN, "catalog", (
            f"catalog was generated from NiFi {cat_version} "
            f"({CATALOG_META.get('generated', '?')}) but this server is "
            f"{live_version} — validation rules may not match; regenerate "
            "with: make catalog"
        ))
    return Check(OK, "catalog", (
        f"catalog matches this server (NiFi {cat_version}, "
        f"generated {CATALOG_META.get('generated', '?')})"
    ))


def format_checks(checks: List[Check]) -> str:
    marks = {OK: "✓", WARN: "⚠", FAIL: "✗"}
    lines = [f"{marks[c.status]} {c.title}: {c.detail}" for c in checks]
    if any(c.status == FAIL for c in checks):
        lines.append("\nFix the ✗ items above (see docs/work-nifi-setup.md), then re-run `niflow doctor`.")
    else:
        lines.append("\nAll good — this configuration connects. Save it in .niflow.env and every niflow command will use it.")
    return "\n".join(lines)
