"""Connection diagnostician: figure out how to talk to a NiFi and prove it.

``run_checks`` walks the layers in order — configuration sanity, which TLS
trust material is actually in effect (and whether an environment CA-bundle
variable is fighting it), reachability, what auth the server supports,
whether the configured credentials actually work, and what identity NiFi
sees — and returns a list of results with actionable next steps. Exposed as
``niflow doctor``.

Built for the "unknown work server" situation: point NIFLOW_NIFI_HOST at
it, run the doctor, and the failure messages tell you what to put in
``.niflow.env`` next (see docs/work-nifi-setup.md for the full playbook).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from niflow.config import NiFiConfig
from niflow.utils import get_logger

logger = get_logger()

OK, WARN, FAIL = "ok", "warn", "fail"

# Environment variables that redirect TLS trust behind your back. The first two
# are read by requests itself (they beat session.verify on any call that doesn't
# pin verify=); SSL_CERT_FILE is read by OpenSSL/ssl for the default store.
_ENV_CA_VARS = ("REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE")


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

    # Offline, and deliberately before the connection attempt: the baseline is
    # a property of the estate, so it is worth stating (and enforcing against
    # flows/) even when the server you happen to be pointed at is down.
    checks.extend(_baseline_checks(config))

    if any(c.status == FAIL for c in checks):
        return checks

    client = NiFiClient(config, session=session)

    # --- 2. what trust material is actually in effect? ---------------------
    checks.extend(_trust_checks(config, client))

    # --- 3. reachability + TLS trust --------------------------------------
    try:
        import requests.exceptions as rex
    except ImportError:  # pragma: no cover
        rex = None

    try:
        # client.probe: same CA bundle / client cert / verify setting as every
        # other niflow call, but without login or error raising.
        resp = client.probe("/access/config", timeout=10)
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

    # --- 4. what auth does the server support? ----------------------------
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

    # --- 5. do the configured credentials actually work? -------------------
    try:
        about = client._get_json("/flow/about").get("about", {})
        live_version = about.get("version", "?")
        checks.append(Check(OK, "authentication", (
            f"authenticated via {config.auth_mode}; NiFi version {live_version}"
        )))
        checks.append(_catalog_check(live_version))
        checks.extend(_cross_version_checks(live_version))
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

    # --- 6. who am I, and can I see the canvas? ----------------------------
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


def _trust_checks(config: NiFiConfig, client) -> List[Check]:
    """Say out loud which TLS trust material niflow will use, and what fights it.

    This is the check that would have saved a day at work. ``requests`` merges
    environment settings into every request: with ``trust_env`` on (default),
    ``REQUESTS_CA_BUNDLE``/``CURL_CA_BUNDLE`` REPLACE ``session.verify`` on any
    call that doesn't pass ``verify=`` itself — so a corporate image exporting
    one silently overrides NIFLOW_NIFI_CA_BUNDLE (and turns verification back
    on when you asked for it off). niflow now pins its own setting per request
    (rest/transport.py), but the variable is still worth naming: it explains why
    curl and every other tool on the box behaves differently.
    """
    checks: List[Check] = []
    verify = client._verify
    if isinstance(verify, str):
        status, detail = OK, f"verifying the server against CA bundle {verify}"
    elif verify:
        status, detail = OK, "verifying the server against the system/certifi CA store"
    else:
        status, detail = WARN, (
            "TLS verification is OFF (NIFLOW_NIFI_VERIFY_SSL=false) — right for "
            "the local dev container's self-signed cert, wrong for a work "
            "server; export its CA and set NIFLOW_NIFI_CA_BUNDLE=<pem>"
        )
    if config.client_cert:
        detail += f"; presenting client certificate {config.client_cert}"
    checks.append(Check(status, "trust material", detail))

    present = [(name, os.environ[name]) for name in _ENV_CA_VARS if os.environ.get(name)]
    if present:
        listed = ", ".join(f"{name}={value}" for name, value in present)
        detail = (
            f"{listed} set in this environment. requests lets these override "
            "session.verify on any call that doesn't pin verify= explicitly; "
            "niflow pins its own on every request, so the trust material above "
            "is what niflow uses — other tools here (curl, pip) will use the "
            "environment bundle instead"
        )
        if not isinstance(verify, str):
            detail += (
                ". If that bundle is the one that trusts this NiFi, point "
                "NIFLOW_NIFI_CA_BUNDLE at it so niflow uses it deliberately"
            )
        checks.append(Check(WARN, "trust environment", detail))
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


def _baseline_checks(config: NiFiConfig) -> List[Check]:
    """State the compatibility baseline, and name the flows that break it.

    The baseline is the oldest NiFi line these flows must keep working on
    (``NIFLOW_MIN_NIFI_VERSION``, default 1.24) — independent of whichever
    server this doctor run is pointed at. Reported as a warning rather than a
    failure because doctor diagnoses the *setup*; `niflow validate` is the gate
    that exits non-zero on a flow that violates it.
    """
    from niflow.compat import baseline_covered, baseline_major, describe_baseline

    declared = config.min_nifi_version
    checks = [Check(OK, "compat baseline", describe_baseline(declared))]
    version = config.compat_baseline
    if version is None:
        return checks
    if not baseline_covered(declared):
        checks.append(Check(WARN, "compat baseline", (
            f"nothing can check the baseline (NiFi {version}) — the generated "
            f"cross-version map does not cover it. Regenerate it against the "
            f"pair you use: make version-map"
        )))
        return checks

    scanned, issues = _scan_flows(baseline_major(declared))
    if scanned == 0:
        return checks
    if not issues:
        checks.append(Check(OK, "flows vs baseline", (
            f"scanned {scanned} flow file(s) in flows/ — every one of them "
            f"would work on NiFi {version}"
        )))
        return checks
    files = sorted({issue["file"] for issue in issues})
    worst = "; ".join(
        f"{issue['file']}: {issue['component']} "
        f"({issue['message'].split(' — ')[0]})"
        for issue in issues[:3]
    )
    checks.append(Check(WARN, "flows vs baseline", (
        f"{len(issues)} propert{'y' if len(issues) == 1 else 'ies'} across "
        f"{len(files)} of {scanned} flow file(s) in flows/ would NOT work on "
        f"your baseline NiFi {version}. {worst}"
        + (f"; and {len(issues) - 3} more" if len(issues) > 3 else "")
        + f". These fail `niflow validate` — run it on {files[0]} for the full list"
    )))
    return checks


def _cross_version_checks(live_version: str) -> List[Check]:
    """Does the authoring namespace match this server — and if not, what breaks?

    ``_catalog_check`` already says "your catalog is 2.7.2, this server is
    1.24". That is only half the answer; the half the user needs is *how much
    it costs them*. This walks the flows on disk through the generated
    cross-version map and names the properties that will not survive, which is
    the difference between a warning you dismiss and one you act on.
    """
    from niflow.compat import map_meta, parse_major

    checks: List[Check] = []
    live_major = parse_major(live_version)
    meta = map_meta()
    if meta is None:
        checks.append(Check(WARN, "version map", (
            "no cross-version property map has been generated — `niflow "
            "validate --target-version` cannot tell you what a 1.x/2.x "
            "crossing would break. Generate it with: make version-map"
        )))
        return checks

    covered = {parse_major(meta.get("new_version", "")),
               parse_major(meta.get("old_version", ""))}
    if live_major not in covered:
        checks.append(Check(WARN, "version map", (
            f"the cross-version map covers NiFi {meta['new_version']} vs "
            f"{meta['old_version']}, but this server is {live_version} — "
            f"cross-version checks are off. Regenerate it against the pair you "
            f"actually use: make version-map"
        )))
        return checks

    checks.append(Check(OK, "version map", (
        f"cross-version map covers this server (NiFi {meta['new_version']} vs "
        f"{meta['old_version']}, generated {meta.get('generated', '?')})"
    )))

    scanned, issues = _scan_flows(live_major)
    if scanned == 0:
        return checks
    if not issues:
        checks.append(Check(OK, "flows vs this server", (
            f"scanned {scanned} flow file(s) in flows/ — every property they "
            f"set exists on NiFi {live_version}"
        )))
        return checks

    worst = "; ".join(
        f"{issue['component']} ({issue['message'].split(' — ')[0]})"
        for issue in issues[:3]
    )
    checks.append(Check(WARN, "flows vs this server", (
        f"{len(issues)} propert{'y' if len(issues) == 1 else 'ies'} across "
        f"{scanned} flow file(s) in flows/ will NOT survive a push to NiFi "
        f"{live_version} — NiFi stores an unknown key as an inert dynamic "
        f"property and runs the real one at its default. {worst}"
        + (f"; and {len(issues) - 3} more" if len(issues) > 3 else "")
        + ". Full list: niflow validate <flow.py> --target-version "
        + live_version
    )))
    return checks


def _scan_flows(target_major: int, directory: str = "flows") -> tuple:
    """``(files_scanned, issues)`` for the flow .py files in *directory*.

    Best-effort by design: a flow module that won't import (missing dependency,
    a path only valid at work) is skipped silently rather than turning a
    connection diagnostic into a stack trace.
    """
    from niflow.compat import flow_issues

    root = Path(directory)
    if not root.is_dir():
        return 0, []
    scanned = 0
    issues: List[dict] = []
    for path in sorted(root.glob("*.py")):
        if path.name.startswith("_"):
            continue
        try:
            from niflow.convert import _load_python_flow

            flow = _load_python_flow(str(path), "flow")
        except Exception as exc:
            logger.debug("doctor: skipped %s (%s)", path, exc)
            continue
        scanned += 1
        for issue in flow_issues(flow, target_major):
            issues.append({**issue, "file": str(path)})
    return scanned, issues


def format_checks(checks: List[Check]) -> str:
    marks = {OK: "✓", WARN: "⚠", FAIL: "✗"}
    lines = [f"{marks[c.status]} {c.title}: {c.detail}" for c in checks]
    if any(c.status == FAIL for c in checks):
        lines.append("\nFix the ✗ items above (see docs/work-nifi-setup.md), then re-run `niflow doctor`.")
    else:
        lines.append("\nAll good — this configuration connects. Save it in .niflow.env and every niflow command will use it.")
    return "\n".join(lines)
