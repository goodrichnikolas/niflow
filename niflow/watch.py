"""Background health watcher: "it was fine, nothing changed here, it broke at 14:02".

NiFi already tells you that *a* processor is unhappy — that is what the
bulletin board is. What it never tells you is the sentence that actually
shortens the hunt::

    CallOrdersApi was healthy for 42m (last processed 13:58:11), broke at 14:02:07
    external: api-frontiers refused the connection on port 9099

This module is the thing that can say that. It does three jobs:

1. **Baseline.** Every tick it takes ONE recursive status snapshot plus ONE
   bulletin-board read and records, per component, whether it looks healthy
   and whether it is actually doing work. That record is persisted under
   ``.niflow-watch/`` (git-ignored, like ``.niflow-backups/`` and
   ``.niflow-follow/``) so "was healthy for 3 hours" survives a restart of
   the watcher and means something over days, not since the page loaded.

2. **Transition.** An alert fires on healthy -> failing, and only for a
   component whose health was *established* (healthy continuously for
   ``baseline_seconds``). A processor that has been broken since before we
   started watching is not news and gets no alert — it is recorded as
   chronic instead.

3. **Attribution.** :func:`classify` maps the bulletin's message onto a
   curated pattern table: external (the endpoint, the broker, the host, the
   certificate), internal (our flow, our config, our disk), or an honest
   ``unknown``. A confidently wrong attribution is worse than none — it
   sends the analyst down exactly the wrong path — so the table says
   "unknown" whenever the evidence genuinely does not separate the two.

The signals, and why there are four of them
-------------------------------------------

Bulletins are the richest signal but they are *not sufficient*, and the
user's own example proves it: on NiFi 1.24 an ``InvokeHTTP`` that starts
getting HTTP 404 emits **no bulletin at all** — a 404 is a normal routing
decision to the "No Retry" relationship. Verified live. So the watcher also
looks at:

* ``runStatus == "Invalid"`` — a component that went yellow (internal).
* Running -> Stopped — someone or something stopped it (internal). Ignored
  when several components in one group stop in the same tick, because that
  is a deliberate mass stop, not a break.
* **A failure route opening.** Every connection in the status snapshot
  carries its relationship names, and a connection whose relationships look
  like an error path ("failure", "No Retry", "retry", "unmatched", ...)
  that carried *nothing* for the whole healthy baseline and now carries
  FlowFiles is the silent-404 detector. It is the one signal that catches a
  break NiFi never logs.

Only when an alert actually fires does the watcher spend an expensive call:
a provenance probe of the offending component, which is where
``invokehttp.status.code`` and ``invokehttp.request.url`` come from — the
difference between "InvokeHTTP has a problem" and "api-frontiers returned
HTTP 404 for http://api-frontiers:9099/v1/orders".

Adding patterns
---------------

Work will have processors and error strings that cannot be seen from here.
Drop a JSON file at ``.niflow-watch/patterns.json`` (or point
``NIFLOW_WATCH_PATTERNS`` at one) with a list of::

    {"name": "acme-gateway", "category": "external", "kind": "gateway",
     "regex": "AcmeGatewayException: (?P<code>\\w+)",
     "summary": "the Acme gateway rejected the call ({code})",
     "hint": "check the Acme status page", "confidence": "high"}

User patterns are tried *before* the built-ins, so they can override.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence
from urllib.parse import urlsplit

from niflow.utils import get_logger

logger = get_logger()

# How long a component must look healthy before we are willing to say it
# *was* healthy. Below this we have no baseline and stay quiet.
DEFAULT_BASELINE_SECONDS = 120
DEFAULT_INTERVAL_SECONDS = 15
# Alerts kept in the store (active ones are never trimmed).
MAX_ALERTS = 200
# A push/backup this recently before the break is worth mentioning.
CHANGE_WINDOW_SECONDS = 30 * 60


def watch_dir() -> Path:
    """Where watcher state lives (git-ignored ``.niflow-watch/``)."""
    return Path(os.environ.get("NIFLOW_WATCH_DIR", ".niflow-watch"))


def _now() -> float:
    return time.time()


def _iso(ts: Optional[float]) -> str:
    if not ts:
        return ""
    return datetime.fromtimestamp(ts, timezone.utc).astimezone().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _clock(ts: Optional[float]) -> str:
    """Just the wall-clock time — "broke at 14:02:07" reads better than a date."""
    return _iso(ts)[11:] if ts else ""


def human_duration(seconds: Optional[float]) -> str:
    """``4512`` -> ``"1h15m"``. Short enough to sit inside a sentence."""
    if seconds is None:
        return "?"
    seconds = int(max(0, seconds))
    if seconds < 60:
        return f"{seconds}s"
    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{sec:02d}s" if sec else f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h{minutes:02d}m"
    days, hours = divmod(hours, 24)
    return f"{days}d{hours:02d}h"


# ``save_backup`` files snapshots as ``<group-slug>-<stamp>[-n].json``.
_BACKUP_STAMP_RE = re.compile(r"-\d{8}-\d{6}(?:-\d+)?\.json$")


def _slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("_") or "nifi"


# ------------------------------------------------------------- classifier


@dataclass(frozen=True)
class Pattern:
    """One row of the cause table: a message shape and what it means.

    ``summary`` is a ``str.format`` template filled from the regex's named
    groups plus whatever :func:`_extract` could pull out of the message
    (``host``/``port``/``url``/``status``). Missing fields render as ``?``
    rather than blowing up, because real error strings are inconsistent.
    """

    name: str
    category: str  # "external" | "internal" | "unknown"
    kind: str
    regex: Any  # compiled
    summary: str
    hint: str = ""
    confidence: str = "high"


def _p(name, category, kind, regex, summary, hint="", confidence="high") -> Pattern:
    return Pattern(name, category, kind, re.compile(regex, re.I | re.S),
                   summary, hint, confidence)


# Ordered most-specific first; the first match wins. Every string here that
# is marked "verified" was captured from a real bulletin on NiFi 1.24.0.
BUILTIN_PATTERNS: List[Pattern] = [
    # ---------------------------------------------------------- external
    _p("dns", "external", "dns",
       r"UnknownHostException:?\s*(?P<host>[A-Za-z0-9._\-]+)",   # verified 1.24
       "cannot resolve the hostname {host} — DNS says it does not exist",
       "DNS or the host itself went away: check the name is still right and "
       "that this NiFi's resolver can see it"),
    _p("dns-os", "external", "dns",
       r"(Name or service not known|Temporary failure in name resolution|"
       r"nodename nor servname provided)",
       "DNS lookup for {host} failed",
       "the resolver could not answer; check DNS from the NiFi host"),
    _p("conn-refused", "external", "connection",
       r"Connection refused",                                    # verified 1.24
       "{host}:{port} refused the connection — nothing is listening there",
       "the remote service is down or moved port: check it is running and "
       "reachable from this NiFi host"),
    _p("conn-timeout", "external", "timeout",
       r"(SocketTimeoutException|connect timed out|Read timed out|"
       r"Connection timed out|ReadTimeoutException)",
       "the call to {host} timed out",
       "the endpoint is up but not answering in time — check it for load, "
       "or a firewall silently dropping the packets"),
    _p("conn-reset", "external", "connection",
       r"(Connection reset|Broken pipe|connection was aborted|"
       r"unexpected end of stream|closed by foreign host)",
       "{host} dropped the connection mid-request",
       "the far end or something between you and it (proxy, load balancer) "
       "cut the connection"),
    _p("tls", "external", "tls",
       r"(SSLHandshakeException|PKIX path building failed|"
       r"unable to find valid certification path|CertificateExpiredException|"
       r"CertificateNotYetValidException|certificate_unknown|handshake_failure|"
       r"SSLPeerUnverifiedException|No subject alternative names|"
       r"received fatal alert)",
       "TLS to {host} failed — certificate or handshake problem",
       "usually the far end's certificate was renewed, expired, or is signed "
       "by a CA this NiFi's truststore does not carry"),
    _p("http-auth", "external", "http",
       r"\b(?P<status>401|403)\b(?!\d)",
       "{host} rejected the credentials (HTTP {status})",
       "the endpoint is up but is refusing us: a token, key, or account "
       "expired on their side"),
    _p("http-status", "external", "http",
       r"(?:status\s*code|response\s*code|statuscode|returned|HTTP/1\.[01])"
       r"[\s:=]*(?P<status>[45]\d\d)",
       "{host} returned HTTP {status}",
       "the endpoint answered but not with success — the path, the payload, "
       "or the service itself changed on their side"),
    _p("sftp", "external", "sftp",
       r"(JSchException|SftpException|Auth fail|"
       r"Failed to obtain connection to remote host|"
       r"Could not (?:connect|establish) .*(?:SFTP|FTP))",
       "the SFTP/FTP server {host} would not accept the session",
       "credentials, host key, or the server being down — try the same "
       "login by hand from the NiFi host"),
    _p("jdbc-pool", "external", "database",
       r"(Cannot get a connection, pool error|"
       r"Connection is not available, request timed out|"
       r"SQLNonTransientConnectionException|Communications link failure|"
       r"CommunicationsException|The TCP/IP connection to the host|"
       r"Io exception: |ORA-(?:12(?:1[0-9]{2}|5[0-9]{2})|01017))",
       "the database connection pool could not reach {host}",
       "the database or its listener is down, or the pool is exhausted — "
       "this is the DB side, not the flow"),
    _p("kafka", "external", "kafka",
       r"(org\.apache\.kafka\.common\.errors\.TimeoutException|"
       r"Broker may not be available|Failed to update metadata|"
       r"NotLeaderOrFollowerException|NotLeaderForPartitionException|"
       r"SaslAuthenticationException|"
       r"Connection to node -?\d+ .*could not be established)",
       "the Kafka broker(s) at {host} are unreachable or not answering",
       "check the broker list and that the brokers are up; a metadata "
       "timeout usually means no broker answered at all"),
    _p("jms", "external", "jms",
       r"(javax\.jms\.JMSException|jakarta\.jms\.JMSException|"
       r"Could not connect to broker|Failed to create session factory|"
       r"AMQ\d{6})",
       "the JMS broker at {host} is unreachable",
       "the message broker is down or refusing this client"),
    _p("aws", "external", "cloud",
       r"(AmazonServiceException|AmazonS3Exception|AmazonClientException|"
       r"SdkClientException|NoSuchBucket|The AWS Access Key Id|"
       r"Unable to execute HTTP request|software\.amazon\.awssdk)",
       "the AWS/S3 call failed ({host})",
       "endpoint, bucket, or credentials on the cloud side — an expired "
       "key or a bucket policy change looks exactly like this"),
    _p("azure-gcp", "external", "cloud",
       r"(com\.azure\.|StorageException|GoogleJsonResponseException|"
       r"com\.google\.cloud\.|BlobStorageException)",
       "the cloud storage call failed ({host})",
       "cloud endpoint or credentials — check the service's own status"),
    _p("proxy", "external", "proxy",
       r"(ProxyException|proxy .*(refused|failed|unreachable)|"
       r"Unable to tunnel through proxy)",
       "the HTTP proxy in front of {host} refused the request",
       "the corporate proxy, not the endpoint, is what broke"),
    _p("socket", "external", "connection",
       r"java\.net\.(SocketException|NoRouteToHostException|"
       r"PortUnreachableException|BindException)",
       "the network call to {host} failed",
       "a socket-level failure: routing, firewall, or the far end going away",
       confidence="medium"),

    # ---------------------------------------------------------- internal
    _p("invalid", "internal", "invalid",
       r"(is invalid because|is not valid because|because it is invalid|"
       r"Processor is invalid)",
       "this component is not valid — its configuration is incomplete",
       "a property is missing or wrong here; NiFi will not schedule it "
       "until the yellow triangle clears"),
    _p("service-disabled", "internal", "controller-service",
       r"(Controller Service .*(is disabled|is not enabled)|"
       r"is disabled and cannot be used|references a Controller Service that "
       r"is not enabled)",
       "a controller service this component needs is disabled",
       "enable the controller service (services are disabled by a stop/"
       "re-import far more often than they break on their own)"),
    _p("expression", "internal", "config",
       r"(AttributeExpressionLanguageException|Invalid Expression|"
       r"Unable to evaluate .*Expression)",
       "an Expression Language expression in this processor failed",
       "the expression here is wrong, or an attribute it depends on is "
       "missing on this FlowFile"),
    _p("script", "internal", "script",
       r"(ScriptException|javax\.script|groovy\.lang\.|jython|"
       r"org\.python\.core)",
       "the script in this processor threw",
       "the script body is ours: read the stack trace in nifi-app.log"),
    _p("disk", "internal", "infrastructure",
       r"(No space left on device|Failed to write to Content Repository|"
       r"FlowFile Repository failed|Provenance Repository .*(failed|full)|"
       r"is not writable|DiskSpace)",
       "the NiFi node itself is out of disk or cannot write its repositories",
       "this is the NiFi host, not the flow — free space on the content/"
       "flowfile/provenance repository volumes"),
    _p("memory", "internal", "infrastructure",
       r"(OutOfMemoryError|GC overhead limit exceeded|Too many open files|"
       r"unable to create native thread)",
       "the NiFi JVM ran out of memory or file handles",
       "a NiFi-host resource problem: heap, ulimits, or a runaway flow"),
    _p("backpressure", "internal", "backpressure",
       r"(back ?pressure|due to backpressure)",
       "back pressure is holding this component up",
       "a downstream queue hit its threshold — the blockage is downstream, "
       "find the full queue"),
    _p("local-fs", "internal", "filesystem",
       r"(NoSuchFileException|FileNotFoundException|AccessDeniedException|"
       r"Permission denied|Directory .*does not exist)",
       "a file or directory this processor uses is missing or unreadable",
       "if the path is a local directory this is ours; if it is a network "
       "mount, the mount going away is an outside failure",
       confidence="medium"),

    # ----------------------------------------------------------- unknown
    # Genuinely ambiguous shapes. Saying "external" here would be a guess,
    # and a wrong guess is the 45-minute detour this whole feature exists
    # to prevent.
    _p("data-format", "unknown", "data-format",
       r"(MalformedRecordException|SchemaNotFoundException|JsonParseException|"
       r"Failed to parse|Could not parse|not a valid (JSON|XML|Avro)|"
       r"Unexpected character|SAXParseException|Failed to read .*Record)",
       "the data did not match the reader/schema configured here",
       "could be either side: the upstream system changed its output "
       "format, or the schema/reader in this flow is wrong. Look at one "
       "failed FlowFile's content before deciding",
       confidence="medium"),
]


def _load_user_patterns(path: Optional[Path] = None) -> List[Pattern]:
    """Patterns from ``.niflow-watch/patterns.json`` (or ``$NIFLOW_WATCH_PATTERNS``).

    A bad file is a warning, never a crash: the watcher's job is to keep
    running.
    """
    if path is None:
        env = os.environ.get("NIFLOW_WATCH_PATTERNS")
        path = Path(env) if env else watch_dir() / "patterns.json"
    if not path.is_file():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - never take the watcher down
        logger.warning("watch: ignoring %s (%s)", path, exc)
        return []
    out: List[Pattern] = []
    for i, entry in enumerate(raw if isinstance(raw, list) else []):
        try:
            out.append(_p(
                entry.get("name") or f"user-{i}",
                entry.get("category") or "unknown",
                entry.get("kind") or "custom",
                entry["regex"],
                entry.get("summary") or "matched custom pattern {name}",
                entry.get("hint", ""),
                entry.get("confidence", "high"),
            ))
        except Exception as exc:  # noqa: BLE001
            logger.warning("watch: bad pattern #%d in %s (%s)", i, path, exc)
    return out


def patterns(extra_path: Optional[Path] = None) -> List[Pattern]:
    """User patterns first (so they can override), then the built-ins."""
    return _load_user_patterns(extra_path) + BUILTIN_PATTERNS


class _Fields(dict):
    """format_map source that renders unknown/absent fields as ``?``."""

    def __missing__(self, key):  # pragma: no cover - trivial
        return "?"

    def __getitem__(self, key):
        value = super().get(key)
        return "?" if value in (None, "") else value


_URL_RE = re.compile(r"https?://[^\s,;'\"<>)\]]+", re.I)
# "Failed to connect to api-frontiers/172.19.0.1:9099" — the shape 1.24's
# InvokeHTTP uses, verified live.
_HOSTPORT_RE = re.compile(
    r"(?:connect(?:ion)?\s+(?:to|refused by)|to)\s+"
    r"(?P<host>[A-Za-z0-9._\-]+)(?:/(?P<ip>[0-9.]+))?:(?P<port>\d{1,5})", re.I)
_BARE_HOSTPORT_RE = re.compile(
    r"\b(?P<host>[A-Za-z0-9][A-Za-z0-9._\-]*\.[A-Za-z0-9._\-]+|localhost)"
    r":(?P<port>\d{2,5})\b")


def _extract(message: str) -> Dict[str, str]:
    """Pull the concrete identifiers out of a message: url, host, port, status.

    This is what turns "InvokeHTTP has a bulletin" into "api-frontiers
    refused the connection on port 9099".
    """
    out: Dict[str, str] = {}
    url = _URL_RE.search(message)
    if url:
        out["url"] = url.group(0).rstrip(".,")
        parts = urlsplit(out["url"])
        if parts.hostname:
            out["host"] = parts.hostname
        if parts.port:
            out["port"] = str(parts.port)
    hp = _HOSTPORT_RE.search(message)
    if hp:
        out.setdefault("host", hp.group("host"))
        out.setdefault("port", hp.group("port"))
    elif "host" not in out:
        bare = _BARE_HOSTPORT_RE.search(message)
        if bare:
            out["host"] = bare.group("host")
            out["port"] = bare.group("port")
    return out


def classify(
    message: str,
    *,
    attributes: Optional[Dict[str, str]] = None,
    table: Optional[Sequence[Pattern]] = None,
) -> dict:
    """Map an error message (plus optional FlowFile attributes) onto a cause.

    Returns ``category`` (``external``/``internal``/``unknown``), ``kind``,
    a one-line ``summary`` naming the concrete thing where it can, a
    ``hint`` for what to check, ``confidence``, and ``pattern`` (which row
    matched, so a wrong call is traceable to a fixable line).

    ``attributes`` are FlowFile attributes from the provenance probe. They
    beat the message when present: ``invokehttp.status.code`` is the ground
    truth for an HTTP failure that produced no bulletin text at all.
    """
    message = message or ""
    fields = _Fields(_extract(message))
    attrs = attributes or {}

    # Attribute evidence first — it is exact where the message is prose.
    status = _http_status(attrs)
    if status:
        fields["status"] = status
        url = attrs.get("invokehttp.request.url") or attrs.get(
            "invokehttp.response.url") or fields.get("url")
        if url:
            fields["url"] = url
            host = urlsplit(url).hostname
            if host:
                fields["host"] = host
        reason = attrs.get("invokehttp.status.message") or ""
        code = int(status)
        if code in (401, 403):
            summary = "{host} rejected the credentials (HTTP {status})"
            hint = ("the endpoint is up but refusing us — a token, key, or "
                    "account expired on their side")
        elif 400 <= code < 500:
            summary = "{host} returned HTTP {status}"
            hint = ("the endpoint answered, but not with success — the path "
                    "or the request changed meaning on their side")
        else:
            summary = "{host} returned HTTP {status} — it is erroring itself"
            hint = "the far end is failing on its own side; ours only called it"
        text = summary.format_map(fields)
        if reason:
            text += f" {reason}"
        if fields.get("url"):
            text += f" for {fields['url']}"
        return {
            "category": "external", "kind": "http", "summary": text,
            "hint": hint, "confidence": "high", "pattern": "http-attributes",
        }

    for pattern in (table if table is not None else patterns()):
        match = pattern.regex.search(message)
        if not match:
            continue
        merged = _Fields(fields)
        merged.update({k: v for k, v in (match.groupdict() or {}).items() if v})
        return {
            "category": pattern.category,
            "kind": pattern.kind,
            "summary": pattern.summary.format_map(merged),
            "hint": pattern.hint,
            "confidence": pattern.confidence,
            "pattern": pattern.name,
        }

    return {
        "category": "unknown", "kind": "unclassified",
        "summary": _first_line(message) or "the component reported an error",
        "hint": ("no pattern matched this message — read it, and if it is a "
                 "shape you will see again add it to "
                 f"{watch_dir()}/patterns.json"),
        "confidence": "low", "pattern": "",
    }


def _http_status(attributes: Dict[str, str]) -> str:
    """An HTTP status from FlowFile attributes, if any processor left one.

    ``invokehttp.status.code`` is InvokeHTTP's; the generic tail match
    catches the other HTTP-ish processors and custom NARs that follow the
    same convention.
    """
    for key in ("invokehttp.status.code", "post.http.status.code"):
        value = attributes.get(key)
        if value and value.isdigit() and int(value) >= 400:
            return value
    for key, value in attributes.items():
        if key.endswith(("status.code", "response.code")) and str(value).isdigit():
            if int(value) >= 400:
                return str(value)
    return ""


def _first_line(text: str, limit: int = 160) -> str:
    line = (text or "").strip().splitlines()[0] if (text or "").strip() else ""
    # Bulletins are prefixed "InvokeHTTP[id=...] " — the id is noise here.
    line = re.sub(r"^\w+\[id=[0-9a-f\-]+\]\s*", "", line)
    return line[:limit] + ("…" if len(line) > limit else "")


# --------------------------------------------------- error-route detection

# Relationship names that mean "this FlowFile did not go the happy way".
# Matched against a connection's relationship names, so it is per-flow
# vocabulary rather than per-processor.
_ERROR_REL_RE = re.compile(
    r"^(failure|failed|error|errors|retry|no ?retry|invalid|unmatched|"
    r"not ?found|not ?matched|comms ?failure|communications ?failure|"
    r"timeout|rejected|unauthorized|parse ?failure|failure ?after ?retries)$",
    re.I)


def _is_error_route(name: str) -> bool:
    """Does a connection's relationship list look like an error path?

    The status snapshot's connection ``name`` is the connection's label,
    which for an unlabelled connection *is* the relationship list
    ("No Retry, Retry, Failure" — verified on 1.24). A deliberately named
    connection loses that, which is a documented gap: name it "to error
    handling" and this signal goes quiet for it.
    """
    return any(_ERROR_REL_RE.match(part.strip())
               for part in (name or "").split(","))


# ------------------------------------------------------------------ store


class WatchStore:
    """The persisted half of the watcher: baselines + alert history.

    One JSON file per NiFi instance under ``.niflow-watch/``. Written
    atomically (tmp + replace) because the web GUI's background thread and a
    ``niflow watch`` in a terminal may both be alive.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.health: Dict[str, dict] = {}
        self.routes: Dict[str, dict] = {}
        self.alerts: List[dict] = []
        self.last_bulletin_id: Optional[int] = None
        self.started: Optional[float] = None

    # -- io ---------------------------------------------------------------
    def load(self) -> "WatchStore":
        if not self.path.is_file():
            return self
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001 - corrupt state must not kill it
            logger.warning("watch: ignoring unreadable state %s (%s)", self.path, exc)
            return self
        self.health = data.get("health") or {}
        self.routes = data.get("routes") or {}
        self.alerts = data.get("alerts") or []
        self.last_bulletin_id = data.get("last_bulletin_id")
        self.started = data.get("started")
        return self

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "started": self.started,
            "saved": _now(),
            "last_bulletin_id": self.last_bulletin_id,
            "health": self.health,
            "routes": self.routes,
            "alerts": self.alerts[-MAX_ALERTS:],
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=1, default=str), encoding="utf-8")
        tmp.replace(self.path)

    # -- alerts -----------------------------------------------------------
    def find_alert(self, alert_id: str) -> Optional[dict]:
        for alert in self.alerts:
            if alert.get("id") == alert_id:
                return alert
        return None

    def trim(self) -> None:
        """Keep every active alert; age out resolved ones past the cap."""
        if len(self.alerts) <= MAX_ALERTS:
            return
        active = [a for a in self.alerts if a.get("state") == "active"]
        resolved = [a for a in self.alerts if a.get("state") != "active"]
        keep = max(0, MAX_ALERTS - len(active))
        self.alerts = sorted(active + resolved[-keep:],
                             key=lambda a: a.get("broke_at") or 0)


def store_path(base: str, directory: Optional[Path] = None) -> Path:
    """State file for one NiFi instance — keyed by host so two do not collide."""
    host = urlsplit(base).netloc or base
    return (Path(directory) if directory else watch_dir()) / f"{_slug(host)}.json"


# ---------------------------------------------------------------- watcher


@dataclass
class _Symptom:
    """Why a component is considered failing this tick."""

    signal: str        # bulletin | invalid | stopped | error-route
    message: str = ""
    detail: str = ""
    connection: str = ""
    relationship: str = ""


class Watcher:
    """Polls one NiFi instance and turns health transitions into alerts.

    Cheap by construction: one recursive status snapshot plus one bulletin
    read per :meth:`tick`, whatever the size of the tree. Expensive calls
    (provenance probe, per-processor validation errors) happen only at the
    moment an alert fires.
    """

    def __init__(
        self,
        client: Any,
        group: str = "root",
        *,
        directory: Optional[Path] = None,
        baseline_seconds: float = DEFAULT_BASELINE_SECONDS,
        include_warnings: bool = False,
        probe: bool = True,
        alert_on_stop: bool = True,
        error_routes: bool = True,
        bulletin_limit: int = 200,
        pattern_table: Optional[Sequence[Pattern]] = None,
    ):
        self.client = client
        self.group = group
        self.baseline_seconds = baseline_seconds
        self.include_warnings = include_warnings
        self.probe = probe
        self.alert_on_stop = alert_on_stop
        self.error_routes = error_routes
        self.bulletin_limit = bulletin_limit
        self.pattern_table = pattern_table
        self.store = WatchStore(store_path(getattr(client, "base", "nifi"), directory))
        self.store.load()
        if self.store.started is None:
            self.store.started = _now()
        self.ticks = 0
        self.last_tick: Optional[float] = None
        self.last_error: str = ""
        self._group_id: Optional[str] = None

    # -- helpers ----------------------------------------------------------
    @property
    def levels(self) -> set:
        return {"ERROR", "WARNING"} if self.include_warnings else {"ERROR"}

    def _resolve_group(self) -> str:
        if self._group_id is None:
            self._group_id = self.client.resolve_group(self.group)
        return self._group_id

    def _snapshot(self) -> Dict[str, dict]:
        """``{component_id: record}`` for every processor under the group.

        One call. Each record carries the status fields we baseline on plus
        the group path, so an alert can say *where* without another walk.
        """
        root = self._resolve_group()
        snapshot = self.client._recursive_status(root)
        comps: Dict[str, dict] = {}
        self._connections: List[dict] = []
        if snapshot is None:
            # Server too old / status unavailable: fall back to the walk. We
            # lose throughput counters (so "was processing" degrades to "was
            # running") but keep state and validity.
            for proc in self.client.find_processors("", self.group):
                comps[proc["id"]] = {
                    "id": proc["id"], "name": proc.get("name", ""),
                    "type": (proc.get("type") or "").split(".")[-1],
                    "path": proc.get("path", ""),
                    "group_id": proc.get("group_id", root),
                    "run_status": (proc.get("state") or "").title(),
                    "flowfiles_in": 0, "flowfiles_out": 0, "tasks": 0,
                }
            return comps

        def visit(snap: dict, prefix: str, group_id: str) -> None:
            for wrapper in snap.get("processorStatusSnapshots", []):
                proc = wrapper.get("processorStatusSnapshot") or {}
                comps[proc["id"]] = {
                    "id": proc["id"],
                    "name": proc.get("name", ""),
                    "type": (proc.get("type") or "").split(".")[-1],
                    "path": prefix,
                    "group_id": proc.get("groupId") or group_id,
                    "run_status": proc.get("runStatus", ""),
                    "flowfiles_in": proc.get("flowFilesIn", 0) or 0,
                    "flowfiles_out": proc.get("flowFilesOut", 0) or 0,
                    "tasks": proc.get("taskCount", 0) or 0,
                }
            for wrapper in snap.get("connectionStatusSnapshots", []):
                conn = wrapper.get("connectionStatusSnapshot") or {}
                self._connections.append({
                    "id": conn.get("id", ""),
                    "name": conn.get("name", ""),
                    "source_id": conn.get("sourceId", ""),
                    "source": conn.get("sourceName", ""),
                    "destination": conn.get("destinationName", ""),
                    "group_id": conn.get("groupId") or group_id,
                    "flowfiles_in": conn.get("flowFilesIn", 0) or 0,
                    "queued": conn.get("flowFilesQueued", 0) or 0,
                    "path": prefix,
                })
            for wrapper in snap.get("processGroupStatusSnapshots", []):
                child = wrapper.get("processGroupStatusSnapshot") or {}
                path = f"{prefix}/{child.get('name')}" if prefix else child.get("name", "")
                visit(child, path, child.get("id") or group_id)

        visit(snapshot, "", root)
        return comps

    def _new_bulletins(self) -> Dict[str, List[dict]]:
        """Bulletins we have not seen yet, grouped by source component id.

        On the very first tick we only record the high-water mark: the board
        holds the last few minutes, and a watcher that has not established a
        baseline has no business claiming anything "just broke".
        """
        try:
            board = self.client.bulletins(self.bulletin_limit)
        except Exception as exc:  # noqa: BLE001
            self.last_error = f"bulletins: {exc}"
            return {}
        ids = [b.get("id") for b in board if isinstance(b.get("id"), int)]
        high = max(ids) if ids else None
        previous = self.store.last_bulletin_id
        if previous is None:
            # 0 rather than None even for an empty board, so the *next* tick
            # knows it has seen one and starts reporting.
            self.store.last_bulletin_id = high if high is not None else 0
            return {}
        if high is not None and high < previous:
            # NiFi restarted: the bulletin counter starts over. Re-baseline
            # instead of replaying the whole board as "new".
            self.store.last_bulletin_id = high
            return {}
        if high is not None:
            self.store.last_bulletin_id = high
        out: Dict[str, List[dict]] = {}
        for bulletin in board:
            bid = bulletin.get("id")
            if isinstance(bid, int) and bid <= previous:
                continue
            if (bulletin.get("level") or "").upper() not in self.levels:
                continue
            out.setdefault(bulletin.get("source_id") or "", []).append(bulletin)
        return out

    # -- the tick ---------------------------------------------------------
    def tick(self) -> List[dict]:
        """One poll. Returns the alerts that were *raised or resolved* now."""
        self.ticks += 1
        now = _now()
        comps = self._snapshot()
        bulletins = self._new_bulletins()
        symptoms = self._symptoms(comps, bulletins, now)
        events: List[dict] = []
        for cid, comp in comps.items():
            record = self.store.health.setdefault(cid, {
                "first_seen": now, "state": "unknown",
                "healthy_since": None, "ever_healthy": False,
                "ever_processed": False, "last_processed": None,
                "failing_since": None, "alert_id": None, "run_status": "",
            })
            record.update({
                "name": comp["name"], "type": comp["type"], "path": comp["path"],
                "group_id": comp["group_id"], "last_seen": now,
            })
            processed = bool(comp["flowfiles_in"] or comp["flowfiles_out"]
                             or comp["tasks"])
            if processed:
                record["ever_processed"] = True
                record["last_processed"] = now
            symptom = symptoms.get(cid)
            if symptom is None:
                event = self._mark_healthy(record, comp, now)
            else:
                event = self._mark_failing(record, comp, symptom, now)
            record["run_status"] = comp["run_status"]
            if event:
                events.append(event)
        self._forget_missing(comps, now)
        self.last_tick = now
        self.store.trim()
        try:
            self.store.save()
        except Exception as exc:  # noqa: BLE001
            self.last_error = f"save: {exc}"
        return events

    def _symptoms(self, comps: Dict[str, dict], bulletins: Dict[str, List[dict]],
                  now: float) -> Dict[str, _Symptom]:
        """Everything that says "this component is not well" this tick."""
        out: Dict[str, _Symptom] = {}
        for cid, items in bulletins.items():
            if cid not in comps:
                continue  # controller-level or outside our group
            worst = items[0]
            out[cid] = _Symptom("bulletin", message=worst.get("message", ""))
        for cid, comp in comps.items():
            status = (comp["run_status"] or "").lower()
            if status == "invalid" and cid not in out:
                out[cid] = _Symptom("invalid", detail="runStatus=Invalid")
        if self.alert_on_stop:
            stopped = self._stopped_now(comps)
            for cid in stopped:
                out.setdefault(cid, _Symptom("stopped"))
        if self.error_routes:
            for cid, symptom in self._error_route_symptoms(comps, now).items():
                out.setdefault(cid, symptom)
        return out

    def _stopped_now(self, comps: Dict[str, dict]) -> List[str]:
        """Components that went Running -> Stopped since the last tick.

        A mass stop (three or more in one group at once) is somebody hitting
        Stop, not a break — the whole point is to not cry wolf.
        """
        stopped: List[str] = []
        per_group: Dict[str, int] = {}
        for cid, comp in comps.items():
            record = self.store.health.get(cid) or {}
            if (record.get("run_status") or "").lower() == "running" and \
                    (comp["run_status"] or "").lower() == "stopped":
                stopped.append(cid)
                per_group[comp["group_id"]] = per_group.get(comp["group_id"], 0) + 1
        return [cid for cid in stopped if per_group.get(comps[cid]["group_id"], 0) < 3]

    def _error_route_symptoms(self, comps: Dict[str, dict],
                              now: float) -> Dict[str, _Symptom]:
        """FlowFiles that just started taking a failure relationship.

        This is the signal for breaks NiFi never logs — the HTTP 404 that is
        merely "routed to No Retry". A route has to have been clean for the
        whole baseline before its first FlowFile counts as news.
        """
        out: Dict[str, _Symptom] = {}
        for conn in getattr(self, "_connections", []):
            if not _is_error_route(conn["name"]):
                continue
            record = self.store.routes.setdefault(conn["id"], {
                "clean_since": now, "opened": False, "name": conn["name"],
            })
            record["name"] = conn["name"]
            moving = conn["flowfiles_in"] > 0 or conn["queued"] > 0
            if not moving:
                # Route is quiet again: the 5-minute status window has drained,
                # so this stops being a symptom and the alert can resolve.
                record["opened"] = False
                if record.get("clean_since") is None:
                    record["clean_since"] = now
                continue
            clean_since = record.get("clean_since")
            if clean_since is not None:
                record["clean_since"] = None
                if now - clean_since >= self.baseline_seconds:
                    record["opened"] = True
            if record.get("opened"):
                source = self._route_source(conn, record, comps)
                if source in comps:
                    out[source] = _Symptom(
                        "error-route",
                        detail=(f"FlowFiles started taking the "
                                f"{conn['name']!r} route to "
                                f"{conn['destination'] or 'downstream'}"),
                        connection=conn["id"], relationship=conn["name"])
        return out

    def _route_source(self, conn: dict, record: dict,
                      comps: Dict[str, dict]) -> str:
        """The processor id feeding a connection.

        NiFi 1.24's connection status snapshot carries ``sourceName`` but
        **no** ``sourceId`` (verified live; 2.x does include it), so fall
        back to matching the name inside the same group, and only if that is
        ambiguous spend a REST call. Whatever we learn is cached in the
        persisted route record, so it costs at most once per connection.
        """
        if conn.get("source_id"):
            return conn["source_id"]
        if record.get("source_id"):
            return record["source_id"]
        named = [cid for cid, comp in comps.items()
                 if comp["name"] == conn["source"]
                 and comp["group_id"] == conn["group_id"]]
        if len(named) != 1:
            try:
                end = self.client.connection_end(conn["id"], "source")
                named = [end.get("id", "")] if end.get("id") else []
            except Exception as exc:  # noqa: BLE001
                logger.debug("watch: connection source lookup failed: %s", exc)
                named = []
        if len(named) == 1:
            record["source_id"] = named[0]
            return named[0]
        return ""

    # -- state machine ----------------------------------------------------
    def _mark_healthy(self, record: dict, comp: dict, now: float) -> Optional[dict]:
        was = record.get("state")
        if was == "failing":
            record["state"] = "healthy"
            record["healthy_since"] = now
            record["failing_since"] = None
            alert = self.store.find_alert(record.get("alert_id") or "")
            record["alert_id"] = None
            if alert and alert.get("state") == "active":
                alert["state"] = "resolved"
                alert["resolved_at"] = now
                alert["down_for"] = human_duration(now - (alert.get("broke_at") or now))
                return dict(alert, event="resolved")
            return None
        if was != "healthy":
            record["state"] = "healthy"
            record["healthy_since"] = now
        if record.get("healthy_since") and \
                now - record["healthy_since"] >= self.baseline_seconds:
            record["ever_healthy"] = True
        return None

    def _mark_failing(self, record: dict, comp: dict, symptom: _Symptom,
                      now: float) -> Optional[dict]:
        if record.get("state") == "failing":
            alert = self.store.find_alert(record.get("alert_id") or "")
            if alert:
                alert["occurrences"] = alert.get("occurrences", 1) + 1
                alert["last_seen"] = now
                if symptom.message and not alert.get("message"):
                    alert["message"] = symptom.message
                return self._re_explain(alert, comp, symptom)
            return None
        healthy_since = record.get("healthy_since")
        healthy_for = (now - healthy_since) if healthy_since else 0
        established = bool(record.get("ever_healthy")) or \
            healthy_for >= self.baseline_seconds
        record["state"] = "failing"
        record["failing_since"] = now
        record["healthy_since"] = None
        if not established:
            # Broken since before we had a baseline: recorded, not shouted.
            record["chronic"] = True
            record["alert_id"] = None
            return None
        record["chronic"] = False
        alert = self._raise(record, comp, symptom, now, healthy_since, healthy_for)
        record["alert_id"] = alert["id"]
        self.store.alerts.append(alert)
        return dict(alert, event="raised")

    def _re_explain(self, alert: dict, comp: dict,
                    symptom: _Symptom) -> Optional[dict]:
        """Have another go at explaining an alert we could not classify.

        Provenance lags reality by a few seconds, so the very tick that
        notices a break often has nothing but successful events to look at.
        Rather than leave the analyst with "unknown" forever, retry the probe
        a few times and upgrade the alert in place when the evidence lands.
        """
        if alert.get("category") != "unknown" or not self.probe:
            return None
        attempts = alert.get("probe_attempts", 1)
        if attempts >= 5 or symptom.signal not in ("error-route", "bulletin"):
            return None
        alert["probe_attempts"] = attempts + 1
        attributes = self._probe_attributes(comp["id"])
        finding = classify(alert.get("message") or symptom.message,
                           attributes=attributes, table=self.pattern_table)
        if finding["category"] == "unknown":
            return None
        alert.update({k: finding[k] for k in
                      ("category", "kind", "summary", "hint", "confidence")})
        alert["pattern"] = finding.get("pattern", "")
        return dict(alert, event="updated")

    def _raise(self, record: dict, comp: dict, symptom: _Symptom, now: float,
               healthy_since: Optional[float], healthy_for: float) -> dict:
        evidence: List[str] = []
        attributes: Dict[str, str] = {}
        message = symptom.message
        if symptom.signal == "invalid":
            errors = self._validation_errors(comp["id"])
            if errors:
                message = "; ".join(errors)
                evidence.append("NiFi validation: " + "; ".join(errors))
        if symptom.detail:
            evidence.append(symptom.detail)
        if self.probe and symptom.signal in ("error-route", "bulletin"):
            attributes = self._probe_attributes(comp["id"])
        finding = classify(message, attributes=attributes, table=self.pattern_table)
        if symptom.signal == "stopped":
            finding = {
                "category": "internal", "kind": "stopped",
                "summary": "this processor was running and is now stopped",
                "hint": ("nothing here failed — somebody or something stopped "
                         "it (a push, a deploy script, or a hand on the Stop "
                         "button)"),
                "confidence": "high", "pattern": "run-status",
            }
        change, ours = self._recent_change(comp)
        if change:
            evidence.append(change)
            if ours and finding["category"] == "unknown":
                finding = dict(
                    finding, category="internal", kind="flow-change",
                    confidence="medium",
                    hint=("a niflow push landed just before this broke — "
                          "compare the flow against the backup, or roll back"))
        url = ""
        try:
            url = self.client.ui_url(comp["group_id"], comp["id"])
        except Exception:  # noqa: BLE001
            pass
        if message:
            evidence.append(_first_line(message, 400))
        return {
            "id": f"{comp['id']}:{int(now)}",
            "event": "raised",
            "state": "active",
            "component_id": comp["id"],
            "component": comp["name"],
            "component_type": comp["type"],
            "group_id": comp["group_id"],
            "path": comp["path"],
            "signal": symptom.signal,
            "category": finding["category"],
            "kind": finding["kind"],
            "summary": finding["summary"],
            "hint": finding["hint"],
            "confidence": finding["confidence"],
            "pattern": finding.get("pattern", ""),
            "message": message,
            "evidence": evidence,
            "healthy_since": healthy_since,
            "healthy_for": human_duration(healthy_for),
            "last_processed": record.get("last_processed"),
            "ever_processed": bool(record.get("ever_processed")),
            "broke_at": now,
            "last_seen": now,
            "resolved_at": None,
            "occurrences": 1,
            "acknowledged": False,
            "url": url,
        }

    # -- enrichment -------------------------------------------------------
    def _validation_errors(self, component_id: str) -> List[str]:
        """The yellow-triangle text for ONE processor (one targeted GET)."""
        try:
            entity = self.client._get_json(f"/processors/{component_id}")
        except Exception as exc:  # noqa: BLE001
            logger.debug("watch: validation lookup failed: %s", exc)
            return []
        return list((entity.get("component") or {}).get("validationErrors") or [])

    def _probe_attributes(self, component_id: str,
                          max_events: int = 8) -> Dict[str, str]:
        """Attributes off the recent provenance events for one component.

        THE expensive call, and the reason an alert can say "HTTP 404" for a
        failure that produced no bulletin: InvokeHTTP records
        ``invokehttp.status.code`` on the FlowFile even when it routes
        quietly to "No Retry". Only ever run when an alert fires (or when a
        still-unexplained alert is retrying its enrichment).

        It looks for the newest event that actually shows a *failure*, not
        merely the newest event: at the instant a break is detected the
        provenance index is usually still full of the successful calls from
        five seconds ago, and returning those would explain nothing.
        """
        newest: Dict[str, str] = {}
        try:
            for event in self.client.recent_events(component_id, max_events):
                detail = self.client.event_detail(event["event_id"])
                attributes = {k: str(v) for k, v in
                              (detail.get("attributes") or {}).items()}
                if _http_status(attributes):
                    return attributes
                newest = newest or attributes
        except Exception as exc:  # noqa: BLE001
            logger.debug("watch: provenance probe failed: %s", exc)
        return newest

    def _recent_change(self, comp: dict) -> Tuple[str, bool]:
        """Did *we* touch **this flow** just before it broke?

        Reads the pre-push snapshot directory — every ``niflow push
        --update`` writes one, named after the group — so the alert can say
        "and by the way, a push to this flow landed 40 seconds earlier".
        This is the single cheapest way to tell "ours" from "theirs".

        Returns ``(note, ours)``. ``ours`` is True only when the backup is
        for *this* flow: a push to some unrelated group is a coincidence, and
        letting a coincidence rewrite an "unknown" into "you broke it" is
        exactly the confidently-wrong attribution this feature must not make.
        The note is still reported as evidence either way.
        """
        # Names a backup for this component's flow could be filed under: the
        # group being watched, and the top-level group the component sits in.
        mine = {_slug(name) for name in
                (self.group, (comp.get("path") or "").split("/")[0]) if name
                and name != "root"}
        try:
            from niflow.backup import list_backups

            # Wall clock deliberately: file mtimes are wall clock, and this
            # comparison has to hold even when the caller drives a test clock.
            wall = time.time()
            for path in list_backups()[:10]:
                age = wall - path.stat().st_mtime
                if not (0 <= age <= CHANGE_WINDOW_SECONDS):
                    continue
                stem = _BACKUP_STAMP_RE.sub("", path.name)
                ours = stem in mine
                where = "this flow" if ours else f"{stem!r} (a different flow)"
                return (f"a niflow push to {where} backed up {path.name} "
                        f"{human_duration(age)} before this broke"), ours
        except Exception as exc:  # noqa: BLE001
            logger.debug("watch: backup scan failed: %s", exc)
        return "", False

    def _forget_missing(self, comps: Dict[str, dict], now: float) -> None:
        """Drop baselines for components that no longer exist (deleted flows)."""
        gone = [cid for cid, rec in self.store.health.items()
                if cid not in comps and now - (rec.get("last_seen") or 0) > 3600]
        for cid in gone:
            self.store.health.pop(cid, None)

    # -- reading / acting -------------------------------------------------
    def alerts(self, *, active_only: bool = False,
               include_acknowledged: bool = True) -> List[dict]:
        rows = [a for a in self.store.alerts
                if not (active_only and a.get("state") != "active")]
        if not include_acknowledged:
            rows = [a for a in rows if not a.get("acknowledged")]
        return sorted(rows, key=lambda a: a.get("broke_at") or 0, reverse=True)

    def summary(self) -> dict:
        """Counts for the badge. Pure in-memory — safe to poll every 3s."""
        active = [a for a in self.store.alerts if a.get("state") == "active"]
        unacked = [a for a in active if not a.get("acknowledged")]
        newest = max((a.get("broke_at") or 0 for a in unacked), default=0)
        return {
            "active": len(active),
            "unacknowledged": len(unacked),
            "external": len([a for a in unacked if a.get("category") == "external"]),
            "internal": len([a for a in unacked if a.get("category") == "internal"]),
            "unknown": len([a for a in unacked if a.get("category") == "unknown"]),
            "newest": newest,
            "newest_summary": next(
                (a.get("summary") for a in sorted(
                    unacked, key=lambda a: a.get("broke_at") or 0, reverse=True)), ""),
            "newest_component": next(
                (a.get("component") for a in sorted(
                    unacked, key=lambda a: a.get("broke_at") or 0, reverse=True)), ""),
            "watching": self.group,
            "ticks": self.ticks,
            "last_tick": self.last_tick,
            "baseline_seconds": self.baseline_seconds,
            "tracked": len(self.store.health),
            "established": len([r for r in self.store.health.values()
                                if r.get("ever_healthy")]),
            "chronic": len([r for r in self.store.health.values()
                            if r.get("chronic")]),
            "since": self.store.started,
            "error": self.last_error,
        }

    def acknowledge(self, alert_id: str, on: bool = True) -> bool:
        alert = self.store.find_alert(alert_id)
        if alert is None:
            return False
        alert["acknowledged"] = bool(on)
        alert["acknowledged_at"] = _now() if on else None
        self.store.save()
        return True

    def dismiss(self, alert_id: str) -> bool:
        """Remove an alert entirely (and free its component to alert again)."""
        alert = self.store.find_alert(alert_id)
        if alert is None:
            return False
        self.store.alerts.remove(alert)
        for record in self.store.health.values():
            if record.get("alert_id") == alert_id:
                record["alert_id"] = None
        self.store.save()
        return True

    def clear_resolved(self) -> int:
        before = len(self.store.alerts)
        self.store.alerts = [a for a in self.store.alerts
                             if a.get("state") == "active"]
        self.store.save()
        return before - len(self.store.alerts)

    # -- loop -------------------------------------------------------------
    def run(self, interval: float = DEFAULT_INTERVAL_SECONDS,
            iterations: Optional[int] = None,
            on_event: Optional[Callable[[dict], None]] = None,
            stop: Optional[threading.Event] = None) -> None:
        """Poll forever (or ``iterations`` times), calling ``on_event`` per event."""
        count = 0
        while iterations is None or count < iterations:
            try:
                for event in self.tick():
                    if on_event:
                        on_event(event)
            except Exception as exc:  # noqa: BLE001 - a watcher that dies is useless
                self.last_error = str(exc)
                logger.warning("watch: tick failed: %s", exc)
            count += 1
            if iterations is not None and count >= iterations:
                return
            if stop is not None:
                if stop.wait(interval):
                    return
            else:
                time.sleep(interval)


# --------------------------------------------------------------- rendering


_CATEGORY_LABEL = {
    "external": "EXTERNAL — not NiFi, not your flow",
    "internal": "INTERNAL — something on our side",
    "unknown": "UNKNOWN — not enough evidence to say",
}


def format_alert(alert: dict, *, verbose: bool = False) -> str:
    """The on-screen sentence. This is the whole point of the feature."""
    head = f"[{alert.get('category', '?').upper()}] {alert.get('component', '?')}"
    if alert.get("path"):
        head += f"  ({alert['path']})"
    healthy = alert.get("healthy_for") or "?"
    processed = alert.get("last_processed")
    when = _clock(alert.get("broke_at"))
    line2 = f"  was healthy for {healthy}"
    if processed:
        line2 += f" (last processed {_clock(processed)})"
    elif not alert.get("ever_processed"):
        line2 += " (running, but no FlowFiles seen through it)"
    line2 += f", broke at {when}"
    lines = [head, line2, f"  {alert.get('summary', '')}"]
    if alert.get("hint"):
        lines.append(f"  -> {alert['hint']}")
    if alert.get("confidence") and alert["confidence"] != "high":
        lines.append(f"  confidence: {alert['confidence']}"
                     + (f" (pattern {alert['pattern']})" if alert.get("pattern") else ""))
    if verbose:
        for item in alert.get("evidence") or []:
            lines.append(f"  · {item}")
    if alert.get("url"):
        lines.append(f"  open: {alert['url']}")
    return "\n".join(lines)


def format_resolved(alert: dict) -> str:
    return (f"[RECOVERED] {alert.get('component', '?')} is healthy again "
            f"at {_clock(alert.get('resolved_at'))} "
            f"(was down {alert.get('down_for', '?')})")


# ------------------------------------------------------------------- CLI


def watch_command(
    client: Any,
    group: str = "root",
    *,
    interval: float = DEFAULT_INTERVAL_SECONDS,
    baseline: float = DEFAULT_BASELINE_SECONDS,
    once: bool = False,
    iterations: Optional[int] = None,
    list_only: bool = False,
    as_json: bool = False,
    include_warnings: bool = False,
    probe: bool = True,
    no_stop_alerts: bool = False,
    ack: Optional[str] = None,
    clear: bool = False,
    out=None,
) -> int:
    """``niflow watch`` — the same watcher, headless.

    Exit code 1 when any alert is active, so it drops into cron next to
    ``niflow drift``.
    """
    import sys

    out = out or sys.stdout
    watcher = Watcher(
        client, group, baseline_seconds=baseline,
        include_warnings=include_warnings, probe=probe,
        alert_on_stop=not no_stop_alerts,
    )
    if ack:
        ok = watcher.acknowledge(ack)
        print(("acknowledged " + ack) if ok else f"no alert {ack!r}", file=out)
        return 0 if ok else 1
    if clear:
        print(f"cleared {watcher.clear_resolved()} resolved alert(s)", file=out)
        return 0
    if list_only:
        rows = watcher.alerts()
        if as_json:
            print(json.dumps(rows, indent=2, default=str), file=out)
        elif not rows:
            print("no alerts recorded yet", file=out)
        else:
            for alert in rows:
                mark = "" if alert.get("state") == "active" else "  (resolved)"
                ack_mark = "  (acknowledged)" if alert.get("acknowledged") else ""
                print(f"{alert['id']}{mark}{ack_mark}", file=out)
                print(format_alert(alert, verbose=True), file=out)
                print("", file=out)
        return 1 if any(a.get("state") == "active" for a in rows) else 0

    def emit(event: dict) -> None:
        if as_json:
            print(json.dumps(event, default=str), file=out, flush=True)
            return
        text = (format_resolved(event) if event.get("event") == "resolved"
                else format_alert(event, verbose=True))
        print(text, file=out, flush=True)
        print("", file=out, flush=True)

    rounds = 1 if once else iterations
    summary = watcher.summary()
    if not as_json:
        print(f"watching {group!r} on {getattr(client, 'base', '?')} "
              f"every {interval:g}s "
              f"(baseline {human_duration(baseline)}; "
              f"{summary['tracked']} component(s) already tracked, "
              f"state in {watcher.store.path})", file=out, flush=True)
        if rounds is None:
            print("Ctrl-C to stop.\n", file=out, flush=True)
    try:
        watcher.run(interval=interval, iterations=rounds, on_event=emit)
    except KeyboardInterrupt:
        print("\nstopped", file=out)
    active = watcher.summary()["active"]
    if not as_json and rounds is not None:
        print(f"{active} active alert(s)", file=out)
    return 1 if active else 0
