"""Direct REST client for NiFi 1.x and 2.x — the pull/push engine.

Why not nipyapi: nipyapi >= 1.0 only speaks NiFi 2.x, and the pull/push
workflow needs exactly two heavyweight endpoints that exist on both lines
(1.11+/1.13+ and 2.x):

* ``GET  /process-groups/{id}/download`` — a process group as a
  ``VersionedFlowSnapshot`` (the JSON :mod:`niflow.formats.json_format` parses).
* ``POST /process-groups/{id}/process-groups`` with an inline
  ``versionedFlowSnapshot`` — create a fully-wired group in one call (with a
  multipart ``/upload`` fallback for servers that dropped the inline form).

Everything else here is small bookkeeping: token login (single-user and
LDAP both POST ``/access/token``), name→id resolution, stop/empty/delete for
replace semantics, and parameter-context updates so Python parameter values —
including sensitive ones supplied via a secrets mapping — win after a push.

Secrets: pass a dict or a path to an env-style file with lines like::

    db.password=hunter2              # applies to that parameter in any context
    etl-context::db.password=hunter2 # scoped to one context

Sensitive parameter *values* never come back from NiFi, so they live only in
that (git-ignored) file and are applied at push time.
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from niflow.config import NiFiConfig
from niflow.core import Flow, ParameterContext, ProcessGroup
from niflow.utils import get_logger

logger = get_logger()

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)

# How long to wait for NiFi's async requests (queue drops, parameter updates).
_POLL_TIMEOUT_S = 120
_POLL_INTERVAL_S = 0.5


class NiFiApiError(RuntimeError):
    """A NiFi REST call failed; carries the status code and response body."""

    def __init__(self, status: int, message: str):
        super().__init__(f"NiFi API error {status}: {message}")
        self.status = status


class NiFiClient:
    """Thin, version-agnostic NiFi REST client.

    ``session`` is injectable for tests; anything with ``request(method, url,
    **kw) -> response`` (a ``requests.Session`` in production) works.
    """

    def __init__(self, config: Optional[NiFiConfig] = None, session: Any = None):
        self.config = config or NiFiConfig.from_env()
        self.base = self.config.host.rstrip("/")
        if session is None:
            import requests

            session = requests.Session()
            session.verify = self.config.verify_ssl
            if not self.config.verify_ssl and self.config.suppress_ssl_warnings:
                import urllib3

                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.session = session
        self._token: Optional[str] = None
        self._root_id: Optional[str] = None

    # ------------------------------------------------------------------ auth

    def login(self) -> None:
        """Fetch a bearer token (single-user and LDAP both use /access/token).

        Skipped for anonymous/plain-HTTP servers (no password configured).
        """
        if not self.config.password:
            logger.info("No password configured; using anonymous access")
            return
        resp = self.session.request(
            "POST",
            f"{self.base}/access/token",
            data={"username": self.config.username, "password": self.config.password},
            timeout=30,
        )
        token = resp.text.strip()
        # NiFi can return an HTML error page (e.g. host-header rejection) with
        # a 2xx status; a real token is a single JWT with no whitespace or '<'.
        if resp.status_code >= 400 or not token or "<" in token or any(c.isspace() for c in token):
            raise NiFiApiError(
                resp.status_code,
                f"login failed for user {self.config.username!r} at {self.base}: {resp.text}",
            )
        self._token = token
        logger.info("Authenticated to %s as %r", self.base, self.config.username)

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        if self._token is None and self.config.password:
            self.login()
        headers = dict(kwargs.pop("headers", {}))
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        kwargs.setdefault("timeout", 60)
        resp = self.session.request(method, f"{self.base}{path}", headers=headers, **kwargs)
        if resp.status_code == 401 and self._token is not None:
            # Token expired mid-session — refresh once and retry.
            self.login()
            headers["Authorization"] = f"Bearer {self._token}"
            resp = self.session.request(method, f"{self.base}{path}", headers=headers, **kwargs)
        if resp.status_code >= 400:
            raise NiFiApiError(resp.status_code, f"{method} {path}: {resp.text}")
        return resp

    def _get_json(self, path: str) -> dict:
        return self._request("GET", path).json()

    # ------------------------------------------------------------- inventory

    def version(self) -> str:
        """The server version string, e.g. ``"1.24.0"`` or ``"2.7.2"``."""
        about = self._get_json("/flow/about").get("about", {})
        return about.get("version", "unknown")

    def root_id(self) -> str:
        if self._root_id is None:
            self._root_id = self._get_json("/flow/process-groups/root")[
                "processGroupFlow"
            ]["id"]
        return self._root_id

    def _child_groups(self, pg_id: str) -> List[dict]:
        flow = self._get_json(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
        return [g["component"] for g in flow.get("processGroups", [])]

    def walk_groups(self) -> Iterator[Tuple[str, dict]]:
        """Yield ``(path, component)`` for every process group, depth-first."""

        def visit(pg_id: str, prefix: str) -> Iterator[Tuple[str, dict]]:
            for comp in self._child_groups(pg_id):
                path = f"{prefix}/{comp['name']}" if prefix else comp["name"]
                yield path, comp
                yield from visit(comp["id"], path)

        yield from visit(self.root_id(), "")

    def resolve_group(self, group: str) -> str:
        """Resolve a process-group reference (UUID, name, or ``a/b`` path) to an id."""
        if group == "root":
            return self.root_id()
        if _UUID_RE.match(group):
            return group
        matches = [
            (path, comp)
            for path, comp in self.walk_groups()
            if comp["name"] == group or path == group
        ]
        if not matches:
            raise ValueError(f"No process group named {group!r} found")
        if len(matches) > 1:
            paths = ", ".join(repr(p) for p, _ in matches)
            raise ValueError(
                f"Process group name {group!r} is ambiguous ({paths}); "
                "use the full path or the id"
            )
        return matches[0][1]["id"]

    def _pg_entity(self, pg_id: str) -> dict:
        return self._get_json(f"/process-groups/{pg_id}")

    # ------------------------------------------------------------------ pull

    def download_snapshot(self, pg_id: str) -> dict:
        """A process group as a ``VersionedFlowSnapshot`` dict."""
        resp = self._request("GET", f"/process-groups/{pg_id}/download")
        return resp.json() if callable(getattr(resp, "json", None)) else json.loads(resp.text)

    def pull_flow(self, group: str) -> Flow:
        """Pull a live process group into a :class:`Flow`.

        ``parent_pg`` is set to the live parent's name (or ``"root"``) so a
        subsequent push lands the group back where it came from.
        """
        from niflow.formats import from_json

        pg_id = self.resolve_group(group)
        snapshot = self.download_snapshot(pg_id)
        flow = from_json(snapshot)

        entity = self._pg_entity(pg_id)
        parent_id = entity["component"].get("parentGroupId")
        if parent_id and parent_id != self.root_id():
            flow.parent_pg = self._pg_entity(parent_id)["component"]["name"]
        flow.nifi_id = pg_id

        # The downloaded snapshot omits sensitive *values* but parameter
        # contexts may also exist with values only NiFi knows. Pull live
        # non-sensitive values so the Python file reflects reality.
        self._refresh_parameter_values(flow)
        return flow

    def _refresh_parameter_values(self, flow: Flow) -> None:
        try:
            live = {
                c["component"]["name"]: c["component"]
                for c in self._get_json("/flow/parameter-contexts").get(
                    "parameterContexts", []
                )
            }
        except NiFiApiError:  # parameter contexts may be permission-restricted
            return
        for ctx in _iter_contexts(flow):
            live_ctx = live.get(ctx.name)
            if not live_ctx:
                continue
            live_params = {
                p["parameter"]["name"]: p["parameter"]
                for p in live_ctx.get("parameters", [])
            }
            for param in ctx.parameters:
                lp = live_params.get(param.name)
                if lp and not param.sensitive and lp.get("value") is not None:
                    param.value = lp["value"]

    # ------------------------------------------------------------------ push

    def push_flow(
        self,
        flow: Flow,
        *,
        start: bool = False,
        secrets: Union[None, dict, str, Path] = None,
    ) -> str:
        """Delete-and-recreate ``flow`` under its ``parent_pg``; returns the new id.

        Order: resolve parent → remember the old group's canvas position →
        tear the old group down → create from snapshot → push parameter values
        (model values + secrets) → optionally enable services and start.
        """
        parent_id = self.resolve_group(flow.parent_pg or "root")

        position = {"x": 0.0, "y": 0.0}
        existing = [c for c in self._child_groups(parent_id) if c["name"] == flow.name]
        if existing:
            position = existing[0].get("position") or position
            logger.info("Replacing existing group %r (%s)", flow.name, existing[0]["id"])
            self._teardown(existing[0]["id"])

        snapshot = json.loads(flow.to_json())
        new_id = self._create_from_snapshot(parent_id, flow.name, snapshot, position)
        flow.nifi_id = new_id
        logger.info("Created group %r (%s) on NiFi %s", flow.name, new_id, self.version())

        self.apply_parameters(flow, secrets)

        if start:
            self.enable_services(new_id)
            self.start_group(new_id)
        return new_id

    def _create_from_snapshot(
        self, parent_id: str, name: str, snapshot: dict, position: dict
    ) -> str:
        """Create a PG from a snapshot — inline first, multipart upload fallback."""
        # NiFi (1.x at least) names the group from the snapshot's embedded
        # flowContents.name, ignoring component.name — stamp both so renames
        # (e.g. copy) actually take effect.
        if "flowContents" in snapshot:
            snapshot = dict(snapshot)
            snapshot["flowContents"] = dict(snapshot["flowContents"])
            snapshot["flowContents"]["name"] = name
        body = {
            "revision": {"version": 0, "clientId": "niflow"},
            "component": {"name": name, "position": position},
            "versionedFlowSnapshot": snapshot,
        }
        try:
            resp = self._request(
                "POST", f"/process-groups/{parent_id}/process-groups", json=body
            )
            return resp.json()["id"]
        except NiFiApiError as exc:
            if exc.status not in (400, 404, 405):
                raise
            logger.info("Inline snapshot create rejected (%s); trying multipart upload", exc.status)

        resp = self._request(
            "POST",
            f"/process-groups/{parent_id}/process-groups/upload",
            files={"file": (f"{name}.json", json.dumps(snapshot), "application/json")},
            data={
                "groupName": name,
                "positionX": str(position.get("x", 0.0)),
                "positionY": str(position.get("y", 0.0)),
                "clientId": "niflow",
            },
        )
        return resp.json()["id"]

    # ------------------------------------------------------------ parameters

    def apply_parameters(
        self, flow: Flow, secrets: Union[None, dict, str, Path] = None
    ) -> None:
        """Make live parameter values match the model (plus secret values).

        Snapshot import creates missing contexts/parameters but never
        overwrites existing values — so after a push we submit one update
        request per context. Sensitive parameters are only sent when the
        secrets mapping provides a value.
        """
        secret_map = _load_secrets(secrets)
        for ctx in _iter_contexts(flow):
            updates = []
            for p in ctx.parameters:
                value = p.value
                if p.sensitive:
                    value = secret_map.get(f"{ctx.name}::{p.name}", secret_map.get(p.name))
                    if value is None:
                        continue  # keep whatever NiFi already has
                elif value is None:
                    continue
                updates.append(
                    {
                        "parameter": {
                            "name": p.name,
                            "sensitive": p.sensitive,
                            "description": p.description or "",
                            "value": value,
                        }
                    }
                )
            if updates:
                self._update_context(ctx.name, updates)

    def _find_context_entity(self, name: str) -> Optional[dict]:
        for entity in self._get_json("/flow/parameter-contexts").get(
            "parameterContexts", []
        ):
            if entity["component"]["name"] == name:
                return entity
        return None

    def _update_context(self, name: str, parameter_updates: List[dict]) -> None:
        entity = self._find_context_entity(name)
        if entity is None:
            logger.warning("Parameter context %r not found on server; skipping update", name)
            return
        ctx_id = entity["component"]["id"]
        body = {
            "revision": entity["revision"],
            "id": ctx_id,
            "component": {"id": ctx_id, "parameters": parameter_updates},
        }
        req = self._request(
            "POST", f"/parameter-contexts/{ctx_id}/update-requests", json=body
        ).json()
        req_id = req["request"]["requestId"]
        try:
            deadline = time.monotonic() + _POLL_TIMEOUT_S
            while not req["request"].get("complete"):
                if time.monotonic() > deadline:
                    raise NiFiApiError(408, f"parameter update for {name!r} timed out")
                time.sleep(_POLL_INTERVAL_S)
                req = self._get_json(
                    f"/parameter-contexts/{ctx_id}/update-requests/{req_id}"
                )
            failure = req["request"].get("failureReason")
            if failure:
                raise NiFiApiError(500, f"parameter update for {name!r} failed: {failure}")
        finally:
            self._request(
                "DELETE", f"/parameter-contexts/{ctx_id}/update-requests/{req_id}"
            )
        logger.info("Updated %d parameter(s) in context %r", len(parameter_updates), name)

    # ---------------------------------------------------------------- copy

    def copy_group(
        self, group: str, new_name: Optional[str] = None, parent: Optional[str] = None
    ) -> str:
        """Clone a process group as a detached working copy; returns the new id.

        The clone is stripped of registry version-control coordinates, so it's
        safe to edit and replace without touching the original — this is the
        manual "copy, disconnect version control" dance in one step.
        """
        src_id = self.resolve_group(group)
        snapshot = self.download_snapshot(src_id)
        _strip_version_control(snapshot.get("flowContents", {}))

        src_entity = self._pg_entity(src_id)["component"]
        name = new_name or f"{src_entity['name']} (copy)"
        parent_id = (
            self.resolve_group(parent) if parent else src_entity.get("parentGroupId", self.root_id())
        )
        pos = src_entity.get("position") or {"x": 0.0, "y": 0.0}
        new_pos = {"x": float(pos.get("x", 0.0)) + 480.0, "y": float(pos.get("y", 0.0))}
        new_id = self._create_from_snapshot(parent_id, name, snapshot, new_pos)
        logger.info("Copied %r -> %r (%s)", src_entity["name"], name, new_id)
        return new_id

    # ------------------------------------------------------------ lifecycle

    def _set_group_state(self, pg_id: str, state: str) -> None:
        self._request(
            "PUT", f"/flow/process-groups/{pg_id}", json={"id": pg_id, "state": state}
        )

    def start_group(self, group: str) -> None:
        self._set_group_state(self.resolve_group(group), "RUNNING")

    def stop_group(self, group: str) -> None:
        self._set_group_state(self.resolve_group(group), "STOPPED")

    def enable_services(self, group: str) -> None:
        pg_id = self.resolve_group(group)
        self._request(
            "PUT",
            f"/flow/process-groups/{pg_id}/controller-services",
            json={"id": pg_id, "state": "ENABLED"},
        )

    def _disable_services(self, pg_id: str) -> None:
        self._request(
            "PUT",
            f"/flow/process-groups/{pg_id}/controller-services",
            json={"id": pg_id, "state": "DISABLED"},
        )

    def _empty_queues(self, pg_id: str) -> None:
        req = self._request(
            "POST", f"/process-groups/{pg_id}/empty-all-connections-requests"
        ).json()["dropRequest"]
        req_id = req["id"]
        try:
            deadline = time.monotonic() + _POLL_TIMEOUT_S
            while not req.get("finished"):
                if time.monotonic() > deadline:
                    raise NiFiApiError(408, f"emptying queues of {pg_id} timed out")
                time.sleep(_POLL_INTERVAL_S)
                req = self._get_json(
                    f"/process-groups/{pg_id}/empty-all-connections-requests/{req_id}"
                )["dropRequest"]
        finally:
            self._request(
                "DELETE", f"/process-groups/{pg_id}/empty-all-connections-requests/{req_id}"
            )

    def delete_group(self, group: str) -> None:
        """Stop, disable, drain, and delete a process group."""
        self._teardown(self.resolve_group(group))

    def _teardown(self, pg_id: str) -> None:
        self._set_group_state(pg_id, "STOPPED")
        self._disable_services(pg_id)
        self._empty_queues(pg_id)
        version = self._pg_entity(pg_id)["revision"]["version"]
        self._request(
            "DELETE",
            f"/process-groups/{pg_id}",
            params={"version": version, "clientId": "niflow", "disconnectedNodeAcknowledged": "false"},
        )
        logger.info("Deleted group %s", pg_id)


# --------------------------------------------------------------------- utils


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
