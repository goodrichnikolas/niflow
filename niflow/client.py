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
from xml.etree import ElementTree

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
        self._bundle_index: Optional[Dict[str, dict]] = None

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

    def ui_url(self, group_id: str = "", component_id: str = "") -> str:
        """A deep link into the NiFi *UI* that selects a component on the canvas.

        Derives the UI base from the API base (``…/nifi-api`` -> ``…/nifi``) and
        appends ``?processGroupId=…&componentIds=…`` so opening it drops you
        right on the processor — no drilling through nested groups by hand.
        """
        ui = self.base
        if ui.endswith("/nifi-api"):
            ui = ui[: -len("/nifi-api")] + "/nifi"
        params = []
        if group_id:
            params.append(f"processGroupId={group_id}")
        if component_id:
            params.append(f"componentIds={component_id}")
        return ui + ("/?" + "&".join(params) if params else "")

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

    # ------------------------------------------------------------- processors

    def walk_processors(self, group: str = "root") -> Iterator[Tuple[str, str, dict]]:
        """Yield ``(group_path, group_id, component)`` for every processor under
        ``group``, depth-first. ``group_path`` is ``""`` for the start group.

        ``group_id`` is the id of the group the processor *lives in* — taken from
        the group we're reading, not the component DTO — so deep links land on
        the right nested level instead of falling back to the root canvas."""

        def visit(pg_id: str, prefix: str) -> Iterator[Tuple[str, str, dict]]:
            flow = self._get_json(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
            for proc in flow.get("processors", []):
                yield prefix, pg_id, proc["component"]
            for child in flow.get("processGroups", []):
                comp = child["component"]
                path = f"{prefix}/{comp['name']}" if prefix else comp["name"]
                yield from visit(comp["id"], path)

        yield from visit(self.resolve_group(group), "")

    def find_processors(self, type_contains: str = "", group: str = "root") -> List[dict]:
        """Processors under ``group`` whose type contains ``type_contains``.

        Returns flat dicts (``id``/``name``/``type``/``state``/``path``) in
        depth-first order — handy for pickers that need a label per processor.
        """
        needle = type_contains.lower()
        return [
            {
                "id": comp["id"],
                "name": comp.get("name", ""),
                "type": comp.get("type", ""),
                "state": comp.get("state", ""),
                "path": path,
                "group_id": group_id,
            }
            for path, group_id, comp in self.walk_processors(group)
            if needle in comp.get("type", "").lower()
        ]

    def validation_errors(self, group: str = "root") -> List[dict]:
        """Processors under ``group`` with validation errors (yellow triangles).

        Each entry carries ``id``/``name``/``path`` plus the ``errors`` list
        NiFi shows in the component tooltip.
        """
        return [
            {
                "id": comp["id"],
                "name": comp.get("name", ""),
                "path": path,
                "group_id": group_id,
                "errors": list(comp.get("validationErrors") or []),
            }
            for path, group_id, comp in self.walk_processors(group)
            if comp.get("validationErrors")
        ]

    # -------------------------------------------------------------- bulletins

    def bulletins(self, limit: int = 100) -> List[dict]:
        """Recent bulletins across the instance, newest first, as flat dicts.

        Bulletins the user lacks permission to read come back without a
        payload; those are skipped.
        """
        board = self._request(
            "GET", "/flow/bulletin-board", params={"limit": limit}
        ).json()["bulletinBoard"]
        out = []
        for entity in board.get("bulletins", []):
            b = entity.get("bulletin")
            if not b:
                continue
            out.append(
                {
                    "id": entity.get("id"),
                    "time": b.get("timestamp", ""),
                    "level": b.get("level", ""),
                    "source": b.get("sourceName", ""),
                    "source_id": b.get("sourceId", ""),
                    "group_id": b.get("groupId", ""),
                    "message": b.get("message", ""),
                }
            )
        out.reverse()  # the board lists oldest first
        return out

    def _set_processor_state(self, proc_id: str, state: str) -> None:
        # Revision is re-fetched per call: every state change bumps the version.
        revision = self._get_json(f"/processors/{proc_id}")["revision"]
        self._request(
            "PUT",
            f"/processors/{proc_id}/run-status",
            json={"revision": revision, "state": state, "disconnectedNodeAcknowledged": False},
        )

    def stop_processor(self, proc_id: str) -> None:
        self._set_processor_state(proc_id, "STOPPED")

    def start_processor(self, proc_id: str) -> None:
        self._set_processor_state(proc_id, "RUNNING")

    def run_processor_once(self, proc_id: str) -> None:
        """Stop a processor, then trigger exactly one scheduling pass.

        Uses NiFi's ``RUN_ONCE`` run-status (1.13+ and 2.x). The stop first
        makes this idempotent whether the processor was running or not.
        """
        self.stop_processor(proc_id)
        self._set_processor_state(proc_id, "RUN_ONCE")

    # -------------------------------------------------------- flowfile inspect

    def list_queues(self, group: str = "root") -> List[dict]:
        """Every connection (queue) under ``group``, with its queued counts.

        Each dict carries ``id`` (the connection id, used to list contents),
        ``source``/``destination`` names, the group ``path``, and ``queued``
        (FlowFile count) / ``queued_label`` (NiFi's "n / size" string).
        """
        out: List[dict] = []

        def visit(pg_id: str, prefix: str) -> None:
            flow = self._get_json(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
            for entity in flow.get("connections", []):
                comp = entity.get("component", {})
                snap = (entity.get("status") or {}).get("aggregateSnapshot") or {}
                out.append({
                    "id": entity["id"],
                    "source": (comp.get("source") or {}).get("name", ""),
                    "destination": (comp.get("destination") or {}).get("name", ""),
                    "path": prefix,
                    "queued": snap.get("flowFilesQueued", 0),
                    "queued_label": snap.get("queued", ""),
                })
            for child in flow.get("processGroups", []):
                c = child["component"]
                path = f"{prefix}/{c['name']}" if prefix else c["name"]
                visit(c["id"], path)

        visit(self.resolve_group(group), "")
        return out

    def list_sinks(self, group: str = "root") -> List[dict]:
        """Terminal processors under ``group`` — those that feed no connection.

        These are the end-of-line components (PutFile, PublishKafka, ...) that
        have no downstream queue to inspect; use :meth:`recent_events` to see
        what actually passed through them.
        """
        procs: List[Tuple[str, dict]] = []
        sources: set = set()

        def visit(pg_id: str, prefix: str) -> None:
            flow = self._get_json(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
            for p in flow.get("processors", []):
                procs.append((prefix, p["component"]))
            for entity in flow.get("connections", []):
                src = (entity.get("component", {}).get("source") or {})
                if src.get("id"):
                    sources.add(src["id"])
            for child in flow.get("processGroups", []):
                c = child["component"]
                path = f"{prefix}/{c['name']}" if prefix else c["name"]
                visit(c["id"], path)

        visit(self.resolve_group(group), "")
        return [
            {"id": c["id"], "name": c.get("name", ""), "type": c.get("type", ""), "path": path}
            for path, c in procs
            if c["id"] not in sources
        ]

    def list_flowfiles(self, connection_id: str, max_results: int = 100) -> List[dict]:
        """Snapshot of the FlowFiles currently queued in a connection.

        Drives NiFi's async listing-request (create → poll → delete). Each dict
        has ``uuid`` (to fetch detail), ``filename``, ``size`` (bytes), and
        ``position`` in the queue.
        """
        base = f"/flowfile-queues/{connection_id}/listing-requests"
        req = self._request("POST", base).json()["listingRequest"]
        req_id = req["id"]
        try:
            deadline = time.monotonic() + _POLL_TIMEOUT_S
            while not req.get("finished"):
                if time.monotonic() > deadline:
                    raise NiFiApiError(408, f"listing queue {connection_id} timed out")
                time.sleep(_POLL_INTERVAL_S)
                req = self._get_json(f"{base}/{req_id}")["listingRequest"]
            summaries = req.get("flowFileSummaries") or []
        finally:
            self._request("DELETE", f"{base}/{req_id}")
        return [
            {
                "uuid": s["uuid"],
                "filename": s.get("filename", ""),
                "size": s.get("size", 0),
                "position": s.get("position", 0),
            }
            for s in summaries[:max_results]
        ]

    def flowfile_detail(self, connection_id: str, uuid: str) -> dict:
        """A queued FlowFile's attributes *and* content payload, in one call."""
        ff = self._get_json(
            f"/flowfile-queues/{connection_id}/flowfiles/{uuid}"
        )["flowFile"]
        size = ff.get("size", 0)
        return {
            "uuid": uuid,
            "filename": ff.get("filename", ""),
            "size": size,
            "attributes": dict(ff.get("attributes") or {}),
            "content": self._content(
                f"/flowfile-queues/{connection_id}/flowfiles/{uuid}/content", size
            ),
        }

    def _content(self, path: str, size: int) -> str:
        """Fetch a content payload, tolerating empty/unavailable content.

        NiFi returns 409 when asked for the content of a zero-byte FlowFile
        ("FlowFile Size is not set" — it can't write a provenance record for it),
        so skip the call when ``size`` is 0, and degrade any other content error
        to a note rather than losing the attributes view that came with it.
        """
        if not size:
            return ""
        try:
            resp = self._request("GET", path)
        except NiFiApiError as exc:
            return f"(content unavailable: {exc})"
        return getattr(resp, "text", "") or ""

    def recent_events(self, component_id: str, max_results: int = 25) -> List[dict]:
        """Recent provenance events for a component, newest first.

        Mirrors "View data provenance" on a processor — the click-path this is
        meant to replace — scoped to one component via a provenance query
        (create → poll → delete).
        """
        body = {"provenance": {"request": {
            "searchTerms": {"ProcessorID": {"value": component_id, "inverse": False}},
            "maxResults": max_results,
            "summarize": True,
        }}}
        prov = self._request("POST", "/provenance", json=body).json()["provenance"]
        prov_id = prov["id"]
        try:
            deadline = time.monotonic() + _POLL_TIMEOUT_S
            while not prov.get("finished"):
                if time.monotonic() > deadline:
                    raise NiFiApiError(408, f"provenance query for {component_id} timed out")
                time.sleep(_POLL_INTERVAL_S)
                prov = self._get_json(f"/provenance/{prov_id}")["provenance"]
            events = ((prov.get("results") or {}).get("provenanceEvents")) or []
        finally:
            self._request("DELETE", f"/provenance/{prov_id}")
        return [
            {
                "event_id": e["eventId"],
                "event_type": e.get("eventType", ""),
                "time": e.get("eventTime", ""),
                "component": e.get("componentName", ""),
                "uuid": e.get("flowFileUuid", ""),
            }
            for e in events
        ]

    def event_detail(self, event_id: str) -> dict:
        """A provenance event's attributes and (output) content payload."""
        ev = self._get_json(f"/provenance-events/{event_id}")["provenanceEvent"]
        attributes = {
            a["name"]: a.get("value", "") for a in ev.get("attributes") or []
        }
        # fileSize is a human string ("0 bytes"); fileSizeBytes is the number we
        # need for the zero-content guard.
        size = ev.get("fileSizeBytes", 0) or 0
        return {
            "event_type": ev.get("eventType", ""),
            "filename": attributes.get("filename", ""),
            "size": size,
            "attributes": attributes,
            "content": self._content(
                f"/provenance-events/{event_id}/content/output", size
            ),
        }

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
        """Apply ``flow`` under its ``parent_pg``; returns the group id.

        Two strategies, chosen automatically:

        * **In-place rebuild** when the target group already exists *and is
          under NiFi Registry version control*. The group id and its registry
          linkage are preserved — only the contents are swapped — so the push
          shows up as *local changes* you can review and commit, instead of
          orphaning a brand-new group that has to be re-added to version
          control by hand.
        * **Delete-and-recreate** otherwise (a fresh group, or one not under
          version control). Simpler, and there's nothing to lose.
        """
        parent_id = self.resolve_group(flow.parent_pg or "root")

        position = {"x": 0.0, "y": 0.0}
        existing = [c for c in self._child_groups(parent_id) if c["name"] == flow.name]
        if existing:
            pg_id = existing[0]["id"]
            position = existing[0].get("position") or position
            if self._under_version_control(pg_id):
                return self._push_in_place(pg_id, flow, start=start, secrets=secrets)
            logger.info("Replacing existing group %r (%s)", flow.name, pg_id)
            self._teardown(pg_id)

        snapshot = json.loads(flow.to_json())
        self._align_bundles(snapshot)
        new_id = self._create_from_snapshot(parent_id, flow.name, snapshot, position)
        flow.nifi_id = new_id
        logger.info("Created group %r (%s) on NiFi %s", flow.name, new_id, self.version())

        self.apply_parameters(flow, secrets)

        if start:
            self.enable_services(new_id)
            self.start_group(new_id)
        return new_id

    # ----------------------------------------------------- in-place rebuild

    def _under_version_control(self, pg_id: str) -> bool:
        """Is ``pg_id`` tracked by a NiFi Registry flow?"""
        component = self._pg_entity(pg_id).get("component", {})
        return bool(component.get("versionControlInformation"))

    def _major_version(self) -> int:
        """Leading integer of the server version (``1`` for ``"1.24.0"``)."""
        try:
            return int(self.version().split(".", 1)[0])
        except (ValueError, AttributeError):
            return 0

    def _push_in_place(
        self,
        pg_id: str,
        flow: Flow,
        *,
        start: bool,
        secrets: Union[None, dict, str, Path],
    ) -> str:
        """Swap a versioned group's contents *without* deleting the group.

        The vehicle that drops a flow's components (services + wiring) straight
        *into* an existing group differs by line: NiFi 1.x uses templates
        (removed in 2.x); NiFi 2.x uses copy/paste (``PUT .../paste``), which is
        the supported template replacement. Either way the group id and its
        ``versionControlInformation`` are preserved, so the push shows up as
        *local changes* to review and commit.
        """
        logger.info("In-place rebuild of versioned group %r (%s)", flow.name, pg_id)
        self._set_group_state(pg_id, "STOPPED")
        self._empty_queues(pg_id)
        self._empty_group_contents(pg_id)
        if self._major_version() >= 2:
            self._paste_into_group(pg_id, flow)
        else:
            self._instantiate_template(pg_id, flow)
        flow.nifi_id = pg_id

        self.apply_parameters(flow, secrets)

        if start:
            self.enable_services(pg_id)
            self.start_group(pg_id)
        logger.info(
            "Rebuilt %r in place; group id and version control preserved "
            "(commit the local changes in the Registry to save a version)",
            flow.name,
        )
        return pg_id

    def _empty_group_contents(self, pg_id: str) -> None:
        """Delete everything *inside* ``pg_id`` but keep the group itself.

        Order matters: connections reference their endpoints, so they go first;
        then the canvas components; then child groups (recursively); finally the
        group-scoped controller services (disabled first). The group and its
        ``versionControlInformation`` are left untouched.
        """
        flow = self._get_json(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
        for conn in flow.get("connections", []):
            self._delete_component("connections", conn["id"])
        for proc in flow.get("processors", []):
            self._delete_component("processors", proc["id"])
        for port in flow.get("inputPorts", []):
            self._delete_component("input-ports", port["id"])
        for port in flow.get("outputPorts", []):
            self._delete_component("output-ports", port["id"])
        for funnel in flow.get("funnels", []):
            self._delete_component("funnels", funnel["id"])
        for label in flow.get("labels", []):
            self._delete_component("labels", label["id"])
        for child in flow.get("processGroups", []):
            self._teardown(child["component"]["id"])

        self._disable_services(pg_id)
        for svc in self._group_owned_services(pg_id):
            self._delete_component("controller-services", svc["id"])

    def _group_owned_services(self, pg_id: str) -> List[dict]:
        """Controller services defined *on* ``pg_id`` (not inherited from above)."""
        services = self._get_json(
            f"/flow/process-groups/{pg_id}/controller-services"
        ).get("controllerServices", [])
        return [
            s["component"]
            for s in services
            if s.get("component", {}).get("parentGroupId") == pg_id
        ]

    def _delete_component(self, kind: str, comp_id: str) -> None:
        """Delete a single component, fetching its current revision first."""
        version = self._get_json(f"/{kind}/{comp_id}")["revision"]["version"]
        self._request(
            "DELETE",
            f"/{kind}/{comp_id}",
            params={
                "version": version,
                "clientId": "niflow",
                "disconnectedNodeAcknowledged": "false",
            },
        )

    def _instantiate_template(self, pg_id: str, flow: Flow) -> None:
        """Upload ``flow`` as a template and drop its contents into ``pg_id``.

        NiFi 1.x only. The components land directly inside ``pg_id`` (no extra
        nesting), services and connections included. The temporary template is
        deleted afterwards so it doesn't linger in the template registry.
        """
        # Templates are instance-global and keyed by name, so an earlier
        # interrupted push can leave one behind and 409 the upload (and even
        # block deleting the group). Clear any same-named template first.
        self._delete_templates_named(flow.name)
        # NiFi 1.x returns the template-upload result as XML (a <templateEntity>),
        # not JSON — parse the id out of it rather than calling .json().
        upload = self._request(
            "POST",
            f"/process-groups/{pg_id}/templates/upload",
            files={"template": (f"{flow.name}.xml", flow.to_xml(), "application/xml")},
        )
        template_id = ElementTree.fromstring(upload.text).findtext("template/id")
        try:
            self._request(
                "POST",
                f"/process-groups/{pg_id}/template-instance",
                json={
                    "templateId": template_id,
                    "originX": 0.0,
                    "originY": 0.0,
                    "disconnectedNodeAcknowledged": False,
                },
            )
        finally:
            self._request("DELETE", f"/templates/{template_id}")

    def _delete_templates_named(self, name: str) -> None:
        """Delete every instance-global template whose name is ``name`` (1.x)."""
        templates = self._get_json("/flow/templates").get("templates", [])
        for entity in templates:
            if entity.get("template", {}).get("name") == name:
                self._request("DELETE", f"/templates/{entity['id']}")

    def _paste_into_group(self, pg_id: str, flow: Flow) -> None:
        """Inject ``flow``'s components into ``pg_id`` via NiFi 2.x copy/paste.

        Paste (``PUT /process-groups/{id}/paste``) is the 2.x replacement for
        templates, but it carries only *references* to a group's own controller
        services, not their definitions. So we recreate the group-level services
        first, remap every reference (processor properties and inter-service
        properties) to the new ids, and hand paste those ids as
        ``externalControllerServiceReferences`` so it wires them back up.
        Components nested in child groups bring their own services along.
        """
        snapshot = json.loads(flow.to_json())
        self._align_bundles(snapshot)
        contents = snapshot["flowContents"]

        id_map, ext_refs = self._recreate_group_services(
            pg_id, contents.get("controllerServices") or []
        )
        self._remap_service_refs(contents, id_map)

        copy_response = {
            key: contents.get(key) or []
            for key in (
                "processGroups", "processors", "inputPorts",
                "outputPorts", "connections", "labels", "funnels",
            )
        }
        copy_response["externalControllerServiceReferences"] = ext_refs
        revision = self._pg_entity(pg_id)["revision"]
        self._request(
            "PUT",
            f"/process-groups/{pg_id}/paste",
            json={
                "copyResponse": copy_response,
                "revision": {"version": revision["version"], "clientId": "niflow"},
            },
        )

    def _recreate_group_services(
        self, pg_id: str, service_dtos: List[dict]
    ) -> Tuple[Dict[str, str], Dict[str, dict]]:
        """Create ``pg_id``'s group-level controller services from the snapshot.

        Returns ``(versioned_id -> new_id, externalControllerServiceReferences)``.
        Done in two passes: create the bare services first (their properties may
        reference each other, and those ids don't exist until all are created),
        then set properties with inter-service references remapped.
        """
        id_map: Dict[str, str] = {}
        ext_refs: Dict[str, dict] = {}
        for svc in service_dtos:
            component = {"name": svc["name"], "type": svc["type"]}
            if svc.get("bundle"):
                component["bundle"] = svc["bundle"]
            created = self._request(
                "POST",
                f"/process-groups/{pg_id}/controller-services",
                json={"revision": {"version": 0, "clientId": "niflow"}, "component": component},
            ).json()
            new_id = created["component"]["id"]
            id_map[svc["identifier"]] = new_id
            ext_refs[new_id] = {"identifier": new_id, "name": svc["name"]}

        for svc in service_dtos:
            props = svc.get("properties") or {}
            if not props:
                continue
            new_id = id_map[svc["identifier"]]
            remapped = {k: id_map.get(v, v) for k, v in props.items()}
            revision = self._get_json(f"/controller-services/{new_id}")["revision"]
            self._request(
                "PUT",
                f"/controller-services/{new_id}",
                json={"revision": revision, "component": {"id": new_id, "properties": remapped}},
            )
        return id_map, ext_refs

    def _remap_service_refs(self, group: dict, id_map: Dict[str, str]) -> None:
        """Rewrite processor service-ref property values (versioned id -> new id),
        recursing into child groups. Only values matching a recreated group-level
        service are touched; services owned by nested groups are left for paste."""
        if not id_map:
            return
        for proc in group.get("processors") or []:
            props = proc.get("properties")
            if not props:
                continue
            for key, value in list(props.items()):
                if value in id_map:
                    props[key] = id_map[value]
        for child in group.get("processGroups") or []:
            self._remap_service_refs(child, id_map)

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

    # --------------------------------------------------------------- bundles

    def bundle_index(self) -> Dict[str, dict]:
        """Map every installed ``type`` to its real NAR bundle on this instance.

        Built from ``/flow/processor-types`` + ``/flow/controller-service-types``
        (version-agnostic, so it works on 1.x and 2.x). Cached for the client's
        lifetime — the installed NAR set doesn't change mid-session.
        """
        if self._bundle_index is None:
            index: Dict[str, dict] = {}
            for endpoint, key in (
                ("/flow/processor-types", "processorTypes"),
                ("/flow/controller-service-types", "controllerServiceTypes"),
            ):
                for dto in self._get_json(endpoint).get(key, []):
                    bundle, type_str = dto.get("bundle"), dto.get("type")
                    if type_str and bundle:
                        index.setdefault(type_str, {
                            "group": bundle.get("group", ""),
                            "artifact": bundle.get("artifact", ""),
                            "version": bundle.get("version", ""),
                        })
            self._bundle_index = index
        return self._bundle_index

    def _align_bundles(self, snapshot: dict) -> None:
        """Rewrite every component's bundle to the target instance's real NAR.

        The offline emitter guesses bundle coordinates (and a placeholder
        version); the *target* is authoritative. Matching each type to the
        instance's installed NAR is what lets a flow import cleanly across NiFi
        1.x/2.x — a wrong artifact or version is exactly what yields the
        "is not a valid processor type" rejection. Types the instance doesn't
        know are left untouched (let NiFi report them honestly).
        """
        index = self.bundle_index()
        if not index:
            return

        def stamp(component: dict) -> None:
            target = index.get(component.get("type"))
            if target:
                component["bundle"] = dict(target)

        def walk(group: dict) -> None:
            for comp in group.get("processors") or []:
                stamp(comp)
            for comp in group.get("controllerServices") or []:
                stamp(comp)
            for child in group.get("processGroups") or []:
                walk(child)

        contents = snapshot.get("flowContents")
        if isinstance(contents, dict):
            walk(contents)

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

    # ------------------------------------------------- registry / versioning

    def create_registry_client(
        self, name: str, url: str, description: str = ""
    ) -> str:
        """Register a NiFi Registry with this NiFi; returns the registry-client id.

        The request body differs across lines: NiFi 1.x takes a flat ``uri``;
        NiFi 2.x turned registry clients into extensions, so it wants a ``type``
        plus a ``url`` *property*. If a client for ``url`` already exists we
        reuse it rather than erroring on a duplicate.
        """
        for existing in self._get_json("/controller/registry-clients").get(
            "registries", []
        ):
            comp = existing.get("component", {})
            if comp.get("uri") == url or (comp.get("properties") or {}).get("url") == url:
                return comp["id"]

        if self._major_version() >= 2:
            component = {
                "name": name,
                "type": "org.apache.nifi.registry.flow.NifiRegistryFlowRegistryClient",
                "properties": {"url": url},
                "description": description,
            }
        else:
            component = {"name": name, "uri": url, "description": description}
        resp = self._request(
            "POST",
            "/controller/registry-clients",
            json={"revision": {"version": 0, "clientId": "niflow"}, "component": component},
        )
        return resp.json()["component"]["id"]

    def list_registry_buckets(self, registry_id: str) -> List[dict]:
        """Buckets visible through ``registry_id``, as ``{id, name}`` dicts.

        NiFi proxies this to the registry, so it works without talking to the
        registry's own API. (Creating a bucket is *not* proxied — do that
        against the registry directly.)
        """
        entity = self._get_json(f"/flow/registries/{registry_id}/buckets")
        out = []
        for b in entity.get("buckets", []):
            bucket = b.get("bucket") or b.get("component") or b
            out.append({"id": bucket.get("id") or b.get("id"), "name": bucket.get("name", "")})
        return out

    def start_version_control(
        self,
        pg_id: str,
        registry_id: str,
        bucket_id: str,
        flow_name: str,
        comment: str = "",
    ) -> dict:
        """Place ``pg_id`` under version control in the given bucket.

        Returns the resulting ``versionControlInformation``. Use this to put a
        group under control from code (the integration tests need a versioned
        group to push against).
        """
        revision = self._pg_entity(pg_id)["revision"]
        body = {
            "processGroupRevision": {
                "version": revision["version"],
                "clientId": "niflow",
            },
            "versionedFlow": {
                "registryId": registry_id,
                "bucketId": bucket_id,
                "flowName": flow_name,
                "comments": comment,
                "description": "",
                # Required on both lines ("Action is required" 400 without it).
                "action": "COMMIT",
            },
        }
        resp = self._request("POST", f"/versions/process-groups/{pg_id}", json=body)
        return resp.json().get("versionControlInformation", {})

    def version_control_state(self, pg_id: str) -> Optional[str]:
        """The registry sync state of ``pg_id`` (``UP_TO_DATE``,
        ``LOCALLY_MODIFIED``, ``STALE``, …), or ``None`` if not versioned."""
        info = self._get_json(f"/versions/process-groups/{pg_id}").get(
            "versionControlInformation"
        )
        return info.get("state") if info else None

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

    def quiesce_group(self, group: str = "root") -> str:
        """Fully quiesce ``group``: stop processors, disable services, drain queues.

        Leaves the group in the exact state NiFi requires before it can be
        deleted (nothing running, no enabled services, no queued FlowFiles) —
        without deleting it. Returns NiFi's drop summary, e.g. ``"12 / 4.2 KB"``.
        """
        pg_id = self.resolve_group(group)
        self._set_group_state(pg_id, "STOPPED")
        self._disable_services(pg_id)
        dropped = self._empty_queues(pg_id)
        logger.info("Quiesced group %r (%s dropped)", group, dropped)
        return dropped

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

    def purge_queues(self, group: str = "root") -> str:
        """Drop the contents of every queue under ``group`` (recursive).

        Returns NiFi's summary of what was dropped, e.g. ``"12 / 4.2 KB"``.
        """
        dropped = self._empty_queues(self.resolve_group(group))
        logger.info("Purged queues under %r: %s dropped", group, dropped)
        return dropped

    def _has_connections(self, pg_id: str) -> bool:
        """Whether any connection exists under ``pg_id`` (recursive)."""
        flow = self._get_json(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
        if flow.get("connections"):
            return True
        return any(
            self._has_connections(child["component"]["id"])
            for child in flow.get("processGroups", [])
        )

    def _empty_queues(self, pg_id: str) -> str:
        # NiFi 1.x returns 500 from empty-all-connections-requests when the group
        # has no connections at all; skip the request rather than trip on it.
        if not self._has_connections(pg_id):
            return ""
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
        return req.get("dropped", "")

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
