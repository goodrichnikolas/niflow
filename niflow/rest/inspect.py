"""Runtime inspection & component ops: state, queues, FlowFiles, provenance."""
from __future__ import annotations

import time
from typing import Iterator, List, Optional, Tuple

from niflow.rest.common import (
    _POLL_INTERVAL_S,
    _POLL_TIMEOUT_S,
    NiFiApiError,
)


class InspectMixin:
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

    def set_port_state(self, kind: str, port_id: str, state: str) -> None:
        """Start/stop one port. ``kind`` is ``input_port`` or ``output_port``."""
        endpoint = "input-ports" if kind == "input_port" else "output-ports"
        revision = self._get_json(f"/{endpoint}/{port_id}")["revision"]
        self._request(
            "PUT",
            f"/{endpoint}/{port_id}/run-status",
            json={"revision": revision, "state": state, "disconnectedNodeAcknowledged": False},
        )

    def create_processor(self, pg_id: str, component: dict) -> str:
        """Create a single processor in a live group; returns its id.

        ``component`` is the raw REST DTO (``type``/``name``/``position``/
        ``config``). Used by the flow-test harness for its temporary
        injector; ``push``/``apply`` remain the way to build real flows.
        """
        entity = self._request(
            "POST",
            f"/process-groups/{pg_id}/processors",
            json={"revision": {"version": 0, "clientId": "niflow"}, "component": component},
        ).json()
        return entity["id"]

    def create_connection(
        self,
        pg_id: str,
        source: dict,
        destination: dict,
        relationships: Optional[List[str]] = None,
    ) -> str:
        """Create a connection between two live endpoint refs; returns its id.

        ``source``/``destination`` are ``{"id", "groupId", "type"}`` refs
        (type ``PROCESSOR``/``INPUT_PORT``/``OUTPUT_PORT``/``FUNNEL``).
        """
        component: dict = {"source": source, "destination": destination}
        if relationships and source.get("type") == "PROCESSOR":
            component["selectedRelationships"] = list(relationships)
        entity = self._request(
            "POST",
            f"/process-groups/{pg_id}/connections",
            json={"revision": {"version": 0, "clientId": "niflow"}, "component": component},
        ).json()
        return entity["id"]

    def drain_connection(self, conn_id: str) -> None:
        """Drop every FlowFile queued in one connection (async drop-request)."""
        req = self._request(
            "POST", f"/flowfile-queues/{conn_id}/drop-requests"
        ).json()["dropRequest"]
        req_id = req["id"]
        try:
            deadline = time.monotonic() + _POLL_TIMEOUT_S
            while not req.get("finished"):
                if time.monotonic() > deadline:
                    raise NiFiApiError(408, f"draining connection {conn_id} timed out")
                time.sleep(_POLL_INTERVAL_S)
                req = self._get_json(
                    f"/flowfile-queues/{conn_id}/drop-requests/{req_id}"
                )["dropRequest"]
        finally:
            self._request("DELETE", f"/flowfile-queues/{conn_id}/drop-requests/{req_id}")

    def list_queues(self, group: str = "root") -> List[dict]:
        """Every connection (queue) under ``group``, with its queued counts.

        Each dict carries ``id`` (the connection id, used to list contents),
        ``source``/``destination`` names, the group ``path``, and ``queued``
        (FlowFile count) / ``queued_label`` (NiFi's "n / size" string).

        One recursive-status call when the server supports it; per-group
        walk otherwise.
        """
        out: List[dict] = []
        snapshot = self._recursive_status(self.resolve_group(group))
        if snapshot is not None:
            def visit_snap(snap: dict, prefix: str) -> None:
                for wrapper in snap.get("connectionStatusSnapshots", []):
                    conn = wrapper.get("connectionStatusSnapshot") or {}
                    out.append({
                        "id": conn["id"],
                        "source": conn.get("sourceName", ""),
                        "destination": conn.get("destinationName", ""),
                        "path": prefix,
                        "queued": conn.get("flowFilesQueued", 0),
                        "queued_label": conn.get("queued", ""),
                    })
                for wrapper in snap.get("processGroupStatusSnapshots", []):
                    child = wrapper.get("processGroupStatusSnapshot") or {}
                    path = f"{prefix}/{child['name']}" if prefix else child["name"]
                    visit_snap(child, path)

            visit_snap(snapshot, "")
            return out

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
