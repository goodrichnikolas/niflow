"""Runtime inspection & component ops: state, queues, FlowFiles, provenance."""
from __future__ import annotations

import time
from typing import Iterator, List, Optional, Tuple

from niflow.rest.common import (
    _POLL_INTERVAL_S,
    _POLL_TIMEOUT_S,
    NiFiApiError,
)
from niflow.utils import get_logger

logger = get_logger()

# NiFi answers a provenance query with an arbitrary subset once it hits
# maxResults (see _provenance_newest), so the cap is escalated by this factor
# until the answer is complete — never past the ceiling, which exists so a
# component with a million events cannot turn one click into a heavy query.
_PROV_ESCALATION = 10
_PROV_RESULT_CEILING = 5000


def _event_order(event: dict) -> int:
    """Provenance event ids are monotonic integers — and event *times* are
    lossy strings, so ordering must go through the id. Non-numeric ids (only
    ever seen in fixtures) sort last rather than raising."""
    try:
        return int(event.get("eventId"))
    except (TypeError, ValueError):
        return 1 << 62


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
        """Components under ``group`` with validation errors (yellow triangles).

        Each entry carries ``id``/``name``/``path``/``kind`` plus the ``errors``
        list NiFi shows in the component tooltip.

        **Controller services count.** They were missing here, which meant a
        service that cannot start — the single most common reason a whole flow
        sits idle — was invisible to the Errors panel, to ``validate --live``
        and to the fuzz harness (which then read niflow's own, correct,
        complaint about it as a false positive).
        """
        out = [
            {
                "id": comp["id"],
                "name": comp.get("name", ""),
                "path": path,
                "group_id": group_id,
                "kind": "processor",
                "errors": list(comp.get("validationErrors") or []),
            }
            for path, group_id, comp in self.walk_processors(group)
            if comp.get("validationErrors")
        ]
        out.extend(
            {
                "id": comp["id"],
                "name": comp.get("name", ""),
                "path": path,
                "group_id": group_id,
                "kind": "controller_service",
                "errors": list(comp.get("validationErrors") or []),
            }
            for path, group_id, comp in self.walk_services(group)
            if comp.get("validationErrors")
        )
        return out

    def walk_services(self, group: str = "root") -> Iterator[Tuple[str, str, dict]]:
        """Yield ``(group_path, group_id, component)`` for every controller
        service under ``group``, depth-first — the service twin of
        :meth:`walk_processors`.

        Services are not part of ``ProcessGroupFlowDTO``, so each group needs
        its own ``/flow/process-groups/{id}/controller-services`` read. The
        endpoint reports services from ancestor groups too (they are in scope
        for the group), so anything whose own ``parentGroupId`` is elsewhere is
        skipped — otherwise a root-level service is reported once per group
        beneath it.
        """

        def visit(pg_id: str, prefix: str) -> Iterator[Tuple[str, str, dict]]:
            try:
                services = self._get_json(
                    f"/flow/process-groups/{pg_id}/controller-services"
                ).get("controllerServices", [])
            except Exception as exc:
                # A user with no read permission on services (or an older
                # endpoint) must not take the whole Errors panel down with it.
                logger.debug("No controller services readable for %s: %s", pg_id, exc)
                services = []
            for entity in services:
                comp = entity.get("component") or {}
                if comp.get("parentGroupId") not in (None, pg_id):
                    continue
                yield prefix, pg_id, comp
            flow = self._get_json(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
            for child in flow.get("processGroups", []):
                child_comp = child["component"]
                path = f"{prefix}/{child_comp['name']}" if prefix else child_comp["name"]
                yield from visit(child_comp["id"], path)

        yield from visit(self.resolve_group(group), "")

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

        Callers should check :meth:`processor_validation` first: NiFi accepts
        RUN_ONCE on an **invalid** processor with a 200 and does nothing —
        and on 2.7.2 it then wedges in ``RUN_ONCE``, where ``RUNNING`` 409s
        and a config change is refused with "cannot modify … while the
        Processor is running". Running one blindly can therefore lock the
        very property that needs fixing.
        """
        self.stop_processor(proc_id)
        self._set_processor_state(proc_id, "RUN_ONCE")

    def processor_validation(self, proc_id: str) -> dict:
        """One processor's ``{"state", "status", "errors"}`` in a single call.

        ``validation_errors`` walks a whole group; this asks about one
        component, which is what a step needs before it runs anything.
        """
        comp = self._get_json(f"/processors/{proc_id}")["component"]
        return {
            "state": comp.get("state", ""),
            "status": comp.get("validationStatus", ""),
            "errors": [str(e) for e in comp.get("validationErrors") or []],
        }

    def connection_end(self, conn_id: str, which: str) -> dict:
        """One end of a connection: the raw ``source``/``destination`` ref.

        ``which`` is ``"source"`` or ``"destination"``; the ref carries
        ``id``/``name``/``type`` (``PROCESSOR``/``OUTPUT_PORT``/``FUNNEL``/…).
        Resolved per call because queue listings come from the status
        snapshot, which carries endpoint names but not ids.
        """
        return self._get_json(f"/connections/{conn_id}")["component"].get(which) or {}

    def connection_relationships(self, conn_id: str) -> List[str]:
        """The relationships a connection carries (``selectedRelationships``).

        The stepper needs this because NiFi's CLONE/FORK events carry no
        relationship (verified on 1.24 and 2.7.2): a fork child's branch name
        — the "failure" an analyst thinks in — is a property of the connection
        it landed in, not of the event that spawned it.
        """
        comp = self._get_json(f"/connections/{conn_id}")["component"]
        return list(comp.get("selectedRelationships") or [])

    def run_queue_endpoint_once(self, conn_id: str, which: str) -> str:
        """Run-once the processor at one end of a queue; returns its name.

        ``which`` is ``"source"`` (feeds the queue) or ``"destination"``
        (consumes it) — the queue-centric view of :meth:`run_processor_once`.
        Raises when that end isn't a processor (funnels and ports have no
        run-once).
        """
        end = self.connection_end(conn_id, which)
        if end.get("type") != "PROCESSOR":
            kind = (end.get("type") or "unknown").replace("_", " ").lower()
            raise NiFiApiError(400, f"the queue's {which} is a {kind}, not a processor")
        self.run_processor_once(end["id"])
        return end.get("name", "")

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

    def drain_connection(self, conn_id: str) -> str:
        """Drop every FlowFile queued in one connection (async drop-request).

        Returns NiFi's summary of what went, e.g. ``"12 / 4.2 KB"`` — the same
        ``dropped`` figure :meth:`_empty_queues` reports for a whole group, so
        callers can tell the user how much they just destroyed.
        """
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
        return req.get("dropped", "")

    def list_queues(self, group: str = "root") -> List[dict]:
        """Every connection (queue) under ``group``, with its queued counts.

        Each dict carries ``id`` (the connection id, used to list contents),
        ``source``/``destination`` names, the group ``path``, and ``queued``
        (FlowFile count) / ``queued_label`` (NiFi's "n / size" string).

        It also carries the ids needed to deep-link into the NiFi canvas:
        ``group_id`` (the group the connection is drawn in) plus
        ``source_id``/``destination_id`` and their own
        ``source_group_id``/``destination_group_id`` — an endpoint can live in
        a *different* group from the connection (a child group's port), and
        the status snapshot doesn't say, so those fall back to ``group_id``.

        One recursive-status call when the server supports it; per-group
        walk otherwise.
        """
        out: List[dict] = []
        snapshot = self._recursive_status(self.resolve_group(group))
        if snapshot is not None:
            def visit_snap(snap: dict, prefix: str) -> None:
                for wrapper in snap.get("connectionStatusSnapshots", []):
                    conn = wrapper.get("connectionStatusSnapshot") or {}
                    group_id = conn.get("groupId", "")
                    out.append({
                        "id": conn["id"],
                        "source": conn.get("sourceName", ""),
                        "destination": conn.get("destinationName", ""),
                        "path": prefix,
                        "queued": conn.get("flowFilesQueued", 0),
                        "queued_label": conn.get("queued", ""),
                        "group_id": group_id,
                        "source_id": conn.get("sourceId", ""),
                        "source_group_id": group_id,
                        "destination_id": conn.get("destinationId", ""),
                        "destination_group_id": group_id,
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
                src = comp.get("source") or {}
                dst = comp.get("destination") or {}
                group_id = comp.get("parentGroupId", "") or pg_id
                snap = (entity.get("status") or {}).get("aggregateSnapshot") or {}
                out.append({
                    "id": entity["id"],
                    "source": src.get("name", ""),
                    "destination": dst.get("name", ""),
                    "path": prefix,
                    "queued": snap.get("flowFilesQueued", 0),
                    "queued_label": snap.get("queued", ""),
                    "group_id": group_id,
                    "source_id": src.get("id", ""),
                    "source_group_id": src.get("groupId", "") or group_id,
                    "destination_id": dst.get("id", ""),
                    "destination_group_id": dst.get("groupId", "") or group_id,
                })
            for child in flow.get("processGroups", []):
                c = child["component"]
                path = f"{prefix}/{c['name']}" if prefix else c["name"]
                visit(c["id"], path)

        visit(self.resolve_group(group), "")
        return out

    def list_ports(self, group: str = "root") -> List[dict]:
        """Input and output ports under ``group``, depth-first.

        Each dict carries ``kind`` (``input_port``/``output_port``),
        ``id``/``name``/``state``, the group ``path`` and the ``group_id`` the
        port lives in. Ports are the other way a FlowFile enters a group, so
        the stepper's start-point menu lists them beside source processors.
        """
        out: List[dict] = []

        def visit(pg_id: str, prefix: str) -> None:
            flow = self._get_json(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
            for kind, key in (("input_port", "inputPorts"), ("output_port", "outputPorts")):
                for entity in flow.get(key, []):
                    comp = entity.get("component", {})
                    out.append({
                        "kind": kind,
                        "id": comp.get("id", entity.get("id", "")),
                        "name": comp.get("name", ""),
                        "state": comp.get("state", ""),
                        "path": prefix,
                        "group_id": pg_id,
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
        has ``uuid`` (to fetch detail), ``filename``, ``size`` (bytes),
        ``position`` in the queue (**1-based**, as NiFi reports it) and
        ``penalized``/``penalty_expires_in`` — a penalised FlowFile is skipped
        by the scheduler, so it is why a run-once can look like it did
        nothing.

        **NiFi caps this listing at 100 FlowFiles per queue and the cap is not
        negotiable** — verified live on 1.24.0: a request body asking for
        ``maxResults: 500`` still comes back with ``maxResults: 100``. A queue
        holding thousands therefore shows only its first hundred, which is
        why :meth:`locate_flowfile` exists.
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
                "penalized": bool(s.get("penalized")),
                "penalty_expires_in": s.get("penaltyExpiresIn", 0) or 0,
            }
            for s in summaries[:max_results]
        ]

    def locate_flowfile(self, connection_id: str, uuid: str) -> Optional[dict]:
        """Is ``uuid`` in this queue? Works past the 100-file listing cap.

        NiFi will not list more than 100 FlowFiles from a queue, but it *will*
        answer for one by id: ``GET /flowfile-queues/{id}/flowfiles/{uuid}``
        resolves a FlowFile at any depth (verified live on 1.24.0 against a
        queue of 200, on a file the listing could not see). Returns the same
        shape :meth:`list_flowfiles` yields — minus ``position``, which the
        single-FlowFile DTO does not carry — or ``None`` when the queue does
        not hold it (NiFi answers 404).
        """
        try:
            ff = self._get_json(
                f"/flowfile-queues/{connection_id}/flowfiles/{uuid}"
            )["flowFile"]
        except NiFiApiError as exc:
            if getattr(exc, "status", None) in (404, 400):
                return None
            raise
        return {
            "uuid": ff.get("uuid", uuid),
            "filename": ff.get("filename", ""),
            "size": ff.get("size", 0),
            "position": None,  # unknown: NiFi only reports it in a listing
            "penalized": bool(ff.get("penalized")),
            "penalty_expires_in": ff.get("penaltyExpiresIn", 0) or 0,
        }

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

    def _provenance_query(self, search_terms: dict, max_results: int, what: str,
                          summarize: bool = True,
                          totals: Optional[dict] = None) -> List[dict]:
        """Run one async provenance query (create → poll → delete); raw DTOs.

        ``summarize=False`` asks NiFi for the *whole* event DTO — attributes,
        parent/child uuids, content availability — which is everything
        :meth:`_hop_from_event` needs, so a journey costs one query instead of
        one query plus a GET per event (verified on 1.24.0: the non-summarized
        payload carries ``attributes``/``parentUuids``/``childUuids``/
        ``contentEqual``/``inputContentAvailable``).

        ``totals`` (a dict the caller passes in) is filled with NiFi's
        ``total``/``totalCount`` so the caller can tell a complete answer from
        a capped one — NiFi reports ``total`` as the string ``"100+"`` when it
        hit ``maxResults``.
        """
        body = {"provenance": {"request": {
            "searchTerms": search_terms,
            "maxResults": max_results,
            "summarize": summarize,
        }}}
        prov = self._request("POST", "/provenance", json=body).json()["provenance"]
        prov_id = prov["id"]
        try:
            deadline = time.monotonic() + _POLL_TIMEOUT_S
            while not prov.get("finished"):
                if time.monotonic() > deadline:
                    raise NiFiApiError(408, f"provenance query for {what} timed out")
                time.sleep(_POLL_INTERVAL_S)
                prov = self._get_json(f"/provenance/{prov_id}")["provenance"]
            results = prov.get("results") or {}
            if totals is not None:
                totals["total"] = results.get("total", "")
                totals["total_count"] = results.get("totalCount", 0) or 0
                totals["oldest_event"] = results.get("oldestEvent", "")
            return results.get("provenanceEvents") or []
        finally:
            self._request("DELETE", f"/provenance/{prov_id}")

    def _provenance_newest(self, search_terms: dict, max_results: int, what: str,
                           summarize: bool = True,
                           ceiling: int = _PROV_RESULT_CEILING,
                           ) -> Tuple[List[dict], bool]:
        """The *newest* ``max_results`` matching events, ascending, + capped?

        **NiFi's ``maxResults`` does not mean "the newest N".** Measured on
        1.24.0 against a component with 800 events: asking for 10 returned
        event ids 932-1071 — from the previous day — while the newest was
        133249; asking for 250 returned a set with a hole in the middle. The
        cap is applied per index shard, so a capped answer is an arbitrary
        subset presented as if it were the whole story. That is why "recent
        events" could show ancient ones, and why a stepper polling a capped
        uuid query can see nothing new and call it an indexing lag.

        The fix is to stop asking for a capped answer: NiFi flags a capped
        result by reporting ``total`` as ``"N+"``, so the cap is raised until
        the answer is complete (or the ceiling is reached) and the newest
        ``max_results`` are then taken here, where the ordering is knowable.
        """
        cap = max(1, max_results)
        events: List[dict] = []
        capped = False
        while True:
            totals: dict = {}
            events = self._provenance_query(
                search_terms, cap, what, summarize=summarize, totals=totals)
            capped = str(totals.get("total") or "").endswith("+")
            if not capped or cap >= ceiling:
                break
            cap = min(cap * _PROV_ESCALATION, ceiling)
        events.sort(key=_event_order)
        return (events[-max_results:] if max_results else events), capped

    def recent_events(self, component_id: str, max_results: int = 25) -> List[dict]:
        """Recent provenance events for a component, newest first.

        Mirrors "View data provenance" on a processor — the click-path this is
        meant to replace — scoped to one component via a provenance query
        (create → poll → delete).
        """
        events, _capped = self._provenance_newest(
            {"ProcessorID": {"value": component_id, "inverse": False}},
            max_results, component_id,
        )
        events.reverse()   # newest first, as the caller (and NiFi's UI) show it
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

    def trace_flowfile(self, uuid: str, max_events: int = 1000) -> dict:
        """One FlowFile's provenance journey as ordered hops with attribute diffs.

        Events come from a ``FlowFileUUID`` provenance query, oldest first
        (event ids are monotonic, event *times* are lossy strings). Each hop
        carries ``parents``/``children`` for callers to jump traces along a
        branch instead of silently merging them.

        Per hop: the post-event ``attributes``, the ``changes`` that event
        made (``before`` is ``None`` for an attribute born there — NiFi omits
        ``previousValue`` for those, and sends ``previousValue == value`` for
        untouched ones), the ``relationship`` taken, and whether each payload
        side is still fetchable via :meth:`event_content`.

        **The query is a lineage query, not a uuid filter.** Verified live on
        1.24.0: asking for a split child's uuid also returns the *parent's*
        FORK event and the *merged* file's JOIN event, because they are on the
        same lineage. Every hop therefore carries ``flowfile_uuid`` (the
        FlowFile the event is really about) and ``own`` (``False`` when it
        belongs to a relative). Callers must not treat a relative's event as
        this file's own hop — it is how a merge is discovered, not something
        that happened to this FlowFile.

        Returns ``{"uuid", "hops", "truncated"}``. ``truncated`` is True when
        the journey is longer than ``max_events`` — the hops are then the
        **newest** ``max_events`` (see :meth:`_provenance_newest`; NiFi's own
        cap does not mean that, which is why the query is widened first).
        ``max_events`` defaults high because a whole journey is the point of a
        trace: 800 events came back in one round trip in 0.6s on 1.24.0.
        """
        totals: dict = {}
        hops = self.flowfile_events_since(uuid, -1, max_events, totals=totals)
        return {"uuid": uuid, "hops": hops,
                "truncated": bool(totals.get("capped")) or len(hops) >= max_events > 0}

    @staticmethod
    def _hop_from_event(ev: dict, of_uuid: str = "") -> dict:
        """A provenance-event DTO as the flat "hop" dict trace/follow render.

        ``of_uuid`` is the FlowFile the caller asked about; it decides ``own``.
        A ``FlowFileUUID`` query is a *lineage* query, so a fork parent's FORK
        event and a merge child's JOIN event come back too, describing a
        different FlowFile entirely.
        """
        attrs = ev.get("attributes") or []
        ff_uuid = ev.get("flowFileUuid", "") or ""
        return {
            "flowfile_uuid": ff_uuid,
            "own": (not of_uuid) or (not ff_uuid) or ff_uuid == of_uuid,
            "event_id": ev.get("eventId"),
            "event_type": ev.get("eventType", ""),
            "time": ev.get("eventTime", ""),
            "component": ev.get("componentName", ""),
            "component_id": ev.get("componentId", ""),
            # NiFi omits groupId once a component leaves the flow; the deep
            # link degrades to "select this id" rather than breaking.
            "group_id": ev.get("groupId", ""),
            "component_type": ev.get("componentType", ""),
            "relationship": ev.get("relationship") or "",
            "size": ev.get("fileSizeBytes", 0) or 0,
            "attributes": {a["name"]: a.get("value") for a in attrs},
            "changes": [
                {"name": a["name"], "before": a.get("previousValue"),
                 "after": a.get("value")}
                for a in attrs if a.get("previousValue") != a.get("value")
            ],
            "input_available": bool(ev.get("inputContentAvailable")),
            "output_available": bool(ev.get("outputContentAvailable")),
            "content_equal": ev.get("contentEqual"),
            "parents": list(ev.get("parentUuids") or []),
            "children": list(ev.get("childUuids") or []),
        }

    def flowfile_events_since(
        self, uuid: str, after_event_id: int = -1, max_events: int = 100,
        totals: Optional[dict] = None,
    ) -> List[dict]:
        """Hops for ``uuid``'s lineage above ``after_event_id``, oldest first
        (event ids are monotonic; event *times* are lossy strings).

        With the default ``-1`` this is the FlowFile's whole recorded journey
        (what :meth:`trace_flowfile` wraps). The live stepper calls it with
        the last event id it has already rendered: run-once a processor, then
        ask what just happened — provenance indexing is effectively instant
        (<0.2s live on 1.24 and 2.7.2), so new events show up on the first or
        second poll.

        The query asks for un-summarized events, so one round trip returns
        everything a hop needs. Servers that answer a non-summarized query
        without attributes still work: those events fall back to a per-event
        detail fetch.
        """
        events, capped = self._provenance_newest(
            {"FlowFileUUID": {"value": uuid, "inverse": False}}, max_events, uuid,
            summarize=False,
        )
        if totals is not None:
            totals["capped"] = capped
        hops = []
        for event in events[:max_events]:
            if int(event["eventId"]) <= after_event_id:
                continue
            if "attributes" not in event:  # summarized-only server: ask for detail
                event = self._get_json(
                    f"/provenance-events/{event['eventId']}")["provenanceEvent"]
            hops.append(self._hop_from_event(event, of_uuid=uuid))
        return hops

    def event_content(self, event_id, direction: str = "output") -> str:
        """One side of a provenance event's payload; ``input`` or ``output``.

        Returns ``""`` when the content repository no longer holds that claim
        (NiFi ages claims out independently of the provenance index).
        """
        ev = self._get_json(f"/provenance-events/{event_id}")["provenanceEvent"]
        if not ev.get(f"{direction}ContentAvailable"):
            return ""
        # fileSizeBytes describes the output side; the input size isn't in the
        # DTO, so only the output fetch gets the zero-byte guard.
        size = (ev.get("fileSizeBytes", 0) or 0) if direction == "output" else 1
        return self._content(
            f"/provenance-events/{event_id}/content/{direction}", size
        )
