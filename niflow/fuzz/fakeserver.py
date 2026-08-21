"""An in-memory stand-in for NiFi, good enough to run the incremental applier.

The applier (:mod:`niflow.apply`) is the one part of niflow that mutates a live
server, and every one of its REST calls can fail: a 409 on a stale revision, a
409 because something started running, a 500, a dropped connection. Until this
existed, that path was only ever exercised against a real NiFi at tier 3, where
faults cannot be induced on demand — so what the applier does *when a call
fails* was untested.

:class:`FakeServer` answers the handful of client methods the applier actually
uses (see ``self.client.`` in ``apply.py``), records every mutating call, and
can be told to fail on the Nth one. It deliberately models very little: the
question these checks ask is not "did NiFi end up in the right state" — tier 3
answers that — but "when a call fails partway through, does the applier still
report honestly, or does it leak an unreadable exception".

Not a test double for *behaviour*: it fabricates ids and echoes components back
without validating them, so a case that would be rejected by a real server
still runs here. That is the point — the fault, not the flow, is the subject.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from niflow.rest.common import NiFiApiError


class _Response:
    """The tiny slice of ``requests.Response`` the applier touches."""

    def __init__(self, payload: dict):
        self._payload = payload

    def json(self) -> dict:
        return self._payload


class FakeServer:
    """A NiFi-shaped object that can fail on demand.

    ``fail_at`` is a 1-based index into the *mutating* calls (POST/PUT/DELETE
    plus the helpers that stand in for them); ``None`` never fails. The failure
    is a :class:`~niflow.rest.common.NiFiApiError`, the same type a real server
    produces, so the applier's own error handling is what gets exercised.
    """

    def __init__(self, fail_at: Optional[int] = None, major_version: int = 2,
                 status: int = 409):
        self.fail_at = fail_at
        self.major_version = major_version
        self.status = status
        self.mutations = 0            # how many mutating calls were made
        self.ops: List[str] = []      # a readable log, for a finding's detail
        self.failed_on: Optional[str] = None
        self._counter = 0
        # Components created by a subtree instantiation, so the applier can
        # resolve them again: it looks a live endpoint up by *listing* the
        # group it landed in (see apply._live_id), which is the one piece of
        # server state a fault check genuinely needs modelled.
        self.listings: Dict[str, Dict[str, List[dict]]] = {}
        self.children: Dict[str, List[dict]] = {}

    # --- internals ------------------------------------------------------

    def _new_id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}-{self._counter}"

    def _mutate(self, description: str) -> None:
        """Count one mutating call, and fail it when it is the chosen one."""
        self.mutations += 1
        self.ops.append(description)
        if self.fail_at is not None and self.mutations == self.fail_at:
            self.failed_on = description
            raise NiFiApiError(self.status, f"injected fault on {description}")

    # --- the client seam the applier uses --------------------------------

    def _major_version(self) -> int:
        return self.major_version

    def _register_subtree(self, gid: str, contents: dict) -> None:
        """Record a created group's contents the way NiFi's listings report them."""
        listing = {"processors": [], "inputPorts": [], "outputPorts": [], "funnels": []}
        for key, dtos in (("processors", contents.get("processors")),
                          ("inputPorts", contents.get("inputPorts")),
                          ("outputPorts", contents.get("outputPorts")),
                          ("funnels", contents.get("funnels"))):
            for dto in dtos or []:
                comp_id = self._new_id("live")
                listing[key].append({
                    "id": comp_id,
                    "position": dto.get("position") or {"x": 0.0, "y": 0.0},
                    "component": {"id": comp_id, "name": dto.get("name", "")},
                })
        self.listings[gid] = listing
        self.children[gid] = []
        for child in contents.get("processGroups") or []:
            child_id = self._new_id("group")
            self.children[gid].append({"id": child_id, "name": child.get("name", "")})
            self._register_subtree(child_id, child)

    def _get_json(self, path: str) -> dict:
        # Reads never fail: a read failure is not interesting here (it would
        # simply become the same ApplyError), and making them fail would hide
        # the mutating call the case is actually about.
        parts = path.strip("/").split("/")
        if len(parts) == 3 and parts[0] == "process-groups":
            key = {"processors": "processors", "input-ports": "inputPorts",
                   "output-ports": "outputPorts", "funnels": "funnels"}.get(parts[2])
            if key:
                return {key: list((self.listings.get(parts[1]) or {}).get(key) or [])}
        comp_id = path.rstrip("/").rsplit("/", 1)[-1]
        return {
            "id": comp_id,
            "revision": {"version": 1, "clientId": "niflow"},
            "component": {"id": comp_id, "state": "STOPPED",
                          "referencingComponents": [],
                          "source": {"id": "src", "type": "PROCESSOR"},
                          "destination": {"id": "dst", "type": "PROCESSOR"}},
            "status": {"aggregateSnapshot": {"activeThreadCount": 0}},
            "request": {"requestId": "req-1", "id": "req-1", "complete": True,
                        "finished": True, "percentCompleted": 100},
            "dropRequest": {"id": "req-1", "finished": True},
        }

    def _request(self, method: str, path: str, **kwargs: Any) -> _Response:
        if method.upper() in ("POST", "PUT", "DELETE"):
            self._mutate(f"{method.upper()} {path}")
        body = (kwargs.get("json") or {}).get("component") or {}
        new_id = body.get("id") or self._new_id("comp")
        return _Response({
            "id": new_id,
            "revision": {"version": 1, "clientId": "niflow"},
            "component": dict(body, id=new_id),
            "request": {"requestId": "req-1", "id": "req-1", "complete": True,
                        "finished": True, "failureReason": None},
            "dropRequest": {"id": "req-1", "finished": True},
        })

    def _delete_component(self, kind: str, comp_id: str) -> None:
        self._mutate(f"DELETE /{kind}/{comp_id}")

    def _set_processor_state(self, proc_id: str, state: str) -> None:
        self._mutate(f"state {proc_id} -> {state}")

    def stop_processor(self, proc_id: str) -> None:
        self._mutate(f"stop {proc_id}")

    def drain_connection(self, conn_id: str) -> None:
        self._mutate(f"drain {conn_id}")

    def _teardown(self, pg_id: str) -> None:
        self._mutate(f"teardown {pg_id}")

    def _create_from_snapshot(self, parent_id: str, name: str, snapshot: dict,
                              position: dict) -> str:
        self._mutate(f"create-subtree {name} under {parent_id}")
        gid = self._new_id("group")
        self.children.setdefault(parent_id, []).append({"id": gid, "name": name})
        self._register_subtree(gid, snapshot.get("flowContents") or snapshot)
        return gid

    def _align_bundles(self, snapshot: dict) -> None:
        return None

    def _child_groups(self, pg_id: str) -> List[dict]:
        return list(self.children.get(pg_id) or [])

    def _find_context_entity(self, name: str) -> Dict[str, Any]:
        return {"id": f"ctx-{name}", "revision": {"version": 0},
                "component": {"id": f"ctx-{name}", "name": name, "parameters": []}}

    def bundle_index(self) -> Dict[str, dict]:
        return {}
