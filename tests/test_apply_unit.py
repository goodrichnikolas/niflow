"""Unit tests for the incremental applier (niflow/apply.py).

A stateful fake stands in for NiFiClient at the helper-method seam the
applier uses (_get_json/_request/stop_processor/...), recording every
mutating operation so tests can assert both the calls and their order.
"""
from __future__ import annotations

import json
from typing import Dict, List

from niflow import Flow, InputPort, OutputPort
from niflow.apply import PlanApplier
from niflow.core import ControllerService, Processor
from niflow.plan import diff_flows


class FakeClient:
    """Just enough NiFiClient for PlanApplier: an entity store + an op log."""

    def __init__(self):
        self.ops: List[str] = []
        self.entities: Dict[str, dict] = {}  # "/processors/p1" -> entity
        self._counter = 0

    # --- helpers used by tests ------------------------------------------
    def seed_processor(self, comp_id: str, state: str = "STOPPED") -> None:
        self.entities[f"/processors/{comp_id}"] = {
            "revision": {"version": 1},
            "component": {"id": comp_id, "state": state},
            "status": {"aggregateSnapshot": {"activeThreadCount": 0}},
        }

    def seed_connection(self, comp_id: str, source: dict, destination: dict) -> None:
        self.entities[f"/connections/{comp_id}"] = {
            "revision": {"version": 1},
            "component": {"id": comp_id, "source": source, "destination": destination},
        }

    def seed_service(self, comp_id: str, state: str = "ENABLED", refs: list = ()) -> None:
        self.entities[f"/controller-services/{comp_id}"] = {
            "revision": {"version": 1},
            "component": {"id": comp_id, "state": state,
                          "referencingComponents": list(refs)},
        }

    def _new_id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}{self._counter}"

    # --- NiFiClient surface ---------------------------------------------
    def _get_json(self, path: str) -> dict:
        if path in self.entities:
            return self.entities[path]
        return {"revision": {"version": 1}, "component": {}}

    def _request(self, method: str, path: str, **kw):
        body = kw.get("json") or {}
        if method == "POST" and path.endswith("/drop-requests"):
            self.ops.append(f"drain {path.split('/')[2]}")
            resp = {"dropRequest": {"id": "dr1", "finished": True}}
        elif method == "PUT" and path.endswith("/run-status"):
            comp_id = path.split("/")[2]
            self.ops.append(f"run-status {comp_id} -> {body['state']}")
            entity = self.entities.get("/" + "/".join(path.split("/")[1:3]))
            if entity is not None:
                entity["component"]["state"] = (
                    "RUNNING" if body["state"] == "RUNNING" else body["state"]
                )
            resp = {}
        elif method == "PUT":
            self.ops.append(f"PUT {path} {json.dumps(body.get('component', {}), sort_keys=True)}")
            resp = {}
        elif method == "POST":
            new_id = self._new_id("new-")
            self.ops.append(f"POST {path} {json.dumps(body.get('component', {}), sort_keys=True)}")
            resp = {"id": new_id, "component": dict(body.get("component", {}), id=new_id)}
        elif method == "DELETE":
            self.ops.append(f"DELETE {path}")
            resp = {}
        else:
            resp = {}

        class R:
            def __init__(self, payload):
                self._payload = payload

            def json(self):
                return self._payload

        return R(resp)

    def stop_processor(self, proc_id: str) -> None:
        self.ops.append(f"stop {proc_id}")
        self.entities[f"/processors/{proc_id}"]["component"]["state"] = "STOPPED"

    def _set_processor_state(self, proc_id: str, state: str) -> None:
        self.ops.append(f"run-status {proc_id} -> {state}")

    def _delete_component(self, kind: str, comp_id: str) -> None:
        self.ops.append(f"DELETE /{kind}/{comp_id}")

    def _child_groups(self, pg_id: str) -> list:
        return []

    def bundle_index(self) -> dict:
        return {"org.x.G": {"group": "org.x", "artifact": "x-nar", "version": "1"}}

    def _align_bundles(self, snapshot: dict) -> None:
        self.ops.append("align-bundles")

    def _create_from_snapshot(self, parent_id, name, snapshot, position) -> str:
        self.ops.append(f"create-subtree {name} under {parent_id}")
        return self._new_id("grp-")

    def _teardown(self, pg_id: str) -> None:
        self.ops.append(f"teardown {pg_id}")

    def _find_context_entity(self, name):
        return {"id": f"ctx-{name}", "revision": {"version": 0},
                "component": {"id": f"ctx-{name}", "name": name}}


def _pair() -> tuple:
    """(live, desired) with live carrying fake canvas ids, like a real pull."""

    def build() -> Flow:
        flow = Flow("F")
        gen = Processor(name="Gen", type="org.x.G", properties={"a": "1"})
        log = Processor(name="Log", type="org.x.L", auto_terminate=["success"])
        flow.add(gen, log)
        flow.add_connection(gen >> log)
        return flow

    live, desired = build(), build()
    live.nifi_id = "root-pg"
    live.processors[0].nifi_id = "p-gen"
    live.processors[1].nifi_id = "p-log"
    live.connections[0].nifi_id = "c-1"
    return live, desired


def _apply(client, live, desired):
    changes = diff_flows(live, desired)
    PlanApplier(client, "root-pg", live, desired).apply(changes)
    return changes


def test_update_running_processor_stops_updates_restarts():
    live, desired = _pair()
    desired.processors[0].properties["a"] = "2"
    client = FakeClient()
    client.seed_processor("p-gen", state="RUNNING")
    _apply(client, live, desired)
    assert client.ops[0] == "stop p-gen"
    assert any(op.startswith("PUT /processors/p-gen") and '"a": "2"' in op for op in client.ops)
    assert client.ops[-1] == "run-status p-gen -> RUNNING"


def test_update_stopped_processor_is_not_restarted():
    live, desired = _pair()
    desired.processors[0].properties["a"] = "2"
    client = FakeClient()
    client.seed_processor("p-gen", state="STOPPED")
    _apply(client, live, desired)
    assert not any("-> RUNNING" in op for op in client.ops)


def test_remove_connection_then_processor_in_order():
    live, desired = _pair()
    del desired.processors[1]  # Log
    del desired.connections[0]
    client = FakeClient()
    client.seed_processor("p-gen", state="RUNNING")
    client.seed_processor("p-log", state="RUNNING")
    client.seed_connection(
        "c-1",
        {"id": "p-gen", "type": "PROCESSOR"},
        {"id": "p-log", "type": "PROCESSOR"},
    )
    _apply(client, live, desired)
    ops = client.ops
    assert ops.index("drain c-1") < ops.index("DELETE /connections/c-1")
    assert ops.index("DELETE /connections/c-1") < ops.index("DELETE /processors/p-log")
    # Gen was stopped as the connection source and restarts at the end; the
    # deleted Log does not.
    assert "run-status p-gen -> RUNNING" in ops
    assert "run-status p-log -> RUNNING" not in ops


def test_add_processor_and_connection_resolves_live_endpoint_ids():
    live, desired = _pair()
    extra = Processor(name="Extra", type="org.x.G", position=(10.0, 20.0))
    desired.add_processor(extra)
    desired.add_connection(desired.processors[0].to(extra, relationships=["success"]))
    client = FakeClient()
    _apply(client, live, desired)
    post_proc = next(op for op in client.ops if op.startswith("POST /process-groups/root-pg/processors"))
    assert '"name": "Extra"' in post_proc and '"artifact": "x-nar"' in post_proc
    post_conn = next(op for op in client.ops if op.startswith("POST /process-groups/root-pg/connections"))
    # Source resolves to the live id from the pulled model; the new
    # processor's id comes from the create response.
    assert '"id": "p-gen"' in post_conn and '"id": "new-' in post_conn
    assert client.ops.index(post_proc) < client.ops.index(post_conn)


def test_service_update_disables_updates_reenables_and_restarts_refs():
    def build(value: str) -> Flow:
        flow = Flow("S")
        svc = ControllerService(name="Svc", type="org.x.Svc", properties={"p": value})
        flow.add(svc)
        return flow

    live, desired = build("old"), build("new")
    live.nifi_id = "root-pg"
    live.controller_services[0].nifi_id = "s-1"
    client = FakeClient()
    client.seed_processor("p-ref", state="RUNNING")
    client.seed_service(
        "s-1", state="ENABLED",
        refs=[{"component": {"id": "p-ref", "referenceType": "Processor", "state": "RUNNING"}}],
    )
    _apply(client, live, desired)
    ops = client.ops
    assert ops.index("stop p-ref") < ops.index("run-status s-1 -> DISABLED")
    put = next(op for op in ops if op.startswith("PUT /controller-services/s-1"))
    assert '"p": "new"' in put
    assert ops.index(put) < ops.index("run-status s-1 -> ENABLED")
    assert ops.index("run-status s-1 -> ENABLED") < ops.index("run-status p-ref -> RUNNING")


def test_group_add_uses_subtree_snapshot():
    live, desired = _pair()
    with desired.process_group("NewChild") as child:
        child.add_processor(Processor(name="Inner", type="org.x.G"))
    client = FakeClient()
    _apply(client, live, desired)
    assert "create-subtree NewChild under root-pg" in client.ops
    assert "align-bundles" in client.ops


def test_disable_requested_processor_is_not_restarted():
    live, desired = _pair()
    desired.processors[0].scheduled_state = "DISABLED"
    client = FakeClient()
    client.seed_processor("p-gen", state="RUNNING")
    _apply(client, live, desired)
    assert "run-status p-gen -> DISABLED" in client.ops
    assert client.ops[-1] != "run-status p-gen -> RUNNING"


def test_cross_boundary_connection_add_to_child_port():
    live, desired = _pair()

    def add_child(flow: Flow, wire: bool) -> None:
        with flow.process_group("Child") as child:
            inp = InputPort("in")
            child.add_port(inp)
        if wire:
            flow.add_connection(flow.processors[0] >> inp)

    add_child(live, wire=False)
    add_child(desired, wire=True)
    live.process_groups[0].nifi_id = "child-pg"
    live.process_groups[0].input_ports[0].nifi_id = "port-in"
    client = FakeClient()
    _apply(client, live, desired)
    post_conn = next(op for op in client.ops if "/connections" in op and op.startswith("POST"))
    body = json.loads(post_conn.split(" ", 2)[2])
    assert body["destination"] == {"id": "port-in", "groupId": "child-pg", "type": "INPUT_PORT"}
    assert body["source"]["id"] == "p-gen"
