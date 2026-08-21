"""Unit tests for the flow-test harness (:mod:`niflow.testing`).

A stateful fake client scripts the sandbox: Gen (source) -> Work -> Audit at
root, plus a child group "Sub" with an input port and a processor. Injection
via run-once "fills" the expect queue; the tests verify orchestration order,
component placement, assertions, and cleanup.
"""
from typing import List

import pytest

from niflow import Flow, Processor
from niflow.testing import CaseResult, FlowTester, TestCase, as_cases, format_results


def _flow() -> Flow:
    flow = Flow("Pipeline")
    flow.add_processor(Processor(name="Gen", type="org.x.G"))
    flow.add_processor(Processor(name="Work", type="org.x.W"))
    flow.add_processor(Processor(name="Audit", type="org.x.A"))
    return flow


class FakeClient:
    def __init__(self):
        self.ops: List[str] = []
        self.audit_queue: List[dict] = []  # flowfiles "arriving" at Audit
        self.next_id = 0
        self.deleted_group = None

    # --- lifecycle -----------------------------------------------------------
    def push_flow(self, flow, secrets=None):
        self.ops.append(f"push {flow.name}")
        flow.nifi_id = "pg-1"
        return "pg-1"

    def delete_group(self, pg_id):
        self.deleted_group = pg_id
        self.ops.append(f"delete-group {pg_id}")

    def enable_services(self, pg_id):
        self.ops.append("enable-services")

    def _major_version(self):
        return 2

    def set_port_state(self, kind, port_id, state):
        self.ops.append(f"port {port_id} -> {state}")

    def _set_group_state(self, pg_id, state):
        self.ops.append(f"group-state {state}")

    def purge_queues(self, pg_id):
        self.ops.append("purge")
        return ""

    # --- topology ------------------------------------------------------------
    def _get_json(self, path):
        if path == "/flow/process-groups/pg-1":
            return {"processGroupFlow": {"flow": {
                "processors": [
                    {"component": {"id": "p-gen", "name": "Gen"}},
                    {"component": {"id": "p-work", "name": "Work"}},
                    {"component": {"id": "p-audit", "name": "Audit"}},
                ],
                "inputPorts": [], "outputPorts": [],
                "processGroups": [{"component": {"id": "pg-sub", "name": "Sub"}}],
            }}}
        if path == "/flow/process-groups/pg-sub":
            return {"processGroupFlow": {"flow": {
                "processors": [{"component": {"id": "p-subwork", "name": "Work"}}],
                "inputPorts": [{"component": {"id": "port-in", "name": "in"}}],
                "outputPorts": [], "processGroups": [],
            }}}
        raise AssertionError(f"unscripted GET {path}")

    def list_queues(self, pg_id):
        return [
            {"id": "q-gw", "source": "Gen", "destination": "Work", "path": "",
             "queued": 0},
            {"id": "q-wa", "source": "Work", "destination": "Audit", "path": "",
             "queued": len(self.audit_queue)},
            {"id": "q-sub", "source": "in", "destination": "Work", "path": "Sub",
             "queued": 0},
        ]

    # --- state changes -------------------------------------------------------
    def stop_processor(self, proc_id):
        self.ops.append(f"stop {proc_id}")

    def start_processor(self, proc_id):
        self.ops.append(f"start {proc_id}")

    def run_processor_once(self, proc_id):
        self.ops.append(f"run-once {proc_id}")
        self.audit_queue.append({
            "uuid": "ff-1",
            "attributes": {"kind": "test", "mime.type": "application/json"},
            "content": '{"ok": true}',
        })

    # --- temp components -----------------------------------------------------
    def create_processor(self, pg_id, component):
        self.next_id += 1
        props = component["config"]["properties"]
        text = props.get("Custom Text", props.get("generate-ff-custom-text", ""))
        self.ops.append(f"create-proc in={pg_id} text={text!r}")
        self.last_processor = component
        return f"tmp-{self.next_id}"

    def create_connection(self, pg_id, source, destination, relationships=None):
        self.ops.append(
            f"create-conn in={pg_id} {source['id']} -> {destination['id']}"
            f" ({destination['type']})"
        )
        self.last_connection = {"source": source, "destination": destination}
        return "tmp-conn"

    def drain_connection(self, conn_id):
        self.ops.append(f"drain {conn_id}")

    def _delete_component(self, kind, comp_id):
        self.ops.append(f"delete {kind}/{comp_id}")

    # --- collection ----------------------------------------------------------
    def list_flowfiles(self, queue_id, max_results=100):
        assert queue_id == "q-wa"
        return [{"uuid": ff["uuid"]} for ff in self.audit_queue]

    def flowfile_detail(self, queue_id, uuid, node_id=""):
        ff = next(f for f in self.audit_queue if f["uuid"] == uuid)
        return {"uuid": uuid, "attributes": ff["attributes"], "content": ff["content"]}


def _run(case: TestCase, keep=False) -> tuple:
    client = FakeClient()
    with FlowTester(client, _flow(), keep=keep) as tester:
        results = tester.run([case])
    return client, results


def test_happy_path_orchestration_and_pass():
    case = TestCase(
        name="works", inject_at="/Work", expect_at="Audit",
        content="hello", attributes={"a": "1"},
        expect_attributes={"mime.type": "application/json"},
        expect_content_contains='"ok"',
    )
    client, results = _run(case)
    (result,) = results
    assert result.passed, result.failures

    ops = client.ops
    # Sandbox pushed under a distinct name, deleted afterwards.
    assert ops[0] == "push Pipeline (niflow-test)"
    assert client.deleted_group == "pg-1"
    # Selective start: the source (Gen) and collector (Audit) are NEVER
    # started; mid-flow processors and ports are.
    assert "start p-gen" not in ops and "start p-audit" not in ops
    assert "start p-work" in ops and "start p-subwork" in ops
    assert "port port-in -> RUNNING" in ops
    # Temp generator carries the content, injected in the root group.
    assert "create-proc in=pg-1 text='hello'" in ops
    assert "create-conn in=pg-1 tmp-1 -> p-work (PROCESSOR)" in ops
    assert "run-once tmp-1" in ops
    # Attributes ride along as dynamic properties.
    assert client.last_processor["config"]["properties"]["a"] == "1"
    # Cleanup: stop the group, remove temp pieces, purge leftovers.
    assert "group-state STOPPED" in ops
    assert "drain tmp-conn" in ops
    assert "delete connections/tmp-conn" in ops
    assert "delete processors/tmp-1" in ops
    assert ops[-2] == "purge" and ops[-1] == "delete-group pg-1"


def test_failing_expectations_are_reported():
    case = TestCase(
        name="wrong", inject_at="/Work", expect_at="Audit",
        expect_attributes={"mime.type": "text/csv"},
        expect_content="something else",
    )
    _, results = _run(case)
    (result,) = results
    assert not result.passed
    assert any("mime.type" in f and "text/csv" in f for f in result.failures)
    assert any("content" in f for f in result.failures)
    text = format_results(results)
    assert text.startswith("✗ wrong") and "0/1" in text


def test_timeout_reports_missing_flowfiles():
    client = FakeClient()
    client.run_processor_once = lambda proc_id: client.ops.append(f"run-once {proc_id}")
    case = TestCase(name="nothing", inject_at="/Work", expect_at="Audit", timeout=0)
    with FlowTester(client, _flow()) as tester:
        (result,) = tester.run([case])
    assert not result.passed
    assert "expected 1 FlowFile(s)" in result.failures[0]


def test_ambiguous_name_needs_path():
    tester = FlowTester(FakeClient(), _flow()).__enter__()
    # Both root Work and Sub/Work exist -> bare name must raise, paths pick.
    with pytest.raises(ValueError, match="ambiguous"):
        tester._resolve("Work", kinds=("processor",))
    assert tester._resolve("Sub/Work", kinds=("processor",))["id"] == "p-subwork"
    assert tester._resolve("/Work", kinds=("processor",))["id"] == "p-work"
    with pytest.raises(ValueError, match="no processor"):
        tester._resolve("Nope", kinds=("processor",))


def test_input_port_injection_goes_through_the_parent_group():
    client = FakeClient()
    case = TestCase(name="via port", inject_at="Sub/in", expect_at="Audit")
    with FlowTester(client, _flow()) as tester:
        tester.run([case])
    dest = client.last_connection["destination"]
    assert dest == {"id": "port-in", "groupId": "pg-sub", "type": "INPUT_PORT"}
    # Generator lives in the PARENT of Sub (the sandbox root).
    assert "create-conn in=pg-1 tmp-1 -> port-in (INPUT_PORT)" in client.ops


def test_keep_leaves_the_sandbox():
    case = TestCase(name="works", inject_at="/Work", expect_at="Audit")
    client, _ = _run(case, keep=True)
    assert client.deleted_group is None


def test_as_cases_accepts_dicts():
    cases = as_cases([{"name": "d", "inject_at": "A", "expect_at": "B"}])
    assert isinstance(cases[0], TestCase) and cases[0].expect_at == "B"
