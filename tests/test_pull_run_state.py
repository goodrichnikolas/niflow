"""``pull``/``plan`` read the *real* run state, not the download's version of it.

``GET /process-groups/{id}/download`` is a flow definition, not a picture of the
canvas: it reports every controller service as ``DISABLED`` however live it is,
and every processor as ``ENABLED`` however hard it is running (verified side by
side against the live endpoints on 1.24 and 2.7.2). Uncorrected that makes
``niflow pull`` write ``enabled=False`` into the checked-in code for a service
that is enabled, and makes a *stated* ``enabled=True`` re-plan forever.

The correction is two calls for the whole subtree, whatever its depth — the
recursive status snapshot plus one controller-service listing.
"""
import json
import re

from niflow import Flow, Processor
from niflow.client import NiFiClient
from niflow.config import NiFiConfig
from niflow.core import ControllerService

BASE = "https://nifi.test/nifi-api"


class FakeResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body or {})

    def json(self):
        return self._body


def _proc(name, state="ENABLED"):
    return {"identifier": f"v-{name}", "instanceIdentifier": f"i-{name}", "name": name,
            "type": "org.apache.nifi.processors.attributes.UpdateAttribute",
            "scheduledState": state, "autoTerminatedRelationships": ["success"]}


def _status_proc(name, run_status):
    return {"id": f"i-{name}",
            "processorStatusSnapshot": {"id": f"i-{name}", "name": name,
                                        "runStatus": run_status}}


class FakeNiFi:
    """A group with a live-ENABLED service, a RUNNING processor, and depth.

    The download deliberately lies the way NiFi does; the status and
    controller-service endpoints tell the truth.
    """

    def __init__(self, version="1.24.0", status_ok=True, services_ok=True):
        self.version = version
        self.status_ok = status_ok
        self.services_ok = services_ok
        self.calls = []

    # --- the three views of the same group -------------------------------
    def _download(self):
        child = {"identifier": "v-child", "instanceIdentifier": "deep-id", "name": "Child",
                 "processors": [_proc("Work")], "controllerServices": [],
                 "processGroups": []}
        return {"flowContents": {
            "identifier": "v-flow", "instanceIdentifier": "pg-1", "name": "Demo",
            "processors": [_proc("Gen"), _proc("Off", "DISABLED")],
            "controllerServices": [{
                "identifier": "v-reader", "instanceIdentifier": "svc-1", "name": "Reader",
                "type": "org.apache.nifi.json.JsonTreeReader",
                "scheduledState": "DISABLED", "properties": {},
            }],
            "processGroups": [child],
        }}

    def _status(self):
        return {"processGroupStatus": {"aggregateSnapshot": {
            "id": "pg-1", "name": "Demo",
            "processorStatusSnapshots": [
                _status_proc("Gen", "Running"), _status_proc("Off", "Disabled")],
            "processGroupStatusSnapshots": [{"id": "deep-id", "processGroupStatusSnapshot": {
                "id": "deep-id", "name": "Child",
                "processorStatusSnapshots": [_status_proc("Work", "Validating")],
                "processGroupStatusSnapshots": [],
            }}],
        }}}

    def request(self, method, url, **kwargs):
        path = url[len(BASE):]
        self.calls.append((method, path))
        if (method, path) == ("POST", "/access/token"):
            resp = FakeResponse(201, {})
            resp.text = "tok"
            return resp
        if (method, path) == ("GET", "/flow/about"):
            return FakeResponse(200, {"about": {"version": self.version}})
        if (method, path) == ("GET", "/flow/process-groups/root"):
            return FakeResponse(200, {"processGroupFlow": {"id": "root-id", "flow": {
                "processGroups": [{"component": {"id": "pg-1", "name": "Demo"}}]}}})
        # Name resolution walks from the root; only the *target* group's status
        # is what this module is about.
        if (method, path) == ("GET", "/flow/process-groups/root-id"):
            return FakeResponse(200, {"processGroupFlow": {"id": "root-id", "flow": {
                "processGroups": [{"component": {"id": "pg-1", "name": "Demo"}}]}}})
        if path == "/flow/process-groups/root-id/status?recursive=true":
            return FakeResponse(404, {})
        if (method, path) == ("GET", "/flow/process-groups/pg-1"):
            return FakeResponse(200, {"processGroupFlow": {"id": "pg-1", "flow": {
                "processGroups": []}}})
        if (method, path) == ("GET", "/process-groups/pg-1/download"):
            return FakeResponse(200, self._download())
        if (method, path) == ("GET", "/process-groups/pg-1"):
            return FakeResponse(200, {"revision": {"version": 1},
                                      "component": {"id": "pg-1", "name": "Demo",
                                                    "parentGroupId": "root-id"}})
        if path == "/flow/process-groups/pg-1/status?recursive=true":
            if not self.status_ok:
                return FakeResponse(404, {})
            return FakeResponse(200, self._status())
        if path.startswith("/flow/process-groups/pg-1/controller-services"):
            if not self.services_ok:
                return FakeResponse(403, {})
            return FakeResponse(200, {"controllerServices": [{"component": {
                "id": "svc-1", "name": "Reader", "parentGroupId": "pg-1",
                "state": "ENABLED"}}]})
        if (method, path) == ("GET", "/flow/parameter-contexts"):
            return FakeResponse(200, {"parameterContexts": []})
        raise AssertionError(f"Unscripted call: {method} {path}")


def _client(fake):
    return NiFiClient(NiFiConfig(host=BASE, username="admin", password="pw"), session=fake)


def test_pull_reports_the_live_service_state_not_the_downloads():
    fake = FakeNiFi()
    flow = _client(fake).pull_flow("Demo")

    reader = flow.controller_services[0]
    assert reader.enabled is True, "the service is live-ENABLED; the download says DISABLED"
    # Stated, so the plan keeps diffing it — an accurate assertion, not a default.
    assert "enabled" in reader.model_fields_set


def test_pull_reports_running_processors_at_every_depth():
    fake = FakeNiFi()
    flow = _client(fake).pull_flow("Demo")

    states = {p.name: p.scheduled_state for p in flow.processors}
    assert states == {"Gen": "RUNNING", "Off": "DISABLED"}
    # "Validating" is neither running nor disabled: the processor may run.
    assert flow.process_groups[0].processors[0].scheduled_state == "ENABLED"


def test_run_state_costs_two_calls_for_the_whole_subtree():
    """One recursive status + one controller-service listing — never a walk."""
    fake = FakeNiFi()
    _client(fake).pull_flow("Demo")

    status = [c for c in fake.calls if c[1].startswith("/flow/process-groups/pg-1/status")]
    services = [c for c in fake.calls if "/controller-services" in c[1]]
    assert len(status) == 1
    assert len(services) == 1
    # ...and it asks for descendants rather than paging group by group.
    assert "includeDescendantGroups=true" in services[0][1]
    assert not any(re.search(r"/flow/process-groups/deep-id", path) for _m, path in fake.calls)


def test_pull_then_plan_converges_for_an_enabled_service():
    """The drift this fixes: a stated enabled=True used to re-plan forever."""
    fake = FakeNiFi()
    client = _client(fake)
    pulled = client.pull_flow("Demo")

    _pg_id, _live, changes = client.plan_flow(pulled)
    assert changes == []


def test_plan_does_not_propose_stopping_a_processor_the_model_never_mentions():
    """A hand-written flow that says nothing about run state must not turn into
    a plan that stops production — the live side now really does read RUNNING."""
    fake = FakeNiFi()
    client = _client(fake)

    desired = Flow("Demo")
    desired.add_controller_service(ControllerService(
        name="Reader", type="org.apache.nifi.json.JsonTreeReader"))
    desired.add_processor(
        Processor(name="Gen", type="org.apache.nifi.processors.attributes.UpdateAttribute",
                  auto_terminate=["success"]),
        Processor(name="Off", type="org.apache.nifi.processors.attributes.UpdateAttribute",
                  scheduled_state="DISABLED", auto_terminate=["success"]))
    with desired.process_group("Child") as child:
        child.add(Processor(name="Work",
                            type="org.apache.nifi.processors.attributes.UpdateAttribute",
                            auto_terminate=["success"]))

    _pg_id, _live, changes = client.plan_flow(desired)
    assert changes == []

    # Spelled out, it is an assertion again: stopping IS the plan.
    desired.processors[0].scheduled_state = "ENABLED"
    _pg_id, _live, changes = client.plan_flow(desired)
    assert [(c.kind, c.name, c.fields) for c in changes] == [
        ("processor", "Gen", {"scheduled_state": ("RUNNING", "ENABLED")})]


def test_run_state_overlay_is_best_effort():
    """An unreadable status or service listing must not break the pull; the
    model just keeps what the download said (where it was before)."""
    for kwargs in ({"status_ok": False}, {"services_ok": False}):
        fake = FakeNiFi(**kwargs)
        flow = _client(fake).pull_flow("Demo")
        assert [p.name for p in flow.processors] == ["Gen", "Off"]
        # Both fall back to the download's (wrong, but pre-existing) DISABLED.
        assert flow.controller_services[0].enabled is False
