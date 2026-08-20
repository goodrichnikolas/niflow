"""Version-control-aware push: a group already under NiFi Registry control is
rebuilt *in place* (same id, same registry linkage) instead of being
delete-and-recreated.

Both lines share the same shape — emit the snapshot, pre-create the group's own
controller services and remap every reference to them, then hand the components
to the group — and differ only in the transport:

* **2.x**: copy/paste (``PUT .../paste``).
* **1.x**: import the snapshot as a temporary child group and *move* its
  contents up with the snippet API (``POST /snippets`` + ``PUT /snippets/{id}``).
  This replaced templates on 2026-08-19: 1.24 escaped every ``#{`` while
  instantiating a template, so parameter references landed dead.
"""
import json
import logging
import re

import pytest

from niflow import Flow, Processor
from niflow.core import ControllerService, Funnel, Label, ParameterContext
from niflow.client import NiFiClient, NiFiApiError
from niflow.config import NiFiConfig

BASE = "https://nifi.test/nifi-api"


class FakeResponse:
    def __init__(self, status_code=200, body=None, text=None):
        self.status_code = status_code
        self._body = body
        self.text = text if text is not None else json.dumps(body or {})

    def json(self):
        return self._body


class FakeNiFi:
    """Scripted server with one version-controlled child group ``vc-id``."""

    def __init__(self, version="1.24.0", snapshot=None):
        self.version = version
        # What /download returns: the pre-push backup, and the "what actually
        # landed" side of the post-rebuild reconciliation.
        self.snapshot = snapshot or {"flowContents": {"name": "Versioned"}}
        self.contexts = []        # /flow/parameter-contexts entities
        self.group_puts = []      # PUT /process-groups/{id} components
        self.calls = []           # (method, path)
        self.deleted = []         # "/{kind}/{id}" deletes
        self.pasted = []          # PUT .../paste bodies (2.x path)
        self.services_created = []  # POST .../controller-services components
        # 1.x snippet-move path
        self.staged = []          # snapshot creates under vc-id (name, snapshot)
        self.snippets = []        # POST /snippets bodies
        self.snippet_moves = []   # PUT /snippets/{id} bodies
        self.groups_deleted = []  # DELETE /process-groups/{id}
        self.move_status = 200    # flip to make the snippet move fail
        self.stale_staging = False  # leave a staging group from an earlier push

    def request(self, method, url, **kwargs):
        assert url.startswith(BASE)
        path = url[len(BASE):]
        self.calls.append((method, path))
        return self.route(method, path, kwargs)

    def route(self, method, path, kw):
        if (method, path) == ("POST", "/access/token"):
            return FakeResponse(201, text="tok-123")
        assert kw.get("headers", {}).get("Authorization") == "Bearer tok-123", path

        if (method, path) == ("GET", "/flow/about"):
            return FakeResponse(200, {"about": {"version": self.version}})
        if (method, path) == ("GET", "/flow/process-groups/root"):
            return FakeResponse(200, {"processGroupFlow": {"id": "root-id", "flow": {}}})
        if (method, path) == ("GET", "/flow/process-groups/root-id"):
            return FakeResponse(200, {"processGroupFlow": {"id": "root-id", "flow": {
                "processGroups": [
                    {"component": {"id": "vc-id", "name": "Versioned",
                                   "position": {"x": 10.0, "y": 20.0}}},
                ]}}})

        # The versioned group's entity advertises version control.
        if (method, path) == ("GET", "/process-groups/vc-id"):
            return FakeResponse(200, {
                "revision": {"version": 5},
                "component": {"id": "vc-id", "name": "Versioned",
                              "versionControlInformation": {
                                  "registryId": "r1", "bucketId": "b1",
                                  "flowId": "f1", "version": 3}},
            })

        # Automatic pre-push backup snapshots the group first; the same
        # snapshot is what the post-rebuild reconciliation diffs against.
        if (method, path) == ("GET", "/process-groups/vc-id/download"):
            return FakeResponse(200, self.snapshot)

        # Contents to be emptied before the new ones are injected. A stale
        # staging group (if the scenario asks for one) only becomes visible
        # *after* the group has been emptied — that is where the push's own
        # sweep has to catch it.
        if (method, path) == ("GET", "/flow/process-groups/vc-id"):
            emptied = "/processors/p1" in self.deleted
            children = []
            if (self.stale_staging and emptied
                    and "stale-id" not in self.groups_deleted):
                children = [{"component": {"id": "stale-id",
                                           "name": "niflow-in-place-staging"}}]
            return FakeResponse(200, {"processGroupFlow": {"id": "vc-id", "flow": {
                "processors": [] if emptied else [{"id": "p1"}],
                "connections": [] if "/connections/c1" in self.deleted else [{"id": "c1"}],
                "inputPorts": [], "outputPorts": [],
                "funnels": [], "labels": [], "processGroups": children,
            }}})
        if (method, path) == ("GET", "/flow/process-groups/stale-id"):
            return FakeResponse(200, {"processGroupFlow": {"id": "stale-id", "flow": {}}})
        if (method, path) == ("GET", "/process-groups/stale-id"):
            return FakeResponse(200, {"revision": {"version": 1},
                                      "component": {"id": "stale-id"}})
        if (method, path) == ("DELETE", "/process-groups/stale-id"):
            self.groups_deleted.append("stale-id")
            return FakeResponse(200, {})
        if (method, path) == ("GET", "/flow/process-groups/vc-id/controller-services"):
            return FakeResponse(200, {"controllerServices": []})

        # Stop / drain.
        if method == "PUT" and re.fullmatch(r"/flow/process-groups/[\w-]+", path):
            return FakeResponse(200, {})
        if method == "PUT" and path.endswith("/controller-services"):
            return FakeResponse(200, {})
        if method == "POST" and path.endswith("/empty-all-connections-requests"):
            return FakeResponse(202, {"dropRequest": {"id": "drop-1", "finished": True}})
        if method == "DELETE" and "/empty-all-connections-requests/" in path:
            return FakeResponse(200, {})

        # Per-component revision lookups + deletes (connections, processors, ...).
        m = re.fullmatch(r"/(connections|processors|input-ports|output-ports|"
                         r"funnels|labels|controller-services)/([\w-]+)", path)
        if m and method == "GET":
            return FakeResponse(200, {"revision": {"version": 3}})
        if m and method == "DELETE":
            assert kw["params"]["version"] == 3
            self.deleted.append(path)
            return FakeResponse(200, {})

        # --- NiFi 1.x snippet-move path -------------------------------------
        # The snapshot is imported as a temporary child group of vc-id...
        if method == "POST" and path == "/process-groups/vc-id/process-groups":
            body = kw["json"]
            self.staged.append((body["component"]["name"], body["versionedFlowSnapshot"]))
            return FakeResponse(201, {"id": "tmp-id"})
        # ...whose contents the snippet covers (and which is empty once moved).
        if (method, path) == ("GET", "/flow/process-groups/tmp-id"):
            if self.snippet_moves:
                return FakeResponse(200, {"processGroupFlow": {"id": "tmp-id", "flow": {}}})
            snapshot = self.staged[-1][1]["flowContents"] if self.staged else {}
            def entities(key):
                return [{"id": f"{key}-{i}", "revision": {"version": 7},
                         "component": {"id": f"{key}-{i}"}}
                        for i, _ in enumerate(snapshot.get(key) or [])]
            return FakeResponse(200, {"processGroupFlow": {"id": "tmp-id", "flow": {
                "processors": entities("processors"),
                "connections": entities("connections"),
                "inputPorts": entities("inputPorts"),
                "outputPorts": entities("outputPorts"),
                "funnels": entities("funnels"),
                "labels": entities("labels"),
                "processGroups": entities("processGroups"),
            }}})
        if (method, path) == ("GET", "/process-groups/tmp-id"):
            return FakeResponse(200, {"revision": {"version": 2},
                                      "component": {"id": "tmp-id", "name": "staging"}})
        if (method, path) == ("DELETE", "/process-groups/tmp-id"):
            self.groups_deleted.append("tmp-id")
            return FakeResponse(200, {})
        if (method, path) == ("POST", "/snippets"):
            self.snippets.append(kw["json"]["snippet"])
            return FakeResponse(201, {"snippet": {"id": "snip-1"}})
        if (method, path) == ("PUT", "/snippets/snip-1"):
            if self.move_status >= 400:
                return FakeResponse(self.move_status, text="move refused")
            self.snippet_moves.append(kw["json"]["snippet"])
            return FakeResponse(200, {"snippet": {"id": "snip-1"}})

        # --- NiFi 2.x copy/paste path ---------------------------------------
        # Bundle alignment probes the installed NARs; empty index -> no-op.
        if (method, path) == ("GET", "/flow/processor-types"):
            return FakeResponse(200, {"processorTypes": []})
        if (method, path) == ("GET", "/flow/controller-service-types"):
            return FakeResponse(200, {"controllerServiceTypes": []})
        if method == "POST" and path == "/process-groups/vc-id/controller-services":
            comp = kw["json"]["component"]
            self.services_created.append(comp)
            return FakeResponse(201, {"component": {"id": f"svc-{len(self.services_created)}"}})
        if method == "PUT" and path == "/process-groups/vc-id/paste":
            self.pasted.append(kw["json"])
            return FakeResponse(200, {"flow": {}})

        if (method, path) == ("GET", "/flow/parameter-contexts"):
            return FakeResponse(200, {"parameterContexts": self.contexts})
        if (method, path) == ("POST", "/parameter-contexts"):
            name = kw["json"]["component"]["name"]
            entity = {"id": f"ctx-{len(self.contexts) + 1}", "revision": {"version": 1},
                      "component": {"id": f"ctx-{len(self.contexts) + 1}", "name": name}}
            self.contexts.append(entity)
            return FakeResponse(201, entity)
        # Rebinding a parameter context / setting group comments after the
        # rebuild (the template has no element for either).
        if method == "PUT" and re.fullmatch(r"/process-groups/[\w-]+", path):
            self.group_puts.append(kw["json"]["component"])
            return FakeResponse(200, {"component": {"id": "vc-id"}})

        raise AssertionError(f"Unscripted call: {method} {path}")


def _client(fake):
    return NiFiClient(NiFiConfig(host=BASE, username="admin", password="pw"), session=fake)


def _versioned_flow():
    flow = Flow("Versioned")
    flow.add_processor(Processor(name="Fetch", type="org.x.Fetch",
                                 auto_terminate=["success"]))
    return flow


def test_push_rebuilds_versioned_group_in_place():
    fake = FakeNiFi(version="1.24.0")
    client = _client(fake)

    group_id = client.push_flow(_versioned_flow())

    # Same id back — the group (and its registry linkage) was preserved.
    assert group_id == "vc-id"
    # The group itself was NEVER deleted (only its contents).
    assert "/process-groups/vc-id" not in fake.deleted
    # Old contents were removed: connection first, then processor.
    assert fake.deleted == ["/connections/c1", "/processors/p1"]
    # New contents arrived as a snapshot staged in a temp child group...
    assert [name for name, _ in fake.staged] == ["niflow-in-place-staging"]
    assert [p["name"] for p in fake.staged[0][1]["flowContents"]["processors"]] == ["Fetch"]
    # ...covered by a snippet whose components carry their revisions...
    assert fake.snippets[0]["parentGroupId"] == "tmp-id"
    assert fake.snippets[0]["processors"] == {
        "processors-0": {"clientId": "niflow", "version": 7}
    }
    # ...and moved into the versioned group itself.
    assert fake.snippet_moves == [{"id": "snip-1", "parentGroupId": "vc-id"}]
    # The staging group was deleted; no template was ever uploaded.
    assert fake.groups_deleted == ["tmp-id"]
    assert not any(path.endswith("/templates/upload") for _m, path in fake.calls)


def test_snippet_move_leaves_no_staging_group_when_it_fails():
    """A temp group left on the canvas is its own bug — and the error has to
    say what state the live (now empty) versioned group is in."""
    fake = FakeNiFi(version="1.24.0")
    fake.move_status = 409

    with pytest.raises(RuntimeError) as exc:
        _client(fake).push_flow(_versioned_flow())

    assert fake.groups_deleted == ["tmp-id"], "the staging group must be cleaned up"
    message = str(exc.value)
    assert "EMPTY" in message and "niflow rollback" in message


def test_snippet_move_discards_a_staging_group_left_by_an_earlier_push():
    """An interrupted push can leave the staging group behind; importing on top
    of it would move a stranger's components into the versioned group."""
    fake = FakeNiFi(version="1.24.0")
    fake.stale_staging = True

    _client(fake).push_flow(_versioned_flow())

    # The stale group was torn down first, then the fresh one after the move.
    assert fake.groups_deleted == ["stale-id", "tmp-id"]


def test_push_sets_flow_nifi_id_to_existing_group():
    fake = FakeNiFi()
    client = _client(fake)
    flow = _versioned_flow()
    client.push_flow(flow)
    assert flow.nifi_id == "vc-id"


def test_push_pastes_in_place_on_nifi_2x():
    fake = FakeNiFi(version="2.7.2")
    client = _client(fake)

    group_id = client.push_flow(_versioned_flow())

    # Same id back — group + registry linkage preserved (no group delete).
    assert group_id == "vc-id"
    assert "/process-groups/vc-id" not in fake.deleted
    # Old contents emptied first (connection before processor), then pasted.
    assert fake.deleted == ["/connections/c1", "/processors/p1"]
    # New contents arrived via copy/paste — no staging group is involved.
    assert fake.staged == []
    assert len(fake.pasted) == 1
    body = fake.pasted[0]
    assert body["revision"]["version"] == 5  # the group's current revision
    assert "Fetch" in [p["name"] for p in body["copyResponse"]["processors"]]


def test_snippet_move_carries_funnels_labels_queues_and_parameter_refs():
    """What the 1.x vehicle now carries natively. The template it replaced
    dropped funnels/labels outright, ignored load-balance compression, and —
    worst — 1.24 escaped every ``#{`` while instantiating one, so a working
    ``#{param}`` landed as the literal ``##{param}``. A snippet move is a
    server-side move of the imported snapshot: nothing is re-serialised."""
    fake = FakeNiFi(version="1.24.0")
    flow = Flow("Versioned")
    source = Processor(name="Fetch", type="org.x.Fetch", scheduled_state="DISABLED",
                       bulletin_level="ERROR", execution_node="PRIMARY",
                       properties={"Token": "#{secret.token}"})
    funnel = Funnel()
    flow.add_processor(source)
    flow.add_funnel(funnel)
    flow.add_label(Label("note", width=300.0, height=90.0))
    flow.add_connection(source.to(funnel, back_pressure_object_threshold=42,
                                  flowfile_expiration="5 min",
                                  load_balance_strategy="ROUND_ROBIN",
                                  load_balance_compression="COMPRESS_ATTRIBUTES_ONLY",
                                  prioritizers=["org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer"]))

    _client(fake).push_flow(flow)

    contents = fake.staged[0][1]["flowContents"]
    assert len(contents["funnels"]) == 1
    assert [label["label"] for label in contents["labels"]] == ["note"]
    proc = contents["processors"][0]
    assert proc["scheduledState"] == "DISABLED"
    assert proc["bulletinLevel"] == "ERROR"
    assert proc["executionNode"] == "PRIMARY"
    # The parameter reference travels verbatim — no '##{' escaping anywhere.
    assert proc["properties"]["Token"] == "#{secret.token}"
    connection = contents["connections"][0]
    assert connection["backPressureObjectThreshold"] == 42
    assert connection["flowFileExpiration"] == "5 min"
    assert connection["loadBalanceCompression"] == "COMPRESS_ATTRIBUTES_ONLY"
    assert connection["prioritizers"] == [
        "org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer"
    ]
    # ...and the whole lot moves in one call, not component by component.
    assert fake.snippet_moves == [{"id": "snip-1", "parentGroupId": "vc-id"}]


def test_push_warns_about_in_place_limits_before_touching_the_group():
    """The warning is worthless after the live contents have been deleted."""
    fake = FakeNiFi(version="1.24.0")
    flow = _versioned_flow()
    flow.parameter_context = ParameterContext(name="Ctx")

    warned_after = []
    handler = logging.Handler()
    handler.emit = lambda record: (
        warned_after.append(list(fake.deleted))
        if record.levelno >= logging.WARNING and "cannot travel" in record.getMessage()
        else None
    )
    logger = logging.getLogger("niflow")
    logger.addHandler(handler)
    try:
        _client(fake).push_flow(flow)
    finally:
        logger.removeHandler(handler)

    # Warned exactly once, while the live group was still intact.
    assert warned_after == [[]]
    assert fake.deleted, "the rebuild does empty the group afterwards"


def test_in_place_rebinds_the_parameter_context_no_vehicle_can_carry():
    """No in-place vehicle has a DTO for the group its contents land *in*, so
    the target group's own binding is re-applied over the REST API afterwards."""
    snapshot = {"flowContents": {
        "name": "Versioned",
        "processors": [{"identifier": "p", "instanceIdentifier": "p1", "name": "Fetch",
                        "type": "org.x.Fetch", "autoTerminatedRelationships": ["success"]}],
    }}
    fake = FakeNiFi(version="1.24.0", snapshot=snapshot)
    fake.contexts = [{"id": "ctx-1", "revision": {"version": 1},
                      "component": {"id": "ctx-1", "name": "Ctx"}}]
    flow = _versioned_flow()
    flow.parameter_context = ParameterContext(name="Ctx")

    _client(fake).push_flow(flow)

    assert fake.group_puts == [{"id": "vc-id", "parameterContext": {"id": "ctx-1"}}]


def test_in_place_recreates_group_services_and_remaps_refs():
    """Group-level controller services are recreated and processor refs rewired
    to the new ids (paste only carries references, not service definitions)."""
    fake = FakeNiFi(version="2.7.2")
    client = _client(fake)

    flow = Flow("Versioned")
    reader = ControllerService(name="Reader", type="org.apache.nifi.json.JsonTreeReader")
    flow.add_controller_service(reader)
    flow.add_processor(Processor(name="Convert", type="org.x.Convert",
                                 properties={"Record Reader": reader},
                                 auto_terminate=["success"]))
    client.push_flow(flow)

    # The service was recreated on the group...
    assert [s["name"] for s in fake.services_created] == ["Reader"]
    # ...and the processor's service-ref property now points at the NEW id,
    # which is also advertised as an external service reference for paste.
    body = fake.pasted[0]
    proc = body["copyResponse"]["processors"][0]
    assert proc["properties"]["Record Reader"] == "svc-1"
    assert "svc-1" in body["copyResponse"]["externalControllerServiceReferences"]
