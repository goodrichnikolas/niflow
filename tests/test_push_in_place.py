"""Version-control-aware push: a group already under NiFi Registry control is
rebuilt *in place* (same id, same registry linkage) instead of being
delete-and-recreated. Templates are the in-place vehicle on NiFi 1.x; on 2.x,
where templates are gone, copy/paste (``PUT .../paste``) takes their place —
recreating group-level controller services and remapping references first,
since paste carries only service references, not their definitions.
"""
import json
import re

from niflow import Flow, Processor
from niflow.core import ControllerService
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

    def __init__(self, version="1.24.0"):
        self.version = version
        self.calls = []           # (method, path)
        self.deleted = []         # "/{kind}/{id}" deletes
        self.templates_uploaded = []
        self.instantiated = []
        self.templates_deleted = []
        self.pasted = []          # PUT .../paste bodies (2.x path)
        self.services_created = []  # POST .../controller-services components

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

        # Contents to be emptied before the template is instantiated.
        if (method, path) == ("GET", "/flow/process-groups/vc-id"):
            return FakeResponse(200, {"processGroupFlow": {"id": "vc-id", "flow": {
                "processors": [{"id": "p1"}],
                "connections": [{"id": "c1"}],
                "inputPorts": [], "outputPorts": [],
                "funnels": [], "labels": [], "processGroups": [],
            }}})
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

        # Template upload / instantiate / cleanup.
        if (method, path) == ("GET", "/flow/templates"):
            return FakeResponse(200, {"templates": []})  # no pre-existing templates
        if method == "POST" and path == "/process-groups/vc-id/templates/upload":
            self.templates_uploaded.append(kw["files"]["template"][1])
            # NiFi 1.x returns the upload result as XML, not JSON.
            return FakeResponse(
                201, text="<templateEntity><template><id>tpl-1</id></template></templateEntity>"
            )
        if method == "POST" and path == "/process-groups/vc-id/template-instance":
            self.instantiated.append(kw["json"])
            return FakeResponse(201, {"flow": {}})
        if (method, path) == ("DELETE", "/templates/tpl-1"):
            self.templates_deleted.append(path)
            return FakeResponse(200, {})

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

        # No parameter contexts in these flows.
        if (method, path) == ("GET", "/flow/parameter-contexts"):
            return FakeResponse(200, {"parameterContexts": []})

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
    # New contents arrived via an uploaded-then-instantiated template.
    assert len(fake.templates_uploaded) == 1
    assert "<template>" in fake.templates_uploaded[0]
    assert fake.instantiated[0]["templateId"] == "tpl-1"
    # The temporary template was cleaned up.
    assert fake.templates_deleted == ["/templates/tpl-1"]


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
    # New contents arrived via copy/paste — not templates.
    assert fake.templates_uploaded == []
    assert len(fake.pasted) == 1
    body = fake.pasted[0]
    assert body["revision"]["version"] == 5  # the group's current revision
    assert "Fetch" in [p["name"] for p in body["copyResponse"]["processors"]]


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
