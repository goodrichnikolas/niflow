"""Web GUI API dispatch against a fake client (no sockets, no NiFi)."""
from __future__ import annotations

import threading
from pathlib import Path

from niflow.webgui import PAGE, dispatch


class FakeClient:
    class config:
        auth_mode = "password"

    base = "https://fake:8443/nifi-api"

    def __init__(self):
        self.ops = []

    def version(self):
        return "1.24.0"

    def find_processors(self):
        return [{"id": "p1", "name": "Gen", "type": "org.x.G", "state": "RUNNING",
                 "path": "", "group_id": "root"}]

    def walk_groups(self):
        yield "Torture", {"id": "g1", "name": "Torture"}
        yield "Torture/Stage", {"id": "g2", "name": "Stage"}

    def list_queues(self):
        return [{"id": "c1", "source": "Gen", "destination": "Log", "path": "",
                 "queued": 2, "queued_label": "2 / 14 bytes"}]

    def run_queue_endpoint_once(self, conn_id, which):
        self.ops.append(("queue_run_once", conn_id, which))
        return "Log" if which == "destination" else "Gen"

    def bulletins(self, limit=100):
        return []

    def validation_errors(self, group="root"):
        return [{"name": "Lonely", "path": "", "errors": ["relationship success unhandled"]}]

    def list_flowfiles(self, connection_id, max_results=100):
        self.ops.append(("list_flowfiles", connection_id))
        return [{"uuid": "u1", "filename": "f.txt", "size": "14 B"}]

    def flowfile_detail(self, connection_id, uuid):
        return {"attributes": {"filename": "f.txt"}, "content": "hello"}

    def run_processor_once(self, proc_id):
        self.ops.append(("run_once", proc_id))

    def stop_processor(self, proc_id):
        self.ops.append(("stop", proc_id))

    def start_processor(self, proc_id):
        self.ops.append(("start", proc_id))

    def stop_group(self, group):
        self.ops.append(("stop_group", group))

    def tidy_group(self, group, layout="horizontal", recurse=True):
        self.ops.append(("tidy", group, layout, recurse))
        return 7

    def quiesce_group(self, group):
        self.ops.append(("quiesce", group))
        return "2 / 14 bytes"

    def explain_status(self, group="root", docs_dir="docs/explanations"):
        self.ops.append(("explain_status", group))
        return {"group": group, "configured": True, "exists": False,
                "outdated": False, "generated": None, "model": None,
                "path": "docs/explanations/x.md", "doc": None}

    def explain_group(self, group="root", docs_dir="docs/explanations",
                      recurse=True, force=False):
        self.ops.append(("explain", group, recurse, force))
        return [{"group": group, "status": "generated", "path": "x.md"}]


def _call(client, method, path, query=None, body=None, flows_dir=Path("flows")):
    return dispatch(client, threading.Lock(), method, path, query or {}, body or {}, flows_dir)


def test_about_and_listings():
    client = FakeClient()
    assert _call(client, "GET", "/api/about")[1]["version"] == "1.24.0"
    assert _call(client, "GET", "/api/processors")[1][0]["name"] == "Gen"
    assert _call(client, "GET", "/api/queues")[1][0]["queued"] == 2
    assert _call(client, "GET", "/api/groups")[1] == [
        {"id": "g1", "path": "Torture"}, {"id": "g2", "path": "Torture/Stage"}]
    assert _call(client, "GET", "/api/errors")[1][0]["name"] == "Lonely"


def test_processor_actions_route_to_client():
    client = FakeClient()
    for action, expect in (("run-once", "run_once"), ("start", "start"), ("stop", "stop")):
        status, payload = _call(client, "POST", f"/api/processors/p1/{action}")
        assert status == 200 and payload["ok"]
    assert [op for op, _ in client.ops] == ["run_once", "start", "stop"]


def test_queue_run_once_routes_to_endpoint():
    client = FakeClient()
    status, payload = _call(client, "POST", "/api/queues/c1/run-destination-once")
    assert status == 200 and payload["ran"] == "Log"
    status, payload = _call(client, "POST", "/api/queues/c1/run-source-once")
    assert status == 200 and payload["ran"] == "Gen"
    assert client.ops == [("queue_run_once", "c1", "destination"),
                          ("queue_run_once", "c1", "source")]
    status, payload = _call(client, "POST", "/api/queues/c1/frobnicate")
    assert status == 404


def test_tidy_passes_scope_and_direction():
    client = FakeClient()
    status, payload = _call(client, "POST", "/api/tidy",
                            body={"group": "Torture", "layout": "vertical"})
    assert status == 200 and payload["moved"] == 7
    status, payload = _call(client, "POST", "/api/tidy",
                            body={"group": "root", "recurse": False})
    assert status == 200
    assert client.ops == [("tidy", "Torture", "vertical", True),
                          ("tidy", "root", "horizontal", False)]


def test_explain_routes_pass_scope_and_flags():
    client = FakeClient()
    status, payload = _call(client, "GET", "/api/explain",
                            query={"group": ["Torture"]})
    assert status == 200 and payload["configured"] and not payload["exists"]
    status, payload = _call(client, "POST", "/api/explain",
                            body={"group": "Torture", "force": True, "recurse": False})
    assert status == 200 and payload["ok"]
    assert payload["results"][0]["status"] == "generated"
    assert ("explain", "Torture", False, True) in client.ops


def test_group_drain_reports_dropped():
    client = FakeClient()
    status, payload = _call(client, "POST", "/api/group/drain")
    assert status == 200 and payload["dropped"] == "2 / 14 bytes"


def test_flowfile_drilldown():
    client = FakeClient()
    _, files = _call(client, "GET", "/api/flowfiles", query={"connection_id": ["c1"]})
    assert files[0]["uuid"] == "u1"
    _, detail = _call(client, "GET", "/api/flowfile",
                      query={"connection_id": ["c1"], "uuid": ["u1"]})
    assert detail["content"] == "hello"


def test_unknown_route_404s_and_errors_are_json():
    client = FakeClient()
    assert _call(client, "GET", "/api/nope")[0] == 404
    status, payload = _call(client, "POST", "/api/processors/p1/explode")
    assert status == 404 and "explode" in payload["error"]


def test_client_exception_becomes_500_json():
    class Boom(FakeClient):
        def list_queues(self):
            raise RuntimeError("nifi down")

    status, payload = _call(Boom(), "GET", "/api/queues")
    assert status == 500 and "nifi down" in payload["error"]


def test_flows_listing(tmp_path):
    (tmp_path / "a.py").write_text("flow = None")
    (tmp_path / "b.txt").write_text("not a flow")
    status, flows = _call(FakeClient(), "GET", "/api/flows", flows_dir=tmp_path)
    assert status == 200 and len(flows) == 1 and flows[0].endswith("a.py")


def test_page_is_selfcontained_html():
    assert PAGE.lstrip().startswith("<!doctype html>")
    assert "src=" not in PAGE  # no external scripts — works offline/airgapped
