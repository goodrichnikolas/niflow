"""Web GUI API dispatch against a fake client (no sockets, no NiFi)."""
from __future__ import annotations

import threading
from pathlib import Path

import pytest

from niflow import webgui
from niflow.webgui import PAGE, dispatch


class FakeClient:
    class config:
        auth_mode = "password"

    base = "https://fake:8443/nifi-api"

    def __init__(self):
        self.ops = []
        # Follow-tab surface: one queued FlowFile that moves when run-once fires.
        self.queued = [{"uuid": "u1", "filename": "f.txt", "size": "14 B",
                        "position": 0}]
        self.events = []

    def version(self):
        return "1.24.0"

    def ui_url(self, group_id="", component_id=""):
        params = []
        if group_id:
            params.append(f"processGroupId={group_id}")
        if component_id:
            params.append(f"componentIds={component_id}")
        return "https://fake:8443/nifi" + ("/?" + "&".join(params) if params else "")

    def find_processors(self, type_contains="", group="root"):
        return [{"id": "p1", "name": "Gen", "type": "org.x.G", "state": "RUNNING",
                 "path": "", "group_id": "root"}]

    def resolve_group(self, group="root"):
        return "g1"

    def list_ports(self, group="root"):
        return []

    def connection_end(self, conn_id, which):
        return {"id": "p2", "name": "Log", "type": "PROCESSOR"}

    def connection_relationships(self, conn_id):
        return ["success"]

    def flowfile_events_since(self, uuid, after_event_id=-1, max_events=100):
        return [h for h in self.events if h["event_id"] > after_event_id]

    def walk_groups(self):
        yield "Torture", {"id": "g1", "name": "Torture"}
        yield "Torture/Stage", {"id": "g2", "name": "Stage"}

    def list_queues(self, group="root"):
        return [{"id": "c1", "source": "Gen", "destination": "Log", "path": "",
                 "queued": 2, "queued_label": "2 / 14 bytes", "group_id": "g1",
                 "source_id": "p1", "source_group_id": "g1",
                 "destination_id": "p2", "destination_group_id": "g2"}]

    def drain_connection(self, conn_id):
        self.ops.append(("drain_connection", conn_id))
        return "2 / 14 bytes"

    def purge_queues(self, group="root"):
        self.ops.append(("purge_queues", group))
        return "9 / 1 KB"

    def run_queue_endpoint_once(self, conn_id, which):
        self.ops.append(("queue_run_once", conn_id, which))
        return "Log" if which == "destination" else "Gen"

    def bulletins(self, limit=100):
        return []

    def validation_errors(self, group="root"):
        return [{"name": "Lonely", "path": "", "errors": ["relationship success unhandled"]}]

    def list_flowfiles(self, connection_id, max_results=100):
        self.ops.append(("list_flowfiles", connection_id))
        return [dict(f) for f in self.queued]

    def flowfile_detail(self, connection_id, uuid):
        return {"attributes": {"filename": "f.txt"}, "content": "hello"}

    def trace_flowfile(self, uuid, max_events=100):
        self.ops.append(("trace", uuid))
        return {"uuid": uuid, "hops": [{"event_id": 10, "component": "Update"}]}

    def event_content(self, event_id, direction="output"):
        self.ops.append(("event_content", event_id, direction))
        return "payload"

    def run_processor_once(self, proc_id):
        self.ops.append(("run_once", proc_id))
        # The stepper's world: the file leaves the queue and an event lands.
        self.queued = []
        self.events.append({
            "event_id": 11, "event_type": "ATTRIBUTES_MODIFIED",
            "time": "12:00:00", "component": "Log", "component_id": "p2",
            "component_type": "LogAttribute", "relationship": "", "size": 14,
            "attributes": {"a": "2"}, "changes": [], "group_id": "g1",
            "input_available": True, "output_available": True,
            "content_equal": True, "parents": [], "children": []})

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

    def explain_status(self, group="root", docs_dir="docs/explanations",
                       depth=1):
        self.ops.append(("explain_status", group, depth))
        return {"group": group, "configured": True, "exists": False,
                "outdated": False, "generated": None, "model": None,
                "path": "docs/explanations/x.md", "doc": None,
                "depth": depth, "plan": [], "documents": 1, "llm_calls": 1,
                "summarised_groups": 3}

    def explain_group(self, group="root", docs_dir="docs/explanations",
                      depth=1, force=False, confirm=None):
        self.ops.append(("explain", group, depth, force))
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


def test_queue_purge_drains_one_connection_and_reports_the_count():
    client = FakeClient()
    status, payload = _call(client, "POST", "/api/queues/c1/purge")
    assert status == 200 and payload["ok"]
    assert payload["dropped"] == "2 / 14 bytes"   # the page shows this to the user
    assert client.ops == [("drain_connection", "c1")]


def test_group_purge_is_scoped_to_the_flow_the_page_selected():
    client = FakeClient()
    status, payload = _call(client, "POST", "/api/group/purge",
                            body={"group": "Torture"})
    assert status == 200 and payload["dropped"] == "9 / 1 KB"
    assert payload["group"] == "Torture"
    # no group (the old global button) still means everything
    _call(client, "POST", "/api/group/purge")
    assert client.ops == [("purge_queues", "Torture"), ("purge_queues", "root")]


def test_about_carries_the_nifi_ui_base_for_deep_links():
    payload = _call(FakeClient(), "GET", "/api/about")[1]
    assert payload["ui"] == "https://fake:8443/nifi"


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
    # No depth given -> the high-level default, and the plan counts ride along.
    assert status == 200 and payload["configured"] and not payload["exists"]
    assert payload["llm_calls"] == 1 and ("explain_status", "Torture", 1) in client.ops
    status, payload = _call(client, "POST", "/api/explain",
                            body={"group": "Torture", "force": True, "depth": 0})
    assert status == 200 and payload["ok"]
    assert payload["results"][0]["status"] == "generated"
    # depth 0 means "everything below" and must survive the route as 0.
    assert ("explain", "Torture", 0, True) in client.ops


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


def test_trace_routes():
    client = FakeClient()
    status, payload = _call(client, "GET", "/api/trace", query={"uuid": ["u1"]})
    assert status == 200 and payload["hops"][0]["component"] == "Update"
    status, payload = _call(client, "GET", "/api/trace/content",
                            query={"event_id": ["10"], "direction": ["input"]})
    assert status == 200 and payload["content"] == "payload"
    # direction defaults to output when the page doesn't say
    _call(client, "GET", "/api/trace/content", query={"event_id": ["10"]})
    assert client.ops == [("trace", "u1"), ("event_content", "10", "input"),
                          ("event_content", "10", "output")]


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


def test_page_defaults_auto_refresh_on_and_remembers_the_choice():
    assert '<input type="checkbox" id="auto" checked>' in PAGE
    assert 'localStorage.setItem(AUTOKEY' in PAGE
    # nothing stored -> on
    assert '(localStorage.getItem(AUTOKEY) ?? "1") === "1"' in PAGE
    # the expensive tabs opt out of the 3s poll
    assert 'const NO_POLL = new Set(["trace", "follow", "explain", "flows"]);' in PAGE


def test_page_renders_component_links_through_one_helper():
    assert PAGE.count("function compLink(") == 1
    assert 'target="_blank"' in PAGE
    assert 'onclick="event.stopPropagation()"' in PAGE  # rows keep their own clicks
    # every place a component is named goes through the helper
    assert PAGE.count("compLink(") >= 9
    # queue endpoints have no ids in NiFi's status snapshot (true on 1.24 and
    # 2.7.2), so they're matched against the cached processor listing
    assert "function procIndex()" in PAGE and "function endpointLink(" in PAGE


# ---------------------------------------------------------------- follow tab


@pytest.fixture(autouse=True)
def _no_leftover_follow_session():
    """The stepper's session is module state — no test may inherit another's."""
    webgui._FOLLOW["follower"] = None
    yield
    webgui._FOLLOW["follower"] = None


def _start(client):
    entries = _call(client, "GET", "/api/follow/entrypoints",
                    query={"group": ["Torture"]})[1]["entries"]
    return _call(client, "POST", "/api/follow/start",
                 body={"group": "Torture", "entry": entries[0]})


def test_follow_entrypoints_are_offered_before_anything_is_stopped():
    client = FakeClient()
    status, payload = _call(client, "GET", "/api/follow/entrypoints",
                            query={"group": ["Torture"]})
    assert status == 200
    assert [e["kind"] for e in payload["entries"]] == ["queue", "source"]
    assert client.ops == []          # listing start points mutates nothing


def test_follow_start_quiesces_and_step_returns_hops_to_flash():
    client = FakeClient()
    status, payload = _start(client)
    assert status == 200 and payload["active"] and payload["current"] == "u1"
    assert ("stop_group", "g1") in client.ops     # the group is quiesced

    status, payload = _call(client, "POST", "/api/follow/step")
    assert status == 200
    assert payload["fresh"] == [11]               # the page flashes this hop
    assert payload["outcome"]["status"] == "advanced"
    assert [h["event_id"] for h in payload["hops"]] == [11]
    # The session is readable again without re-running anything.
    assert _call(client, "GET", "/api/follow/session")[1]["hops"][0]["event_id"] == 11


def test_follow_mute_routes_change_the_view_not_nifi():
    client = FakeClient()
    _start(client)
    before = list(client.ops)
    status, payload = _call(client, "POST", "/api/follow/mute",
                            body={"spec": "rel:failure"})
    assert status == 200 and payload["mute_rules"] == ["rel:failure"]
    status, payload = _call(client, "POST", "/api/follow/unmute",
                            body={"spec": "rel:failure"})
    assert status == 200 and payload["mute_rules"] == []
    assert client.ops == before                   # not one REST call


def test_follow_actions_need_a_session_and_stop_ends_it():
    client = FakeClient()
    assert _call(client, "POST", "/api/follow/step")[0] == 409
    _start(client)
    status, payload = _call(client, "POST", "/api/follow/stop",
                            body={"restore": True})
    assert status == 200 and payload["active"] is False
    assert ("start", "p1") in client.ops          # Gen was RUNNING before
    assert _call(client, "POST", "/api/follow/step")[0] == 409


def test_follow_session_offers_the_saved_one_after_a_restart():
    client = FakeClient()
    _start(client)
    webgui._FOLLOW["follower"] = None             # as if the server restarted
    payload = _call(client, "GET", "/api/follow/session")[1]
    assert payload["active"] is False
    assert payload["resumable"]["current"] == "u1"
    status, payload = _call(client, "POST", "/api/follow/start",
                            body={"group": "Torture", "resume": True})
    assert status == 200 and payload["current"] == "u1"


def test_trace_route_annotates_hops_like_the_stepper_does():
    client = FakeClient()
    payload = _call(client, "GET", "/api/trace", query={"uuid": ["u1"]})[1]
    # Both tabs render the same shape, so both go through annotate_hops.
    assert "diff" in payload["hops"][0] and "content_change" in payload["hops"][0]


def test_page_has_a_follow_tab_that_reuses_the_trace_renderer():
    assert '["follow", "Follow"]' in PAGE
    assert PAGE.count("function hopCard(") == 1   # one renderer, two tabs
    assert PAGE.count("=> hopCard(h, i,") == 2   # called from Trace and Follow
    assert "keep running in NiFi" in PAGE         # the mute contract, on screen
