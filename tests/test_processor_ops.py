"""Unit tests for processor-level client ops (list / stop / run-once).

Same fake-session approach as test_client_unit: a scripted server exposing a
nested tree — root holds Gen1 + a Child group, Child holds Gen2, a PutFile,
and a Grand group with Gen3 — so recursion and path-building are honest.
"""
import json

import pytest

from niflow.client import NiFiClient
from niflow.config import NiFiConfig

BASE = "https://nifi.test/nifi-api"


class FakeResponse:
    def __init__(self, status_code=200, body=None, text=None):
        self.status_code = status_code
        self._body = body
        self.text = text if text is not None else json.dumps(body or {})

    def json(self):
        return self._body


def _proc(pid, name, ptype, state, errors=None, group_id=""):
    component = {"id": pid, "name": name, "type": ptype, "state": state,
                 "parentGroupId": group_id}
    if errors:
        component["validationErrors"] = errors
    return {"id": pid, "component": component}


def _group(gid, name):
    return {"component": {"id": gid, "name": name}}


GEN = "org.apache.nifi.processors.standard.GenerateFlowFile"
PUT = "org.apache.nifi.processors.standard.PutFile"


class FakeNiFi:
    def __init__(self):
        self.run_status_calls = []  # (proc_id, body)
        self.revisions = {"g1": 3, "g2": 0}
        self.drop_polls = 0
        self.drop_deleted = False
        self.group_ops = []  # ordered (method, path) for state/service/drain calls

    def request(self, method, url, **kw):
        assert url.startswith(BASE)
        path = url[len(BASE):]

        if (method, path) == ("POST", "/access/token"):
            return FakeResponse(201, text="tok-123")
        assert kw.get("headers", {}).get("Authorization") == "Bearer tok-123", path

        if (method, path) == ("GET", "/flow/process-groups/root"):
            return FakeResponse(200, {"processGroupFlow": {"id": "root-id", "flow": {}}})
        if (method, path) == ("GET", "/flow/process-groups/root-id"):
            return FakeResponse(200, {"processGroupFlow": {"id": "root-id", "flow": {
                "processors": [_proc("g1", "Gen1", GEN, "STOPPED")],
                "connections": [{"id": "c1"}],  # so _empty_queues runs the drain
                "processGroups": [_group("child-id", "Child")],
            }}})
        if (method, path) == ("GET", "/flow/process-groups/child-id"):
            return FakeResponse(200, {"processGroupFlow": {"id": "child-id", "flow": {
                "processors": [
                    _proc("g2", "Gen2", GEN, "RUNNING", group_id="child-id"),
                    _proc("p1", "Sink", PUT, "RUNNING",
                          errors=["'Directory' is invalid: required"],
                          group_id="child-id"),
                ],
                "processGroups": [_group("grand-id", "Grand")],
            }}})
        if (method, path) == ("GET", "/flow/process-groups/grand-id"):
            return FakeResponse(200, {"processGroupFlow": {"id": "grand-id", "flow": {
                "processors": [_proc("g3", "Gen3", GEN, "STOPPED")],
            }}})

        if method == "GET" and path.startswith("/processors/"):
            pid = path.rsplit("/", 1)[-1]
            return FakeResponse(200, {"id": pid, "revision": {"version": self.revisions[pid]}})
        if method == "PUT" and path.endswith("/run-status"):
            pid = path.split("/")[2]
            body = kw["json"]
            assert body["revision"]["version"] == self.revisions[pid], "stale revision"
            self.run_status_calls.append((pid, body))
            self.revisions[pid] += 1  # NiFi bumps the version on every change
            return FakeResponse(200, {"id": pid})

        if (method, path) == ("GET", "/flow/bulletin-board"):
            assert kw["params"] == {"limit": 100}
            return FakeResponse(200, {"bulletinBoard": {"bulletins": [
                {"id": 1, "bulletin": {"timestamp": "12:00:00 UTC", "level": "ERROR",
                                       "sourceName": "Sink", "sourceId": "p1",
                                       "groupId": "child-id",
                                       "message": "old failure"}},
                {"id": 2},  # payload withheld: no permission
                {"id": 3, "bulletin": {"timestamp": "12:00:05 UTC", "level": "WARN",
                                       "sourceName": "Gen2", "sourceId": "g2",
                                       "groupId": "child-id",
                                       "message": "new warning"}},
            ]}})

        if (method, path) == ("PUT", "/flow/process-groups/root-id"):
            assert kw["json"]["state"] == "STOPPED"
            self.group_ops.append(("stop", path))
            return FakeResponse(200, {})
        if (method, path) == ("PUT", "/flow/process-groups/root-id/controller-services"):
            assert kw["json"]["state"] == "DISABLED"
            self.group_ops.append(("disable", path))
            return FakeResponse(200, {})

        if (method, path) == ("POST", "/process-groups/root-id/empty-all-connections-requests"):
            self.group_ops.append(("drain", path))
            return FakeResponse(200, {"dropRequest": {"id": "dr-1", "finished": False}})
        if (method, path) == ("GET", "/process-groups/root-id/empty-all-connections-requests/dr-1"):
            self.drop_polls += 1
            return FakeResponse(200, {"dropRequest": {
                "id": "dr-1", "finished": True, "dropped": "12 / 4.2 KB"}})
        if (method, path) == ("DELETE", "/process-groups/root-id/empty-all-connections-requests/dr-1"):
            self.drop_deleted = True
            return FakeResponse(200, {})

        raise AssertionError(f"unexpected call: {method} {path}")


@pytest.fixture()
def client():
    config = NiFiConfig(host=BASE, username="admin", password="pw")
    return NiFiClient(config, session=FakeNiFi())


def test_find_processors_recurses_with_paths(client):
    found = client.find_processors("GenerateFlowFile")
    assert [(p["id"], p["path"]) for p in found] == [
        ("g1", ""),
        ("g2", "Child"),
        ("g3", "Child/Grand"),
    ]
    assert all(p["type"] == GEN for p in found)


def test_find_processors_filter_excludes_other_types(client):
    assert [p["id"] for p in client.find_processors("PutFile")] == ["p1"]
    # No filter -> everything.
    assert len(client.find_processors()) == 4


def test_run_once_stops_then_triggers_one_pass(client):
    client.run_processor_once("g1")
    states = [body["state"] for pid, body in client.session.run_status_calls]
    assert states == ["STOPPED", "RUN_ONCE"]
    # Both calls hit the same processor with fresh revisions (asserted in the fake).
    assert {pid for pid, _ in client.session.run_status_calls} == {"g1"}


def test_purge_queues_polls_and_cleans_up(client):
    assert client.purge_queues() == "12 / 4.2 KB"
    assert client.session.drop_polls == 1  # polled until finished
    assert client.session.drop_deleted  # request cleaned up afterwards


def test_quiesce_group_stops_disables_then_drains_in_order(client):
    dropped = client.quiesce_group()
    assert dropped == "12 / 4.2 KB"
    # The exact order matters: a running processor or enabled service would
    # refuse the drain / block a later delete.
    assert [op for op, _ in client.session.group_ops] == ["stop", "disable", "drain"]
    assert client.session.drop_deleted


def test_validation_errors_lists_invalid_processors_with_paths(client):
    invalid = client.validation_errors()
    assert invalid == [{
        "id": "p1",
        "name": "Sink",
        "path": "Child",
        "group_id": "child-id",
        "errors": ["'Directory' is invalid: required"],
    }]


def test_bulletins_flatten_newest_first_and_skip_unreadable(client):
    bulletins = client.bulletins()
    assert [b["message"] for b in bulletins] == ["new warning", "old failure"]
    assert bulletins[0] == {
        "id": 3,
        "time": "12:00:05 UTC",
        "level": "WARN",
        "source": "Gen2",
        "source_id": "g2",
        "group_id": "child-id",
        "message": "new warning",
    }


def test_ui_url_builds_deep_link_into_the_canvas(client):
    assert client.ui_url("child-id", "p1") == (
        "https://nifi.test/nifi/?processGroupId=child-id&componentIds=p1"
    )
    # No ids -> just the bare UI root (still a valid link to open).
    assert client.ui_url() == "https://nifi.test/nifi"


def test_processor_label_shows_nested_path():
    pytest.importorskip("PyQt6")
    from niflow.gui import processor_label

    assert processor_label({"path": "Child/Grand", "name": "Gen3"}) == "Child / Grand / Gen3"
    assert processor_label({"path": "", "name": "Gen1"}) == "Gen1"
