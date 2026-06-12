"""Unit tests for :mod:`niflow.client` against a scripted fake HTTP session.

The fake implements just enough of the NiFi REST surface (1.x/2.x common
endpoints) to verify the client's orchestration: auth headers, name→id
resolution, pull, delete-and-recreate push ordering, parameter/secrets
application, and version-control stripping on copy.
"""
import json
import re

import pytest

from niflow import Flow, Parameter, ParameterContext, Processor
from niflow.client import NiFiClient, NiFiApiError, _load_secrets
from niflow.config import NiFiConfig

BASE = "https://nifi.test/nifi-api"


class FakeResponse:
    def __init__(self, status_code=200, body=None, text=None):
        self.status_code = status_code
        self._body = body
        self.text = text if text is not None else json.dumps(body or {})

    def json(self):
        return self._body


def _snapshot_for(name: str) -> dict:
    """A real snapshot produced by niflow itself, so parsing is honest."""
    flow = Flow(name)
    ctx = ParameterContext(
        "etl-params",
        parameters=[
            Parameter("db.url", value="jdbc:old"),
            Parameter("db.password", sensitive=True),
        ],
    )
    flow.parameter_context = ctx
    flow.add_processor(Processor(name="Fetch", type="org.x.Fetch"))
    return json.loads(flow.to_json())


class FakeNiFi:
    """Scripted server: root → {Prod Flow, Other, Other/Sub}."""

    def __init__(self):
        self.calls = []  # (method, path, kwargs)
        self.deleted = []
        self.created = []  # bodies of create calls
        self.param_updates = []  # update-request bodies
        self.fail_inline_create = False

    # -- session interface ----------------------------------------------------
    def request(self, method, url, **kwargs):
        assert url.startswith(BASE)
        path = url[len(BASE):]
        self.calls.append((method, path, kwargs))
        return self.route(method, path, kwargs)

    def route(self, method, path, kw):
        if (method, path) == ("POST", "/access/token"):
            assert kw["data"] == {"username": "admin", "password": "pw"}
            return FakeResponse(201, text="tok-123")
        # Everything below requires the bearer token.
        assert kw.get("headers", {}).get("Authorization") == "Bearer tok-123", path

        if (method, path) == ("GET", "/flow/about"):
            return FakeResponse(200, {"about": {"version": "1.24.0"}})
        if (method, path) == ("GET", "/flow/process-groups/root"):
            return FakeResponse(200, {"processGroupFlow": {"id": "root-id", "flow": {}}})
        if (method, path) == ("GET", "/flow/process-groups/root-id"):
            return FakeResponse(200, {"processGroupFlow": {"id": "root-id", "flow": {
                "processGroups": [
                    {"component": {"id": "pf-id", "name": "Prod Flow",
                                   "position": {"x": 123.0, "y": 456.0}}},
                    {"component": {"id": "ot-id", "name": "Other", "position": {"x": 0, "y": 0}}},
                ]}}})
        if (method, path) == ("GET", "/flow/process-groups/pf-id"):
            return FakeResponse(200, {"processGroupFlow": {"id": "pf-id", "flow": {"processGroups": []}}})
        if (method, path) == ("GET", "/flow/process-groups/ot-id"):
            return FakeResponse(200, {"processGroupFlow": {"id": "ot-id", "flow": {
                "processGroups": [{"component": {"id": "sub-id", "name": "Sub", "position": {"x": 0, "y": 0}}}]}}})
        if (method, path) == ("GET", "/flow/process-groups/sub-id"):
            return FakeResponse(200, {"processGroupFlow": {"id": "sub-id", "flow": {"processGroups": []}}})

        m = re.fullmatch(r"GET /process-groups/(\w+-id)", f"{method} {path}")
        if m:
            pg = m.group(1)
            parent = {"pf-id": "root-id", "ot-id": "root-id", "sub-id": "ot-id"}.get(pg, "root-id")
            name = {"pf-id": "Prod Flow", "ot-id": "Other", "sub-id": "Sub"}.get(pg, pg)
            return FakeResponse(200, {
                "revision": {"version": 7},
                "component": {"id": pg, "name": name, "parentGroupId": parent,
                              "position": {"x": 123.0, "y": 456.0}},
            })

        if (method, path) == ("GET", "/process-groups/pf-id/download"):
            snap = _snapshot_for("Prod Flow")
            # Simulate a registry-tracked nested group.
            snap["flowContents"]["versionedFlowCoordinates"] = {"registryId": "r1"}
            return FakeResponse(200, snap)

        if method == "PUT" and re.fullmatch(r"/flow/process-groups/[\w-]+", path):
            return FakeResponse(200, {})
        if method == "PUT" and path.endswith("/controller-services"):
            return FakeResponse(200, {})
        if method == "POST" and path.endswith("/empty-all-connections-requests"):
            return FakeResponse(202, {"dropRequest": {"id": "drop-1", "finished": True}})
        if method == "DELETE" and "/empty-all-connections-requests/" in path:
            return FakeResponse(200, {})
        if method == "DELETE" and re.fullmatch(r"/process-groups/[\w-]+", path):
            self.deleted.append(path.rsplit("/", 1)[-1])
            assert kw["params"]["version"] == 7
            return FakeResponse(200, {})

        if method == "POST" and re.fullmatch(r"/process-groups/[\w-]+/process-groups", path):
            if self.fail_inline_create:
                return FakeResponse(405, {}, text="inline create not supported")
            self.created.append(("inline", kw["json"]))
            return FakeResponse(201, {"id": "new-id"})
        if method == "POST" and path.endswith("/process-groups/upload"):
            self.created.append(("upload", kw))
            return FakeResponse(201, {"id": "new-id"})

        if (method, path) == ("GET", "/flow/parameter-contexts"):
            return FakeResponse(200, {"parameterContexts": [{
                "revision": {"version": 2},
                "component": {
                    "id": "ctx-1",
                    "name": "etl-params",
                    "parameters": [
                        {"parameter": {"name": "db.url", "sensitive": False, "value": "jdbc:live"}},
                        {"parameter": {"name": "db.password", "sensitive": True, "value": None}},
                    ],
                },
            }]})
        if (method, path) == ("POST", "/parameter-contexts/ctx-1/update-requests"):
            self.param_updates.append(kw["json"])
            return FakeResponse(200, {"request": {"requestId": "req-1", "complete": True}})
        if method == "DELETE" and "/update-requests/" in path:
            return FakeResponse(200, {})

        raise AssertionError(f"Unscripted call: {method} {path}")


@pytest.fixture()
def fake():
    return FakeNiFi()


@pytest.fixture()
def client(fake):
    config = NiFiConfig(host=BASE, username="admin", password="pw")
    return NiFiClient(config, session=fake)


def test_login_and_version(client):
    assert client.version() == "1.24.0"


def test_resolve_group_by_name_path_and_id(client):
    assert client.resolve_group("Prod Flow") == "pf-id"
    assert client.resolve_group("Other/Sub") == "sub-id"
    uuid = "12345678-1234-1234-1234-123456789abc"
    assert client.resolve_group(uuid) == uuid
    with pytest.raises(ValueError, match="No process group"):
        client.resolve_group("Nope")


def test_pull_flow_refreshes_live_parameter_values(client):
    flow = client.pull_flow("Prod Flow")
    assert flow.name == "Prod Flow"
    assert flow.parent_pg == "root"
    assert flow.nifi_id == "pf-id"
    params = {p.name: p for p in flow.parameter_context.parameters}
    assert params["db.url"].value == "jdbc:live"  # refreshed from the server
    assert params["db.password"].value is None    # sensitive: never pulled


def test_push_replaces_existing_group_in_order(client, fake):
    flow = Flow("Prod Flow")
    flow.parameter_context = ParameterContext(
        "etl-params",
        parameters=[Parameter("db.url", value="jdbc:new"),
                    Parameter("db.password", sensitive=True)],
    )
    flow.add_processor(Processor(name="Fetch", type="org.x.Fetch"))

    new_id = client.push_flow(flow, secrets={"db.password": "hunter2"})
    assert new_id == "new-id"
    assert flow.nifi_id == "new-id"

    # Old group was torn down before the new one was created.
    assert fake.deleted == ["pf-id"]
    kind, body = fake.created[0]
    assert kind == "inline"
    assert body["component"]["name"] == "Prod Flow"
    # Canvas position of the replaced group is preserved.
    assert body["component"]["position"] == {"x": 123.0, "y": 456.0}
    assert body["versionedFlowSnapshot"]["flowContents"]["name"] == "Prod Flow"

    # Parameter update carries the model value AND the secret.
    sent = {p["parameter"]["name"]: p["parameter"]
            for p in fake.param_updates[0]["component"]["parameters"]}
    assert sent["db.url"]["value"] == "jdbc:new"
    assert sent["db.password"]["value"] == "hunter2"
    assert sent["db.password"]["sensitive"] is True


def test_push_falls_back_to_multipart_upload(client, fake):
    fake.fail_inline_create = True
    flow = Flow("Fresh Flow")
    flow.add_processor(Processor(name="P", type="org.x.P"))
    client.push_flow(flow)
    kinds = [k for k, _ in fake.created]
    assert kinds == ["upload"]


def test_copy_strips_version_control(client, fake):
    new_id = client.copy_group("Prod Flow")
    assert new_id == "new-id"
    _, body = fake.created[0]
    contents = body["versionedFlowSnapshot"]["flowContents"]
    assert "versionedFlowCoordinates" not in json.dumps(contents)
    assert body["component"]["name"] == "Prod Flow (copy)"
    # Copy is offset on the canvas so it doesn't stack on the original.
    assert body["component"]["position"]["x"] == 123.0 + 480.0


def test_load_secrets_file(tmp_path):
    f = tmp_path / "secrets.env"
    f.write_text(
        "# comment\n"
        "db.password=hunter2\n"
        "etl-params::api.key = abc=def \n"
        "\n"
    )
    secrets = _load_secrets(f)
    assert secrets == {"db.password": "hunter2", "etl-params::api.key": "abc=def"}


def test_load_secrets_missing_explicit_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        _load_secrets(tmp_path / "nope.env")


def test_api_error_carries_status(client, fake):
    fake.route = lambda m, p, k: FakeResponse(403, {}, text="forbidden")
    with pytest.raises(NiFiApiError, match="403"):
        client.version()
