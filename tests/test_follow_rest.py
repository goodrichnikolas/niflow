"""Unit tests for the REST helpers the live stepper added to InspectMixin:
connection_end and the incremental flowfile_events_since. Scripted fake
server (same pattern as test_inspect.py), no live NiFi.
"""
import json

import pytest

from niflow.client import NiFiApiError, NiFiClient
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
    def __init__(self):
        self.deleted = []
        self.run_states = []  # (proc id, state) PUTs, in order

    def request(self, method, url, **kw):
        path = url[len(BASE):]
        if (method, path) == ("POST", "/access/token"):
            return FakeResponse(201, text="tok-123")

        # --- connections: c1 ends in a processor, c2 in a funnel ---
        if (method, path) == ("GET", "/connections/c1"):
            return FakeResponse(200, {"component": {
                "source": {"id": "g1", "name": "Gen", "type": "PROCESSOR"},
                "destination": {"id": "p1", "name": "Sink", "type": "PROCESSOR"},
            }})
        if (method, path) == ("GET", "/connections/c2"):
            return FakeResponse(200, {"component": {
                "source": {"id": "g1", "name": "Gen", "type": "PROCESSOR"},
                "destination": {"id": "f1", "name": "", "type": "FUNNEL"},
            }})

        # --- run-once plumbing for run_queue_endpoint_once ---
        if (method, path) == ("GET", "/processors/p1"):
            return FakeResponse(200, {"revision": {"version": 3},
                                      "component": {"id": "p1"}})
        if (method, path) == ("PUT", "/processors/p1/run-status"):
            self.run_states.append(("p1", kw["json"]["state"]))
            return FakeResponse(200, {})

        # --- provenance query for ff-1: newest first, ids 10 and 11 ---
        if (method, path) == ("POST", "/provenance"):
            terms = kw["json"]["provenance"]["request"]["searchTerms"]
            assert terms["FlowFileUUID"]["value"] == "ff-1"
            return FakeResponse(200, {"provenance": {"id": "pq-1", "finished": True,
                "results": {"provenanceEvents": [
                    {"eventId": 11, "eventType": "ROUTE"},
                    {"eventId": 10, "eventType": "CREATE"},
                ]}}})
        if (method, path) == ("DELETE", "/provenance/pq-1"):
            self.deleted.append("pq-1")
            return FakeResponse(200, {})
        if (method, path) == ("GET", "/provenance-events/10"):
            raise AssertionError(
                "must not fetch the detail of an already-seen event")
        if (method, path) == ("GET", "/provenance-events/11"):
            return FakeResponse(200, {"provenanceEvent": {
                "eventId": 11, "eventType": "ROUTE", "eventTime": "12:00:02",
                "componentName": "Router", "componentId": "r1",
                "componentType": "RouteOnAttribute", "relationship": "matched",
                "fileSizeBytes": 9, "inputContentAvailable": True,
                "outputContentAvailable": True, "contentEqual": True,
                "attributes": [
                    {"name": "a", "value": "2", "previousValue": "1"},
                    {"name": "fresh", "value": "x"},
                ]}})

        raise AssertionError(f"unexpected call: {method} {path}")


@pytest.fixture()
def client():
    return NiFiClient(NiFiConfig(host=BASE, username="admin", password="pw"),
                      session=FakeNiFi())


def test_connection_end_returns_the_raw_ref(client):
    assert client.connection_end("c1", "destination") == {
        "id": "p1", "name": "Sink", "type": "PROCESSOR"}
    assert client.connection_end("c2", "destination")["type"] == "FUNNEL"


def test_run_queue_endpoint_once_still_works_through_connection_end(client):
    assert client.run_queue_endpoint_once("c1", "destination") == "Sink"
    # run-once = stop first, then the RUN_ONCE trigger.
    assert client.session.run_states == [("p1", "STOPPED"), ("p1", "RUN_ONCE")]


def test_run_queue_endpoint_once_refuses_a_funnel(client):
    with pytest.raises(NiFiApiError, match="funnel"):
        client.run_queue_endpoint_once("c2", "destination")


def test_flowfile_events_since_fetches_only_new_events(client):
    # The fake asserts event 10's detail endpoint is never hit.
    hops = client.flowfile_events_since("ff-1", after_event_id=10)
    assert [h["event_id"] for h in hops] == [11]
    assert hops[0]["relationship"] == "matched"
    assert hops[0]["changes"] == [
        {"name": "a", "before": "1", "after": "2"},
        {"name": "fresh", "before": None, "after": "x"},
    ]
    assert "pq-1" in client.session.deleted
