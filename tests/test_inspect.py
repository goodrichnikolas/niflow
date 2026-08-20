"""Unit tests for the FlowFile-inspection client ops (queues, contents,
provenance) that back the Inspector window. Scripted fake server, no live NiFi.
"""
import json

import pytest

from niflow.client import NiFiClient
from niflow.config import NiFiConfig

BASE = "https://nifi.test/nifi-api"
GEN = "org.apache.nifi.processors.standard.GenerateFlowFile"
PUT = "org.apache.nifi.processors.standard.PutFile"


class FakeResponse:
    def __init__(self, status_code=200, body=None, text=None):
        self.status_code = status_code
        self._body = body
        self.text = text if text is not None else json.dumps(body or {})

    def json(self):
        return self._body


def _proc(pid, name, ptype):
    return {"id": pid, "component": {"id": pid, "name": name, "type": ptype}}


def _conn(cid, src_id, src_name, dst_id, dst_name, queued=0, label=""):
    return {
        "id": cid,
        "component": {"id": cid, "name": "",
                      "source": {"id": src_id, "name": src_name, "type": "PROCESSOR"},
                      "destination": {"id": dst_id, "name": dst_name, "type": "PROCESSOR"}},
        "status": {"aggregateSnapshot": {"flowFilesQueued": queued, "queued": label}},
    }


class FakeNiFi:
    def __init__(self):
        self.deleted = []  # request ids we cleaned up

    def request(self, method, url, **kw):
        path = url[len(BASE):]
        if (method, path) == ("POST", "/access/token"):
            return FakeResponse(201, text="tok-123")

        if (method, path) == ("GET", "/flow/process-groups/root"):
            return FakeResponse(200, {"processGroupFlow": {"id": "root-id", "flow": {}}})
        if (method, path) == ("GET", "/flow/process-groups/root-id"):
            return FakeResponse(200, {"processGroupFlow": {"flow": {
                "processors": [_proc("g1", "Gen1", GEN), _proc("p1", "Sink", PUT)],
                "connections": [_conn("c1", "g1", "Gen1", "p1", "Sink",
                                      queued=2, label="2 / 1.5 KB")],
                "processGroups": [{"component": {"id": "child-id", "name": "Child"}}],
            }}})
        if (method, path) == ("GET", "/flow/process-groups/child-id"):
            return FakeResponse(200, {"processGroupFlow": {"flow": {
                "processors": [_proc("m1", "Mid", PUT)],
                "connections": [],
                "processGroups": [],
            }}})

        # --- queue listing-request lifecycle ---
        if (method, path) == ("POST", "/flowfile-queues/c1/listing-requests"):
            return FakeResponse(200, {"listingRequest": {"id": "lr-1", "finished": False}})
        if (method, path) == ("GET", "/flowfile-queues/c1/listing-requests/lr-1"):
            return FakeResponse(200, {"listingRequest": {"id": "lr-1", "finished": True,
                "flowFileSummaries": [
                    {"uuid": "ff-1", "filename": "a.json", "size": 9, "position": 0},
                    {"uuid": "ff-2", "filename": "b.json", "size": 9, "position": 1},
                ]}})
        if (method, path) == ("DELETE", "/flowfile-queues/c1/listing-requests/lr-1"):
            self.deleted.append("lr-1")
            return FakeResponse(200, {})

        # --- drop-request lifecycle (per-queue purge) ---
        if (method, path) == ("POST", "/flowfile-queues/c1/drop-requests"):
            return FakeResponse(200, {"dropRequest": {"id": "dr-1", "finished": False}})
        if (method, path) == ("GET", "/flowfile-queues/c1/drop-requests/dr-1"):
            return FakeResponse(200, {"dropRequest": {"id": "dr-1", "finished": True,
                                                      "dropped": "2 / 1.5 KB"}})
        if (method, path) == ("DELETE", "/flowfile-queues/c1/drop-requests/dr-1"):
            self.deleted.append("dr-1")
            return FakeResponse(200, {})

        # --- flowfile detail + content ---
        if (method, path) == ("GET", "/flowfile-queues/c1/flowfiles/ff-1"):
            return FakeResponse(200, {"flowFile": {"uuid": "ff-1", "filename": "a.json",
                "size": 9, "attributes": {"filename": "a.json", "a": "1"}}})
        if (method, path) == ("GET", "/flowfile-queues/c1/flowfiles/ff-1/content"):
            return FakeResponse(200, text='{"a":"1"}')

        # ff-2 is a zero-byte FlowFile: NiFi 409s on its content endpoint.
        if (method, path) == ("GET", "/flowfile-queues/c1/flowfiles/ff-2"):
            return FakeResponse(200, {"flowFile": {"uuid": "ff-2", "filename": "empty",
                "size": 0, "attributes": {"filename": "empty"}}})
        if path == "/flowfile-queues/c1/flowfiles/ff-2/content":
            raise AssertionError("must not fetch content of a zero-byte FlowFile")

        # --- provenance query lifecycle ---
        if (method, path) == ("POST", "/provenance"):
            terms = kw["json"]["provenance"]["request"]["searchTerms"]
            if "FlowFileUUID" in terms:
                assert terms["FlowFileUUID"]["value"] == "ff-1"
                return FakeResponse(200, {"provenance": {"id": "pq-2", "finished": False}})
            assert terms["ProcessorID"]["value"] == "p1"
            return FakeResponse(200, {"provenance": {"id": "pq-1", "finished": False}})
        if (method, path) == ("GET", "/provenance/pq-1"):
            return FakeResponse(200, {"provenance": {"id": "pq-1", "finished": True,
                "results": {"provenanceEvents": [
                    {"eventId": "ev-1", "eventType": "SEND", "eventTime": "12:00:00",
                     "componentName": "Sink", "flowFileUuid": "ff-1"},
                ]}}})
        if (method, path) == ("DELETE", "/provenance/pq-1"):
            self.deleted.append("pq-1")
            return FakeResponse(200, {})

        # --- FlowFile trace: the query returns summaries newest first ---
        if (method, path) == ("GET", "/provenance/pq-2"):
            return FakeResponse(200, {"provenance": {"id": "pq-2", "finished": True,
                "results": {"provenanceEvents": [
                    {"eventId": 11, "eventType": "ROUTE"},
                    {"eventId": 10, "eventType": "ATTRIBUTES_MODIFIED"},
                ]}}})
        if (method, path) == ("DELETE", "/provenance/pq-2"):
            self.deleted.append("pq-2")
            return FakeResponse(200, {})
        if (method, path) == ("GET", "/provenance-events/10"):
            return FakeResponse(200, {"provenanceEvent": {
                "eventId": 10, "eventType": "ATTRIBUTES_MODIFIED",
                "eventTime": "12:00:01", "componentName": "Update",
                "componentId": "u1", "componentType": "UpdateAttribute",
                "groupId": "root-id",
                "fileSizeBytes": 9, "inputContentAvailable": True,
                "outputContentAvailable": True, "contentEqual": True,
                "attributes": [
                    # changed / untouched (previousValue == value) / born here
                    {"name": "a", "value": "2", "previousValue": "1"},
                    {"name": "filename", "value": "a.json", "previousValue": "a.json"},
                    {"name": "fresh", "value": "x"},
                ]}})
        if (method, path) == ("GET", "/provenance-events/11"):
            return FakeResponse(200, {"provenanceEvent": {
                "eventId": 11, "eventType": "ROUTE", "eventTime": "12:00:02",
                "componentName": "Router", "componentId": "r1",
                "componentType": "RouteOnAttribute", "relationship": "unmatched",
                "fileSizeBytes": 9, "inputContentAvailable": True,
                "outputContentAvailable": False, "contentEqual": True,
                "childUuids": ["ff-9"],
                "attributes": [{"name": "a", "value": "2", "previousValue": "2"}]}})
        if (method, path) == ("GET", "/provenance-events/11/content/input"):
            return FakeResponse(200, text="payload-in")
        if path == "/provenance-events/11/content/output":
            raise AssertionError("must not fetch content NiFi says is unavailable")

        # --- provenance event detail + content ---
        if (method, path) == ("GET", "/provenance-events/ev-1"):
            return FakeResponse(200, {"provenanceEvent": {"eventType": "SEND",
                "fileSize": "9 bytes", "fileSizeBytes": 9,
                "attributes": [{"name": "filename", "value": "a.json"},
                               {"name": "a", "value": "1"}]}})
        if (method, path) == ("GET", "/provenance-events/ev-1/content/output"):
            return FakeResponse(200, text='{"a":"1"}')

        raise AssertionError(f"unexpected call: {method} {path}")


@pytest.fixture()
def client():
    return NiFiClient(NiFiConfig(host=BASE, username="admin", password="pw"), session=FakeNiFi())


def test_list_queues_walks_the_tree(client):
    queues = client.list_queues()
    assert queues == [{
        "id": "c1", "source": "Gen1", "destination": "Sink",
        "path": "", "queued": 2, "queued_label": "2 / 1.5 KB",
        # ids for the GUIs' NiFi deep links; endpoints fall back to the
        # connection's own group when the DTO doesn't say where they live
        "group_id": "root-id",
        "source_id": "g1", "source_group_id": "root-id",
        "destination_id": "p1", "destination_group_id": "root-id",
    }]


def test_drain_connection_reports_what_it_dropped(client):
    # The GUI purge buttons tell the user how much they just destroyed.
    assert client.drain_connection("c1") == "2 / 1.5 KB"
    assert "dr-1" in client.session.deleted  # request cleaned up either way


def test_list_sinks_are_processors_that_feed_nothing(client):
    sinks = {(s["name"], s["path"]) for s in client.list_sinks()}
    # Gen1 feeds c1 so it's excluded; Sink and the lone child Mid are terminal.
    assert sinks == {("Sink", ""), ("Mid", "Child")}


def test_list_flowfiles_polls_and_cleans_up(client):
    files = client.list_flowfiles("c1")
    assert [f["uuid"] for f in files] == ["ff-1", "ff-2"]
    assert files[0] == {"uuid": "ff-1", "filename": "a.json", "size": 9,
                        "position": 0, "penalized": False, "penalty_expires_in": 0}
    assert "lr-1" in client.session.deleted


def test_flowfile_detail_returns_attributes_and_content(client):
    detail = client.flowfile_detail("c1", "ff-1")
    assert detail["attributes"] == {"filename": "a.json", "a": "1"}
    assert detail["content"] == '{"a":"1"}'


def test_flowfile_detail_skips_content_for_zero_byte_flowfile(client):
    # The fake asserts the content endpoint is never hit for ff-2; attributes
    # still come through so the detail view isn't lost to a 409.
    detail = client.flowfile_detail("c1", "ff-2")
    assert detail["size"] == 0
    assert detail["content"] == ""
    assert detail["attributes"] == {"filename": "empty"}


def test_recent_events_queries_provenance_for_a_component(client):
    events = client.recent_events("p1")
    assert [e["event_id"] for e in events] == ["ev-1"]
    assert events[0]["event_type"] == "SEND"
    assert "pq-1" in client.session.deleted


def test_event_detail_flattens_attributes_and_fetches_content(client):
    detail = client.event_detail("ev-1")
    assert detail["attributes"] == {"filename": "a.json", "a": "1"}
    assert detail["content"] == '{"a":"1"}'
    assert detail["event_type"] == "SEND"


def test_trace_orders_hops_and_diffs_attributes(client):
    trace = client.trace_flowfile("ff-1")
    hops = trace["hops"]
    # Query answered newest first; the trace reads oldest first.
    assert [h["event_id"] for h in hops] == [10, 11]
    # Only what the event changed — untouched attributes stay out of the diff.
    assert hops[0]["changes"] == [
        {"name": "a", "before": "1", "after": "2"},
        {"name": "fresh", "before": None, "after": "x"},
    ]
    assert hops[0]["attributes"] == {"a": "2", "filename": "a.json", "fresh": "x"}
    # the group the component lives in — the GUIs deep-link the hop with it
    assert hops[0]["group_id"] == "root-id"
    assert hops[1]["group_id"] == ""  # NiFi omits it for components gone from the flow
    assert hops[1]["relationship"] == "unmatched"
    assert hops[1]["children"] == ["ff-9"]
    assert not hops[1]["output_available"]
    assert "pq-2" in client.session.deleted


def test_event_content_fetches_only_available_sides(client):
    assert client.event_content(11, "input") == "payload-in"
    # The fake asserts the output endpoint is never hit: NiFi already said
    # the claim is gone, so the client returns "" without asking.
    assert client.event_content(11, "output") == ""


def test_an_unfiltered_provenance_query_is_refused():
    """T7e: NiFi 2.x under-reports events when searchTerms is empty.

    Observed on 2.7.2: events of since-deleted process groups are counted in
    totalCount but never returned, so one unfiltered query answered total '0'
    at maxResults 100 and 500 and '106' at 1000. Refusing beats silently
    returning a fraction.
    """
    import pytest

    from niflow.client import NiFiClient

    client = NiFiClient.__new__(NiFiClient)
    with pytest.raises(ValueError) as caught:
        client._provenance_query({}, 100, "recent events")
    assert "unfiltered provenance query" in str(caught.value)
    assert "FlowFileUUID" in str(caught.value)
