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


# --- T7a: above the escalation ceiling, "recent" is bounded by time ----------

class _BusyProvenance:
    """A server whose provenance behaves the way NiFi's actually does.

    Measured on 1.24.0: a capped query answers with an *arbitrary* subset, not
    the newest N — once seen as events from the previous day while the newest
    were 130k event ids later. This fake reproduces exactly that: when the
    match count exceeds the cap it returns the OLDEST slice and flags the
    answer with a trailing "+" the way NiFi does.
    """

    def __init__(self, events, ceiling=5000):
        self.events = events            # ascending by event id
        self.ceiling = ceiling
        self.windows = []               # (start, end) actually requested

    def query(self, search_terms, cap, what, summarize=True, totals=None,
              start=None, end=None):
        self.windows.append((start, end))
        matched = [
            e for e in self.events
            if (start is None or e["when"] >= start)
            and (end is None or e["when"] <= end)
        ]
        capped = len(matched) > cap
        served = matched[:cap] if capped else matched
        if totals is not None:
            totals["total"] = f"{len(served)}+" if capped else str(len(served))
            totals["total_count"] = len(served)
            totals["generated"] = "12:00:00 UTC"
            totals["time_offset_ms"] = 0
        return [dict(e["dto"]) for e in served]


def _busy_client(events, ceiling=5000):
    from niflow.client import NiFiClient

    client = NiFiClient.__new__(NiFiClient)
    fake = _BusyProvenance(events, ceiling)
    client._provenance_query = fake.query
    client._server_now = staticmethod(lambda totals: NOW)
    return client, fake


import datetime as _dt

NOW = _dt.datetime(2026, 8, 20, 12, 0, 0, tzinfo=_dt.timezone.utc)


def _stream(count, seconds_apart=0.01, first_id=1):
    """`count` events ending just before NOW, newest last."""
    return [
        {"when": NOW - _dt.timedelta(seconds=(count - i) * seconds_apart),
         "dto": {"eventId": str(first_id + i), "eventType": "CREATE"}}
        for i in range(count)
    ]


def test_a_component_past_the_ceiling_still_gets_its_newest_events():
    """The bug: the count escalation gives up and the subset is arbitrary."""
    events = _stream(20000)
    client, fake = _busy_client(events, ceiling=5000)

    got, capped = client._provenance_newest(
        {"ProcessorID": {"value": "p1", "inverse": False}}, 25, "p1")

    assert [e["eventId"] for e in got] == [str(i) for i in range(19976, 20001)]
    assert capped is False           # every window it used answered completely
    assert any(start is not None for start, _ in fake.windows), "no window was used"


def test_the_walk_narrows_until_a_window_answers_completely():
    events = _stream(20000, seconds_apart=0.001)   # 20 events per ms — dense
    client, fake = _busy_client(events, ceiling=100)

    got, capped = client._provenance_newest(
        {"ProcessorID": {"value": "p1", "inverse": False}}, 10, "p1")

    assert [e["eventId"] for e in got] == [str(i) for i in range(19991, 20001)]
    widths = [(end - start).total_seconds()
              for start, end in fake.windows if start is not None]
    assert widths[0] > widths[-1], "the walk never narrowed"


def test_one_second_busier_than_the_ceiling_is_reported_capped():
    """The honest last resort: an arbitrary subset OF THE NEWEST SECOND."""
    events = _stream(20000, seconds_apart=0.0)      # all at the same instant
    client, _ = _busy_client(events, ceiling=100)

    got, capped = client._provenance_newest(
        {"ProcessorID": {"value": "p1", "inverse": False}}, 10, "p1")

    assert capped is True
    assert len(got) == 10


def test_a_quiet_component_never_pays_for_a_window():
    events = _stream(30)
    client, fake = _busy_client(events, ceiling=5000)

    got, capped = client._provenance_newest(
        {"ProcessorID": {"value": "p1", "inverse": False}}, 25, "p1")

    assert capped is False
    assert [e["eventId"] for e in got] == [str(i) for i in range(6, 31)]
    assert all(start is None for start, _ in fake.windows), "windowed a quiet component"


def test_events_older_than_the_look_back_still_answer_something():
    """Never worse than before: the capped subset is the floor, not an empty list."""
    old = [
        {"when": NOW - _dt.timedelta(days=30),
         "dto": {"eventId": str(i), "eventType": "CREATE"}}
        for i in range(1, 20001)
    ]
    client, _ = _busy_client(old, ceiling=100)

    got, capped = client._provenance_newest(
        {"ProcessorID": {"value": "p1", "inverse": False}}, 10, "p1")

    assert len(got) == 10
    assert capped is True


def test_the_server_clock_anchors_the_window_not_ours():
    """A laptop ahead of the server would ask for a slice that hasn't happened."""
    from niflow.client import NiFiClient

    now = _dt.datetime.now(_dt.timezone.utc)
    totals = {"generated": (now - _dt.timedelta(hours=2)).strftime("%H:%M:%S") + " UTC",
              "time_offset_ms": 0}
    anchored = NiFiClient._server_now(totals)
    assert abs((now - anchored).total_seconds() - 7200) < 5

    # No clock in the answer (old server, fixture): fall back to ours.
    assert abs((NiFiClient._server_now({}) - now).total_seconds()) < 5


def test_the_server_clock_survives_midnight():
    from niflow.client import NiFiClient

    now = _dt.datetime.now(_dt.timezone.utc)
    minutes_ago = now - _dt.timedelta(minutes=10)
    totals = {"generated": minutes_ago.strftime("%H:%M:%S") + " UTC",
              "time_offset_ms": 0}
    anchored = NiFiClient._server_now(totals)
    assert abs((anchored - minutes_ago).total_seconds()) < 2
