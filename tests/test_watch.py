"""Health watcher: the classifier's pattern table and the healthy/failing
state machine, driven by scripted status + bulletin payloads. No live NiFi.
"""
from __future__ import annotations

import json

import pytest

from niflow import watch
from niflow.watch import Watcher, classify, format_alert

# Real bulletin text captured from NiFi 1.24.0 (see the demo in todo.md).
CONN_REFUSED = (
    "InvokeHTTP[id=68ea3db9-32aa-3e6c-60e3-86f352e000e2] Request Processing "
    "failed: FlowFile[filename=9559adcc]: java.net.ConnectException: Failed to "
    "connect to api-frontiers/172.19.0.1:9099\n- Caused by: "
    "java.net.ConnectException: Connection refused (Connection refused)"
)
DNS_FAIL = (
    "InvokeHTTP[id=68ea3db9] Request Processing failed: FlowFile[filename=96e5]: "
    "java.net.UnknownHostException: api-frontiers-gone.invalid"
)


# ------------------------------------------------------------- fake server


class FakeClient:
    """Scripted NiFi: one processor + one error connection, both settable."""

    base = "https://fake:8444/nifi-api"

    def __init__(self):
        self.run_status = "Running"
        self.flowfiles = 5
        self.error_route_in = 0
        self.bulletin_rows = []
        self.attributes = {}
        self.validation = []
        self.probes = 0
        self.extra_processors = {}

    # -- what the watcher calls -------------------------------------------
    def resolve_group(self, group="root"):
        return "g1"

    def ui_url(self, group_id="", component_id=""):
        return f"https://fake:8444/nifi/?processGroupId={group_id}&componentIds={component_id}"

    def _recursive_status(self, pg_id):
        procs = [{"processorStatusSnapshot": {
            "id": "p1", "groupId": "g1", "name": "CallOrdersApi",
            "type": "org.apache.nifi.processors.standard.InvokeHTTP",
            "runStatus": self.run_status, "flowFilesIn": self.flowfiles,
            "flowFilesOut": self.flowfiles, "taskCount": self.flowfiles,
        }}]
        for pid, (name, status) in self.extra_processors.items():
            procs.append({"processorStatusSnapshot": {
                "id": pid, "groupId": "g1", "name": name, "type": "x.Y",
                "runStatus": status, "flowFilesIn": 1, "flowFilesOut": 1,
                "taskCount": 1,
            }})
        return {
            "id": "g1", "name": "WatchDemo",
            "processorStatusSnapshots": procs,
            "connectionStatusSnapshots": [{"connectionStatusSnapshot": {
                "id": "c1", "groupId": "g1", "name": "No Retry, Retry, Failure",
                "sourceId": "p1", "sourceName": "CallOrdersApi",
                "destinationName": "OrdersFailed",
                "flowFilesIn": self.error_route_in, "flowFilesQueued": 0,
            }}],
            "processGroupStatusSnapshots": [],
        }

    def bulletins(self, limit=100):
        return list(self.bulletin_rows)

    def _get_json(self, path):
        return {"component": {"validationErrors": list(self.validation)}}

    def recent_events(self, component_id, max_results=25):
        self.probes += 1
        return [{"event_id": 1}] if self.attributes else []

    def event_detail(self, event_id):
        return {"attributes": dict(self.attributes)}

    def find_processors(self, type_contains="", group="root"):  # fallback path
        return [{"id": "p1", "name": "CallOrdersApi", "type": "x.InvokeHTTP",
                 "state": "RUNNING", "path": "", "group_id": "g1"}]

    # -- helpers for the tests --------------------------------------------
    def bulletin(self, message, level="ERROR", bid=None):
        bid = len(self.bulletin_rows) + 1 if bid is None else bid
        self.bulletin_rows = [{"id": bid, "level": level, "source": "CallOrdersApi",
                               "source_id": "p1", "group_id": "g1",
                               "message": message, "time": "00:00:00 UTC"}]


@pytest.fixture
def clock(monkeypatch):
    """A controllable clock — the whole feature is about elapsed time."""
    state = {"t": 1_000_000.0}
    monkeypatch.setattr(watch, "_now", lambda: state["t"])

    def advance(seconds):
        state["t"] += seconds
        return state["t"]

    advance.now = lambda: state["t"]
    return advance


def make_watcher(tmp_path, client, **kw):
    kw.setdefault("baseline_seconds", 60)
    kw.setdefault("probe", False)
    return Watcher(client, "WatchDemo", directory=tmp_path, **kw)


# ---------------------------------------------------------------- classifier


def test_classifies_connection_refused_and_names_the_endpoint():
    found = classify(CONN_REFUSED)
    assert found["category"] == "external"
    assert found["kind"] == "connection"
    # The whole value of the ticket: the concrete thing, not "InvokeHTTP failed".
    assert "api-frontiers" in found["summary"] and "9099" in found["summary"]


def test_classifies_dns_failure():
    found = classify(DNS_FAIL)
    assert (found["category"], found["kind"]) == ("external", "dns")
    assert "api-frontiers-gone.invalid" in found["summary"]


@pytest.mark.parametrize("message, kind", [
    ("javax.net.ssl.SSLHandshakeException: PKIX path building failed", "tls"),
    ("java.net.SocketTimeoutException: Read timed out", "timeout"),
    ("org.apache.kafka.common.errors.TimeoutException: Topic not present", "kafka"),
    ("com.jcraft.jsch.JSchException: Auth fail", "sftp"),
    ("Cannot get a connection, pool error Timeout waiting for idle object", "database"),
    ("com.amazonaws.services.s3.model.AmazonS3Exception: NoSuchBucket", "cloud"),
])
def test_external_families(message, kind):
    found = classify(message)
    assert found["category"] == "external", message
    assert found["kind"] == kind


@pytest.mark.parametrize("message, kind", [
    ("PutFile[id=1] is invalid because Directory is required", "invalid"),
    ("Failed to write to Content Repository: No space left on device", "infrastructure"),
    ("java.lang.OutOfMemoryError: Java heap space", "infrastructure"),
    ("Controller Service DBCPConnectionPool is disabled", "controller-service"),
])
def test_internal_families(message, kind):
    found = classify(message)
    assert found["category"] == "internal", message
    assert found["kind"] == kind


def test_ambiguous_parse_failure_is_honest_about_being_unknown():
    # A malformed payload could be the upstream system changing its output OR
    # our schema being wrong. Guessing here is what sends analysts the wrong way.
    found = classify("MalformedRecordException: Failed to parse incoming data")
    assert found["category"] == "unknown"
    assert "either" in found["hint"]


def test_unmatched_message_says_unknown_not_external():
    found = classify("SomeVendorNarException: widget 7 refused to widget")
    assert found["category"] == "unknown"
    assert found["confidence"] == "low"
    assert "patterns.json" in found["hint"]


def test_http_status_from_flowfile_attributes_beats_the_message():
    # The silent break: a 404 that produces no bulletin at all. The status code
    # only exists as a FlowFile attribute.
    found = classify("", attributes={
        "invokehttp.status.code": "404",
        "invokehttp.status.message": "Not Found",
        "invokehttp.request.url": "http://api-frontiers:9099/v1/orders",
    })
    assert found["category"] == "external"
    assert "api-frontiers" in found["summary"] and "404" in found["summary"]


def test_http_200_attribute_is_not_a_failure():
    found = classify("kaboom", attributes={"invokehttp.status.code": "200"})
    assert found["pattern"] != "http-attributes"


def test_user_patterns_are_loaded_and_win(tmp_path, monkeypatch):
    path = tmp_path / "patterns.json"
    path.write_text(json.dumps([{
        "name": "acme", "category": "external", "kind": "gateway",
        "regex": r"AcmeGatewayException: (?P<code>\w+)",
        "summary": "the Acme gateway said {code}", "hint": "check Acme",
    }]))
    monkeypatch.setenv("NIFLOW_WATCH_PATTERNS", str(path))
    found = classify("AcmeGatewayException: BACKEND_DOWN")
    assert found["category"] == "external"
    assert found["summary"] == "the Acme gateway said BACKEND_DOWN"


def test_bad_pattern_file_does_not_break_the_classifier(tmp_path, monkeypatch):
    path = tmp_path / "patterns.json"
    path.write_text("{ not json")
    monkeypatch.setenv("NIFLOW_WATCH_PATTERNS", str(path))
    assert classify(DNS_FAIL)["kind"] == "dns"


# ------------------------------------------------------------ state machine


def test_no_alert_without_an_established_baseline(tmp_path, clock):
    """A processor that has always been broken is not news."""
    client = FakeClient()
    watcher = make_watcher(tmp_path, client)
    watcher.tick()                       # first tick: learn the bulletin id
    client.bulletin(CONN_REFUSED)
    clock(10)                            # only 10s of "health" behind it
    events = watcher.tick()
    assert events == []
    assert watcher.store.health["p1"]["chronic"] is True
    assert watcher.summary()["active"] == 0


def test_healthy_then_failing_then_recovered(tmp_path, clock):
    client = FakeClient()
    watcher = make_watcher(tmp_path, client)
    watcher.tick()
    clock(120)
    watcher.tick()                       # baseline established
    assert watcher.store.health["p1"]["ever_healthy"] is True

    clock(60)
    client.bulletin(CONN_REFUSED)
    raised = watcher.tick()
    assert len(raised) == 1
    alert = raised[0]
    assert alert["event"] == "raised"
    assert alert["category"] == "external"
    assert alert["component"] == "CallOrdersApi"
    assert alert["healthy_for"] == "3m"          # 120s + 60s of health
    assert "api-frontiers" in alert["summary"]
    assert alert["url"].endswith("componentIds=p1")

    clock(30)                                     # still failing: no new alert
    client.bulletin(CONN_REFUSED, bid=99)
    assert watcher.tick() == []
    assert watcher.store.alerts[0]["occurrences"] == 2

    clock(30)                                     # the endpoint comes back
    client.bulletin_rows = []
    resolved = watcher.tick()
    assert len(resolved) == 1 and resolved[0]["event"] == "resolved"
    assert watcher.store.alerts[0]["state"] == "resolved"
    assert watcher.summary()["active"] == 0


def test_alert_text_says_was_healthy_when_and_why(tmp_path, clock):
    client = FakeClient()
    watcher = make_watcher(tmp_path, client)
    watcher.tick()
    clock(300)
    watcher.tick()
    clock(10)
    client.bulletin(DNS_FAIL)
    text = format_alert(watcher.tick()[0], verbose=True)
    assert "CallOrdersApi" in text
    assert "was healthy for" in text and "broke at" in text
    assert "api-frontiers-gone.invalid" in text
    assert "[EXTERNAL]" in text


def test_baseline_survives_a_restart(tmp_path, clock):
    """The point of persisting: "was healthy for hours" must outlive the process."""
    client = FakeClient()
    first = make_watcher(tmp_path, client)
    first.tick()
    clock(600)
    first.tick()
    assert first.store.path.is_file()

    second = make_watcher(tmp_path, client)      # "restart"
    assert second.store.health["p1"]["ever_healthy"] is True
    clock(30)
    client.bulletin(CONN_REFUSED)
    events = second.tick()
    assert len(events) == 1
    assert events[0]["category"] == "external"
    # And the alert is readable by a third process (the CLI vs the web GUI).
    third = make_watcher(tmp_path, client)
    assert third.summary()["active"] == 1
    assert third.alerts(active_only=True)[0]["component"] == "CallOrdersApi"


def test_invalid_processor_is_classified_internal(tmp_path, clock):
    client = FakeClient()
    watcher = make_watcher(tmp_path, client)
    watcher.tick()
    clock(120)
    watcher.tick()
    clock(10)
    client.run_status = "Invalid"
    client.validation = ["'Remote URL' is invalid because Remote URL is required"]
    alert = watcher.tick()[0]
    assert alert["category"] == "internal"
    assert alert["signal"] == "invalid"
    assert "Remote URL" in " ".join(alert["evidence"])


def test_running_to_stopped_is_internal_but_a_mass_stop_is_not(tmp_path, clock):
    client = FakeClient()
    client.extra_processors = {"p2": ("Two", "Running"), "p3": ("Three", "Running")}
    watcher = make_watcher(tmp_path, client)
    watcher.tick()
    clock(120)
    watcher.tick()
    clock(10)
    client.run_status = "Stopped"                      # one processor only
    alert = watcher.tick()[0]
    assert (alert["category"], alert["kind"]) == ("internal", "stopped")

    # Now everything stops at once — somebody hit "Stop All", not a break.
    client.run_status = "Running"
    clock(200)
    watcher.tick()
    client.extra_processors = {"p2": ("Two", "Stopped"), "p3": ("Three", "Stopped")}
    client.run_status = "Stopped"
    clock(10)
    assert watcher.tick() == []


def test_error_route_opening_catches_a_break_with_no_bulletin(tmp_path, clock):
    """The silent HTTP 404: NiFi logs nothing, FlowFiles just start going
    down the No Retry relationship. Verified live on 1.24."""
    client = FakeClient()
    client.attributes = {
        "invokehttp.status.code": "404", "invokehttp.status.message": "Not Found",
        "invokehttp.request.url": "http://api-frontiers:9099/v1/orders",
    }
    watcher = make_watcher(tmp_path, client, probe=True)
    watcher.tick()
    clock(120)
    watcher.tick()

    clock(10)
    client.error_route_in = 4                 # the failure route opens
    alert = watcher.tick()[0]
    assert alert["signal"] == "error-route"
    assert alert["category"] == "external"
    assert "404" in alert["summary"] and "api-frontiers" in alert["summary"]
    assert client.probes == 1                 # probed once, at fire time only
    assert "No Retry" in " ".join(alert["evidence"])

    clock(30)                                 # keeps failing: still one alert
    assert watcher.tick() == []
    clock(30)                                 # the route goes quiet again
    client.error_route_in = 0
    assert watcher.tick()[0]["event"] == "resolved"


def test_error_route_needs_a_clean_baseline_first(tmp_path, clock):
    client = FakeClient()
    client.error_route_in = 4                 # dirty from the very first look
    watcher = make_watcher(tmp_path, client)
    watcher.tick()
    clock(120)
    assert watcher.tick() == []


def test_a_recent_push_is_offered_as_evidence(tmp_path, clock, monkeypatch):
    backups = tmp_path / "backups"
    backups.mkdir()
    snapshot = backups / "WatchDemo-20260819-140100.json"
    snapshot.write_text("{}")
    monkeypatch.setenv("NIFLOW_BACKUP_DIR", str(backups))
    client = FakeClient()
    watcher = make_watcher(tmp_path, client)
    watcher.tick()
    clock(120)
    watcher.tick()
    clock(10)
    client.bulletin("SomeVendorNarException: widget refused to widget")
    alert = watcher.tick()[0]
    # Unclassifiable message + a push seconds earlier -> look at ourselves first.
    assert alert["category"] == "internal"
    assert alert["kind"] == "flow-change"
    assert any("backed up" in e for e in alert["evidence"])


def test_first_tick_does_not_alert_on_the_existing_bulletin_board(tmp_path, clock):
    """The board holds the last few minutes; a watcher that just started has
    no baseline and no business claiming anything "just broke"."""
    client = FakeClient()
    client.bulletin(CONN_REFUSED)
    watcher = make_watcher(tmp_path, client)
    assert watcher.tick() == []
    assert watcher.store.alerts == []


def test_acknowledge_and_dismiss(tmp_path, clock):
    client = FakeClient()
    watcher = make_watcher(tmp_path, client)
    watcher.tick()
    clock(120)
    watcher.tick()
    clock(10)
    client.bulletin(CONN_REFUSED)
    alert_id = watcher.tick()[0]["id"]

    assert watcher.summary()["unacknowledged"] == 1
    assert watcher.acknowledge(alert_id) is True
    assert watcher.summary()["unacknowledged"] == 0
    assert watcher.summary()["active"] == 1          # still broken, just quiet
    assert watcher.acknowledge("nope") is False

    assert watcher.dismiss(alert_id) is True
    assert watcher.summary()["active"] == 0
    assert watcher.store.health["p1"]["alert_id"] is None


def test_summary_counts_by_category(tmp_path, clock):
    client = FakeClient()
    watcher = make_watcher(tmp_path, client)
    watcher.tick()
    clock(120)
    watcher.tick()
    clock(10)
    client.bulletin(CONN_REFUSED)
    watcher.tick()
    summary = watcher.summary()
    assert summary["external"] == 1 and summary["internal"] == 0
    assert summary["established"] == 1
    assert "api-frontiers" in summary["newest_summary"]


def test_corrupt_state_file_is_ignored_not_fatal(tmp_path, clock):
    client = FakeClient()
    watcher = make_watcher(tmp_path, client)
    watcher.tick()
    watcher.store.path.write_text("{{{ not json")
    again = make_watcher(tmp_path, client)
    assert again.store.health == {}
    assert again.tick() == []


def test_run_stops_after_the_requested_iterations(tmp_path, clock):
    client = FakeClient()
    watcher = make_watcher(tmp_path, client)
    watcher.run(interval=0, iterations=3)
    assert watcher.ticks == 3


def test_warnings_are_ignored_unless_asked_for(tmp_path, clock):
    client = FakeClient()
    watcher = make_watcher(tmp_path, client)
    watcher.tick()
    clock(120)
    watcher.tick()
    clock(10)
    client.bulletin(CONN_REFUSED, level="WARNING")
    assert watcher.tick() == []

    loud = make_watcher(tmp_path, client, include_warnings=True)
    clock(120)
    loud.tick()
    clock(10)
    client.bulletin(CONN_REFUSED, level="WARNING", bid=77)
    assert len(loud.tick()) == 1


# ---------------------------------------------------------------- web GUI


@pytest.fixture
def gui(tmp_path, clock, monkeypatch):
    """The Alerts tab's API, wired to a watcher we drive by hand.

    The background thread is deliberately not started: these tests are about
    the routes, and a live thread would race the assertions.
    """
    import threading

    from niflow import webgui

    client = FakeClient()
    watcher = make_watcher(tmp_path, client)
    monkeypatch.setitem(webgui._WATCH, "watcher", watcher)
    monkeypatch.setitem(webgui._WATCH, "thread", None)
    monkeypatch.setitem(webgui._WATCH, "stop", None)
    lock = threading.Lock()

    def call(method, path, body=None):
        return webgui.dispatch(client, lock, method, path, {}, body or {},
                               tmp_path)

    call.client = client
    call.watcher = watcher
    return call


def test_alerts_tab_serves_the_watchers_state(gui, clock):
    gui.watcher.tick()
    clock(120)
    gui.watcher.tick()
    clock(10)
    gui.client.bulletin(CONN_REFUSED)
    gui.watcher.tick()

    status, payload = gui("GET", "/api/alerts")
    assert status == 200
    assert payload["summary"]["active"] == 1
    assert payload["alerts"][0]["category"] == "external"
    assert "api-frontiers" in payload["alerts"][0]["summary"]
    assert payload["state_file"].endswith(".json")


def test_summary_route_is_the_badge_and_touches_no_nifi(gui, clock):
    gui.watcher.tick()
    clock(120)
    gui.watcher.tick()
    clock(10)
    gui.client.bulletin(CONN_REFUSED)
    gui.watcher.tick()
    before = gui.client.probes

    status, payload = gui("GET", "/api/alerts/summary")
    assert status == 200
    assert payload["unacknowledged"] == 1
    assert payload["external"] == 1
    assert "api-frontiers" in payload["newest_summary"]
    assert gui.client.probes == before          # no NiFi work behind the badge


def test_ack_and_dismiss_routes(gui, clock):
    gui.watcher.tick()
    clock(120)
    gui.watcher.tick()
    clock(10)
    gui.client.bulletin(CONN_REFUSED)
    alert_id = gui.watcher.tick()[0]["id"]

    status, payload = gui("POST", "/api/alerts/ack", {"id": alert_id})
    assert status == 200 and payload["summary"]["unacknowledged"] == 0
    status, _ = gui("POST", "/api/alerts/ack", {"id": "nope"})
    assert status == 404

    status, payload = gui("POST", "/api/alerts/dismiss", {"id": alert_id})
    assert status == 200 and payload["summary"]["active"] == 0


def test_check_now_route_runs_one_poll(gui, clock):
    status, payload = gui("POST", "/api/alerts/check")
    assert status == 200
    assert payload["summary"]["ticks"] == 1


def test_alerts_tab_is_registered_in_the_page(gui):
    from niflow.webgui import PAGE

    assert '["alerts", "Alerts"]' in PAGE
    assert 'alertPoll' in PAGE
    # The badge poll must run on its own timer, not only inside render().
    assert 'setInterval(alertPoll' in PAGE
    assert 'NO_POLL = new Set(["trace", "follow", "explain", "flows"])' in PAGE


def test_error_route_source_resolves_without_a_sourceid(tmp_path, clock):
    """NiFi 1.24's connection status snapshot has sourceName but NO sourceId
    (verified live; 2.x has both). The route signal has to survive that."""
    client = FakeClient()
    original = client._recursive_status

    def strip_source_id(pg_id):
        snap = original(pg_id)
        for wrapper in snap["connectionStatusSnapshots"]:
            wrapper["connectionStatusSnapshot"].pop("sourceId", None)
        return snap

    client._recursive_status = strip_source_id
    watcher = make_watcher(tmp_path, client)
    watcher.tick()
    clock(120)
    watcher.tick()
    clock(10)
    client.error_route_in = 3
    alert = watcher.tick()[0]
    assert alert["component_id"] == "p1"        # matched by name within the group
    assert alert["signal"] == "error-route"


def test_a_push_to_a_DIFFERENT_flow_does_not_become_the_explanation(
        tmp_path, clock, monkeypatch):
    """Somebody else's push is a coincidence, not a cause. Blaming it would be
    the confidently-wrong attribution this whole feature exists to avoid."""
    backups = tmp_path / "backups"
    backups.mkdir()
    (backups / "SomeOtherFlow-20260819-140100.json").write_text("{}")
    monkeypatch.setenv("NIFLOW_BACKUP_DIR", str(backups))
    client = FakeClient()
    watcher = make_watcher(tmp_path, client)
    watcher.tick()
    clock(120)
    watcher.tick()
    clock(10)
    client.bulletin("SomeVendorNarException: widget refused to widget")
    alert = watcher.tick()[0]
    assert alert["category"] == "unknown"           # still honest
    assert any("a different flow" in e for e in alert["evidence"])


def test_unexplained_alert_is_re_probed_until_provenance_catches_up(
        tmp_path, clock):
    """Provenance lags the break by a few seconds, so the tick that spots a
    failure often sees only the successful calls from just before it."""
    client = FakeClient()
    client.attributes = {"invokehttp.status.code": "200"}   # still the old, good call
    watcher = make_watcher(tmp_path, client, probe=True)
    watcher.tick()
    clock(120)
    watcher.tick()
    clock(10)
    client.error_route_in = 3
    alert = watcher.tick()[0]
    assert alert["category"] == "unknown"

    # A tick later the failed call is indexed and the alert upgrades itself.
    client.attributes = {"invokehttp.status.code": "503",
                         "invokehttp.request.url": "http://api-frontiers:9099/v1/orders"}
    clock(10)
    updated = watcher.tick()
    assert len(updated) == 1 and updated[0]["event"] == "updated"
    assert updated[0]["category"] == "external"
    assert "503" in updated[0]["summary"]
    assert watcher.alerts(active_only=True)[0]["category"] == "external"


def test_re_probe_gives_up_rather_than_probing_forever(tmp_path, clock):
    client = FakeClient()
    client.attributes = {"invokehttp.status.code": "200"}
    watcher = make_watcher(tmp_path, client, probe=True)
    watcher.tick()
    clock(120)
    watcher.tick()
    clock(10)
    client.error_route_in = 3
    watcher.tick()
    probes = client.probes
    for _ in range(10):
        clock(10)
        watcher.tick()
    assert client.probes - probes <= 4          # capped, not once per tick
