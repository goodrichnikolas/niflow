"""Adversarial live tests for trace/follow — the T7 hardening pass.

Every assertion here failed, or was untested, against a real NiFi 1.24.0
before this file existed. They run against whatever ``NIFLOW_NIFI_HOST``
points at and are expected to pass on **both** lines (``make
test-integration`` for 2.x, ``test-integration-v1`` for 1.24); anything that
genuinely differs between the lines is asserted loosely on purpose, and said
so at the assertion.

The fixture is ``flows/labyrinth.py``: four levels of nested groups joined by
ports, a fan-in, a 50-way split into a 50-way merge, a followable 2-way
split/merge, a route-to-failure lane with no provenance event, and a 200-file
batch that overflows NiFi's queue listing.
"""
import datetime
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "flows"))

import labyrinth  # noqa: E402

from niflow.follow import (  # noqa: E402
    FlowFollower,
    FollowError,
    annotate_hops,
    compare_runs,
    entry_points,
    format_run_comparison,
    format_watch_table,
    watch_rows,
)

pytestmark = pytest.mark.integration

GROUP = "NiflowFollowLive"


@pytest.fixture(scope="module")
def deployed(nifi_client):
    labyrinth.flow.name = GROUP
    nifi_client.push_flow(labyrinth.flow)
    yield nifi_client
    nifi_client.delete_group(GROUP)


@pytest.fixture()
def follower(deployed):
    """A quiesced follower over an emptied group — one journey per test."""
    f = FlowFollower(deployed, GROUP, poll_timeout=8.0)
    f.quiesce()
    _drain(deployed, f.pg_id)
    return f


def _drain(client, pg_id):
    for queue in client.list_queues(pg_id):
        if queue.get("queued"):
            client.drain_connection(queue["id"])


def _entry(client, suffix):
    for entry in entry_points(client, GROUP):
        if entry["label"].endswith(suffix):
            return entry
    raise AssertionError(f"no start point ending in {suffix!r}")


def _proc(client, pg_id, name):
    for proc in client.find_processors(group=pg_id):
        if proc["name"] == name:
            return proc
    raise AssertionError(f"no processor named {name!r}")


# --------------------------------------------------------------- deep nesting


def test_follow_crosses_nested_group_boundaries_in_both_directions(follower):
    """The case work hits on hop one: run-once cannot drive a port.

    Before this, a journey dead-ended at the first input port with
    ``terminal`` — and work's tree is four to five groups deep, entered
    through a port every time.
    """
    follower.start_from(_entry(follower.client, "DeepGen"), wait=20)
    seen, crossings = [], []
    for _ in range(14):
        outcome = follower.step()
        assert outcome["status"] in ("advanced", "crossed"), outcome.get("message")
        if outcome["status"] == "crossed":
            crossings.append(outcome["hops"][0]["component_type"])
        seen += [hop["component"] for hop in outcome["hops"]
                 if hop["event_type"] not in ("CROSS", "MOVED")]
        if seen[-1:] == ["Back1"]:
            break
    # Down through four input ports and back up through four output ports.
    assert seen == ["Mark1", "Mark2", "Mark3", "Mark4", "Back3", "Back2", "Back1"]
    assert crossings.count("INPUT_PORT") == 4
    assert crossings.count("OUTPUT_PORT") == 3


def test_a_port_the_stepper_started_is_stopped_again(follower):
    """A debugger must hand the flow back the way it found it."""
    client = follower.client
    follower.start_from(_entry(client, "DeepGen"), wait=20)
    outcome = follower.step()
    assert outcome["status"] == "crossed"
    port_id = outcome["end"]["id"]
    state = client._get_json(f"/input-ports/{port_id}")["component"]["state"]
    assert state == "STOPPED"


# ------------------------------------------------------------------- merging


def test_following_a_flowfile_into_a_merge_continues_in_the_merged_file(follower):
    """A merge consumes the uuid you were following. Say so, and follow on.

    NiFi's ``FlowFileUUID`` search is a lineage query (1.24.0 and 2.7.2), so
    the merged file's JOIN comes back inside the child's journey describing a
    *different* FlowFile. It must be labelled, not diffed, and its child is
    where the journey continues.
    """
    follower.start_from(_entry(follower.client, "PairGen"), wait=20)
    outcome = follower.step()                     # PairGen -> PairSplit: 2 children
    assert outcome["status"] == "advanced"
    assert len(outcome["branches"]) == 2
    assert {b["relationship"] for b in outcome["branches"]} == {"split"}

    joins, merged = [], None
    for _ in range(8):
        nxt = follower.next_live()
        if not nxt:
            break
        follower.switch_to(nxt)
        outcome = follower.step()
        hops = outcome["hops"]
        annotate_hops(hops)
        for hop in hops:
            if hop["event_type"] == "JOIN":
                joins.append(hop)
                merged = merged or (hop["children"] or [None])[0]

    assert joins, "the JOIN that consumed the followed FlowFile was never seen"
    join = joins[0]
    assert join["own"] is False               # it is the merged file's event
    assert join["diff"] == []                 # …so it must not be diffed
    assert "merged into" in join["lineage"]
    assert merged, "the merged FlowFile never became a branch"

    branches = {b["uuid"]: b for b in follower.branches()}
    assert merged in branches
    assert branches[merged]["relationship"] == "merged"
    assert branches[merged]["destination"] == "PairSink"
    # …and the merged file was actually walked to its end.
    assert branches[merged]["state"] == "done"


def test_a_split_child_does_not_adopt_its_own_siblings(follower):
    """The parent's FORK is in every child's lineage; its children are siblings."""
    follower.start_from(_entry(follower.client, "SplitGen"), wait=20)
    outcome = follower.step()
    assert outcome["status"] == "advanced"
    assert len(outcome["branches"]) == labyrinth.CHILDREN

    before = len(follower.branches())
    child = follower.pending_children[0]
    follower.switch_to(child)
    follower.step()
    # Stepping a child must not register 49 more branches for its siblings.
    assert len(follower.branches()) == before


# -------------------------------------------------------------- deep queues


def test_a_flowfile_past_the_listing_cap_is_still_found(deployed):
    """NiFi lists 100 FlowFiles per queue and will not raise the cap.

    A work queue holds thousands, so "not in the listing" used to be reported
    as "it was dropped or expired". The targeted lookup resolves it instead.
    """
    client = deployed
    f = FlowFollower(client, GROUP, poll_timeout=8.0)
    f.quiesce()
    _drain(client, f.pg_id)
    bulk = _proc(client, f.pg_id, "BulkGen")
    client.run_processor_once(bulk["id"])

    queue = None
    for _ in range(40):
        time.sleep(0.5)
        matches = [q for q in client.list_queues(f.pg_id)
                   if q["destination"] == "BulkMark" and q.get("queued")]
        if matches and matches[0]["queued"] >= labyrinth.BATCH:
            queue = matches[0]
            break
    assert queue, "BulkGen did not produce its batch"

    listed = client.list_flowfiles(queue["id"])
    assert len(listed) == 100 < queue["queued"]      # the cap, on both lines
    assert listed[0]["position"] == 1                # 1-based, on both lines

    visible = {summary["uuid"] for summary in listed}
    created = [event["uuid"] for event
               in client.recent_events(bulk["id"], max_results=labyrinth.BATCH)]
    buried = [uuid for uuid in created if uuid not in visible]
    assert buried, "expected part of the batch to be past the listing cap"

    found = f.pick_flowfile(uuid=buried[0])
    assert found["uuid"] == buried[0]
    assert found["beyond_listing"] is True
    assert found["flowfile"]["position"] is None     # depth is unknowable
    _drain(client, f.pg_id)


def test_recent_events_returns_the_newest_events_not_an_arbitrary_slice(deployed):
    """NiFi's maxResults is not "the newest N" — measured on 1.24 and 2.7.2.

    A capped provenance query answers with an arbitrary subset (once seen as
    events from the previous day when the newest were 130k ids later), so the
    cap has to be widened until the answer is complete.
    """
    client = deployed
    pg_id = client.resolve_group(GROUP)
    bulk = _proc(client, pg_id, "BulkGen")
    client.run_processor_once(bulk["id"])
    time.sleep(3)

    complete, capped = client._provenance_newest(
        {"ProcessorID": {"value": bulk["id"], "inverse": False}}, 0, "bulk")
    assert not capped, "the fixture should not exceed the escalation ceiling"
    newest = max(int(event["eventId"]) for event in complete)

    for size in (5, 25):
        events = client.recent_events(bulk["id"], max_results=size)
        assert len(events) == size
        assert int(events[0]["event_id"]) == newest      # newest first
        assert events == sorted(events, key=lambda e: -int(e["event_id"]))
    _drain(client, pg_id)


def test_a_component_past_the_escalation_ceiling_still_answers_with_the_newest(
        deployed):
    """T7a: above the ceiling the count can never settle it, so bound by time.

    NiFi's ``maxResults`` is applied per index shard, so a capped answer is an
    arbitrary subset — and a component with more events than
    ``_PROV_RESULT_CEILING`` is *always* capped, however far the cap is
    escalated. The walk instead asks for windows small enough to be answered
    completely, which is both correct and much cheaper (measured on 1.24
    against ~200k events: 0.06s and three queries, against 0.64s before).
    """
    from niflow.rest.inspect import _PROV_RESULT_CEILING

    client = deployed
    pg_id = client.resolve_group(GROUP)
    bulk = _proc(client, pg_id, "BulkGen")

    # Overflow the ceiling: each run is BATCH creates, and every file is
    # dropped at the auto-terminated end of the lane.
    runs = (_PROV_RESULT_CEILING // labyrinth.BATCH) + 4
    for _ in range(runs):
        client.run_processor_once(bulk["id"])
    time.sleep(4)

    terms = {"ProcessorID": {"value": bulk["id"], "inverse": False}}
    totals = {}
    client._provenance_query(terms, _PROV_RESULT_CEILING, "bulk", totals=totals)
    assert str(totals["total"]).endswith("+"), (
        "fixture did not exceed the escalation ceiling — nothing to prove")

    # Ground truth: one more batch, in its own second, so a window covering
    # only that batch comes back complete and names the newest events exactly.
    mark = client._server_now(totals)
    time.sleep(1.5)          # the request format has second resolution
    client.run_processor_once(bulk["id"])
    time.sleep(3)
    truth_totals = {}
    truth = client._provenance_query(
        terms, _PROV_RESULT_CEILING, "bulk", totals=truth_totals,
        start=mark + datetime.timedelta(seconds=1),
        end=mark + datetime.timedelta(seconds=120))
    assert not str(truth_totals["total"]).endswith("+"), (
        "the ground-truth window is itself capped — narrow it")
    assert truth, "the final batch produced no events"
    newest = sorted(int(e["eventId"]) for e in truth)[-25:]

    events = client.recent_events(bulk["id"], max_results=25)
    assert [int(e["event_id"]) for e in events] == sorted(newest, reverse=True)
    _drain(client, pg_id)


# ------------------------------------------------- transfers without an event


def test_a_transfer_with_no_provenance_event_is_not_reported_as_a_stall(follower):
    """A plain ``session.transfer`` writes nothing to provenance.

    On 1.24.0 SplitJson routing to ``failure`` records **no event at all**,
    and the step used to report ``stalled`` with advice to retry a poll that
    could never succeed.
    """
    follower.start_from(_entry(follower.client, "BadGen"), wait=20)
    outcome = follower.step()
    assert outcome["status"] in ("moved", "advanced"), outcome.get("message")
    assert follower.locate() is not None, "the FlowFile should have moved on"
    if outcome["status"] == "moved":
        assert outcome["retryable"] is False
        assert outcome["hops"][0]["synthetic"] is True
        assert "BadSink" in outcome["message"]


# ------------------------------------------------------- invalid processors


def test_an_invalid_destination_is_blocked_before_run_once_is_sent(follower):
    """NiFi accepts RUN_ONCE on an invalid processor and wedges it.

    1.24.0: the processor is stuck in ``RUN_ONCE``/``VALIDATING`` and no REST
    call clears it — stop-group, terminate-threads and run-status STOPPED all
    return 200 and change nothing. 2.7.2 additionally refuses to accept a
    config change while wedged. So the stepper must never send run-once to an
    invalid processor: it would brick the flow it is debugging.
    """
    client = follower.client
    router = _proc(client, follower.pg_id, "Router")
    good = client._get_json(f"/processors/{router['id']}")
    original = dict(good["component"]["config"]["properties"])

    def set_property(value):
        entity = client._get_json(f"/processors/{router['id']}")
        client._request("PUT", f"/processors/{router['id']}", json={
            "revision": entity["revision"],
            "component": {"id": router["id"],
                          "config": {"properties": {"hot": value}}}})

    try:
        set_property("${literal(true)}")          # fails the BOOLEAN validator
        for _ in range(20):
            time.sleep(0.5)
            if client.processor_validation(router["id"])["errors"]:
                break
        assert client.processor_validation(router["id"])["errors"]

        follower.start_from(_entry(client, "RouteGen"), wait=20)
        outcome = follower.step()
        assert outcome["status"] == "blocked"
        assert outcome["runs"] == 0               # nothing was sent
        assert "invalid" in outcome["message"]
        assert client.processor_validation(router["id"])["state"] != "RUN_ONCE"
    finally:
        set_property(original.get("hot", "${filename:isEmpty():not()}"))


# ---------------------------------------------------------------- fan-in


def test_a_fan_in_destination_advances_the_queue_we_are_following(follower):
    """Three inbound queues: run-once serves one of them, not necessarily ours."""
    client = follower.client
    for name in ("FanA", "FanB"):
        client.run_processor_once(_proc(client, follower.pg_id, name)["id"])
    time.sleep(2)
    follower.start_from(_entry(client, "FanC"), wait=20)
    outcome = follower.step()
    assert outcome["status"] in ("advanced", "moved"), outcome.get("message")
    assert [hop["component"] for hop in outcome["hops"]] == ["Collector"] \
        or outcome["status"] == "moved"


# ------------------------------------------------------ fixture injection


def test_injecting_a_fixture_mints_exactly_the_file_you_asked_for(follower):
    """The debugger's own input: your bytes, your attributes, your processor.

    Everything else about the stepper waits for the flow to produce something.
    This is the half that lets you ask "what does this processor do to THIS
    file" without arranging for the flow to make one.
    """
    picked = follower.inject("L1/Mark1", content="fixture-body",
                             attributes={"case": "urgent"})
    try:
        assert picked["injected"] == "L1/Mark1"
        outcome = follower.step()
        assert outcome["status"] == "advanced", outcome.get("message")
        hop = outcome["hops"][0]
        # Mark1 stamps depth=1; our attributes rode in with the file, and the
        # body is the custom text (the per-line property name has to be right
        # or NiFi files it as an inert dynamic property and mints 0 bytes).
        assert hop["attributes"]["case"] == "urgent"
        assert hop["attributes"]["depth"] == "1"
        assert hop["attributes"]["filename"] == "niflow-fixture"
        assert hop["size"] == len("fixture-body")
        assert follower.client.event_content(hop["event_id"], "input") == "fixture-body"
    finally:
        follower.cleanup_injector()


def test_the_injector_is_removed_once_its_file_has_moved_on(follower):
    follower.inject("L1/Mark1", content="x")
    injector = dict(follower.session.injector)
    assert follower.injector_holds_file()      # unstepped: removing it drops the file

    follower.step()
    assert follower.injector_holds_file() is False
    assert follower.cleanup_injector() is True
    live = {p["id"] for p in follower.client.find_processors(group=follower.pg_id)}
    assert injector["processor"] not in live


def test_injecting_at_a_nested_input_port_crosses_into_the_group(follower):
    """A port is fed from OUTSIDE, so the injector has to live in the parent.

    Which also means the followed group's OWN port cannot be injected at: the
    connection would sit outside the journey entirely.
    """
    # Four groups deep, every one of them with a port called "in".
    with pytest.raises(FollowError, match="ambiguous"):
        follower.inject("in")

    inner = FlowFollower(follower.client, f"{GROUP}/L1/L2/L3/L4",
                         poll_timeout=8.0)
    with pytest.raises(FollowError, match="fed from outside"):
        inner.inject("in")

    follower.inject("L1/L2/in", content="ported")
    try:
        outcome = follower.step()
        # Crossing a port records no provenance event of its own.
        assert outcome["status"] in ("advanced", "crossed"), outcome.get("message")
        depth = None
        for _ in range(3):
            for hop in outcome["hops"]:
                depth = hop["attributes"].get("depth", depth)
            if depth:
                break
            outcome = follower.step()
        assert depth == "2", "the fixture never reached Mark2 inside L2"
    finally:
        follower.cleanup_injector()


# ------------------------------------------------------- replay after a fix


def test_replaying_a_fixture_reruns_the_same_journey_and_compares(follower):
    """The fix-push-retest loop, end to end on a real server.

    Router routes on ``${filename:isEmpty():not()}``, so a pinned filename
    always takes the ``hot`` lane — which makes two runs of the same fixture
    genuinely comparable, and any difference a difference the flow made.
    """
    follower.inject("Router", content="one", attributes={"case": "urgent"})
    for _ in range(2):
        if follower.step()["status"] not in ("advanced", "crossed"):
            break
    first = list(follower.session.flat_hops())
    assert first, "the fixture produced no hops to compare against"

    picked = follower.replay()
    assert picked["run"] == 2
    assert len(follower.session.runs) == 1
    for _ in range(2):
        if follower.step()["status"] not in ("advanced", "crossed"):
            break
    second = follower.session.flat_hops()
    try:
        rows = compare_runs(follower.session.run_hops(1), second)
        # Same flow, same fixture, no fix in between: the journeys match, and
        # the per-file identity attributes are excluded so they can.
        assert [row["status"] for row in rows] == ["same"] * len(rows)
        assert "Identical journey" in format_run_comparison(1, 2, rows)
        assert [h["component"] for h in first] == [h["component"] for h in second]
        assert any(h.get("relationship") == "hot" for h in second)
    finally:
        follower.cleanup_injector()


def test_a_watch_table_tracks_one_attribute_across_the_journey(follower):
    follower.inject("L1/Mark1", content="x", attributes={"case": "urgent"})
    try:
        for _ in range(3):
            if follower.step()["status"] not in ("advanced", "crossed"):
                break
        columns, rows = watch_rows(follower.session.history(),
                                   ["case", "depth", "@size"])
        assert columns == ["case", "depth", "@size"]
        assert rows, "no hops to tabulate"
        # `case` rode in with the fixture and nothing touches it; `depth` is
        # stamped by Mark1 at the first hop.
        # Port crossings are synthetic: they carry no attributes of their own,
        # so their cells are blank rather than "everything was removed".
        real = [row for row in rows if row["cells"]["case"]["status"] != "n/a"]
        assert real and all(row["cells"]["case"]["value"] == "urgent"
                            for row in real)
        assert real[0]["cells"]["depth"]["value"] == "1"
        assert format_watch_table(columns, rows).startswith("hop")
    finally:
        follower.cleanup_injector()


# ----------------------------------------------- back pressure and expiry


def _connection(client, pg_id, source, destination):
    for queue in client.list_queues(pg_id):
        if queue["source"] == source and queue["destination"] == destination:
            return queue
    raise AssertionError(f"no connection {source} -> {destination}")


def _set_connection(client, conn_id, **fields):
    entity = client._get_json(f"/connections/{conn_id}")
    client._request("PUT", f"/connections/{conn_id}", json={
        "revision": entity["revision"],
        "component": dict({"id": conn_id}, **fields),
    })


def test_back_pressure_is_named_instead_of_blamed_on_the_queue_index(follower):
    """T7h: a full downstream queue looks exactly like "it didn't run".

    NiFi refuses to let a component transfer into a connection that has hit
    its back-pressure threshold — the component runs and produces nothing.
    Before this, the stepper reported "ran X 8x and this FlowFile has not
    moved", which sends you hunting an indexing bug instead of draining a
    queue one hop downstream.
    """
    client = follower.client
    downstream = _connection(client, follower.pg_id, "Mark1", "in")
    _set_connection(client, downstream["id"], backPressureObjectThreshold=1)
    try:
        # Fill it: one injected file stepped through Mark1 lands there and is
        # already at (or over) the threshold of one.
        filler = follower.inject("L1/Mark1", content="filler")
        follower.step()
        follower.cleanup_injector()

        follower.inject("L1/Mark1", content="blocked")
        outcome = follower.step()
        assert outcome["status"] == "blocked", outcome.get("message")
        assert "BACK PRESSURE" in outcome["message"]
        assert "Mark1 -> in" in outcome["message"]
        assert outcome.get("retryable") is not True
    finally:
        follower.cleanup_injector()
        _set_connection(client, downstream["id"], backPressureObjectThreshold=10000)
        _drain(client, follower.pg_id)


def test_a_flowfile_that_expires_mid_journey_says_so(follower):
    """T7h: quiescing a group is exactly when a short expiration fires.

    The file vanishes between two steps and every queue reports it missing.
    "It was dropped, sent onward, or consumed" is true and unhelpful when the
    queue it was sitting in deletes anything older than a second.
    """
    client = follower.client
    picked = follower.inject("L1/Mark1", content="ephemeral")
    try:
        conn_id = picked["queue"]["id"]
        _set_connection(client, conn_id, flowFileExpiration="1 sec")
        # NiFi expires on its own housekeeping pass, not instantly.
        deadline = time.time() + 60
        while time.time() < deadline:
            if not any(q["id"] == conn_id and q.get("queued")
                       for q in client.list_queues(follower.pg_id)):
                break
            time.sleep(2)
        else:
            pytest.skip("NiFi did not expire the FlowFile within 60s")

        outcome = follower.step()
        assert outcome["status"] == "gone"
        # Either NiFi handed us the EXPIRE event or it did not; both answers
        # have to name expiry rather than shrug at a vanished FlowFile.
        assert ("EXPIRED out of its queue" in outcome["message"]
                or "expires FlowFiles after 1 sec" in outcome["message"]), \
            outcome["message"]
    finally:
        follower.cleanup_injector()
        _drain(client, follower.pg_id)


def _drain(client, pg_id):
    for queue in client.list_queues(pg_id):
        if queue.get("queued"):
            client.drain_connection(queue["id"])
