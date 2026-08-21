"""Provenance under REAL load, and a repository that rolls over — T7h.

Every provenance decision niflow makes was reasoned out on a quiet container
with one client and a repository that never filled up: the per-shard
``maxResults`` cap, the time-window walk that replaced count escalation, and
"no events" meaning the uuid aged out. None of those conditions hold at work.

``docker compose --profile load`` (``make load-up``) runs a NiFi 1.24 whose
provenance repository is deliberately tiny — 5 MB, one minute of history,
rolling over every ten seconds — so a test can generate genuine volume, watch
events age out underneath a query, and restart the node to catch it
re-indexing. (Found the slow way: NiFi purges only when the repository rolls
over, so on an idle server events outlive their retention indefinitely.) The module targets that server explicitly and skips when it is not
there, so it costs nothing in the normal run.

    make load-up load-wait
    make test-load

What it confirmed: under load a **single second** of one processor's history
overflows the query ceiling, which is the last-resort case ``recent_events``
was written for. It answers in about a second, returns that second's events,
and sets ``capped`` — rather than silently handing back an arbitrary slice of
the day, which is what NiFi's own ``maxResults`` does.
"""
import os
import subprocess
import threading
import time

import pytest

from niflow import Flow
from niflow.client import NiFiClient
from niflow.config import NiFiConfig
from niflow.core import Processor
from niflow.rest.common import NiFiApiError

pytestmark = pytest.mark.integration

#: The tiny-provenance NiFi (docker-compose profile "load").
LOAD_HOST = os.environ.get("NIFLOW_LOAD_HOST", "https://localhost:8446/nifi-api")
LOAD_CONTAINER = os.environ.get("NIFLOW_LOAD_CONTAINER", "niflow-nifi-load")

GROUP = "NiflowLoadLive"
GEN = "org.apache.nifi.processors.standard.GenerateFlowFile"
UPDATE = "org.apache.nifi.processors.attributes.UpdateAttribute"
LOG = "org.apache.nifi.processors.standard.LogAttribute"

#: Seconds of full-rate generation. Twenty is already several million events —
#: enough to overflow one second of history, which is the case under test.
LOAD_SECONDS = 20
#: The container keeps one minute of history. Expiry needs headroom *and*
#: traffic: NiFi purges when the repository rolls over, so on an idle server
#: events outlive their retention indefinitely (found here the slow way).
EXPIRY_WAIT_S = 240


@pytest.fixture(scope="module")
def load_client():
    config = NiFiConfig.from_env().model_copy(update={"host": LOAD_HOST})
    client = NiFiClient(config)
    try:
        client.login()
        client.version()
    except Exception as exc:  # not running, or not reachable from here
        pytest.skip(f"no load NiFi at {LOAD_HOST} ({exc}) — 'make load-up load-wait'")
    return client


def _load_flow():
    flow = Flow(GROUP)
    firehose = Processor(
        name="Firehose", type=GEN,
        properties={"File Size": "0B", "Batch Size": "50",
                    "generate-ff-custom-text": "load"},
        scheduling_period="0 sec", concurrent_tasks=2)
    mark = Processor(name="Mark", type=UPDATE, properties={"seen": "yes"},
                     concurrent_tasks=2)
    drain = Processor(name="Drain", type=LOG, properties={"Log Level": "debug"},
                      auto_terminate=["success"], concurrent_tasks=2)
    flow.add_processor(firehose, mark, drain)
    flow.add_connection(firehose >> mark, mark >> drain)
    return flow


@pytest.fixture(scope="module")
def loaded(load_client):
    """A group that has just produced millions of provenance events."""
    pg_id = load_client.push_flow(_load_flow())
    load_client._set_group_state(pg_id, "RUNNING")
    time.sleep(LOAD_SECONDS)
    load_client._set_group_state(pg_id, "STOPPED")
    time.sleep(3)  # let the last events land in the index
    yield load_client, pg_id
    try:
        load_client.delete_group(pg_id)
    except Exception:  # a wedged queue must not fail the suite
        pass


def _component(client, pg_id, name):
    return next(p for p in client.find_processors(group=pg_id)
                if p["name"] == name)


def _run_load(client, pg_id, seconds):
    client._set_group_state(pg_id, "RUNNING")
    time.sleep(seconds)
    client._set_group_state(pg_id, "STOPPED")


# ------------------------------------------------------------- under load


def test_recent_events_is_fast_and_honest_when_one_second_overflows(loaded):
    """The last-resort case, finally reproduced instead of reasoned about.

    NiFi caps a provenance query per index shard and then answers with an
    arbitrary subset — measured on a quiet 1.24 against a component with 800
    events, asking for 10 returned events from the previous *day*. niflow
    walks backwards in time windows instead. Under real load the walk hits its
    floor: one second of this processor's history holds more events than the
    ceiling, so that second is the best answer that exists, and the point is
    that it says so.
    """
    client, pg_id = loaded
    mark = _component(client, pg_id, "Mark")

    totals = {}
    started = time.monotonic()
    events = client.recent_events(mark["id"], max_results=25, totals=totals)
    elapsed = time.monotonic() - started

    assert events, "the load produced no queryable provenance"
    assert len(events) <= 25
    assert elapsed < 15, f"recent_events took {elapsed:.1f}s under load"
    # Newest first, and every one of them belongs to this component.
    ids = [int(event["event_id"]) for event in events]
    assert ids == sorted(ids, reverse=True)
    assert {event["component"] for event in events} == {"Mark"}

    if totals.get("capped"):
        # Capped means "this whole answer is inside one second" — which is the
        # only honest thing to claim when a second overflows the ceiling.
        seconds = {event["time"][:19] for event in events}
        assert len(seconds) == 1, seconds


def test_two_queries_agree_about_which_event_is_newest(loaded):
    """A different ``max_results`` must not change what "newest" means.

    That is exactly what NiFi's own cap does — the reason the window walk
    exists — so it is worth proving the walk does not inherit it.
    """
    client, pg_id = loaded
    mark = _component(client, pg_id, "Mark")

    one = client.recent_events(mark["id"], max_results=1)
    many = client.recent_events(mark["id"], max_results=200)

    assert one and many
    newest_of_many = max(int(event["event_id"]) for event in many)
    assert int(one[0]["event_id"]) >= newest_of_many


def test_concurrent_provenance_queries_all_succeed(loaded):
    """Work has more than one analyst, and the GUI polls."""
    client, pg_id = loaded
    mark = _component(client, pg_id, "Mark")
    results, errors = [], []

    def query():
        try:
            results.append(client.recent_events(mark["id"], max_results=10))
        except Exception as exc:  # noqa: BLE001 — the point is to record it
            errors.append(exc)

    threads = [threading.Thread(target=query) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)

    assert not errors, errors
    assert len(results) == 6
    assert all(result for result in results)


def test_queries_during_an_active_rollover_do_not_raise(loaded):
    """The repository rolls over every ten seconds; query straight through it."""
    client, pg_id = loaded
    mark = _component(client, pg_id, "Mark")

    worker = threading.Thread(target=_run_load, args=(client, pg_id, 25))
    worker.start()
    try:
        deadline = time.monotonic() + 25
        answers = 0
        while time.monotonic() < deadline:
            # Any NiFi-side refusal is allowed to surface as NiFiApiError; what
            # must not happen is a hang or an exception nothing accounts for.
            events = client.recent_events(mark["id"], max_results=10)
            assert isinstance(events, list)
            answers += 1
        assert answers >= 3, f"only managed {answers} queries in 25s"
    finally:
        worker.join(timeout=120)
        client._set_group_state(pg_id, "STOPPED")


# --------------------------------------------------------- ageing out


def test_a_flowfile_whose_events_aged_out_traces_to_nothing(loaded):
    """"No events" has to mean "aged out", not "niflow lost it".

    The repository keeps two minutes. A uuid from before that is simply gone,
    and the honest answer is an empty journey — which `niflow trace` reports as
    "no events found", rather than an empty screen.
    """
    client, pg_id = loaded
    mark = _component(client, pg_id, "Mark")
    # Make our own history rather than borrowing the fixture's: with a minute
    # of retention, anything an earlier test produced may already be gone.
    _run_load(client, pg_id, 5)
    events = client.recent_events(mark["id"], max_results=1)
    assert events, "the burst produced no queryable provenance"
    uuid = events[0]["uuid"]

    # It is traceable right now...
    assert client.trace_flowfile(uuid)["hops"], "a fresh uuid should trace"

    # ...and after the retention window, with traffic still rolling the
    # repository over, it is not. The traffic is not incidental: an idle NiFi
    # never purges, because purging happens at rollover.
    deadline = time.monotonic() + EXPIRY_WAIT_S
    while time.monotonic() < deadline:
        _run_load(client, pg_id, 5)
        trace = client.trace_flowfile(uuid)
        if not trace["hops"]:
            break
        time.sleep(5)
    else:
        pytest.fail(f"{uuid} still had provenance after {EXPIRY_WAIT_S}s")

    assert trace["hops"] == []
    assert trace["truncated"] is False   # empty because it is gone, not capped


# ------------------------------------------------- restart / re-indexing


def _docker(*args):
    return subprocess.run(("docker",) + args, capture_output=True, text=True,
                          timeout=180)


def test_provenance_queries_are_sane_while_the_node_reindexes(load_client):
    """After a restart NiFi rebuilds its provenance index.

    The work version of this: someone restarts a node, traces a FlowFile, and
    gets nothing. That answer is allowed — the index is still being built —
    but niflow must reach it by answering or by raising a NiFiApiError, never
    by hanging or leaking an exception nobody handles.
    """
    if _docker("inspect", LOAD_CONTAINER).returncode != 0:
        pytest.skip(f"container {LOAD_CONTAINER} not visible to docker here")

    assert _docker("restart", LOAD_CONTAINER).returncode == 0

    deadline = time.monotonic() + 300
    answered = False
    while time.monotonic() < deadline:
        try:
            load_client.login()
            groups = list(load_client.walk_groups())
        except Exception:
            time.sleep(5)
            continue
        target = next((comp["id"] for path, comp in groups if path == GROUP), None)
        if target is None:
            time.sleep(5)
            continue
        procs = load_client.find_processors(group=target)
        mark = next((p for p in procs if p["name"] == "Mark"), None)
        if mark is None:
            time.sleep(5)
            continue
        try:
            events = load_client.recent_events(mark["id"], max_results=5)
        except NiFiApiError:
            # An explicit refusal while the index rebuilds is a fine answer.
            answered = True
            break
        assert isinstance(events, list)
        answered = True
        break

    assert answered, "the node never came back within 5 minutes"
