"""Cluster-only live tests — the half of T7h that needed a real cluster.

Everything here is a no-op on a standalone NiFi, which is exactly why it went
untested: primary-node-only scheduling, load-balanced connections actually
moving FlowFiles between nodes, and what niflow does when a node drops out.
``make cluster-up && make cluster-wait`` brings up the two-node 1.24 cluster
these run against (see docker-compose.yml, profile ``cluster``); the whole
module skips itself when the server it is pointed at is not clustered, so it
is safe to leave in the default integration run.

    make cluster-up cluster-wait
    make test-cluster

The findings that came out of writing them are recorded in todo.md under T7h;
the two that changed code are pinned here — a per-FlowFile lookup needs
``clusterNodeId`` (or NiFi answers 400 and the queue browser, ``niflow test``
and the stepper's deep lookup all break), and **run-once fires on every node**,
so one trigger mints one FlowFile per node.
"""
import collections
import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "flows"))

import cluster as cluster_flow  # noqa: E402
import labyrinth  # noqa: E402

from niflow.follow import FlowFollower  # noqa: E402

pytestmark = pytest.mark.integration

GROUP = "NiflowClusterLive"
FOLLOW_GROUP = "NiflowClusterFollowLive"

#: One cluster node reachable *directly* (not through the coordinator), and the
#: address it goes by inside the cluster. The compose fixture publishes
#: cluster-n2 on :8181; a different cluster sets these two.
ALT_HOST = os.environ.get("NIFLOW_CLUSTER_ALT_HOST",
                          "http://localhost:8181/nifi-api")
ALT_NODE = os.environ.get("NIFLOW_CLUSTER_ALT_NODE", "cluster-n2")


@pytest.fixture(scope="module")
def cluster(nifi_client):
    """A client whose server really is a cluster, or a skip."""
    summary = nifi_client.cluster_summary()
    if not summary["clustered"]:
        pytest.skip(
            f"{nifi_client.base} is not clustered — 'make cluster-up "
            "cluster-wait', then point NIFLOW_NIFI_HOST at http://localhost:8180"
            "/nifi-api (no password: the cluster profile is unsecured)")
    if summary["connected_nodes"] < 2:
        pytest.skip(f"cluster has only {summary['connected_nodes']} connected node(s)")
    return nifi_client


@pytest.fixture(scope="module")
def deployed(cluster):
    cluster_flow.flow.name = GROUP
    cluster.push_flow(cluster_flow.flow)
    yield cluster
    cluster.delete_group(GROUP)


def _queue(client, pg_id, source):
    for queue in client.list_queues(pg_id):
        if queue["source"] == source:
            return queue
    raise AssertionError(f"no queue out of {source!r}")


def _drain(client, pg_id):
    for queue in client.list_queues(pg_id):
        if queue.get("queued"):
            client.drain_connection(queue["id"])


def _settle(client, conn_id, expected, timeout=30.0):
    """Wait until a queue holds ``expected`` FlowFiles, then list them.

    The status snapshot is heartbeat-driven and lags (two nodes can report
    counts from different instants, which is how "80 + 40 = 80" happens); the
    *listing* is exact, so it is what every assertion here reads.
    """
    deadline = time.monotonic() + timeout
    while True:
        files = client.list_flowfiles(conn_id, max_results=1000)
        if len(files) >= expected or time.monotonic() > deadline:
            return files
        time.sleep(1.0)


# ------------------------------------------------------------ cluster shape


def test_the_client_can_describe_the_cluster(cluster):
    summary = cluster.cluster_summary()
    assert summary["clustered"] and summary["connected"]
    assert summary["connected_nodes"] == summary["total_nodes"] >= 2

    nodes = cluster.cluster_nodes()
    assert len(nodes) == summary["total_nodes"]
    assert all(node["status"] == "CONNECTED" for node in nodes)
    # Exactly one primary and one coordinator — that is what makes
    # primary-node-only scheduling mean something.
    roles = [role for node in nodes for role in node["roles"]]
    assert roles.count("Primary Node") == 1
    assert roles.count("Cluster Coordinator") == 1
    assert cluster.disconnected_nodes() == []


def test_doctor_reports_the_cluster_and_its_roles(cluster):
    from niflow.doctor import run_checks

    checks = {check.title: check for check in run_checks(cluster.config)}
    assert "cluster" in checks, "doctor said nothing about the cluster"
    assert checks["cluster"].status == "ok"
    assert "Primary Node" in checks["cluster"].detail


# ------------------------------------------------------ primary node only


def test_a_primary_node_only_processor_lands_as_primary_and_stays_converged(deployed):
    """The drift fix, finally proved where PRIMARY is not a formality.

    ``ListFTP`` is ``@PrimaryNodeOnly``: NiFi forces ``executionNode=PRIMARY``
    however the model was written, and the model here deliberately leaves the
    default (``ALL``). Before the annotation was harvested, that mismatch
    planned a change on every run that the server would never accept.
    """
    pg_id = deployed.resolve_group(GROUP)
    proc = next(p for p in deployed.find_processors(group=pg_id)
                if p["name"] == "ListOnPrimary")
    live = deployed._get_json(f"/processors/{proc['id']}")["component"]
    assert live["config"]["executionNode"] == "PRIMARY"

    _, _, changes = deployed.plan_flow(cluster_flow.flow)
    assert changes == [], f"plan did not converge on a cluster: {changes}"


def test_only_one_node_holds_the_primary_role(cluster):
    primaries = [node["address"] for node in cluster.cluster_nodes()
                 if "Primary Node" in node["roles"]]
    assert len(primaries) == 1, primaries


# ---------------------------------------------------------- load balancing


def test_load_balance_settings_survive_a_push_to_a_cluster(deployed):
    pg_id = deployed.resolve_group(GROUP)
    flow = deployed._get_json(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
    by_pair = {(c["component"]["source"]["name"], c["component"]["destination"]["name"]):
               c["component"] for c in flow["connections"]}

    balanced = by_pair[("Source", "Spread")]
    assert balanced["loadBalanceStrategy"] == "ROUND_ROBIN"
    # Compression is a separate field from the strategy and the one most
    # likely to be dropped on the way out.
    assert balanced["loadBalanceCompression"] == "COMPRESS_ATTRIBUTES_ONLY"

    partitioned = by_pair[("Spread", "Keyed")]
    assert partitioned["loadBalanceStrategy"] == "PARTITION_BY_ATTRIBUTE"
    assert partitioned["loadBalancePartitionAttribute"] == "partition"

    assert by_pair[("Keyed", "Sink")]["loadBalanceStrategy"] == "DO_NOT_LOAD_BALANCE"


def test_round_robin_really_spreads_flowfiles_across_the_nodes(deployed):
    """Not "the setting round-tripped" — the FlowFiles actually moved.

    The listing is the measurement, not the status snapshot: each summary says
    which node holds that FlowFile, while the nodewise status counts come from
    heartbeats and can disagree with themselves mid-transfer.
    """
    pg_id = deployed.resolve_group(GROUP)
    _drain(deployed, pg_id)
    source = next(p for p in deployed.find_processors(group=pg_id)
                  if p["name"] == "Source")
    queue = _queue(deployed, pg_id, "Source")

    deployed.run_processor_once(source["id"])
    # One trigger per node, BATCH files each — see the run-once test below.
    nodes = len(deployed.cluster_nodes())
    files = _settle(deployed, queue["id"], cluster_flow.BATCH * nodes)

    per_node = collections.Counter(f["node_address"] for f in files)
    assert len(per_node) == nodes, f"round robin used {len(per_node)} node(s): {per_node}"
    assert min(per_node.values()) > 0
    _drain(deployed, pg_id)


def test_a_queue_listing_says_which_node_holds_each_flowfile(deployed):
    pg_id = deployed.resolve_group(GROUP)
    _drain(deployed, pg_id)
    source = next(p for p in deployed.find_processors(group=pg_id)
                  if p["name"] == "Source")
    queue = _queue(deployed, pg_id, "Source")
    deployed.run_processor_once(source["id"])
    files = _settle(deployed, queue["id"], 1)

    assert files and all(f["node_id"] and f["node_address"] for f in files)
    _drain(deployed, pg_id)


# -------------------------------------------- per-FlowFile calls on a cluster


def test_a_flowfiles_attributes_and_content_are_readable_on_a_cluster(deployed):
    """The bug this file was written to catch.

    A queue is per-node, and ``GET /flowfile-queues/{id}/flowfiles/{uuid}``
    answers **400 "The id of the node in the cluster"** unless told which node
    to ask. Everything that reads one FlowFile went through that call:
    the queue browser's drill-down in both GUIs, ``niflow test``'s result
    collection, and the stepper's lookup past the 100-file listing cap — so
    all three were broken on every clustered NiFi, and ``locate_flowfile``
    reported "not here" for a file that was right there.
    """
    pg_id = deployed.resolve_group(GROUP)
    _drain(deployed, pg_id)
    source = next(p for p in deployed.find_processors(group=pg_id)
                  if p["name"] == "Source")
    queue = _queue(deployed, pg_id, "Source")
    deployed.run_processor_once(source["id"])
    files = _settle(deployed, queue["id"], 2)

    for summary in (files[0], files[-1]):
        detail = deployed.flowfile_detail(queue["id"], summary["uuid"],
                                          node_id=summary["node_id"])
        assert detail["content"] == "cluster"
        assert detail["node_address"] == summary["node_address"]

        # And without the hint: every connected node is asked, wrong ones 404.
        assert deployed.locate_flowfile(queue["id"], summary["uuid"]) is not None
        assert deployed.flowfile_detail(queue["id"], summary["uuid"])["content"] == "cluster"

    assert deployed.locate_flowfile(queue["id"], "no-such-uuid") is None
    _drain(deployed, pg_id)


# ----------------------------------------------------------------- run-once


def test_run_once_fires_on_every_node(deployed):
    """One trigger, one FlowFile per node — which the stepper has to admit to.

    Not a bug, but it is invisible from a standalone server and it changes what
    "I injected one file" means: a two-node cluster mints two.
    """
    pg_id = deployed.resolve_group(GROUP)
    _drain(deployed, pg_id)
    source = next(p for p in deployed.find_processors(group=pg_id)
                  if p["name"] == "Source")
    queue = _queue(deployed, pg_id, "Source")

    deployed.run_processor_once(source["id"])
    nodes = len(deployed.cluster_nodes())
    files = _settle(deployed, queue["id"], cluster_flow.BATCH * nodes)
    assert len(files) == cluster_flow.BATCH * nodes
    _drain(deployed, pg_id)


# ------------------------------------------------------- the stepper, clustered


@pytest.fixture(scope="module")
def follow_group(cluster):
    labyrinth.flow.name = FOLLOW_GROUP
    cluster.push_flow(labyrinth.flow)
    yield cluster
    cluster.delete_group(FOLLOW_GROUP)


def test_the_stepper_injects_and_steps_on_a_cluster(follow_group):
    """`niflow follow` end to end on a cluster, including the port crossing."""
    follower = FlowFollower(follow_group, FOLLOW_GROUP, poll_timeout=10.0)
    follower.quiesce()
    _drain(follow_group, follower.pg_id)

    picked = follower.inject("L1/Mark1", content="cluster-fixture",
                             attributes={"case": "urgent"})
    try:
        # The injector minted one file per node; the stepper follows one and
        # says so rather than pretending it minted a single file.
        assert picked["siblings"] == len(follow_group.cluster_nodes()) - 1
        assert picked["node"], "the followed FlowFile should name its node"

        seen = []
        for _ in range(4):
            outcome = follower.step()
            if outcome["status"] not in ("advanced", "crossed"):
                break
            seen += [(hop["component"], hop["attributes"].get("depth"))
                     for hop in outcome["hops"]]
        # Mark1 stamps depth=1, then a port crossing, then Mark2 stamps 2 —
        # the same journey the single-node suite asserts, on a cluster.
        assert ("Mark1", "1") in seen
        assert any(component == "Mark2" for component, _ in seen)
    finally:
        follower.cleanup_injector()


# ------------------------------------------------------- a node drops out


def _set_node(client, node_id, status, want, timeout=90.0):
    client._request("PUT", f"/controller/cluster/nodes/{node_id}",
                    json={"node": {"nodeId": node_id, "status": status}})
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        client._node_id_cache = None
        nodes = {n["id"]: n["status"] for n in client.cluster_nodes()}
        if nodes.get(node_id) == want:
            return
        time.sleep(2.0)
    raise AssertionError(f"node {node_id} never reached {want}")


def test_a_disconnected_node_blocks_deletes_and_niflow_says_why(deployed):
    """What niflow does when the cluster is not whole.

    niflow sends ``disconnectedNodeAcknowledged: false`` on every mutating
    call — acknowledging it means "apply this knowing a node will never see
    it", which is not a tool's decision. The consequence, measured rather than
    assumed: creates, updates and starts still go through; **deletes are
    refused**, which is what makes a full ``niflow push`` (it replaces the
    group) fail. The refusal now explains itself.
    """
    spare = next(node for node in deployed.cluster_nodes() if not node["roles"])
    pg_id = deployed.resolve_group(GROUP)
    _set_node(deployed, spare["id"], "DISCONNECTING", "DISCONNECTED")
    try:
        assert [n["address"] for n in deployed.disconnected_nodes()] == [spare["address"]]

        from niflow.doctor import run_checks
        cluster_check = next(c for c in run_checks(deployed.config)
                             if c.title == "cluster")
        assert cluster_check.status == "warn"
        assert spare["address"] in cluster_check.detail

        # A create still works...
        probe = deployed._request(
            "POST", f"/process-groups/{pg_id}/process-groups",
            json={"revision": {"version": 0, "clientId": "niflow"},
                  "component": {"name": "NiflowDisconnectProbe",
                                "position": {"x": 0.0, "y": 0.0}}},
        ).json()["id"]

        # ...and the delete is refused, with an explanation attached.
        with pytest.raises(Exception) as caught:
            deployed._delete_component("process-groups", probe)
        message = str(caught.value)
        assert "not connected" in message
        assert "niflow doctor" in message, "the refusal did not explain itself"
    finally:
        _set_node(deployed, spare["id"], "CONNECTING", "CONNECTED")
        deployed._node_id_cache = None
    # Once the cluster is whole again the leftover probe can go.
    for child in deployed._child_groups(pg_id):
        if child["name"] == "NiflowDisconnectProbe":
            deployed._delete_component("process-groups", child["id"])


def test_doctor_fails_loudly_when_talking_to_a_disconnected_node(cluster):
    """The work scenario: a load balancer hands you a node that fell out.

    Reads all succeed, so everything looks fine until a change is refused with
    a 400 that never mentions the cluster.

    Needs to reach one node *directly* — the compose fixture publishes
    ``cluster-n2`` on :8181 — which is what the two env vars below name; the
    test skips rather than guessing when they do not match the cluster.
    """
    from niflow.client import NiFiClient
    from niflow.doctor import run_checks

    node = next((n for n in cluster.cluster_nodes()
                 if n["address"] == ALT_NODE), None)
    if node is None:
        pytest.skip(f"no node named {ALT_NODE!r} in this cluster "
                    "(set NIFLOW_CLUSTER_ALT_NODE)")
    direct = cluster.config.model_copy(update={"host": ALT_HOST})
    probe = NiFiClient(direct)
    try:
        probe._get_json("/flow/about")
    except Exception:
        pytest.skip(f"{ALT_HOST} is not reachable from here "
                    "(set NIFLOW_CLUSTER_ALT_HOST)")

    _set_node(cluster, node["id"], "DISCONNECTING", "DISCONNECTED")
    try:
        check = next(c for c in run_checks(direct) if c.title == "cluster")
        assert check.status == "fail"
        assert "DISCONNECTED from its own cluster" in check.detail
    finally:
        _set_node(cluster, node["id"], "CONNECTING", "CONNECTED")
        cluster._node_id_cache = None
