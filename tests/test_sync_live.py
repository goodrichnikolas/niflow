"""Multi-group sync against a REAL NiFi: mirror, drift, reconcile.

Builds a parent group with two child flows, mirrors them to a directory with
``pull --all``, verifies the mirror plans to zero, edits one generated module
the way a user would, and pushes the whole directory back incrementally.
"""
import pytest

from niflow import Flow, Processor
from niflow.sync import load_flows, plan_all, pull_all, push_all

pytestmark = pytest.mark.integration

PARENT = "NiflowSyncE2E"


def _child(name: str, size: str = "0B") -> Flow:
    flow = Flow(name, parent_pg=PARENT)
    gen = Processor(
        name="Gen", type="org.apache.nifi.processors.standard.GenerateFlowFile",
        properties={"File Size": size},
    )
    log = Processor(
        name="Log", type="org.apache.nifi.processors.standard.LogAttribute",
        auto_terminate=["success"],
    )
    flow.add(gen, log)
    flow.add_connection(gen >> log)
    return flow


@pytest.fixture(scope="module")
def synced(nifi_client):
    nifi_client.push_flow(Flow(PARENT))
    nifi_client.push_flow(_child("Sync Alpha"))
    nifi_client.push_flow(_child("Sync Beta"))
    yield nifi_client
    nifi_client.delete_group(PARENT)


def test_mirror_edit_reconcile_loop(synced, tmp_path):
    client = synced

    # Mirror the parent's children into a directory.
    reports = pull_all(client, str(tmp_path), parent=PARENT)
    assert sorted(r["name"] for r in reports) == ["Sync Alpha", "Sync Beta"]
    assert all(not r["warnings"] for r in reports)

    # A fresh mirror plans to zero across the board.
    assert all(not r["changes"] for r in plan_all(client, str(tmp_path)))

    # Edit one module the way a user would: load, tweak, regenerate.
    path, flow = next(
        (p, f) for p, f in load_flows(str(tmp_path)) if f.name == "Sync Alpha"
    )
    gen = next(p for p in flow.processors if p.name == "Gen")
    gen.properties["File Size"] = "2KB"
    path.write_text(flow.to_python())

    plans = {r["name"]: r["changes"] for r in plan_all(client, str(tmp_path))}
    assert not plans["Sync Beta"]
    (change,) = plans["Sync Alpha"]
    assert change.fields["properties[File Size]"] == ("0B", "2KB")

    # push --all applies only the drifted flow, then everything is in sync.
    results = push_all(client, str(tmp_path))
    assert {r["name"]: len(r["changes"]) for r in results} == {
        "Sync Alpha": 1, "Sync Beta": 0,
    }
    assert all(not r["changes"] for r in plan_all(client, str(tmp_path)))
