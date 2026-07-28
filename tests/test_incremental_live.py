"""End-to-end proof of the core loop against a REAL NiFi (1.x or 2.x).

Push the kitchen-sink flow, put real FlowFiles in a queue, make the kind of
edit the tool exists for (route a debug tap off an auto-terminated
relationship, tweak a property), apply it incrementally, and verify the
things `push --update` promises:

* the group and untouched components keep their live ids;
* queued FlowFiles survive the apply;
* the plan converges to zero afterwards;
* the automatic pre-apply backup restores the original via ``rollback``.

Runs against whatever ``NIFLOW_NIFI_HOST`` points at (``make
test-integration`` / ``test-integration-v1``); skipped when no NiFi is up.
"""
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "examples"))

from kitchen_sink import build_flow  # noqa: E402

pytestmark = pytest.mark.integration

GROUP = "NiflowIncrementalE2E"


@pytest.fixture(scope="module")
def deployed(nifi_client):
    nifi_client.push_flow(build_flow(GROUP))
    yield nifi_client
    # Resolve by name: a rollback inside a test replaces the group (new id).
    nifi_client.delete_group(GROUP)


def _queued(client, pg_id, source, destination) -> int:
    for q in client.list_queues(pg_id):
        if q["source"] == source and q["destination"] == destination:
            return q["queued"]
    raise AssertionError(f"no queue {source} -> {destination}")


def _proc_ids(flow) -> dict:
    ids = {}

    def visit(group):
        for p in group.processors:
            ids[p.name] = p.nifi_id
        for child in group.process_groups:
            visit(child)

    visit(flow)
    return ids


def test_incremental_edit_preserves_ids_and_queues(deployed, monkeypatch, tmp_path):
    client = deployed
    pg_id = client.resolve_group(GROUP)
    monkeypatch.setenv("NIFLOW_BACKUP_DIR", str(tmp_path))

    # The real workflow starts from a pull, never from scratch.
    pulled = client.pull_flow(pg_id)
    _, _, changes = client.plan_flow(pulled)
    assert changes == [], "freshly pulled flow must plan to zero"
    ids_before = _proc_ids(pulled)

    # Put a real FlowFile in the Ingest -> Stamp queue (Stamp stays stopped).
    ingest_id = ids_before["Ingest"]
    client.run_processor_once(ingest_id)
    for _ in range(30):
        if _queued(client, pg_id, "Ingest", "Stamp") > 0:
            break
        time.sleep(1)
    queued_before = _queued(client, pg_id, "Ingest", "Stamp")
    assert queued_before > 0, "run-once produced no FlowFile"

    # The debugging edit: stop auto-terminating Route's 'unmatched' and tap it.
    from niflow.core import Processor

    route = next(p for p in pulled.processors if p.name == "Route")
    route.auto_terminate = []
    debug = Processor(
        name="DebugTap",
        type="org.apache.nifi.processors.standard.LogAttribute",
        auto_terminate=["success"],
    )
    pulled.add_processor(debug)
    pulled.add_connection(route.to(debug, relationships=["unmatched"]))
    stamp = next(p for p in pulled.processors if p.name == "Stamp")
    stamp.properties["env"] = "e2e"

    applied = client.push_update(pulled)
    ops = {(c.op, c.kind, c.name) for c in applied}
    assert ("add", "processor", "DebugTap") in ops
    assert ("update", "processor", "Stamp") in ops

    # Everything push --update promises:
    after = client.pull_flow(pg_id)
    ids_after = _proc_ids(after)
    assert after.nifi_id == pg_id
    for name in ("Ingest", "Stamp", "Route", "Audit", "ToJson", "Tag"):
        assert ids_after[name] == ids_before[name], f"{name} was recreated"
    assert _queued(client, pg_id, "Ingest", "Stamp") == queued_before
    _, _, converged = client.plan_flow(after)
    assert converged == []

    # ... and the automatic backup can undo the whole edit.
    from niflow.backup import latest_backup

    backup = latest_backup(GROUP)
    assert backup is not None, "push_update must back up before applying"
    client.rollback(GROUP, backup)
    restored = client.pull_flow(GROUP)
    assert "DebugTap" not in _proc_ids(restored)
    restamp = next(p for p in restored.processors if p.name == "Stamp")
    assert "env" not in restamp.properties
    _, _, rb_changes = client.plan_flow(restored)
    assert rb_changes == []


def test_rename_shows_up_in_live_plan(deployed):
    client = deployed
    pulled = client.pull_flow(GROUP)
    audit = next(p for p in pulled.processors if p.name == "Audit")
    audit.name = "AuditLog"
    _, _, changes = client.plan_flow(pulled)
    add = next(c for c in changes if c.op == "add" and c.name == "AuditLog")
    assert add.note and "RENAME" in add.note
