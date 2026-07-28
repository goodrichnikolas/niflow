"""Unit tests for pre-apply backups and rollback (:mod:`niflow.backup`)."""
import json

import pytest

from niflow import Flow, Processor
from niflow.backup import backup_dir, latest_backup, list_backups, save_backup
from niflow.client import NiFiClient
from niflow.config import NiFiConfig


@pytest.fixture(autouse=True)
def _isolated_backup_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("NIFLOW_BACKUP_DIR", str(tmp_path / "backups"))


def _snapshot(name="Probe") -> dict:
    flow = Flow(name)
    flow.add_processor(
        Processor(name="Gen", type="org.apache.nifi.processors.standard.GenerateFlowFile",
                  auto_terminate=["success"])
    )
    return json.loads(flow.to_json())


def test_save_list_latest_roundtrip():
    p1 = save_backup("My Flow", _snapshot())
    p2 = save_backup("My Flow", _snapshot())
    save_backup("Other", _snapshot("Other"))

    assert p1 != p2 and p1.parent == backup_dir()
    assert json.loads(p1.read_text())["flowContents"]["name"] == "Probe"

    mine = list_backups("My Flow")
    assert mine == sorted({p1, p2}, key=lambda p: p.name, reverse=True)
    assert latest_backup("My Flow") == mine[0]
    assert len(list_backups()) == 3
    assert latest_backup("Nope") is None


def test_slug_keeps_names_filesystem_safe():
    path = save_backup("prod/ETL (copy) #2", _snapshot())
    assert "/" not in path.name.replace(path.suffix, "")
    assert path.name.startswith("prod_ETL_copy_2-")


def _client() -> NiFiClient:
    return NiFiClient(NiFiConfig(host="https://nifi.test/nifi-api"))


def test_push_update_backs_up_before_applying(monkeypatch):
    client = _client()
    events = []
    live, desired = Flow("F"), Flow("F")
    desired.add_processor(Processor(name="P", type="org.x.Y"))
    from niflow.plan import diff_flows

    changes = diff_flows(live, desired)
    monkeypatch.setattr(client, "plan_flow", lambda f: ("pg-1", live, changes))
    monkeypatch.setattr(client, "ensure_parameter_contexts", lambda f: None)
    monkeypatch.setattr(client, "apply_parameters", lambda f, s: None)
    monkeypatch.setattr(
        client, "_backup", lambda g, name=None: events.append(("backup", g, name))
    )

    class FakeApplier:
        def __init__(self, *a, **kw):
            pass

        def apply(self, changes):
            events.append(("apply", len(changes)))

    import niflow.apply

    monkeypatch.setattr(niflow.apply, "PlanApplier", FakeApplier)
    client.push_update(desired)
    assert events == [("backup", "pg-1", "F"), ("apply", 1)]


def test_push_update_skips_backup_when_no_changes(monkeypatch):
    client = _client()
    events = []
    live = Flow("F")
    monkeypatch.setattr(client, "plan_flow", lambda f: ("pg-1", live, []))
    monkeypatch.setattr(client, "ensure_parameter_contexts", lambda f: None)
    monkeypatch.setattr(client, "apply_parameters", lambda f, s: None)
    monkeypatch.setattr(
        client, "_backup", lambda g, name=None: events.append(("backup", g))
    )
    assert client.push_update(live) == []
    assert events == []


def test_rollback_restores_latest_backup(monkeypatch):
    client = _client()
    save_backup("Probe", _snapshot())

    def fail_resolve(group):
        raise ValueError("gone")

    pushed = {}

    def fake_push(flow, start=False, secrets=None):
        pushed["flow"] = flow
        pushed["start"] = start
        return "new-id"

    monkeypatch.setattr(client, "resolve_group", fail_resolve)
    monkeypatch.setattr(client, "push_flow", fake_push)

    assert client.rollback("Probe", start=True) == "new-id"
    assert pushed["flow"].name == "Probe" and pushed["start"] is True
    assert [p.name for p in pushed["flow"].processors] == ["Gen"]


def test_rollback_without_backup_raises():
    client = _client()
    with pytest.raises(ValueError, match="no backups"):
        client.rollback("Never-Pushed")
