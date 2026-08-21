"""The `niflow plan` and `niflow push --update` CLI commands (fake client)."""
import argparse

from niflow import Flow, Processor
from niflow.cli import main


class FakeClient:
    def __init__(self, live: Flow):
        self._live = live
        self.updated = None

    def plan_flow(self, flow):
        from niflow.plan import diff_flows

        return "pg-1", self._live, diff_flows(self._live, flow)

    def push_update(self, flow, *, start=False, secrets=None, env=None):
        _, _, changes = self.plan_flow(flow)
        self.updated = flow
        flow.nifi_id = "pg-1"
        return changes


def _live_flow() -> Flow:
    flow = Flow("Demo")
    flow.add_processor(Processor(name="Gen", type="org.x.G", properties={"a": "1"}))
    return flow


def _write_flow(tmp_path, prop_value: str):
    path = tmp_path / "flow.py"
    path.write_text(
        "from niflow import Flow, Processor\n"
        "flow = Flow('Demo')\n"
        f"flow.add_processor(Processor(name='Gen', type='org.x.G', properties={{'a': {prop_value!r}}}))\n"
    )
    return str(path)


def test_plan_prints_changes_and_exits_one(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr("niflow.cli._client", lambda: FakeClient(_live_flow()))
    assert main(["plan", _write_flow(tmp_path, "2")]) == 1
    out = capsys.readouterr().out
    assert "~ processor .: Gen" in out
    assert "properties[a]: '1' -> '2'" in out
    assert "1 to change" in out


def test_plan_in_sync_exits_zero(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr("niflow.cli._client", lambda: FakeClient(_live_flow()))
    assert main(["plan", _write_flow(tmp_path, "1")]) == 0
    assert "No changes" in capsys.readouterr().out


def test_push_update_applies_and_reports(tmp_path, capsys, monkeypatch):
    fake = FakeClient(_live_flow())
    monkeypatch.setattr("niflow.cli._client", lambda: fake)
    assert main(["push", _write_flow(tmp_path, "2"), "--update"]) == 0
    out = capsys.readouterr().out
    assert "Applied to 'Demo'" in out and "1 to change" in out
    assert fake.updated is not None


def test_push_update_noop_reports_clean(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr("niflow.cli._client", lambda: FakeClient(_live_flow()))
    assert main(["push", _write_flow(tmp_path, "1"), "--update"]) == 0
    assert "nothing to do" in capsys.readouterr().out


def test_drift_does_not_cry_wolf_over_a_password(monkeypatch, capsys):
    """`drift` is the CI gate; a flow holding a secret must not fail it forever.

    NiFi never returns a sensitive value, so the difference is permanent and
    says nothing about whether the live flow changed.
    """
    from niflow import cli
    from niflow.core import ControllerService, Flow
    from niflow.plan import diff_flows

    def pool(properties):
        flow = Flow("Prod")
        flow.add(ControllerService(name="Pool",
                                   type="org.apache.nifi.dbcp.DBCPConnectionPool",
                                   properties=properties))
        return flow

    changes = diff_flows(pool({}), pool({"Password": "hunter2"}), 2)
    monkeypatch.setattr(cli, "_client", lambda: object())
    monkeypatch.setattr(
        "niflow.sync.plan_all",
        lambda client, directory, var="flow": [
            {"name": "Prod", "file": "flows/prod.py", "exists": True,
             "changes": changes}],
    )
    args = argparse.Namespace(dir="flows", var="flow")
    assert cli.cmd_drift(args) == 0
    out = capsys.readouterr().out
    assert out.startswith("ok     Prod")
    assert "1 sensitive-only change(s) ignored" in out


def test_drift_still_fails_on_a_real_change(monkeypatch, capsys):
    from niflow import cli
    from niflow.core import Flow, Processor
    from niflow.plan import diff_flows

    live, desired = Flow("Prod"), Flow("Prod")
    live.add(Processor(name="A", type="org.x.P", properties={"k": "1"}))
    desired.add(Processor(name="A", type="org.x.P", properties={"k": "2"}))
    changes = diff_flows(live, desired)

    monkeypatch.setattr(cli, "_client", lambda: object())
    monkeypatch.setattr(
        "niflow.sync.plan_all",
        lambda client, directory, var="flow": [
            {"name": "Prod", "file": "flows/prod.py", "exists": True,
             "changes": changes}],
    )
    assert cli.cmd_drift(argparse.Namespace(dir="flows", var="flow")) == 1
    assert "DRIFT  Prod" in capsys.readouterr().out
