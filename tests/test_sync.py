"""Unit tests for multi-group sync (:mod:`niflow.sync`)."""
from pathlib import Path

import pytest

from niflow import Flow, Processor
from niflow.sync import _slug, load_flows, plan_all, pull_all, push_all


def _mini_flow(name: str) -> Flow:
    flow = Flow(name)
    flow.add_processor(
        Processor(name="Gen", type="org.apache.nifi.processors.standard.GenerateFlowFile",
                  auto_terminate=["success"])
    )
    return flow


class FakeClient:
    def __init__(self, flows):
        self.flows = {f.name: f for f in flows}
        self.pushed = []

    def resolve_group(self, group):
        return "parent-id"

    def _child_groups(self, pg_id):
        return [{"id": f"id-{n}", "name": n} for n in self.flows]

    def pull_flow(self, ref):
        return self.flows[ref.split("id-", 1)[1]]

    def plan_flow(self, flow):
        live = self.flows.get(flow.name)
        if live is None:
            from niflow.plan import diff_flows

            return None, Flow(flow.name), diff_flows(Flow(flow.name), flow)
        from niflow.plan import diff_flows

        return "pg-1", live, diff_flows(live, flow)

    def push_update(self, flow, start=False, secrets=None):
        self.pushed.append(flow.name)
        _, _, changes = self.plan_flow(flow)
        return changes


def test_slug():
    assert _slug("Prod ETL (copy) #2") == "prod_etl_copy_2"
    assert _slug("---") == "flow"


def test_pull_all_writes_loadable_modules(tmp_path):
    client = FakeClient([_mini_flow("Alpha Flow"), _mini_flow("Beta")])
    reports = pull_all(client, str(tmp_path))
    assert [r["name"] for r in reports] == ["Alpha Flow", "Beta"]
    assert (tmp_path / "alpha_flow.py").is_file() and (tmp_path / "beta.py").is_file()

    loaded = load_flows(str(tmp_path))
    assert [f.name for _, f in loaded] == ["Alpha Flow", "Beta"]
    assert [p.name for _, f in loaded for p in f.processors] == ["Gen", "Gen"]


def test_pull_all_handles_slug_collisions(tmp_path):
    client = FakeClient([_mini_flow("My Flow"), _mini_flow("my-flow")])
    reports = pull_all(client, str(tmp_path))
    files = sorted(Path(r["file"]).name for r in reports)
    assert files == ["my_flow.py", "my_flow_2.py"]


def test_load_flows_rejects_non_flow_modules(tmp_path):
    (tmp_path / "junk.py").write_text("x = 1\n")
    with pytest.raises(ValueError, match="does not define"):
        load_flows(str(tmp_path))


def test_load_flows_skips_underscore_and_requires_some(tmp_path):
    (tmp_path / "_helpers.py").write_text("raise RuntimeError('must not import')\n")
    with pytest.raises(ValueError, match="no flow modules"):
        load_flows(str(tmp_path))


def test_plan_and_push_all_roundtrip(tmp_path):
    live = [_mini_flow("Alpha"), _mini_flow("Beta")]
    client = FakeClient(live)
    pull_all(client, str(tmp_path))

    # Everything in sync right after a mirror.
    plans = plan_all(client, str(tmp_path))
    assert all(r["exists"] and not r["changes"] for r in plans)

    # Drift one flow (live side changes under us).
    client.flows["Beta"].processors[0].properties["File Size"] = "1KB"
    plans = plan_all(client, str(tmp_path))
    by_name = {r["name"]: r for r in plans}
    assert not by_name["Alpha"]["changes"] and by_name["Beta"]["changes"]

    # push --all reconciles every module (incremental).
    reports = push_all(client, str(tmp_path))
    assert client.pushed == ["Alpha", "Beta"]
    assert {r["name"]: bool(r["changes"]) for r in reports} == {"Alpha": False, "Beta": True}


def test_root_comment_roundtrips_through_python(tmp_path):
    """Regression: to_python dropped the ROOT group's comment (children kept
    theirs), so every mirror of a commented group showed permanent drift."""
    flow = _mini_flow("Gamma")
    flow.comment = "the root comment"
    client = FakeClient([flow])
    pull_all(client, str(tmp_path))
    ((_, loaded),) = load_flows(str(tmp_path))
    assert loaded.comment == "the root comment"
    assert not plan_all(client, str(tmp_path))[0]["changes"]
