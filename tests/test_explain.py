"""niflow.explain — digests, fingerprints, and doc lifecycle (no NiFi, no LLM)."""
from __future__ import annotations

import niflow.llm
from niflow.explain import (
    digest_tree,
    doc_path,
    explain_group,
    explanation_status,
)


class FakeNiFi:
    """Two-level canvas: Parent {Gen -> Stage::in} with nested Stage {Log}."""

    def __init__(self):
        self.data = {
            "/process-groups/g1": {"component": {
                "id": "g1", "name": "Parent", "comments": "top-level ETL",
                "parameterContext": {"component": {"name": "etl-ctx"}},
            }},
            "/process-groups/g2": {"component": {"id": "g2", "name": "Stage"}},
            "/flow/process-groups/g1": {"processGroupFlow": {
                "breadcrumb": {
                    "breadcrumb": {"name": "Parent"},
                    "parentBreadcrumb": {"breadcrumb": {"name": "NiFi Flow"}},
                },
                "flow": {
                    "processors": [{"component": {
                        "id": "p1", "name": "Gen",
                        "type": "org.apache.nifi.GenerateFlowFile",
                        "state": "RUNNING",
                        "config": {
                            "schedulingStrategy": "TIMER_DRIVEN",
                            "schedulingPeriod": "60 sec",
                            "autoTerminatedRelationships": [],
                            "properties": {"Custom Text": "hello",
                                           "Password": None},
                        },
                    }}],
                    "connections": [{"component": {
                        "source": {"id": "p1", "groupId": "g1", "name": "Gen"},
                        "destination": {"id": "port1", "groupId": "g2",
                                        "name": "in"},
                        "selectedRelationships": ["success"],
                    }}],
                    "processGroups": [{"component": {"id": "g2", "name": "Stage"}}],
                    "inputPorts": [], "outputPorts": [], "funnels": [],
                    "labels": [{"component": {"label": "docs on canvas"}}],
                },
            }},
            "/flow/process-groups/g1/controller-services": {
                "controllerServices": [
                    {"component": {"parentGroupId": "g1", "name": "Reader",
                                   "type": "org.x.CSVReader", "state": "ENABLED",
                                   "properties": {}}},
                    {"component": {"parentGroupId": "g0", "name": "Inherited",
                                   "type": "org.x.Other", "state": "ENABLED",
                                   "properties": {}}},
                ]},
            "/flow/process-groups/g2": {"processGroupFlow": {
                "breadcrumb": {
                    "breadcrumb": {"name": "Stage"},
                    "parentBreadcrumb": {
                        "breadcrumb": {"name": "Parent"},
                        "parentBreadcrumb": {"breadcrumb": {"name": "NiFi Flow"}},
                    },
                },
                "flow": {
                    "processors": [{"component": {
                        "id": "p2", "name": "Log", "type": "org.x.LogAttribute",
                        "state": "STOPPED",
                        "config": {"properties": {"Log Level": "info"}},
                    }}],
                    "connections": [], "processGroups": [],
                    "inputPorts": [{"component": {"name": "in"}}],
                    "outputPorts": [], "funnels": [], "labels": [],
                },
            }},
            "/flow/process-groups/g2/controller-services": {"controllerServices": []},
        }

    def resolve_group(self, group):
        return {"root": "g1", "Parent": "g1"}.get(group, group)

    def _get_json(self, path):
        return self.data[path]


def _fake_complete(log):
    def complete(system, prompt):
        log.append(prompt)
        return "**Summary:** Moves data along.\n\n## Walkthrough\nIt flows."
    return complete


def test_depth_1_is_the_default_and_summarises_children(tmp_path):
    """The T14 default: one document, children as one structural line each."""
    prompts = []
    results = explain_group(FakeNiFi(), "Parent", docs_dir=tmp_path,
                            complete=_fake_complete(prompts))
    assert [(r["group"], r["status"]) for r in results] == [("Parent", "generated")]
    assert not doc_path(tmp_path, "Parent/Stage").exists()
    assert len(prompts) == 1  # one LLM call, not one per nested group
    # The child line is derived from its digest, no document needed.
    assert "- Stage: (structure only) 1 processors (LogAttribute); " \
           "input ports: in" in prompts[0]


def test_depth_2_documents_the_children_too(tmp_path):
    prompts = []
    results = explain_group(FakeNiFi(), "Parent", docs_dir=tmp_path, depth=2,
                            complete=_fake_complete(prompts))
    assert [(r["group"], r["status"]) for r in results] == [
        ("Parent/Stage", "generated"), ("Parent", "generated")]
    # With a document written, the parent quotes its real summary instead.
    assert "- Stage: Moves data along." in prompts[1]


def test_deepening_later_refreshes_the_parent_too(tmp_path):
    """A child that gains a document makes the parent's summary line stale."""
    client = FakeNiFi()
    explain_group(client, "Parent", docs_dir=tmp_path,
                  complete=_fake_complete([]))
    plan = explanation_status(client, "Parent", docs_dir=tmp_path, depth=0)
    assert plan["llm_calls"] == 2  # the child, plus the parent that quotes it
    prompts = []
    results = explain_group(client, "Parent", docs_dir=tmp_path, depth=0,
                            complete=_fake_complete(prompts))
    assert [r["status"] for r in results] == ["generated", "generated"]
    assert "- Stage: Moves data along." in prompts[1]  # no longer structural


def test_plan_counts_documents_before_any_llm_call(tmp_path):
    client = FakeNiFi()
    shallow = explanation_status(client, "Parent", docs_dir=tmp_path)
    assert (shallow["depth"], shallow["documents"], shallow["llm_calls"],
            shallow["summarised_groups"]) == (1, 1, 1, 1)
    deep = explanation_status(client, "Parent", docs_dir=tmp_path, depth=0)
    assert (deep["documents"], deep["llm_calls"], deep["summarised_groups"]) \
        == (2, 2, 0)
    assert [e["group"] for e in deep["plan"]] == ["Parent/Stage", "Parent"]


def test_confirm_can_abort_before_anything_is_written(tmp_path):
    seen = {}

    def refuse(plan):
        seen.update(plan)
        return False

    def explode(system, prompt):
        raise AssertionError("LLM called although the plan was refused")

    assert explain_group(FakeNiFi(), "Parent", docs_dir=tmp_path, depth=0,
                         complete=explode, confirm=refuse) == []
    assert seen["llm_calls"] == 2
    assert not list(tmp_path.iterdir())


def test_run_state_is_out_of_the_digest_and_the_prompt(tmp_path):
    """T12: half the canvas is stopped; that's operations, not flow logic."""
    prompts = []
    explain_group(FakeNiFi(), "Parent", docs_dir=tmp_path, depth=0,
                  complete=_fake_complete(prompts))
    assert "RUNNING" not in "".join(prompts)
    assert "STOPPED" not in "".join(prompts)
    node = digest_tree(FakeNiFi(), "g1")
    assert "state" not in node["digest"]["processors"][0]
    assert "state" not in node["digest"]["services"][0]


def test_pre_state_fingerprints_still_count_as_current(tmp_path):
    """Docs written before run state was dropped must not all go stale."""
    client = FakeNiFi()
    explain_group(client, "Parent", docs_dir=tmp_path,
                  complete=_fake_complete([]))
    node = digest_tree(client, "g1")
    path = doc_path(tmp_path, "Parent")
    path.write_text(path.read_text().replace(
        f'fingerprint="{node["fingerprint"]}"',
        f'fingerprint="{node["legacy_fingerprint"]}"'))
    assert not explanation_status(client, "Parent", docs_dir=tmp_path)["outdated"]
    # ...but a real logic change still is a change.
    client.data["/flow/process-groups/g1"]["processGroupFlow"]["flow"][
        "processors"][0]["component"]["config"]["properties"]["Custom Text"] = "bye"
    assert explanation_status(client, "Parent", docs_dir=tmp_path)["outdated"]


def test_starting_a_processor_does_not_outdate_the_doc(tmp_path):
    client = FakeNiFi()
    explain_group(client, "Parent", docs_dir=tmp_path, depth=0,
                  complete=_fake_complete([]))
    client.data["/flow/process-groups/g2"]["processGroupFlow"]["flow"][
        "processors"][0]["component"]["state"] = "RUNNING"
    assert not explanation_status(client, "Parent", docs_dir=tmp_path)["outdated"]


def test_digest_paths_fingerprint_and_scrubbing():
    node = digest_tree(FakeNiFi(), "g1")
    assert node["path"] == "Parent"
    assert node["children"][0]["path"] == "Parent/Stage"
    digest = node["digest"]
    # A connection into a nested group's port names the group.
    assert digest["connections"][0]["to"] == "Stage :: in"
    # Sensitive properties come back null from NiFi and stay out.
    assert digest["processors"][0]["properties"] == {"Custom Text": "hello"}
    # Inherited services belong to the ancestor's document.
    assert [s["name"] for s in digest["services"]] == ["Reader"]
    assert digest["parameter_context"] == "etl-ctx"
    # A child's fingerprint feeds the parent's.
    assert digest["children"] == [{
        "name": "Stage", "fingerprint": node["children"][0]["fingerprint"]}]
    assert node["fingerprint"] == digest_tree(FakeNiFi(), "g1")["fingerprint"]


def test_full_depth_generates_children_first_with_summaries(tmp_path):
    prompts = []
    results = explain_group(FakeNiFi(), "Parent", docs_dir=tmp_path, depth=0,
                            complete=_fake_complete(prompts))
    assert [(r["group"], r["status"]) for r in results] == [
        ("Parent/Stage", "generated"), ("Parent", "generated")]
    assert doc_path(tmp_path, "Parent/Stage").exists()
    assert doc_path(tmp_path, "Parent").exists()
    # The parent prompt carries the child's just-written one-line summary.
    assert "Stage: Moves data along." in prompts[1]
    text = doc_path(tmp_path, "Parent").read_text()
    assert 'fingerprint="' in text and text.count("# Parent") == 1


def test_second_run_is_current_and_needs_no_llm(tmp_path):
    client = FakeNiFi()
    explain_group(client, "Parent", docs_dir=tmp_path, depth=0,
                  complete=_fake_complete([]))

    def explode(system, prompt):
        raise AssertionError("LLM called although everything is current")

    results = explain_group(client, "Parent", docs_dir=tmp_path, depth=0,
                            complete=explode)
    assert {r["status"] for r in results} == {"current"}


def test_deep_change_outdates_the_whole_chain(tmp_path):
    client = FakeNiFi()
    explain_group(client, "Parent", docs_dir=tmp_path, depth=0,
                  complete=_fake_complete([]))
    # Change something inside the nested group only.
    client.data["/flow/process-groups/g2"]["processGroupFlow"]["flow"][
        "processors"][0]["component"]["config"]["properties"]["Log Level"] = "warn"
    status = explanation_status(client, "Parent", docs_dir=tmp_path)
    assert status["exists"] and status["outdated"]
    results = explain_group(client, "Parent", docs_dir=tmp_path, depth=0,
                            complete=_fake_complete([]))
    assert [r["status"] for r in results] == ["generated", "generated"]


def test_moving_boxes_changes_nothing(tmp_path):
    client = FakeNiFi()
    explain_group(client, "Parent", docs_dir=tmp_path,
                  complete=_fake_complete([]))
    # Positions aren't digested, so cosmetic edits keep docs current.
    client.data["/flow/process-groups/g1"]["processGroupFlow"]["flow"][
        "processors"][0]["component"]["position"] = {"x": 999.0, "y": 999.0}
    assert not explanation_status(client, "Parent", docs_dir=tmp_path)["outdated"]


def test_hand_written_files_are_never_overwritten(tmp_path):
    hand = doc_path(tmp_path, "Parent")
    hand.parent.mkdir(parents=True, exist_ok=True)
    hand.write_text("# My notes\nhands off\n")
    results = explain_group(FakeNiFi(), "Parent", docs_dir=tmp_path, depth=0,
                            complete=_fake_complete([]), force=True)
    assert dict((r["group"], r["status"]) for r in results) == {
        "Parent/Stage": "generated", "Parent": "skipped"}
    assert hand.read_text() == "# My notes\nhands off\n"


def test_status_reports_llm_availability(tmp_path, monkeypatch):
    monkeypatch.setattr(niflow.llm, "llm_config", lambda: None)
    status = explanation_status(FakeNiFi(), "Parent", docs_dir=tmp_path)
    # No LLM -> nothing to name as the backend either. Displays read
    # `backend`, never a URL: the claude-code provider hasn't got one.
    assert status["configured"] is False and status["backend"] is None
    assert {k: status[k] for k in ("group", "exists", "outdated", "generated",
                                   "model", "path", "doc")} == {
        "group": "Parent", "exists": False, "outdated": False,
        "generated": None, "model": None,
        "path": str(doc_path(tmp_path, "Parent")), "doc": None,
    }


def test_status_names_the_llm_backend(tmp_path, monkeypatch):
    monkeypatch.setattr(
        niflow.llm, "llm_config",
        lambda: niflow.llm.LLMConfig(provider="claude-code", model="claude-code",
                                     binary="/usr/bin/claude"))
    status = explanation_status(FakeNiFi(), "Parent", docs_dir=tmp_path)
    assert status["configured"] and status["backend"] == "Claude Code (local CLI)"
