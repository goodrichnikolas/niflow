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


def test_generate_children_first_with_summaries(tmp_path):
    prompts = []
    results = explain_group(FakeNiFi(), "Parent", docs_dir=tmp_path,
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
    explain_group(client, "Parent", docs_dir=tmp_path,
                  complete=_fake_complete([]))

    def explode(system, prompt):
        raise AssertionError("LLM called although everything is current")

    results = explain_group(client, "Parent", docs_dir=tmp_path,
                            complete=explode)
    assert {r["status"] for r in results} == {"current"}


def test_deep_change_outdates_the_whole_chain(tmp_path):
    client = FakeNiFi()
    explain_group(client, "Parent", docs_dir=tmp_path,
                  complete=_fake_complete([]))
    # Change something inside the nested group only.
    client.data["/flow/process-groups/g2"]["processGroupFlow"]["flow"][
        "processors"][0]["component"]["config"]["properties"]["Log Level"] = "warn"
    status = explanation_status(client, "Parent", docs_dir=tmp_path)
    assert status["exists"] and status["outdated"]
    results = explain_group(client, "Parent", docs_dir=tmp_path,
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
    results = explain_group(FakeNiFi(), "Parent", docs_dir=tmp_path,
                            complete=_fake_complete([]), force=True)
    assert dict((r["group"], r["status"]) for r in results) == {
        "Parent/Stage": "generated", "Parent": "skipped"}
    assert hand.read_text() == "# My notes\nhands off\n"


def test_status_reports_llm_availability(tmp_path, monkeypatch):
    monkeypatch.setattr(niflow.llm, "llm_config", lambda: None)
    status = explanation_status(FakeNiFi(), "Parent", docs_dir=tmp_path)
    assert status == {
        "group": "Parent", "configured": False, "exists": False,
        "outdated": False, "generated": None, "model": None,
        "path": str(doc_path(tmp_path, "Parent")), "doc": None,
    }
