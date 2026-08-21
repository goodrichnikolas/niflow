"""Unit tests for the fuzz harness itself — no live NiFi required.

The harness only earns trust if it is (a) deterministic, so a finding replays;
(b) honest about classification, so "niflow bug" means niflow's fault; and
(c) able to hand back a repro that actually rebuilds the failing flow.
"""
from __future__ import annotations

import json

import pytest

from niflow.core import Flow, Processor
from niflow.formats import to_json
from niflow.fuzz import (
    KINDS,
    NIFI_REJECTED,
    NIFLOW_BUG,
    PASSED,
    SANDBOX_PREFIX,
    Case,
    CaseResult,
    Finding,
    SweepConfig,
    _classify_live_error,
    _server_normalised,
    build_case_flow,
    check_offline,
    check_plan_sensitivity,
    cleanup_sandboxes,
    find_case,
    format_report,
    generate_cases,
    normalise_message,
    sweep,
    write_repro,
)

UPDATE_ATTR = "org.apache.nifi.processors.attributes.UpdateAttribute"


# --- generation is deterministic ---------------------------------------------


def test_same_seed_generates_identical_cases():
    first = generate_cases(seed=7, count=200)
    second = generate_cases(seed=7, count=200)
    assert [c.case_id for c in first] == [c.case_id for c in second]
    assert [c.spec for c in first] == [c.spec for c in second]


def test_different_seed_changes_the_sampled_cases():
    first = {c.case_id for c in generate_cases(seed=1, kinds=("pair",), count=100)}
    second = {c.case_id for c in generate_cases(seed=2, kinds=("pair",), count=100)}
    assert first != second


def test_case_id_is_derived_from_the_spec():
    assert Case("solo", {"type": "a"}).case_id == Case("solo", {"type": "a"}).case_id
    assert Case("solo", {"type": "a"}).case_id != Case("solo", {"type": "b"}).case_id
    assert Case("solo", {"type": "a"}).case_id.startswith("solo-")


def test_count_truncates_a_representative_interleaved_sample():
    full = generate_cases(seed=0)
    sample = generate_cases(seed=0, count=25)
    assert [c.case_id for c in sample] == [c.case_id for c in full[:25]]
    # Round-robin interleaving means a small sample still spans several kinds.
    assert len({c.kind for c in sample}) > 1


def test_type_filter_restricts_generation():
    cases = generate_cases(seed=0, type_pattern=r"standard\.UpdateAttribute$")
    assert cases == [] or all(
        "standard.UpdateAttribute" in json.dumps(c.spec) for c in cases
    )


def test_every_kind_builds_a_flow():
    seen = set()
    for case in generate_cases(seed=3):
        if case.kind in seen:
            continue
        seen.add(case.kind)
        flow = case.build()
        assert isinstance(flow, Flow)
        if seen == set(KINDS):
            break
    assert seen == set(KINDS)


# --- classification / signatures ----------------------------------------------


def test_signatures_collapse_the_same_bug_reported_twice():
    left = normalise_message("property 'Batch Size' of org.apache.nifi.foo.Bar is wrong (3 op(s))")
    right = normalise_message("property 'Directory' of org.apache.nifi.baz.Qux is wrong (17 op(s))")
    assert left == right


def test_nifi_rejections_are_not_niflow_bugs():
    assert _classify_live_error(
        "Processor is invalid because 'Directory' is required") == NIFI_REJECTED
    assert _classify_live_error(
        "org.apache.nifi.Foo is not a valid processor type") == NIFI_REJECTED


def test_server_faults_from_our_snapshot_are_niflow_bugs():
    assert _classify_live_error("java.lang.NullPointerException") == NIFLOW_BUG
    assert _classify_live_error(
        "identifiesControllerService is not of required type boolean") == NIFLOW_BUG


def test_case_status_is_the_worst_finding():
    result = CaseResult(Case("solo", {"type": UPDATE_ATTR}))
    assert result.status == PASSED
    result.add(Finding("live_push", NIFI_REJECTED, "m", "s"))
    assert result.status == NIFI_REJECTED
    result.add(Finding("emit_json", NIFLOW_BUG, "m", "s"))
    assert result.status == NIFLOW_BUG
    result.add(Finding("live_push", NIFI_REJECTED, "m", "s"))
    assert result.status == NIFLOW_BUG


# --- the checks themselves ------------------------------------------------------


def test_a_plain_processor_passes_every_offline_check():
    result = check_offline(Case("solo", {"type": UPDATE_ATTR}))
    assert result.status == PASSED, [f.message for f in result.findings]


def test_a_service_referenced_but_never_registered_is_refused_by_name():
    """It used to be a bare KeyError with a memory address, mid-emit.

    The model really is broken, so refusing it is correct — what was wrong was
    refusing it unreadably, from inside the emitter, after a push had started.
    `validate` sees it offline now and `to_json` names the service, so the
    harness records a clean rejection rather than a niflow bug.
    """
    from niflow.core import find_unregistered_components
    from niflow.formats import to_json

    case = Case("shape", {
        "shape": "unregistered_service",
        "source": UPDATE_ATTR, "target": UPDATE_ATTR,
    })
    flow = case.build()
    assert find_unregistered_components(flow)
    with pytest.raises(ValueError) as caught:
        to_json(flow)
    assert "not part of this flow" in str(caught.value)
    assert "KeyError" not in str(caught.value)

    result = check_offline(case)
    assert result.status != NIFLOW_BUG
    assert not [f for f in result.findings
                if f.signature.startswith("emit_json:KeyError")]


def test_server_normalisation_stringifies_property_values():
    flow = Flow("F")
    flow.add_processor(Processor(name="A", type=UPDATE_ATTR,
                                 properties={"n": 3, "b": True, "s": "x", "u": None},
                                 auto_terminate=["success"]))
    snapshot = _server_normalised(json.loads(to_json(flow)))
    props = snapshot["flowContents"]["processors"][0]["properties"]
    assert props == {"n": "3", "b": "true", "s": "x"}  # None is dropped on emit


def test_plan_sensitivity_flags_a_field_the_differ_cannot_see(monkeypatch):
    import niflow.plan

    monkeypatch.setattr(niflow.plan, "diff_flows", lambda live, desired: [])
    flow = build_case_flow("solo", {"type": UPDATE_ATTR})
    findings = check_plan_sensitivity(flow)
    assert findings
    assert all(f.check == "plan_blind" for f in findings)
    assert any("processor.comments" in f.signature for f in findings)


def test_plan_sensitivity_is_clean_on_the_real_differ():
    flow = build_case_flow("shape", {"shape": "queue_settings",
                                     "source": UPDATE_ATTR, "target": UPDATE_ATTR})
    assert check_plan_sensitivity(flow) == []


# --- repro emission -------------------------------------------------------------


def test_repro_rebuilds_exactly_the_failing_flow(tmp_path):
    case = Case("shape", {"shape": "funnel_chain",
                          "source": UPDATE_ATTR, "target": UPDATE_ATTR})
    result = CaseResult(case)
    result.add(Finding("emit_xml", NIFLOW_BUG, "boom", "emit_xml:KeyError"))
    path = write_repro(result, tmp_path)

    namespace: dict = {}
    exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), namespace)
    assert to_json(namespace["flow"]) == to_json(case.build())
    assert (tmp_path / "repro" / case.case_id / "case.json").exists()
    assert "boom" in (tmp_path / "repro" / case.case_id / "findings.txt").read_text()


# --- sweep persistence / resume / replay ------------------------------------------


def _config(tmp_path, **kwargs) -> SweepConfig:
    return SweepConfig(out_dir=tmp_path, count=12, quiet=True, **kwargs)


def test_sweep_writes_results_and_resumes(tmp_path):
    config = _config(tmp_path)
    first = sweep(config)
    assert len(first.results) == 12
    lines = (tmp_path / "results.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 12
    assert (tmp_path / "run.json").exists()
    assert (tmp_path / "summary.txt").exists()

    resumed = sweep(_config(tmp_path, resume=True))
    assert resumed.skipped == 12
    assert len(resumed.results) == 12
    # Nothing re-ran, so the log did not grow.
    assert len((tmp_path / "results.jsonl").read_text(encoding="utf-8").splitlines()) == 12


def test_sweep_without_resume_starts_a_fresh_log(tmp_path):
    sweep(_config(tmp_path))
    second = sweep(_config(tmp_path))
    assert second.skipped == 0
    assert len((tmp_path / "results.jsonl").read_text(encoding="utf-8").splitlines()) == 12


def test_find_case_recovers_a_case_by_id(tmp_path):
    config = _config(tmp_path)
    report = sweep(config)
    wanted = report.results[5].case
    found = find_case(wanted.case_id, config)
    assert found is not None
    assert found.kind == wanted.kind and found.spec == wanted.spec


def test_find_case_returns_none_for_an_unknown_id(tmp_path):
    assert find_case("solo-deadbeef00", _config(tmp_path)) is None


def test_report_groups_findings_by_signature(tmp_path):
    config = _config(tmp_path)
    report = sweep(config)
    for index, result in enumerate(report.results[:4]):
        result.add(Finding("emit_json", NIFLOW_BUG, f"boom {index}", "emit_json:shared"))
    grouped = report.by_signature(NIFLOW_BUG)
    assert grouped["emit_json:shared"] and len(grouped["emit_json:shared"]) == 4
    text = format_report(report)
    assert "emit_json:shared" in text and "×4" in text


@pytest.mark.parametrize("tier", (1, 2, 3))
def test_tiers_without_a_client_stay_offline(tmp_path, tier):
    # Tiers 2 and 3 degrade to the offline checks when no client is supplied,
    # so a sweep never silently skips the cheap tier.
    report = sweep(_config(tmp_path, tier=tier))
    assert len(report.results) == 12


# --- the sweep never litters someone's canvas ------------------------------------


class _FakeClient:
    """Just enough NiFiClient to exercise sandbox cleanup."""

    def __init__(self, names, undeletable=()):
        self.groups = {f"id-{i}": name for i, name in enumerate(names)}
        self.undeletable = set(undeletable)
        self.deleted = []

    def walk_groups(self):
        return [("", {"id": gid, "name": name}) for gid, name in self.groups.items()]

    def delete_group(self, pg_id):
        if self.groups[pg_id] in self.undeletable:
            raise RuntimeError("409 conflict")
        self.deleted.append(pg_id)
        del self.groups[pg_id]


def test_cleanup_removes_only_fuzz_sandboxes():
    client = _FakeClient([f"{SANDBOX_PREFIX}solo-1", "Production ETL",
                          f"{SANDBOX_PREFIX}shape-2 (niflow-validate)"])
    assert cleanup_sandboxes(client) == 2
    assert list(client.groups.values()) == ["Production ETL"]


def test_cleanup_gives_up_quietly_on_an_undeletable_group():
    stuck = f"{SANDBOX_PREFIX}solo-stuck"
    client = _FakeClient([stuck, f"{SANDBOX_PREFIX}solo-ok"], undeletable=[stuck])
    assert cleanup_sandboxes(client) == 1
    assert list(client.groups.values()) == [stuck]


# --- the coverage gaps closed on 2026-08-21 -----------------------------------

def test_a_controller_service_is_a_case_in_its_own_right():
    """`svc` exercises a service alone — not as some processor's far end.

    That blind spot is exactly how every 2.x service property key came to land
    on 1.24 as an inert dynamic property, and how a service kept a legacy key
    through an offline emit.
    """
    from niflow.core import ControllerService

    cases = [c for c in generate_cases(kinds=["svc"]) if c.spec.get("variant")]
    assert cases, "no property variants generated for services"
    flow = cases[0].build()
    assert not flow.processors
    assert len(flow.controller_services) == 1
    assert isinstance(flow.controller_services[0], ControllerService)
    assert check_offline(cases[0]).status == PASSED


def test_a_params_case_carries_a_real_secret_in_the_model():
    from niflow.fuzz import SECRET_VALUE

    case = generate_cases(kinds=["params"])[0]
    flow = case.build()
    context = flow.parameter_context
    assert context is not None
    sensitive = [p for p in context.parameters if p.sensitive]
    assert [p.value for p in sensitive] == [SECRET_VALUE]
    assert any("#{fuzz." in str(v) for v in flow.processors[0].properties.values())


def test_a_leaked_secret_is_a_finding():
    """The check has to fail when a value really does reach a file."""
    from niflow.fuzz import SECRET_VALUE, check_secret_containment

    flow = generate_cases(kinds=["params"])[0].build()
    clean = check_secret_containment(flow, {"JSON snapshot": '"fuzz.secret": null'})
    assert clean == []

    leaked = check_secret_containment(
        flow, {"JSON snapshot": f'"fuzz.secret": "{SECRET_VALUE}"'})
    assert [f.check for f in leaked] == ["secret_leak"]
    assert "sensitive parameter" in leaked[0].message


def test_dropping_the_parameter_is_not_a_way_to_pass_the_secret_check():
    from niflow.fuzz import check_secret_containment

    flow = generate_cases(kinds=["params"])[0].build()
    findings = check_secret_containment(flow, {"JSON snapshot": "{}"})
    assert [f.check for f in findings] == ["secret_dropped"]


def test_nothing_niflow_emits_contains_the_secret():
    from niflow.formats import to_python, to_xml
    from niflow.fuzz import SECRET_VALUE

    flow = generate_cases(kinds=["params"])[0].build()
    for text in (to_json(flow), to_xml(flow), to_python(flow)):
        assert SECRET_VALUE not in text


# --- apply fault injection ----------------------------------------------------

def _add_plan(flow):
    from niflow.plan import diff_flows

    live = Flow(name=flow.name)
    return live, diff_flows(live, flow)


def test_the_fake_server_fails_on_the_call_it_was_told_to():
    from niflow.apply import ApplyError, PlanApplier
    from niflow.fuzz import FakeServer

    flow = build_case_flow("pair", {
        "source": "org.apache.nifi.processors.standard.GenerateFlowFile",
        "target": "org.apache.nifi.processors.standard.LogAttribute",
        "relationship": "success"})
    live, changes = _add_plan(flow)

    clean = FakeServer()
    PlanApplier(clean, "root-pg", live, flow).apply(changes)
    assert clean.mutations == len(changes) and clean.failed_on is None

    for position in range(1, clean.mutations + 1):
        server = FakeServer(fail_at=position)
        # The plan holds references into the models it was built from, so an
        # attempt has to diff its own copies rather than reuse the first plan.
        attempt_desired = flow.model_copy(deep=True)
        attempt_live, attempt_changes = _add_plan(attempt_desired)
        with pytest.raises(ApplyError) as caught:
            PlanApplier(server, "root-pg", attempt_live,
                        attempt_desired).apply(attempt_changes)
        error = caught.value
        assert server.failed_on, "the fault never fired"
        # One change can be several REST calls (an add that also has to set a
        # state, an update that stops and restarts), so the invariant is that
        # the progress adds up — not that it equals the call index.
        assert 0 <= error.applied <= len(attempt_changes)
        assert error.applied + error.remaining == len(attempt_changes)
        assert "were applied before the failure" in str(error)


def test_apply_faults_finds_an_error_that_is_not_an_ApplyError(monkeypatch):
    """The invariant that matters: no unreadable exception escapes a push."""
    from niflow.apply import PlanApplier
    from niflow.fuzz import check_apply_faults

    case = Case("solo", {"type": "org.apache.nifi.processors.standard.LogAttribute"})

    def explode(self, changes):
        raise KeyError("0x7f9c deep inside")

    monkeypatch.setattr(PlanApplier, "apply", explode)
    findings, _ = check_apply_faults(case, case.build())
    assert findings and all(f.classification == NIFLOW_BUG for f in findings)
    assert any("apply_clean" in f.check for f in findings)


def test_apply_faults_are_clean_on_the_real_applier():
    from niflow.fuzz import check_apply_faults

    for kind, spec in (
        ("solo", {"type": "org.apache.nifi.processors.standard.LogAttribute"}),
        ("service", {"type": "org.apache.nifi.processors.standard.ConvertRecord",
                     "property": "Record Reader",
                     "service_type": "org.apache.nifi.csv.CSVReader"}),
    ):
        case = Case(kind, spec)
        findings, observation = check_apply_faults(case, case.build())
        assert findings == []
        assert observation["kind"] == "apply_faults" and observation["probes"] > 0


def test_the_generated_context_is_named_so_a_sweep_can_clean_it_up():
    """Parameter contexts are global and outlive the group that used them.

    A stale one poisons the next run: a leftover sensitive parameter left
    thirteen later cases unable to export their group at all on 1.24 (500 from
    /download).
    """
    flow = generate_cases(kinds=["params"])[0].build()
    assert flow.parameter_context.name.startswith(SANDBOX_PREFIX)


def test_cleanup_removes_the_contexts_a_sweep_created():
    deleted = []

    class Client:
        def walk_groups(self):
            return []

        def _get_json(self, path):
            assert path == "/flow/parameter-contexts"
            return {"parameterContexts": [
                {"revision": {"version": 3},
                 "component": {"id": "ctx-1", "name": f"{SANDBOX_PREFIX}context"}},
                {"revision": {"version": 1},
                 "component": {"id": "ctx-2", "name": "Production Secrets"}},
            ]}

        def _request(self, method, path, **kwargs):
            deleted.append((method, path, kwargs.get("params", {}).get("version")))

    assert cleanup_sandboxes(Client()) == 1
    assert deleted == [("DELETE", "/parameter-contexts/ctx-1", 3)]


def test_cleanup_leaves_a_context_it_did_not_create():
    class Client:
        def walk_groups(self):
            return []

        def _get_json(self, path):
            return {"parameterContexts": [
                {"revision": {"version": 1},
                 "component": {"id": "ctx-2", "name": "Production Secrets"}}]}

        def _request(self, method, path, **kwargs):  # pragma: no cover - must not run
            raise AssertionError(f"deleted something it did not own: {path}")

    assert cleanup_sandboxes(Client()) == 0


def test_a_sensitive_only_change_is_not_a_convergence_failure():
    """A password can never read back, so it is not evidence of drift."""
    from niflow.core import ControllerService
    from niflow.fuzz.checks import _comparable
    from niflow.plan import diff_flows

    def pool(properties):
        flow = Flow("F")
        flow.add(ControllerService(name="Pool",
                                   type="org.apache.nifi.dbcp.DBCPConnectionPool",
                                   properties=properties))
        return flow

    changes = diff_flows(pool({}), pool({"Password": "hunter2"}), 2)
    assert changes
    result = CaseResult(Case("svc", {"service_type": "x"}))
    assert _comparable(changes, result) == []
    assert result.observations == [{"kind": "sensitive_not_comparable", "changes": 1}]
