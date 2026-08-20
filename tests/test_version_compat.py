"""Cross-version property map: the diff builder, the lookups, and the wiring.

Splits cleanly in two:

* the **builder** (:mod:`niflow.versiondiff`) is tested on synthetic rulebooks,
  so the rename-matching rules are pinned by construction rather than by
  whatever two NiFi releases happen to differ by; and
* the **lookups and integration** are tested against the committed map
  (``niflow/version_map.py``, generated from real 2.7.2 and 1.24.0 servers by
  ``make version-map``) using types whose difference is a documented fact —
  ``AttributesToJSON``'s ``Pretty Print`` does not exist on 1.24, and
  ``ConvertRecord``'s ``Record Reader`` is ``record-reader`` there.

No live NiFi anywhere in this file.
"""
from __future__ import annotations

import pytest

from niflow import Flow
from niflow.core import ControllerService, Processor
from niflow.versiondiff import (
    build_map,
    detect_renames,
    emit_module,
    rank_offenders,
    render_report,
    summarise,
)

ATTRS_TO_JSON = "org.apache.nifi.processors.standard.AttributesToJSON"
CONVERT = "org.apache.nifi.processors.standard.ConvertRecord"
GFF = "org.apache.nifi.processors.standard.GenerateFlowFile"
JSON_READER = "org.apache.nifi.json.JsonTreeReader"
S3_PUT = "org.apache.nifi.processors.aws.s3.PutS3Object"


@pytest.fixture(autouse=True)
def _pin_baseline(monkeypatch):
    """Pin the compatibility baseline for every test in this file.

    The baseline is real configuration (``NIFLOW_MIN_NIFI_VERSION``, read from
    ``.niflow.env``), and configuration that leaks into a test suite makes it
    pass or fail on whose laptop it runs. A real environment variable beats the
    file, so this pins it to the shipped default.
    """
    monkeypatch.setenv("NIFLOW_MIN_NIFI_VERSION", "1.24")


def _prop(display=None, description="", required=False, default=None, allowable=()):
    return {
        "display": display, "description": description, "required": required,
        "default": default, "allowable": list(allowable), "sensitive": False,
        "dynamic": False, "service": None,
    }


def _book(version, processors=None, services=None):
    return {
        "nifi_version": version,
        "generated": "2026-01-01",
        "processors": {t: {"raw": raw} for t, raw in (processors or {}).items()},
        "services": {t: {"raw": raw} for t, raw in (services or {}).items()},
    }


# ------------------------------------------------------------ rename matching


def test_rename_matched_on_display_name():
    new = {"Record Reader": _prop(display="Record Reader")}
    old = {"record-reader": _prop(display="Record Reader")}
    renamed, only_new, only_old = detect_renames(new, old)
    assert renamed == {"Record Reader": "record-reader"}
    assert only_new == [] and only_old == []


def test_rename_matched_on_description_when_display_also_changed():
    # 2.x sometimes rewords the display name too; the description is the only
    # surviving link between the two keys.
    new = {"Attributes to Log Regular Expression":
           _prop(display="Attributes to Log Regular Expression",
                 description="A regular expression to match attribute names")}
    old = {"attributes-to-log-regex":
           _prop(display="Attributes to Log by Regular Expression",
                 description="A regular expression to match attribute names")}
    renamed, only_new, only_old = detect_renames(new, old)
    assert renamed == {"Attributes to Log Regular Expression": "attributes-to-log-regex"}


def test_ambiguous_display_name_is_never_paired():
    # CopyS3Object really does show two properties as "Bucket". Guessing wrong
    # would silently write a value to the other property, so we refuse to guess.
    new = {"Source Bucket": _prop(display="Bucket"),
           "Destination Bucket": _prop(display="Bucket")}
    old = {"source-bucket": _prop(display="Bucket"),
           "dest-bucket": _prop(display="Bucket")}
    renamed, only_new, only_old = detect_renames(new, old)
    assert renamed == {}
    assert only_new == ["Destination Bucket", "Source Bucket"]
    assert only_old == ["dest-bucket", "source-bucket"]


def test_curated_alias_is_the_last_resort():
    # Neither display nor description matches; only the curated table knows.
    new = {"Search Value": _prop(display="Search Value", description="new words")}
    old = {"Regular Expression": _prop(display="Regular Expression",
                                       description="old words")}
    renamed, _, _ = detect_renames(new, old, {"Regular Expression": "Search Value"})
    assert renamed == {"Search Value": "Regular Expression"}


def test_genuinely_added_and_removed_properties_stay_unmatched():
    new = {"Pretty Print": _prop(display="Pretty Print", description="indent it")}
    old = {"Legacy Knob": _prop(display="Legacy Knob", description="ancient")}
    renamed, only_new, only_old = detect_renames(new, old)
    assert renamed == {}
    assert only_new == ["Pretty Print"]
    assert only_old == ["Legacy Knob"]


# ------------------------------------------------------------ map building


def _synthetic_map():
    new = _book("2.7.2", processors={
        "x.Proc": {
            "Record Reader": _prop(display="Record Reader"),
            "Pretty Print": _prop(display="Pretty Print"),
            "Mode": _prop(display="Mode", allowable=["fast", "slow"], required=True),
        },
        "x.OnlyNew": {"A": _prop()},
    }, services={
        "x.Svc": {"Schema Access Strategy": _prop(display="Schema Access Strategy")},
    })
    old = _book("1.24.0", processors={
        "x.Proc": {
            "record-reader": _prop(display="Record Reader"),
            "Legacy": _prop(display="Legacy"),
            "Mode": _prop(display="Mode", allowable=["fast"], required=False,
                          default="fast"),
        },
        "x.OnlyOld": {"B": _prop()},
    }, services={
        "x.Svc": {"schema-access-strategy": _prop(display="Schema Access Strategy")},
    })
    return build_map(new, old)


def test_build_map_classifies_every_bucket():
    entry = _synthetic_map()["kinds"]["processors"]["types"]["x.Proc"]
    assert entry["renamed"] == {"Record Reader": "record-reader"}
    assert entry["only_new"] == ["Pretty Print"]
    assert entry["only_old"] == ["Legacy"]
    assert entry["allowable_changed"] == {"Mode": {"only_new": ["slow"], "only_old": []}}
    assert entry["required_changed"] == {"Mode": {"new": True, "old": False}}
    assert entry["default_changed"] == {"Mode": {"new": None, "old": "fast"}}


def test_build_map_records_one_sided_types_for_both_kinds():
    kinds = _synthetic_map()["kinds"]
    assert kinds["processors"]["only_new"] == ["x.OnlyNew"]
    assert kinds["processors"]["only_old"] == ["x.OnlyOld"]
    # Controller services get the identical treatment, not a second-class one.
    assert kinds["services"]["types"]["x.Svc"]["renamed"] == {
        "Schema Access Strategy": "schema-access-strategy"
    }


def test_service_reference_allowable_values_are_never_diffed():
    # NiFi reports the *ids of the service instances that existed on the harvest
    # server* as a service-reference property's allowable values. Diffing those
    # would make the map non-deterministic and invent bogus "not an allowed
    # value" warnings for correct service references.
    new = _book("2.7.2", processors={"x.P": {
        "Record Reader": dict(_prop(display="Record Reader",
                                    allowable=["uuid-a", "uuid-b"]),
                              service="o.a.n.RecordReaderFactory")}})
    old = _book("1.24.0", processors={"x.P": {
        "Record Reader": dict(_prop(display="Record Reader",
                                    allowable=["uuid-c"]),
                              service="o.a.n.RecordReaderFactory")}})
    entry = build_map(new, old)["kinds"]["processors"]["types"].get("x.P", {})
    assert "allowable_changed" not in entry


def test_committed_map_holds_no_instance_ids():
    # Guards the above against a regression in the real generated artefact.
    import re

    from niflow.version_map import PROCESSOR_DIFF, SERVICE_DIFF

    uuid_re = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}")
    for diff in (PROCESSOR_DIFF, SERVICE_DIFF):
        for entry in diff.values():
            for change in (entry.get("allowable_changed") or {}).values():
                values = change["only_new"] + change["only_old"]
                assert not [v for v in values if uuid_re.search(str(v))]


def test_summarise_counts_properties_not_types():
    counts = summarise(_synthetic_map())["processors"]
    assert counts["types_both"] == 1
    assert counts["renamed"] == 1
    assert counts["only_new"] == 1 and counts["only_old"] == 1
    assert counts["types_only_new"] == 1 and counts["types_only_old"] == 1


def test_emitted_module_is_importable_python_with_the_same_data():
    namespace: dict = {}
    exec(emit_module(_synthetic_map(), "2026-01-01"), namespace)
    assert namespace["VERSION_MAP_META"]["old_version"] == "1.24.0"
    assert namespace["PROCESSOR_DIFF"]["x.Proc"]["only_new"] == ["Pretty Print"]
    assert namespace["PROCESSOR_TYPES_ONLY_OLD"] == frozenset({"x.OnlyOld"})
    # TYPES_BOTH is what lets a caller tell "compared, identical" from "unknown".
    assert namespace["PROCESSOR_TYPES_BOTH"] == frozenset({"x.Proc"})


def test_report_names_the_versions_and_the_blind_spot():
    report = render_report(_synthetic_map(), "2026-01-01")
    assert "2.7.2" in report and "1.24.0" in report
    assert "Pretty Print" in report
    # The honesty section must survive any future edit of the renderer.
    assert "Behavioural drift" in report
    assert "cannot tell you" in report


def test_offender_ranking_puts_unsupported_properties_above_renames():
    version_map = _synthetic_map()
    version_map["kinds"]["processors"]["types"]["x.Renamey"] = {
        "renamed": {str(i): str(i) for i in range(20)}
    }
    ranked = rank_offenders(version_map, "processors", 5)
    assert ranked[0][1] == "x.Proc"  # 2 unsupported props beat 20 renames


def test_cli_refuses_the_rulebooks_in_the_wrong_order(tmp_path, monkeypatch):
    # <new> <old>, not the other way round: silently reversing them would emit a
    # map whose "only_new" bucket is really "only_old" and mislead every caller.
    import json
    import sys

    from niflow import versiondiff

    new_path = tmp_path / "new.json"
    old_path = tmp_path / "old.json"
    new_path.write_text(json.dumps(_book("2.7.2")))
    old_path.write_text(json.dumps(_book("1.24.0")))
    monkeypatch.setattr(
        sys, "argv", ["versiondiff", str(old_path), str(new_path)]
    )
    with pytest.raises(SystemExit, match="argument order"):
        versiondiff.main()


def test_cli_rejects_a_wrong_number_of_arguments(monkeypatch):
    import sys

    from niflow import versiondiff

    monkeypatch.setattr(sys, "argv", ["versiondiff", "only-one.json"])
    with pytest.raises(SystemExit, match="usage"):
        versiondiff.main()


# ------------------------------------------------- committed map + lookups


def test_committed_map_covers_both_lines_and_both_kinds():
    from niflow.compat import map_meta
    from niflow.version_map import PROCESSOR_DIFF, SERVICE_DIFF

    meta = map_meta()
    assert meta["new_version"].startswith("2.")
    assert meta["old_version"].startswith("1.")
    # Services are mapped, not just processors — that was the original gap.
    assert SERVICE_DIFF and PROCESSOR_DIFF
    assert CONVERT in PROCESSOR_DIFF and JSON_READER in SERVICE_DIFF


def test_known_2x_only_property_is_reported_unsupported_on_1x():
    from niflow.compat import unsupported_property_names

    assert unsupported_property_names(
        ATTRS_TO_JSON, {"Pretty Print": "true", "Destination": "flowfile-content"}, 1
    ) == ["Pretty Print"]


def test_properties_shared_by_both_lines_are_not_flagged():
    from niflow.compat import unsupported_property_names

    assert unsupported_property_names(
        ATTRS_TO_JSON, {"Destination": "flowfile-content"}, 1
    ) == []


def test_dynamic_properties_are_never_flagged():
    # A user-defined key belongs to the user, not to either NiFi's namespace.
    from niflow.compat import unsupported_property_names

    assert unsupported_property_names(GFF, {"my.own.attribute": "v"}, 1) == []


def test_unknown_target_version_disables_every_check():
    # The map covers 1.x vs 2.x; asked about NiFi 9 it must stay silent rather
    # than guess, so a future line never produces confident nonsense.
    from niflow.compat import component_issues, unsupported_property_names

    assert unsupported_property_names(ATTRS_TO_JSON, {"Pretty Print": "true"}, 9) == []
    assert component_issues(ATTRS_TO_JSON, {"Pretty Print": "true"}, 9) == []


def test_component_issues_explains_the_silent_failure():
    from niflow.compat import component_issues

    (message,) = component_issues(ATTRS_TO_JSON, {"Pretty Print": "true"}, 1)
    assert "does not exist on NiFi 1.24" in message
    assert "dynamic property" in message  # names the actual failure mode


def test_type_missing_on_target_is_reported_and_short_circuits():
    from niflow.compat import type_missing_on
    from niflow.version_map import PROCESSOR_TYPES_ONLY_NEW

    only_new = sorted(PROCESSOR_TYPES_ONLY_NEW)[0]
    message = type_missing_on(only_new, 1)
    assert message and "does not exist on NiFi 1.24" in message


def test_service_properties_translate_for_a_1x_target():
    # Controller services previously skipped the compat join entirely, so their
    # 2.x keys landed on 1.24 as inert dynamic properties.
    from niflow.processors.rules import properties_for_target

    translated, unsupported = properties_for_target(
        JSON_READER, {"Schema Access Strategy": "infer-schema"}, 1
    )
    assert translated == {"schema-access-strategy": "infer-schema"}
    assert unsupported == []


def test_convert_record_keys_translate_for_a_1x_target():
    from niflow.processors.rules import properties_for_target

    translated, _ = properties_for_target(CONVERT, {"Record Reader": "id-1"}, 1)
    assert translated == {"record-reader": "id-1"}


def test_allowable_value_added_in_2x_is_flagged_on_a_1x_target():
    from niflow.compat import component_issues

    messages = component_issues(S3_PUT, {"Region": "aws-global"}, 1)
    assert any("'Region'" in m and "not an allowed value" in m for m in messages)


def test_required_on_target_uses_the_targets_own_default(monkeypatch):
    # A property the old line makes mandatory is NOT a problem if the old line
    # also gives it a default — and the default that counts is the target's,
    # which may differ from the catalog's.
    import niflow.compat as compat

    entry = {"required_changed": {"Knob": {"new": False, "old": True}},
             "default_changed": {"Knob": {"new": None, "old": "on"}}}
    monkeypatch.setattr(compat, "_entry", lambda type_str: entry)
    monkeypatch.setattr(compat, "descriptors_for", lambda type_str: {})
    monkeypatch.setattr(compat, "property_names_for", lambda type_str: ["Knob"])
    assert compat.component_issues("x.P", {}, 1) == []

    entry["default_changed"] = {}
    assert any("is required on NiFi" in m
               for m in compat.component_issues("x.P", {}, 1))


def test_expression_language_values_are_never_value_checked():
    from niflow.compat import component_issues

    messages = component_issues(S3_PUT, {"Region": "${region}"}, 1)
    assert not any("not an allowed value" in m for m in messages)


# ------------------------------------------------------------ flow wiring


def _cross_version_flow() -> Flow:
    reader = ControllerService(
        name="Reader", type=JSON_READER,
        properties={"Schema Access Strategy": "infer-schema",
                    "Schema Reference Reader": "svc"},
    )
    flow = Flow(name="XVer")
    flow.add_controller_service(reader)
    flow.add(Processor(name="ToJson", type=ATTRS_TO_JSON,
                       properties={"Pretty Print": "true"},
                       auto_terminate=["success", "failure"]))
    return flow


def test_flow_issues_walk_processors_and_services():
    from niflow.compat import flow_issues

    issues = flow_issues(_cross_version_flow(), 1)
    components = {issue["component"] for issue in issues}
    assert "XVer/ToJson" in components
    assert "XVer/Reader" in components  # services are walked too


def test_validate_checks_the_baseline_with_no_flag():
    # The whole point of the baseline: 1.24 has to keep working, so a plain
    # `niflow validate` says so without being asked.
    flow = _cross_version_flow()
    plain = [i["message"] for i in flow.validate()]
    assert any("Pretty Print" in m and "does not exist on NiFi 1.24" in m for m in plain)


def test_validate_baseline_can_be_switched_off():
    flow = _cross_version_flow()
    assert not any("does not exist on NiFi" in i["message"]
                   for i in flow.validate(baseline=False))


def test_validate_with_target_version_replaces_the_baseline():
    # An explicit target is an ad-hoc question ("what breaks on THAT line?");
    # answering it and the baseline at once would double-report every issue.
    flow = _cross_version_flow()
    targeted = flow.validate("1.24")
    assert any("Pretty Print" in i["message"] for i in targeted)
    assert targeted == flow.validate()
    assert len(flow.validate("2.7.2")) < len(targeted)


def test_validate_target_version_accepts_bare_major_and_full_version():
    flow = _cross_version_flow()
    assert flow.validate("1") == flow.validate("1.24.0")


def test_clean_flow_passes_its_target_version_check():
    flow = Flow(name="Clean")
    flow.add(Processor(name="ToJson", type=ATTRS_TO_JSON,
                       properties={"Destination": "flowfile-content"},
                       auto_terminate=["success", "failure"]))
    assert not [i for i in flow.validate("1.24")
                if "does not exist on NiFi" in i["message"]]


# ------------------------------------------------------------ doctor wiring


def test_doctor_reports_a_map_that_does_not_cover_the_server(monkeypatch):
    from niflow import doctor

    monkeypatch.setattr(
        "niflow.compat.map_meta",
        lambda: {"new_version": "2.7.2", "old_version": "1.24.0",
                 "generated": "2026-01-01"},
    )
    (check,) = [c for c in doctor._cross_version_checks("3.0.0")
                if c.title == "version map"]
    assert check.status == doctor.WARN
    assert "make version-map" in check.detail


def test_doctor_confirms_a_map_that_does_cover_the_server():
    from niflow import doctor

    checks = doctor._cross_version_checks("1.24.0")
    (check,) = [c for c in checks if c.title == "version map"]
    assert check.status == doctor.OK


def test_doctor_names_the_properties_that_will_not_survive(monkeypatch):
    from niflow import doctor

    monkeypatch.setattr(
        doctor, "_scan_flows",
        lambda major, directory="flows": (
            2, [{"component": "AbcToJson/AbcJson",
                 "message": "property 'Pretty Print' does not exist on NiFi "
                            "1.24.0 — inert dynamic property"}],
        ),
    )
    (check,) = [c for c in doctor._cross_version_checks("1.24.0")
                if c.title == "flows vs this server"]
    assert check.status == doctor.WARN
    assert "AbcToJson/AbcJson" in check.detail
    assert "1 property" in check.detail


def test_doctor_flow_scan_tolerates_a_missing_directory():
    from niflow.doctor import _scan_flows

    assert _scan_flows(1, directory="no-such-directory-here") == (0, [])


# ------------------------------------------------------------ push wiring


class _FakeClient:
    """Enough of NiFiClient for _warn_cross_version: it only needs a version."""

    def __init__(self, version="1.24.0"):
        self._version = version

    def version(self):
        return self._version

    def _major_version(self):
        return int(self._version.split(".", 1)[0])


def test_push_warns_before_touching_the_server(caplog):
    from niflow.rest.flows import FlowsMixin

    client = _FakeClient()
    with caplog.at_level("WARNING", logger="niflow"):
        issues = FlowsMixin._warn_cross_version(client, _cross_version_flow())
    assert len(issues) == 2
    text = caplog.text
    assert "cross-version issue(s)" in text
    assert "Pretty Print" in text
    assert "--target-version 1.24.0" in text  # tells you how to check offline


def test_push_is_silent_when_the_flow_is_compatible(caplog):
    from niflow.rest.flows import FlowsMixin

    flow = Flow(name="Clean")
    flow.add(Processor(name="ToJson", type=ATTRS_TO_JSON,
                       properties={"Destination": "flowfile-content"},
                       auto_terminate=["success", "failure"]))
    with caplog.at_level("WARNING", logger="niflow"):
        assert FlowsMixin._warn_cross_version(_FakeClient(), flow) == []
    assert "cross-version" not in caplog.text


def test_push_warning_never_breaks_on_an_unreachable_server():
    from niflow.rest.flows import FlowsMixin

    class Unreachable(_FakeClient):
        def _major_version(self):
            raise OSError("connection refused")

    assert FlowsMixin._warn_cross_version(Unreachable(), _cross_version_flow()) == []


def test_snapshot_emission_still_omits_unsupported_keys(caplog):
    # The emitter's own drop-with-a-warning is the last line of defence; it must
    # keep firing, and now it fires for controller services too.
    import json

    from niflow.formats.json_format import to_json

    with caplog.at_level("WARNING", logger="niflow"):
        snapshot = json.loads(to_json(_cross_version_flow(), target_major=1))
    (proc,) = snapshot["flowContents"]["processors"]
    (service,) = snapshot["flowContents"]["controllerServices"]
    assert "Pretty Print" not in proc["properties"]
    assert "Schema Reference Reader" not in service["properties"]
    assert "schema-access-strategy" in service["properties"]
    assert "omitted from the snapshot" in caplog.text


# ------------------------------------------------- curated (hand-mined) renames


def test_curated_rename_is_applied_by_the_builder_before_any_guessing():
    # A hand-confirmed pair must win outright: it is checked against both
    # harvests by a human, and taking it out of the running early stops the
    # fuzzy passes mis-pairing either half with something else.
    new = {"SQL Query": _prop(display="SQL Query", description="totally reworded")}
    old = {"SQL select query": _prop(display="SQL select query",
                                     description="ancient wording")}
    renamed, only_new, only_old = detect_renames(
        new, old, curated_pairs={"SQL Query": "SQL select query"}
    )
    assert renamed == {"SQL Query": "SQL select query"}
    assert only_new == [] and only_old == []


def test_curated_rename_is_ignored_when_the_keys_already_match():
    # Same key on both lines is not a rename; a stale curated entry must not
    # invent one (that would translate a key onto itself and lose the value).
    new = {"SQL Query": _prop(display="SQL Query")}
    old = {"SQL Query": _prop(display="SQL Query")}
    renamed, _, _ = detect_renames(
        new, old, curated_pairs={"SQL Query": "SQL select query"}
    )
    assert renamed == {}


def test_curated_processor_rename_translates_on_push():
    from niflow.processors.rules import properties_for_target

    out, unsupported = properties_for_target(
        "org.apache.nifi.processors.standard.ExecuteSQL",
        {"SQL Query": "select 1"}, 1,
    )
    assert out == {"SQL select query": "select 1"}
    assert unsupported == []


def test_curated_service_rename_translates_on_push():
    # T13 found services were skipped entirely; a curated rename has to reach
    # them too, or the value lands as an inert dynamic property on the service.
    from niflow.processors.rules import properties_for_target

    out, unsupported = properties_for_target(
        "org.apache.nifi.dbcp.DBCPConnectionPool",
        {"Maximum Connection Lifetime": "-1"}, 1,
    )
    assert out == {"dbcp-max-conn-lifetime": "-1"}
    assert unsupported == []


def test_curated_renames_are_no_longer_reported_as_unsupported():
    from niflow.compat import component_issues

    assert component_issues(
        "org.apache.nifi.processors.standard.FetchFile",
        {"Permission Denied Log Level": "WARN"}, 1,
    ) == []


def test_curated_renames_are_all_in_the_generated_map():
    # The committed map and the curated table must not drift apart: a pair
    # curated after the last `make version-map` would translate at push time
    # while still being counted as unsupported by validate.
    from niflow.processors.rules import CURATED_TYPE_RENAMES, property_renames_for

    for type_str, pairs in CURATED_TYPE_RENAMES.items():
        renames = property_renames_for(type_str)
        assert renames is not None, type_str
        for new_key, old_key in pairs.items():
            assert renames.get(new_key) == old_key, (type_str, new_key)


def test_pretty_print_on_attributes_to_json_is_not_a_rename():
    # The property that started this: it is genuinely 2.x-only on
    # AttributesToJSON (1.24 has no pretty-print there at all). The similarly
    # named JsonRecordSetWriter property exists on BOTH lines under the same
    # key, so nothing should be translated in either case.
    from niflow.processors.rules import properties_for_target

    out, unsupported = properties_for_target(
        ATTRS_TO_JSON, {"Pretty Print": "true"}, 1)
    assert unsupported == ["Pretty Print"] and out["Pretty Print"] is None

    out, unsupported = properties_for_target(
        "org.apache.nifi.json.JsonRecordSetWriter", {"Pretty Print JSON": "true"}, 1)
    assert out == {"Pretty Print JSON": "true"} and unsupported == []


def test_possible_renames_are_documented_but_never_translated():
    # The "verify these" list is documentation only. Translating a pair we are
    # not sure about would write a value onto the wrong property, which is the
    # one outcome worse than not translating at all.
    from niflow.processors.rules import CURATED_TYPE_RENAMES
    from niflow.versiondiff import POSSIBLE_RENAMES

    assert POSSIBLE_RENAMES
    for row in POSSIBLE_RENAMES:
        curated = CURATED_TYPE_RENAMES.get(row["type"], {})
        assert curated.get(row["new"]) != row["old"]


# ------------------------------------------------------ compatibility baseline


def test_baseline_defaults_to_1_24(monkeypatch):
    from niflow.compat import baseline_major, baseline_version

    monkeypatch.delenv("NIFLOW_MIN_NIFI_VERSION", raising=False)
    monkeypatch.setenv("NIFLOW_CONFIG", "/nonexistent/.niflow.env")
    assert baseline_version() == "1.24"
    assert baseline_major() == 1


def test_baseline_can_be_switched_off_by_configuration(monkeypatch):
    from niflow.compat import baseline_issues, baseline_version

    monkeypatch.setenv("NIFLOW_MIN_NIFI_VERSION", "none")
    assert baseline_version() is None
    assert baseline_issues(_cross_version_flow()) == []


def test_baseline_is_read_from_the_config_file(tmp_path, monkeypatch):
    from niflow.compat import baseline_version

    path = tmp_path / ".niflow.env"
    path.write_text("NIFLOW_NIFI_HOST=https://h:8443/nifi-api\n"
                    "NIFLOW_MIN_NIFI_VERSION=1.28\n")
    monkeypatch.delenv("NIFLOW_MIN_NIFI_VERSION", raising=False)
    monkeypatch.setenv("NIFLOW_CONFIG", str(path))
    assert baseline_version() == "1.28"


def test_baseline_describes_itself_for_humans():
    from niflow.compat import describe_baseline

    assert "1.24" in describe_baseline()
    assert "none" in describe_baseline("none")
    assert "no generated map" in describe_baseline("9.9")


def test_unparseable_baseline_never_raises(monkeypatch):
    from niflow.compat import baseline_issues, baseline_major

    monkeypatch.setenv("NIFLOW_MIN_NIFI_VERSION", "banana")
    assert baseline_major() is None
    assert baseline_issues(_cross_version_flow()) == []


def test_push_warns_about_the_baseline_when_pushing_to_the_other_line(caplog):
    # Pushing 2.x-only properties to a 2.x server is legitimate, so it must not
    # be blocked — but it still has to be said out loud.
    from niflow.rest.flows import FlowsMixin

    client = _FakeClient("2.7.2")
    with caplog.at_level("WARNING", logger="niflow"):
        FlowsMixin._warn_cross_version(client, _cross_version_flow())
    assert "compatibility baseline (NiFi 1.24)" in caplog.text
    assert "would NOT work on the baseline line" in caplog.text
    assert "Pretty Print" in caplog.text


def test_push_does_not_say_it_twice_on_the_baseline_line(caplog):
    from niflow.rest.flows import FlowsMixin

    with caplog.at_level("WARNING", logger="niflow"):
        FlowsMixin._warn_cross_version(_FakeClient("1.24.0"), _cross_version_flow())
    assert "compatibility baseline (NiFi" not in caplog.text
    assert "cross-version issue(s)" in caplog.text


def test_doctor_states_the_baseline_and_names_the_flows_that_break_it(tmp_path):
    from niflow.config import NiFiConfig
    from niflow.doctor import _baseline_checks

    checks = _baseline_checks(NiFiConfig())
    assert any("1.24" in c.detail for c in checks if c.title == "compat baseline")

    off = _baseline_checks(NiFiConfig(min_nifi_version="none"))
    assert len(off) == 1 and "none" in off[0].detail


def test_doctor_flags_a_flow_directory_that_violates_the_baseline(tmp_path):
    from niflow.doctor import _scan_flows

    (tmp_path / "bad.py").write_text(
        "from niflow import Flow\n"
        "from niflow.core import Processor\n"
        "flow = Flow(name='Bad')\n"
        f"flow.add(Processor(name='ToJson', type={ATTRS_TO_JSON!r},\n"
        "                   properties={'Pretty Print': 'true'},\n"
        "                   auto_terminate=['success', 'failure']))\n"
    )
    scanned, issues = _scan_flows(1, str(tmp_path))
    assert scanned == 1
    assert any("Pretty Print" in i["message"] for i in issues)


def test_push_warns_when_a_type_has_no_1x_data_to_translate_with(caplog):
    """A custom NAR is the case the stock harvests can never cover.

    `properties_for_target` returns identity for a type `compat_v1` has never
    seen, so the properties go under their 2.x keys and 1.24 files the ones it
    doesn't recognise as inert dynamic properties — silently, which is the
    whole failure mode the cross-version work exists to stop.
    """
    from niflow.rest.flows import _warn_untranslatable_types

    flow = Flow(name="Custom")
    flow.add(Processor(name="Ours", type="com.work.nifi.SecretSauce",
                       auto_terminate=["success"]))
    with caplog.at_level("WARNING", logger="niflow"):
        blind = _warn_untranslatable_types(flow, 1, "https://work:8443/nifi-api")
    assert blind == ["com.work.nifi.SecretSauce"]
    assert "no NiFi 1.x property data" in caplog.text
    assert "make catalog-v1" in caplog.text


def test_a_2x_only_type_is_not_reported_as_untranslatable(caplog):
    """flow_issues already says the push will fail — twice is noise."""
    from niflow.rest.flows import _warn_untranslatable_types

    flow = Flow(name="TwoX")
    flow.add(Processor(name="Del", type="org.apache.nifi.processors.standard.DeleteSFTP",
                       auto_terminate=["success", "failure", "not found"]))
    with caplog.at_level("WARNING", logger="niflow"):
        assert _warn_untranslatable_types(flow, 1) == []


def test_nothing_is_said_when_pushing_to_2x():
    from niflow.rest.flows import _warn_untranslatable_types

    flow = Flow(name="Custom")
    flow.add(Processor(name="Ours", type="com.work.nifi.SecretSauce"))
    assert _warn_untranslatable_types(flow, 2) == []
