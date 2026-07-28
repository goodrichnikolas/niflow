"""Unit tests for the semantic diff/plan engine (niflow/plan.py)."""
from __future__ import annotations

import copy

from niflow import Flow, InputPort, OutputPort
from niflow.core import ControllerService, Funnel, Parameter, ParameterContext, Processor
from niflow.plan import diff_flows, format_plan


def _base_flow() -> Flow:
    flow = Flow("Base")
    gen = Processor(
        name="Gen",
        type="org.apache.nifi.processors.standard.GenerateFlowFile",
        properties={"File Size": "0B"},
    )
    log = Processor(
        name="Log",
        type="org.apache.nifi.processors.standard.LogAttribute",
        auto_terminate=["success"],
    )
    flow.add(gen, log)
    flow.add_connection(gen >> log)

    with flow.process_group("Child") as child:
        inp, outp = InputPort("in"), OutputPort("out")
        tag = Processor(
            name="Tag",
            type="org.apache.nifi.processors.attributes.UpdateAttribute",
            properties={"k": "v"},
        )
        child.add(inp, outp, tag)
        child.add_connection(inp >> tag)
        child.add_connection(tag >> outp)
    return flow


def test_identical_flows_produce_empty_plan():
    assert diff_flows(_base_flow(), _base_flow()) == []


def test_position_changes_are_ignored():
    live, desired = _base_flow(), _base_flow()
    live.processors[0].position = (100.0, 200.0)
    desired.processors[0].position = (999.0, 999.0)
    assert diff_flows(live, desired) == []


def test_property_update_detected():
    live, desired = _base_flow(), _base_flow()
    desired.processors[0].properties["File Size"] = "1KB"
    desired.processors[0].scheduling_period = "10 sec"
    (change,) = diff_flows(live, desired)
    assert change.op == "update" and change.kind == "processor"
    assert change.name == "Gen" and change.path == ()
    assert change.fields["properties[File Size]"] == ("0B", "1KB")
    assert change.fields["scheduling_period"] == ("0 sec", "10 sec")


def test_nested_property_update_carries_path():
    live, desired = _base_flow(), _base_flow()
    desired.process_groups[0].processors[0].properties["k"] = "v2"
    (change,) = diff_flows(live, desired)
    assert change.path == ("Child",) and change.name == "Tag"


def test_add_and_remove_processor():
    live, desired = _base_flow(), _base_flow()
    extra = Processor(name="Extra", type="org.x.Y")
    desired.add_processor(extra)
    del desired.processors[1]  # drop Log (and its connection)
    del desired.connections[0]
    ops = {(c.op, c.kind, c.name) for c in diff_flows(live, desired)}
    assert ("add", "processor", "Extra") in ops
    assert ("remove", "processor", "Log") in ops
    assert ("remove", "connection", "Gen -[success]-> Log") in ops


def test_connection_relationship_change_is_update():
    live, desired = _base_flow(), _base_flow()
    desired.connections[0].relationships = ["success", "failure"]
    (change,) = diff_flows(live, desired)
    assert change.op == "update" and change.kind == "connection"
    assert change.fields["relationships"] == (["success"], ["success", "failure"])


def test_connection_queue_setting_change_is_update():
    live, desired = _base_flow(), _base_flow()
    desired.connections[0].back_pressure_object_threshold = 500
    (change,) = diff_flows(live, desired)
    assert change.fields["back_pressure_object_threshold"] == (10000, 500)


def test_service_ref_compares_by_service_name():
    def with_service() -> Flow:
        flow = Flow("Svc")
        reader = ControllerService(name="Reader", type="org.x.JsonTreeReader")
        proc = Processor(name="P", type="org.x.Convert", properties={"Record Reader": reader})
        flow.add(reader, proc)
        return flow

    # Distinct instances on each side must still compare equal by name.
    assert diff_flows(with_service(), with_service()) == []

    changed = with_service()
    changed.controller_services[0].name = "Reader2"
    changed.processors[0].properties["Record Reader"] = changed.controller_services[0]
    plan = diff_flows(with_service(), changed)
    kinds = {(c.op, c.kind) for c in plan}
    assert ("update", "processor") in kinds  # ref points at a different service


def test_group_add_and_remove_are_whole_subtree():
    live, desired = _base_flow(), _base_flow()
    with desired.process_group("NewGroup") as new_group:
        new_group.add_processor(Processor(name="Inner", type="org.x.Z"))
    del live.process_groups[0]
    ops = {(c.op, c.kind, c.name) for c in diff_flows(live, desired)}
    assert ("add", "process_group", "NewGroup") in ops
    assert ("add", "process_group", "Child") in ops


def test_group_settings_diff():
    live, desired = _base_flow(), _base_flow()
    desired.variables = {"env": "prod"}
    desired.parameter_context = ParameterContext(
        "params", parameters=[Parameter("a", value="1")]
    )
    (change,) = diff_flows(live, desired)
    assert change.kind == "group_settings"
    assert change.fields["variables"] == ({}, {"env": "prod"})
    assert change.fields["parameter_context"] == (None, "params")


def test_funnel_ordinal_matching():
    live, desired = _base_flow(), _base_flow()
    live.add_funnel(Funnel())
    desired.add_funnel(Funnel(), Funnel())
    plan = diff_flows(live, desired)
    assert [(c.op, c.name) for c in plan] == [("add", "funnel[1]")]


def test_ports_in_different_children_do_not_collide():
    def two_children() -> Flow:
        flow = Flow("Two")
        gen = Processor(name="Gen", type="org.x.G")
        flow.add_processor(gen)
        outs = []
        for name in ("A", "B"):
            with flow.process_group(name) as child:
                inp = InputPort("in")
                child.add_port(inp)
                outs.append(inp)
            flow.add_connection(gen >> outs[-1])
        return flow

    assert diff_flows(two_children(), two_children()) == []


def test_format_plan_renders_summary():
    live, desired = _base_flow(), _base_flow()
    desired.processors[0].properties["File Size"] = "1KB"
    text = format_plan(diff_flows(live, desired))
    assert "~ processor .: Gen" in text
    assert "properties[File Size]: '0B' -> '1KB'" in text
    assert "Plan: 0 to add, 1 to change, 0 to remove." in text
    assert "No changes" in format_plan([])


def test_rename_detected_for_same_type_processor():
    live, desired = _base_flow(), _base_flow()
    desired.processors[0].name = "Generate"  # Gen -> Generate, same type
    plan = diff_flows(live, desired)
    add = next(c for c in plan if c.op == "add" and c.kind == "processor")
    remove = next(c for c in plan if c.op == "remove" and c.kind == "processor")
    assert add.name == "Generate" and "RENAME" in add.note and "'Gen'" in add.note
    assert remove.name == "Gen" and "rename" in remove.note
    text = format_plan(plan)
    assert "probable rename" in text and "! looks like a RENAME" in text


def test_no_rename_flag_for_different_types():
    live, desired = _base_flow(), _base_flow()
    extra = Processor(name="Extra", type="org.x.CompletelyDifferent")
    desired.add_processor(extra)
    del desired.processors[1]
    del desired.connections[0]
    plan = diff_flows(live, desired)
    assert all(c.note is None for c in plan)
    assert "probable rename" not in format_plan(plan)


def test_rename_detected_for_child_group():
    live, desired = _base_flow(), _base_flow()
    desired.process_groups[0].name = "Child2"
    # keep connections consistent: base flow has none crossing into Child
    plan = diff_flows(live, desired)
    add = next(c for c in plan if c.op == "add" and c.kind == "process_group")
    assert add.note and "destroyed" in add.note


def test_port_rename_flagged_only_when_unambiguous():
    live, desired = _base_flow(), _base_flow()
    child = desired.process_groups[0]
    child.input_ports[0].name = "input"
    plan = diff_flows(live, desired)
    add = next(c for c in plan if c.op == "add" and c.kind == "input_port")
    assert add.note and "RENAME" in add.note
