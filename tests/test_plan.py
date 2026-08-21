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


# --- parallel edges pair by similarity (torture-flow P1: plans "rotated"
# --- same-endpoint clones and re-applied forever) -----------------------------


def _parallel_flow(order=(0, 1, 2)) -> Flow:
    """Three connections between the same endpoints, declared in ``order``."""
    flow = Flow("P")
    route = Processor(name="Route", type="org.x.Route")
    work = Processor(name="Work", type="org.x.Work")
    flow.add(route, work)
    conns = [
        route.to(work, relationships=["hot"], name="hot"),
        route.to(work, relationships=["cold"], name="cold",
                 back_pressure_object_threshold=500),
        route.to(work, relationships=["audit"], name="audit",
                 flowfile_expiration="60 sec"),
    ]
    flow.add_connection(*(conns[i] for i in order))
    return flow


def test_parallel_edges_pair_by_similarity_not_listing_order():
    # The server lists the clones in a different order than the model declares.
    assert diff_flows(_parallel_flow((2, 0, 1)), _parallel_flow()) == []


def test_parallel_edge_update_targets_its_twin():
    live, desired = _parallel_flow((2, 0, 1)), _parallel_flow()
    desired.connections[1].back_pressure_object_threshold = 100  # the cold path
    (change,) = diff_flows(live, desired)
    assert change.op == "update" and change.kind == "connection"
    assert change.live.name == "cold"
    assert change.fields["back_pressure_object_threshold"] == (500, 100)


def test_parallel_edges_plan_apply_plan_converges():
    """Simulate an apply (copy planned values onto the paired live twins):
    the follow-up plan must be empty — no more perpetual rotation."""
    live, desired = _parallel_flow((2, 0, 1)), _parallel_flow()
    for conn in desired.connections:  # rewire every clone at once
        conn.relationships = ["retry"]
    plan = diff_flows(live, desired)
    assert plan and all(c.op == "update" for c in plan)
    for change in plan:
        for fname, (_, new) in change.fields.items():
            setattr(change.live, fname, new)
    assert diff_flows(live, desired) == []


# --- funnels match by topology (torture-flow P2: ordinal identity churned
# --- funnel connections when server order != declaration order) ---------------


def _funnel_chains(reversed_funnels: bool) -> Flow:
    """GenA -> funnel -> LogA and GenB -> funnel -> LogB; funnel list order varies."""
    flow = Flow("FN")
    gen_a, gen_b = (Processor(name=n, type="org.x.G") for n in ("GenA", "GenB"))
    log_a, log_b = (Processor(name=n, type="org.x.L") for n in ("LogA", "LogB"))
    fa, fb = Funnel(), Funnel()
    flow.add(gen_a, gen_b, log_a, log_b)
    flow.add_funnel(*((fb, fa) if reversed_funnels else (fa, fb)))
    flow.add_connection(gen_a >> fa, fa >> log_a, gen_b >> fb, fb >> log_b)
    return flow


def test_funnels_match_by_topology_not_listing_order():
    assert diff_flows(_funnel_chains(True), _funnel_chains(False)) == []


def test_funnel_remove_targets_the_topological_orphan():
    def build(with_a: bool) -> Flow:
        flow = Flow("FN")
        gen_b = Processor(name="GenB", type="org.x.G")
        log_b = Processor(name="LogB", type="org.x.L")
        fb = Funnel()
        flow.add(gen_b, log_b)
        if with_a:
            gen_a = Processor(name="GenA", type="org.x.G")
            fa = Funnel()
            flow.add(gen_a)
            flow.add_funnel(fa)  # the orphan-to-be is listed FIRST
            flow.add_connection(gen_a >> fa)
        flow.add_funnel(fb)
        flow.add_connection(gen_b >> fb, fb >> log_b)
        return flow

    live, desired = build(True), build(False)
    funnel_changes = [c for c in diff_flows(live, desired) if c.kind == "funnel"]
    assert [c.op for c in funnel_changes] == ["remove"]
    # Ordinal matching would have removed the still-wired second funnel.
    assert funnel_changes[0].live is live.funnels[0]


# --- autoTerminatedRelationships compare as a set (torture-flow P2) -----------


def test_auto_terminate_order_is_ignored():
    live, desired = _base_flow(), _base_flow()
    # Assign post-construction so the model normaliser can't tidy them first.
    live.processors[1].auto_terminate = ["success", "failure"]
    desired.processors[1].auto_terminate = ["failure", "success"]
    assert diff_flows(live, desired) == []


# --- things that must never drift forever (fuzz "cries wolf" cluster) --------
#
# `niflow drift` is meant for cron/CI: a plan against a freshly-pushed,
# unmodified flow must be empty, or a real divergence hides in the noise.


def _service_flow(**service_kwargs) -> Flow:
    flow = Flow("Svc")
    flow.add_controller_service(
        ControllerService(name="Reader", type="org.apache.nifi.json.JsonTreeReader",
                          **service_kwargs)
    )
    return flow


def test_unstated_service_enabled_is_not_drift():
    # NiFi imports every service DISABLED whatever the snapshot asked for, so
    # the model's `enabled=True` default must not plan a change forever.
    live = _service_flow(enabled=False)  # what a pulled flow looks like
    assert diff_flows(live, _service_flow()) == []


def test_explicitly_enabled_service_still_plans_when_live_is_disabled():
    live = _service_flow(enabled=False)
    changes = diff_flows(live, _service_flow(enabled=True))
    assert [c.fields["enabled"] for c in changes] == [(False, True)]


def test_deliberately_disabled_service_stays_pushable():
    live = _service_flow(enabled=True)
    changes = diff_flows(live, _service_flow(enabled=False))
    assert [c.fields["enabled"] for c in changes] == [(True, False)]


def test_service_enabled_assigned_after_construction_counts_as_stated():
    live = _service_flow(enabled=True)
    desired = _service_flow()
    desired.controller_services[0].enabled = False
    assert [c.fields["enabled"] for c in diff_flows(live, desired)] == [(True, False)]


def _one_processor(type_str: str, **kwargs) -> Flow:
    flow = Flow("P")
    flow.add_processor(Processor(name="A", type=type_str, **kwargs))
    return flow


PRIMARY_ONLY = "org.apache.nifi.processors.standard.ListFTP"
NORMAL = "org.apache.nifi.processors.attributes.UpdateAttribute"


def test_primary_node_only_type_does_not_drift_to_all():
    # NiFi forces executionNode=PRIMARY on @PrimaryNodeOnly types and refuses
    # ALL, so the model default cannot be drift.
    live = _one_processor(PRIMARY_ONLY, execution_node="PRIMARY")
    assert diff_flows(live, _one_processor(PRIMARY_ONLY)) == []
    # ...even when the flow file says ALL: that value is unreachable.
    assert diff_flows(live, _one_processor(PRIMARY_ONLY, execution_node="ALL")) == []


def test_execution_node_still_diffs_for_ordinary_types():
    live = _one_processor(NORMAL, execution_node="PRIMARY")
    changes = diff_flows(live, _one_processor(NORMAL))
    assert [c.fields["execution_node"] for c in changes] == [("PRIMARY", "ALL")]


def test_int_and_bool_property_values_match_their_server_strings():
    live = _one_processor(NORMAL, properties={"n": "10", "b": "true", "f": "1.5"})
    desired = _one_processor(NORMAL)
    # Assigned post-construction, so the model normaliser never saw them.
    desired.processors[0].properties = {"n": 10, "b": True, "f": 1.5}
    assert diff_flows(live, desired) == []


def test_a_genuinely_different_number_still_drifts():
    live = _one_processor(NORMAL, properties={"n": "10"})
    desired = _one_processor(NORMAL, properties={"n": 11})
    assert [c.fields["properties[n]"] for c in diff_flows(live, desired)] == [("10", "11")]


# --- components the server creates for itself on import ----------------------

AWS_PUT_S3 = "org.apache.nifi.processors.aws.s3.PutS3Object"
AWS_CREDS = ("org.apache.nifi.processors.aws.credentials.provider.service."
             "AWSCredentialsProviderControllerService")


def _aws_pair():
    """(live, desired) where the live side has what NiFi's 2.x import added."""
    from niflow.core import ControllerService, Flow, Processor

    desired = Flow("F")
    desired.add(Processor(name="Put", type=AWS_PUT_S3, auto_terminate=["success",
                                                                       "failure"]))
    live = Flow("F")
    creds = ControllerService(name="AWSCredentialsProviderControllerService",
                              type=AWS_CREDS)
    live.add(creds)
    live.add(Processor(name="Put", type=AWS_PUT_S3,
                       properties={"AWS Credentials Provider Service": creds},
                       auto_terminate=["success", "failure"]))
    return live, desired


def test_a_service_the_import_created_is_not_planned_for_removal():
    """NiFi 2.x makes an AWS credentials service and wires it in by itself.

    Removing it is not tidying up — it deletes the thing the processor
    requires, on every plan, forever.
    """
    live, desired = _aws_pair()
    assert diff_flows(live, desired, 2) == []


def test_writing_the_service_in_the_flow_makes_it_diffable_again():
    from niflow.core import ControllerService

    live, desired = _aws_pair()
    desired.add(ControllerService(name="AWSCredentialsProviderControllerService",
                                  type=AWS_CREDS, comments="ours now"))
    changes = diff_flows(live, desired, 2)
    assert [(c.op, c.kind) for c in changes] == [("update", "controller_service")]


def test_a_service_the_import_does_not_create_is_still_removed():
    from niflow.core import ControllerService, Flow

    live, desired = Flow("F"), Flow("F")
    live.add(ControllerService(name="Pool", type="org.apache.nifi.dbcp.DBCPConnectionPool"))
    changes = diff_flows(live, desired, 2)
    assert [(c.op, c.kind, c.name) for c in changes] == [
        ("remove", "controller_service", "Pool")]


def test_a_property_the_target_line_cannot_have_is_not_drift():
    """The emitter omits it and says so; the plan must not cry wolf forever.

    `Headers Source` is a 2.x-only PublishAMQP property. Pushed to 1.24 it is
    dropped from the snapshot with a warning (and `validate` fails on it
    against the baseline), so the live side will never have it — reporting it
    as a change on every plan buries the real ones.
    """
    from niflow.core import Flow, Processor

    amqp = "org.apache.nifi.amqp.processors.PublishAMQP"
    live = Flow("F")
    live.add(Processor(name="Pub", type=amqp, properties={"AMQP Version": "0.9.1"}))
    desired = Flow("F")
    desired.add(Processor(name="Pub", type=amqp, properties={
        "AMQP Version": "0.9.1", "Headers Source": "FLOWFILE_ATTRIBUTES"}))

    assert diff_flows(live, desired, 1) == []
    # On the line that HAS the property it is an ordinary change.
    changes = diff_flows(live, desired, 2)
    assert [c.fields for c in changes] == [
        {"properties[Headers Source]": (None, "FLOWFILE_ATTRIBUTES")}]


# --- values NiFi will not disclose -------------------------------------------

DBCP = "org.apache.nifi.dbcp.DBCPConnectionPool"


def _pool(properties):
    from niflow.core import ControllerService, Flow

    flow = Flow("F")
    flow.add(ControllerService(name="Pool", type=DBCP, properties=properties))
    return flow


def test_a_sensitive_property_is_flagged_as_not_comparable():
    """A password reads back as nothing however it was set.

    So a model that states one differs from the live flow forever. The change
    stays — sending the model's value is the only way an intended change can
    land — but an eternal "1 to change" with no explanation is how people learn
    to ignore a plan.
    """
    from niflow.plan import format_plan, only_unknowable

    live = _pool({"Database Connection URL": "jdbc:h2:mem:x"})
    desired = _pool({"Database Connection URL": "jdbc:h2:mem:x", "Password": "hunter2"})
    changes = diff_flows(live, desired, 2)

    assert len(changes) == 1
    assert changes[0].unknowable == ("properties[Password]",)
    assert only_unknowable(changes[0])
    rendered = format_plan(changes)
    assert "sensitive" in rendered and "not comparable" in rendered


def test_a_real_change_alongside_a_secret_is_still_drift():
    from niflow.plan import only_unknowable

    live = _pool({"Database Connection URL": "jdbc:h2:mem:x"})
    desired = _pool({"Database Connection URL": "jdbc:h2:mem:y", "Password": "hunter2"})
    changes = diff_flows(live, desired, 2)
    assert not only_unknowable(changes[0])


def test_a_live_value_that_came_back_is_compared_normally():
    """Only an absent or masked live value is unknowable."""
    from niflow.plan import only_unknowable

    live = _pool({"Password": "old"})       # a server that did hand it back
    desired = _pool({"Password": "new"})
    changes = diff_flows(live, desired, 2)
    assert changes and not only_unknowable(changes[0])
    assert changes[0].unknowable == ()


def test_a_masked_live_value_is_unknowable():
    from niflow.plan import only_unknowable

    live = _pool({"Password": "********"})   # what a live read returns
    desired = _pool({"Password": "hunter2"})
    changes = diff_flows(live, desired, 2)
    assert only_unknowable(changes[0])


def test_a_custom_types_secret_is_unknowable_from_the_servers_own_descriptors():
    """The catalog has never seen a custom NAR — but the snapshot has.

    Live-found on a real custom processor: every plan re-proposed the password
    and `niflow drift` failed in CI forever, because the catalogs were the only
    thing that knew which properties NiFi refuses to read back. The server says
    so itself in the snapshot's propertyDescriptors, and `from_json` now
    records them on the model.
    """
    from niflow.core import Flow, Processor
    from niflow.plan import only_unknowable

    def flow(props, sensitive=()):
        f = Flow("F")
        f.add_processor(Processor(name="Stamp", type="com.example.NoSuchType",
                                  properties=dict(props),
                                  sensitive_keys=list(sensitive)))
        return f

    live = flow({"Stamp Value": "v"}, sensitive=["Stamp Secret"])
    desired = flow({"Stamp Value": "v", "Stamp Secret": "hunter2"})
    changes = diff_flows(live, desired, 1)

    assert len(changes) == 1
    assert changes[0].unknowable == ("properties[Stamp Secret]",)
    assert only_unknowable(changes[0])

    # Without the server's word for it there is nothing to go on, and the
    # change is ordinary drift — which is the right answer, not a silent guess.
    plain = diff_flows(flow({"Stamp Value": "v"}), desired, 1)
    assert plain[0].unknowable == ()


def test_sensitive_keys_never_reach_the_wire_or_the_diff():
    """It is knowledge about the type, not part of the flow's definition."""
    from niflow.core import Processor

    proc = Processor(name="Stamp", type="com.example.NoSuchType",
                     properties={"a": "1"}, sensitive_keys=["Secret"])
    assert "sensitive_keys" not in proc.model_dump()

    other = Processor(name="Stamp", type="com.example.NoSuchType",
                      properties={"a": "1"})
    live = Flow("F")
    live.add_processor(other)
    desired = Flow("F")
    desired.add_processor(proc)
    # Differing only in sensitive_keys is not a change.
    assert diff_flows(live, desired, 2) == []
