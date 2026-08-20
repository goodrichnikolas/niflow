"""Round-trip tests for niflow.formats.xml_format.

Mirror of test_json_format.py for the NiFi 1.x template-XML format. The wiki
fixture only exercises processors + connections, so a synthetic flow covers
controller services, ports, nested groups, and service-ref properties.
"""
from __future__ import annotations

import ast
from pathlib import Path

from niflow import (
    ControllerService,
    Flow,
    InputPort,
    OutputPort,
    Processor,
)
from niflow.core import Funnel, Label, Parameter, ParameterContext, Port
from niflow.formats.xml_format import template_limitations
from niflow.plan import diff_flows

FIXTURE = Path(__file__).parent / "fixtures" / "simple_httpget_route.template.xml"


def _normalize_port_rels(group):
    """Same normalisation as the JSON test: port-source connections drop relationships."""
    for c in group.connections:
        if isinstance(c.source, Port):
            c.relationships = []
    for child in group.process_groups:
        _normalize_port_rels(child)


def _build_synthetic_flow() -> Flow:
    """Cover the model surface the wiki XML fixture doesn't: services, ports,
    nested groups, service-ref properties, multi-relationship connections."""
    flow = Flow("Synthetic")

    reader = ControllerService(
        name="Reader",
        type="org.apache.nifi.json.JsonTreeReader",
    )
    flow.add_controller_service(reader)

    fetch = Processor(
        name="Fetch",
        type="org.apache.nifi.processors.standard.GetFile",
        properties={"Input Directory": "/in"},
    )
    convert = Processor(
        name="Convert",
        type="org.apache.nifi.processors.standard.ConvertRecord",
        properties={"Record Reader": reader},  # service-ref
    )
    flow.add_processor(fetch, convert)
    flow.add_connection(fetch.to(convert, relationships=["success", "failure"]))

    with flow.process_group("Inner") as inner:
        inp = InputPort("in")
        outp = OutputPort("out")
        tag = Processor(
            name="Tag",
            type="org.apache.nifi.processors.attributes.UpdateAttribute",
            properties={"x": "1"},
        )
        inner.add_port(inp, outp)
        inner.add_processor(tag)
        inner.add_connection(inp >> tag)
        inner.add_connection(tag >> outp)

    flow.add_connection(convert >> inp)
    flow.add_connection(outp >> fetch)
    return flow


# --- from_xml ---------------------------------------------------------------
def test_from_xml_parses_fixture_shape():
    flow = Flow.from_xml(FIXTURE)
    assert flow.name == "Simple Web Pull, Extract, Route, Store"
    assert len(flow.processors) == 4
    assert len(flow.connections) == 3
    # Types we expect to see somewhere in the four processors.
    types = {p.type.rsplit(".", 1)[-1] for p in flow.processors}
    # The wiki template uses InvokeHTTP + ExtractText + RouteOnAttribute + PutFile.
    assert "PutFile" in types


# --- to_xml round-trip ------------------------------------------------------
def test_to_xml_is_byte_stable():
    flow = Flow.from_xml(FIXTURE)
    assert flow.to_xml() == flow.to_xml()


def test_xml_round_trip_preserves_fixture_structure():
    f1 = Flow.from_xml(FIXTURE)
    f2 = Flow.from_xml(f1.to_xml())
    d1 = f1.model_dump(exclude={"nifi_id", "nifi_entity"})
    d2 = f2.model_dump(exclude={"nifi_id", "nifi_entity"})
    assert d1 == d2


def test_xml_round_trip_handles_services_ports_groups():
    """The fixture has none of these; build a synthetic flow that does."""
    original = _build_synthetic_flow()
    parsed = Flow.from_xml(original.to_xml())

    _normalize_port_rels(original)
    _normalize_port_rels(parsed)

    d_orig = original.model_dump(exclude={"nifi_id", "nifi_entity"})
    d_back = parsed.model_dump(exclude={"nifi_id", "nifi_entity"})

    # Positions default to (0,0) on re-emit; harmonise.
    _normalise_positions(d_orig)
    _normalise_positions(d_back)

    assert d_orig == d_back


# --- parity with the JSON emitter -------------------------------------------
# to_xml is what the NiFi 1.x in-place (version-controlled) push uploads, so
# anything it drops is silently lost from a live registry-versioned group.
def _tuned_flow() -> Flow:
    """A flow whose every field is deliberately off the model default."""
    flow = Flow("Tuned")
    flow.comment = "top-level comment"
    flow.variables = {"rootvar": "rootval"}
    flow.parameter_context = ParameterContext(name="Ctx", parameters=[Parameter("p", "v")])

    source = Processor(
        name="Source",
        type="org.apache.nifi.processors.standard.GenerateFlowFile",
        properties={"Custom Text": "hello #{p}"},
        scheduling_strategy="CRON_DRIVEN",
        scheduling_period="0 0/5 * * * ?",
        concurrent_tasks=4,
        execution_node="PRIMARY",
        scheduled_state="DISABLED",
        comments="a comment",
        penalty_duration="90 sec",
        yield_duration="5 sec",
        bulletin_level="ERROR",
        run_duration_millis=25,
        retry_count=2,
        retried_relationships=["failure"],
        backoff_mechanism="YIELD_PROCESSOR",
        max_backoff_period="2 mins",
        auto_terminate=["failure"],
    )
    funnel = Funnel()
    flow.add_processor(source)
    flow.add_funnel(funnel)
    flow.add_label(Label("a note", width=360.0, height=80.0))
    flow.add_connection(
        source.to(
            funnel,
            name="tuned",
            back_pressure_object_threshold=0,
            back_pressure_data_size_threshold="10 TB",
            flowfile_expiration="5 min",
            prioritizers=["org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer"],
            load_balance_strategy="PARTITION_BY_ATTRIBUTE",
            partitioning_attribute="code",
            load_balance_compression="COMPRESS_ATTRIBUTES_ONLY",
        )
    )
    with flow.process_group("Child") as child:
        child.variables = {"childvar": "childval"}
        inp = InputPort("in")
        child.add_port(inp)
        child.add_processor(Processor(name="Work", type="org.x.Work", auto_terminate=["success"]))
        child.add_connection(inp >> child.processors[0])
    flow.add_connection(funnel >> flow.process_groups[0].input_ports[0])
    return flow


def test_to_xml_emits_funnels_and_labels():
    """Both were missing entirely; a connection to a funnel raised KeyError."""
    xml = _tuned_flow().to_xml()
    assert "<funnels>" in xml
    assert "<labels>" in xml
    assert "<type>FUNNEL</type>" in xml  # connection endpoint resolves


def test_xml_round_trip_keeps_queue_and_processor_state():
    original = _tuned_flow()
    parsed = Flow.from_xml(original.to_xml())

    proc = parsed.processors[0]
    assert proc.scheduled_state == "DISABLED"
    assert (proc.execution_node, proc.concurrent_tasks) == ("PRIMARY", 4)
    assert (proc.bulletin_level, proc.comments) == ("ERROR", "a comment")
    assert (proc.penalty_duration, proc.yield_duration) == ("90 sec", "5 sec")
    assert proc.run_duration_millis == 25
    assert (proc.retry_count, proc.retried_relationships) == (2, ["failure"])
    assert (proc.backoff_mechanism, proc.max_backoff_period) == ("YIELD_PROCESSOR", "2 mins")

    conn = parsed.connections[0]
    assert conn.back_pressure_object_threshold == 0
    assert conn.back_pressure_data_size_threshold == "10 TB"
    assert conn.flowfile_expiration == "5 min"
    assert conn.prioritizers == ["org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer"]
    assert conn.load_balance_strategy == "PARTITION_BY_ATTRIBUTE"
    assert conn.partitioning_attribute == "code"

    assert len(parsed.funnels) == 1
    assert (parsed.labels[0].text, parsed.labels[0].width) == ("a note", 360.0)
    # Nested groups travel as ProcessGroupDTOs, which do carry variables.
    assert parsed.process_groups[0].variables == {"childvar": "childval"}


def test_xml_round_trip_converges_to_an_empty_plan():
    """The push contract: what comes back out must re-plan to nothing."""
    original = _tuned_flow()
    parsed = Flow.from_xml(original.to_xml())
    changes = diff_flows(parsed, original)
    # Only the state a template genuinely cannot carry may differ, and every
    # item of it is reported by template_limitations().
    assert {c.kind for c in changes} <= {"group_settings"}
    assert all(set(c.fields) <= {"variables", "parameter_context"} for c in changes)


def test_cross_group_connection_endpoints_carry_their_own_group():
    """A funnel -> child-port connection must name the *child* as the port's
    group; using the connection's own group made NiFi reject the endpoint."""
    flow = _tuned_flow()
    parsed = Flow.from_xml(flow.to_xml())
    # The endpoint resolved to the child group's port, not a dangling id.
    cross = [c for c in parsed.connections if isinstance(c.target, InputPort)]
    assert len(cross) == 1
    assert cross[0].target is parsed.process_groups[0].input_ports[0]


def test_template_limitations_names_everything_that_cannot_cross():
    limits = template_limitations(_tuned_flow())
    messages = " | ".join(item["message"] for item in limits)
    assert "parameter-context binding" in messages
    assert "variables" in messages
    assert "group comment" in messages
    assert "load-balance compression" in messages
    # NiFi 1.x escapes '#{' while instantiating a snippet, which silently
    # unwires a parameter reference; the push repairs it afterwards.
    assert "parameter references" in messages
    assert all(item["repair"] for item in limits)


def test_template_limitations_is_empty_for_a_plain_flow():
    flow = Flow("Plain")
    flow.add_processor(Processor(name="P", type="org.x.P", auto_terminate=["success"]))
    assert template_limitations(flow) == []


# --- triangle: XML -> Python -> XML -----------------------------------------
def test_xml_to_python_round_trip():
    """XML -> Flow -> to_python -> exec -> to_xml equals the direct to_xml."""
    flow = Flow.from_xml(FIXTURE)
    src = flow.to_python()
    ast.parse(src)

    ns: dict = {}
    exec(compile(src, "<test>", "exec"), ns)
    f2 = ns["flow"]
    assert isinstance(f2, Flow)
    assert f2.name == flow.name
    assert len(f2.processors) == len(flow.processors)


def _normalise_positions(node):
    """Blank every position: to_xml auto-lays-out components that have none
    (like to_json), so a model built without coordinates comes back with them."""
    if isinstance(node, dict):
        if "position" in node:
            node["position"] = None
        for v in node.values():
            _normalise_positions(v)
    elif isinstance(node, list):
        for v in node:
            _normalise_positions(v)
