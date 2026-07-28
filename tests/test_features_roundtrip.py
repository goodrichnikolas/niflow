"""Round-trip tests for parameter contexts, funnels, labels, and fidelity fields.

Each test builds a Flow in code, pushes it through a format (JSON or generated
Python source), parses it back, and asserts the model dumps match — the same
contract the original round-trip tests enforce for the base feature set.
"""
import pytest

from niflow import (
    Bundle,
    Flow,
    Funnel,
    Label,
    Parameter,
    ParameterContext,
    Processor,
)
from niflow.formats import from_json, to_json, to_python
from niflow.layout import apply_layout

DUMP_EXCLUDE = {"nifi_id", "nifi_entity"}


def _rich_flow() -> Flow:
    """A flow exercising every new feature at once."""
    ctx = ParameterContext(
        "etl-params",
        description="shared etl settings",
        parameters=[
            Parameter("db.url", value="jdbc:postgresql://db/x"),
            Parameter("db.password", sensitive=True, description="set via secrets"),
        ],
        inherited_contexts=["global-params"],
    )
    flow = Flow("Rich Flow")
    flow.parameter_context = ctx
    flow.variables = {"env": "dev", "region": "us-east-1"}

    src = Processor(
        name="Fetch",
        type="com.corp.processors.FetchThing",
        properties={"URL": "#{db.url}"},
        scheduled_state="DISABLED",
        penalty_duration="1 min",
        yield_duration="5 sec",
        bulletin_level="ERROR",
        run_duration_millis=50,
        execution_node="PRIMARY",
        comments="pulls from corp api",
        retry_count=3,
        retried_relationships=["failure"],
        backoff_mechanism="YIELD_PROCESSOR",
        max_backoff_period="1 min",
        bundle=Bundle(group="com.corp", artifact="corp-nar", version="3.1.0"),
    )
    sink = Processor(name="Store", type="org.apache.nifi.processors.standard.PutFile")
    merge = Funnel(position=(400.0, 100.0))
    note = Label("data path", position=(10.0, 10.0), width=300.0, height=80.0)

    flow.add(src, sink, merge, note)
    flow.add_connection(
        src.to(
            merge,
            back_pressure_object_threshold=500,
            back_pressure_data_size_threshold="100 MB",
            flowfile_expiration="1 hour",
            prioritizers=["org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer"],
        )
    )
    flow.add_connection(merge >> sink)

    with flow.process_group("Child", comment="nested work") as child:
        inner = Processor(name="Inner", type="org.x.C")
        child.add_processor(inner)
        child.parameter_context = ctx  # shared instance with the parent

    return flow


def _dump(flow: Flow) -> dict:
    """Model dump with positions normalised.

    Serialisation materialises auto-layout coordinates for components without
    a position, so resolve them on the model first; the group itself still
    defaults to NiFi's (0,0)."""
    dump = apply_layout(flow).model_dump(exclude=DUMP_EXCLUDE)

    def normalise(node):
        if isinstance(node, dict):
            if node.get("position") is None and "position" in node:
                node["position"] = (0.0, 0.0)
            for value in node.values():
                normalise(value)
        elif isinstance(node, list):
            for item in node:
                normalise(item)

    normalise(dump)
    return dump


def test_json_round_trip_preserves_everything():
    flow = _rich_flow()
    back = from_json(to_json(flow))
    assert _dump(back) == _dump(flow)


def test_json_emission_is_deterministic_and_stable():
    flow = _rich_flow()
    once = to_json(flow)
    assert to_json(from_json(once)) == once


def test_sensitive_values_never_serialise():
    flow = _rich_flow()
    flow.parameter_context.parameters[1].value = "SUPER-SECRET"
    assert "SUPER-SECRET" not in to_json(flow)
    assert "SUPER-SECRET" not in to_python(flow)


def test_python_round_trip_preserves_everything():
    flow = _rich_flow()
    source = to_python(flow)
    namespace = {"__name__": "_test_roundtrip"}
    exec(compile(source, "<generated>", "exec"), namespace)
    back = namespace["flow"]
    assert _dump(back) == _dump(flow)


def test_shared_context_stays_shared_through_json():
    back = from_json(to_json(_rich_flow()))
    assert back.parameter_context is back.process_groups[0].parameter_context


def test_shared_context_stays_shared_through_python():
    source = to_python(_rich_flow())
    namespace = {"__name__": "_test_shared"}
    exec(compile(source, "<generated>", "exec"), namespace)
    back = namespace["flow"]
    assert back.parameter_context is back.process_groups[0].parameter_context


def test_funnel_connections_round_trip_through_json():
    back = from_json(to_json(_rich_flow()))
    funnel_conns = [c for c in back.connections if isinstance(c.source, Funnel)]
    assert len(funnel_conns) == 1
    assert funnel_conns[0].relationships == []
    assert funnel_conns[0].source is back.funnels[0]


def test_port_and_funnel_sources_normalise_relationships():
    f = Funnel()
    p = Processor(name="p", type="t")
    conn = f >> p
    assert conn.relationships == []


def test_standard_bundle_collapses_to_none():
    flow = Flow("b")
    flow.add_processor(Processor(name="p", type="org.apache.nifi.processors.standard.GetFile"))
    back = from_json(to_json(flow))
    assert back.processors[0].bundle is None
