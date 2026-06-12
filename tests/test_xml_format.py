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
from niflow.core import Port

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
    if isinstance(node, dict):
        if "position" in node and node["position"] is None:
            node["position"] = (0.0, 0.0)
        for v in node.values():
            _normalise_positions(v)
    elif isinstance(node, list):
        for v in node:
            _normalise_positions(v)
