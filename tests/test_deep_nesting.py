"""Round-trip proof for deeply nested process groups (5 levels).

The recursion in from_json/to_json/to_python is generic, but until now no
fixture went deeper than one level. Real flows nest 4-5 process groups deep,
with connections crossing every boundary through ports and processors
referencing controller services defined on an ancestor group. This locks
that shape end-to-end, offline.
"""
from __future__ import annotations

import ast

from niflow import Flow, InputPort, OutputPort
from niflow.core import ControllerService, Funnel, Label, Parameter, ParameterContext, Port, Processor
from niflow.layout import apply_layout

DUMP_EXCLUDE = {"nifi_id", "nifi_entity"}


def _deep_flow() -> Flow:
    """Flow -> Stage -> Substage -> Detail -> Leaf, wired through ports.

    A controller service declared at level 2 (Stage) is referenced by a
    processor at level 4 (Detail) — ancestor-scoped, the way shared readers/
    writers are used in real trees.
    """
    flow = Flow("DeepFlow")
    src = Processor(
        name="Generate",
        type="org.apache.nifi.processors.standard.GenerateFlowFile",
        auto_terminate=[],
    )
    sink = Processor(
        name="Log",
        type="org.apache.nifi.processors.standard.LogAttribute",
        auto_terminate=["success"],
    )
    flow.add(src, sink, Label("main path", position=(5.0, 5.0)))

    with flow.process_group("Stage", comment="level 2") as stage:
        reader = ControllerService(
            name="SharedReader",
            type="org.apache.nifi.json.JsonTreeReader",
        )
        stage_in, stage_out = InputPort("in"), OutputPort("out")
        tag = Processor(
            name="TagStage",
            type="org.apache.nifi.processors.attributes.UpdateAttribute",
            properties={"stage": "true"},
        )
        stage.add(reader, stage_in, stage_out, tag)
        stage.add_connection(stage_in >> tag)

        with stage.process_group("Substage", layout="vertical") as substage:
            substage.variables = {"env": "dev"}
            sub_in, sub_out = InputPort("in"), OutputPort("out")
            funnel = Funnel()
            substage.add(sub_in, sub_out, funnel)
            substage.add_connection(sub_in >> funnel)

            with substage.process_group("Detail") as detail:
                detail.parameter_context = ParameterContext(
                    "detail-params", parameters=[Parameter("batch.size", value="100")]
                )
                det_in, det_out = InputPort("in"), OutputPort("out")
                convert = Processor(
                    name="Convert",
                    type="org.apache.nifi.processors.standard.ConvertRecord",
                    # Service instance declared two levels up.
                    properties={"Record Reader": reader, "Batch Size": "#{batch.size}"},
                    auto_terminate=["failure"],
                )
                detail.add(det_in, det_out, convert)
                detail.add_connection(det_in >> convert)
                detail.add_connection(convert.to(det_out, relationships=["success"]))

                with detail.process_group("Leaf") as leaf:
                    leaf_in, leaf_out = InputPort("in"), OutputPort("out")
                    stamp = Processor(
                        name="Stamp",
                        type="org.apache.nifi.processors.attributes.UpdateAttribute",
                        properties={"leaf": "true"},
                    )
                    leaf.add(leaf_in, leaf_out, stamp)
                    leaf.add_connection(leaf_in >> stamp)
                    leaf.add_connection(stamp >> leaf_out)

                # Parallel path: Detail's input also fans into Leaf, and
                # Leaf's output merges back into Detail's out port.
                detail.add_connection(det_in >> leaf_in)
                detail.add_connection(leaf_out >> det_out)

            substage.add_connection(funnel >> det_in)
            substage.add_connection(det_out >> sub_out)

        stage.add_connection(tag >> sub_in)
        stage.add_connection(sub_out >> stage_out)

    flow.add_connection(src >> stage_in)
    flow.add_connection(stage_out >> sink)
    return flow


def _normalize_port_rels(group) -> None:
    for c in group.connections:
        if isinstance(c.source, (Port, Funnel)):
            c.relationships = []
    for child in group.process_groups:
        _normalize_port_rels(child)


def _normalise_positions(node) -> None:
    if isinstance(node, dict):
        if "position" in node and node["position"] is None:
            node["position"] = (0.0, 0.0)
        for v in node.values():
            _normalise_positions(v)
    elif isinstance(node, list):
        for v in node:
            _normalise_positions(v)


def _dump(flow: Flow) -> dict:
    dump = apply_layout(flow).model_dump(exclude=DUMP_EXCLUDE)
    _normalise_positions(dump)
    return dump


def _level(flow: Flow, *names: str):
    group = flow
    for name in names:
        group = next(g for g in group.process_groups if g.name == name)
    return group


def test_deep_flow_is_five_levels():
    flow = _deep_flow()
    leaf = _level(flow, "Stage", "Substage", "Detail", "Leaf")
    assert [p.name for p in leaf.processors] == ["Stamp"]


def test_deep_json_round_trip():
    """5-level tree -> JSON -> Flow: identical model, wiring intact.

    ``layout`` is excluded: it's a niflow authoring hint the JSON edge
    *consumes* (materialised into component positions), not NiFi state —
    the positions themselves round-trip.
    """
    original = _deep_flow()
    parsed = Flow.from_json(original.to_json())
    _normalize_port_rels(original)
    _normalize_port_rels(parsed)
    d1, d2 = _dump(original), _dump(parsed)
    _strip_key(d1, "layout")
    _strip_key(d2, "layout")
    assert d1 == d2


def _strip_key(node, key: str) -> None:
    if isinstance(node, dict):
        node.pop(key, None)
        for v in node.values():
            _strip_key(v, key)
    elif isinstance(node, list):
        for v in node:
            _strip_key(v, key)


def test_deep_json_is_byte_stable():
    flow = _deep_flow()
    assert flow.to_json() == flow.to_json()


def test_ancestor_service_ref_survives_round_trip():
    """A level-4 processor referencing a level-2 service resolves back to the
    parsed service *instance*, not a dangling id string."""
    parsed = Flow.from_json(_deep_flow().to_json())
    stage = _level(parsed, "Stage")
    detail = _level(parsed, "Stage", "Substage", "Detail")
    convert = next(p for p in detail.processors if p.name == "Convert")
    ref = convert.properties["Record Reader"]
    assert isinstance(ref, ControllerService)
    assert ref is stage.controller_services[0]


def test_deep_python_round_trip_idempotent():
    """to_python at depth 5 execs back to a Flow that emits identical source."""
    flow = _deep_flow()
    src1 = flow.to_python()
    ast.parse(src1)
    ns: dict = {}
    exec(compile(src1, "<deep>", "exec"), ns)
    rebuilt = ns["flow"]
    assert isinstance(rebuilt, Flow)
    assert rebuilt.to_python() == src1


def test_deep_python_preserves_cross_boundary_wiring():
    """Every boundary-crossing connection survives Python emission + exec."""
    flow = _deep_flow()
    ns: dict = {}
    exec(compile(flow.to_python(), "<deep>", "exec"), ns)
    rebuilt = ns["flow"]
    _normalize_port_rels(flow)
    _normalize_port_rels(rebuilt)
    assert _dump(flow) == _dump(rebuilt)
