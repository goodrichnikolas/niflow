"""Unit tests for niflow.layout — connection-graph auto-placement."""
import json

from niflow import Flow, Funnel, Processor
from niflow.core import InputPort, OutputPort
from niflow.layout import X_SPACING, Y_SPACING, apply_layout, compute_layout


def _proc(name: str, **kwargs) -> Processor:
    return Processor(name=name, type="org.x.P", **kwargs)


def _chain(layout: str) -> Flow:
    flow = Flow("Chain", layout=layout)
    a, b, c = _proc("A"), _proc("B"), _proc("C")
    flow.add_processor(a, b, c)
    flow.add_connection(a >> b, b >> c)
    return flow


def test_horizontal_chain_marches_right():
    flow = _chain("horizontal")
    pos = compute_layout(flow)
    assert [pos[id(p)] for p in flow.processors] == [
        (0.0, 0.0),
        (X_SPACING, 0.0),
        (2 * X_SPACING, 0.0),
    ]


def test_vertical_chain_marches_down():
    flow = _chain("vertical")
    pos = compute_layout(flow)
    assert [pos[id(p)] for p in flow.processors] == [
        (0.0, 0.0),
        (0.0, Y_SPACING),
        (0.0, 2 * Y_SPACING),
    ]


def test_branches_fan_out_on_the_cross_axis():
    flow = Flow("Branchy")
    src, a, b, sink = _proc("Src"), _proc("A"), _proc("B"), _proc("Sink")
    flow.add_processor(src, a, b, sink)
    flow.add_connection(src >> a, src >> b, a >> sink, b >> sink)
    pos = compute_layout(flow)
    assert pos[id(a)] == (X_SPACING, 0.0)
    assert pos[id(b)] == (X_SPACING, Y_SPACING)
    # Sink ranks after the longest path through either branch.
    assert pos[id(sink)] == (2 * X_SPACING, 0.0)


def test_explicit_position_wins_and_is_not_returned():
    flow = Flow("Pinned")
    a = _proc("A")
    b = _proc("B", position=(123.0, 456.0))
    flow.add_processor(a, b)
    flow.add_connection(a >> b)
    pos = compute_layout(flow)
    assert id(b) not in pos
    assert id(a) in pos


def test_unwired_components_get_a_spare_lane():
    flow = _chain("horizontal")
    loose = _proc("Loose")
    flow.add_processor(loose)
    pos = compute_layout(flow)
    # Off the chain's lane 0, so nothing overlaps.
    assert pos[id(loose)] == (0.0, Y_SPACING)


def test_ports_and_funnels_participate():
    flow = Flow("Ported")
    inp, out = InputPort("in"), OutputPort("out")
    work, merge = _proc("Work"), Funnel()
    flow.add_port(inp, out)
    flow.add_processor(work)
    flow.add_funnel(merge)
    flow.add_connection(inp >> work, work >> merge, merge >> out)
    pos = compute_layout(flow)
    ranks = [pos[id(c)][0] / X_SPACING for c in (inp, work, merge, out)]
    assert ranks == [0, 1, 2, 3]


def test_connection_to_nested_group_port_ranks_the_group():
    flow = Flow("Nested")
    src = _proc("Src")
    flow.add_processor(src)
    with flow.process_group("Child") as child:
        inner_in = InputPort("in")
        child.add_port(inner_in)
    flow.add_connection(src >> inner_in)
    pos = compute_layout(flow)
    assert pos[id(flow.process_groups[0])] == (X_SPACING, 0.0)


def test_cycles_are_tolerated():
    flow = Flow("Loopy")
    a, b = _proc("A"), _proc("B")
    flow.add_processor(a, b)
    flow.add_connection(a >> b, b >> a)  # retry loop
    pos = compute_layout(flow)
    assert pos[id(a)] != pos[id(b)]


def test_json_emission_uses_layout():
    flow = _chain("vertical")
    snapshot = json.loads(flow.to_json())
    positions = [
        (p["position"]["x"], p["position"]["y"])
        for p in snapshot["flowContents"]["processors"]
    ]
    assert positions == [(0.0, 0.0), (0.0, Y_SPACING), (0.0, 2 * Y_SPACING)]


def test_apply_layout_materialises_positions():
    flow = _chain("horizontal")
    apply_layout(flow)
    assert all(p.position is not None for p in flow.processors)
