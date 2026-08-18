"""Unit tests for niflow.layout — connection-graph auto-placement."""
import json

from niflow import Flow, Funnel, Processor
from niflow.core import InputPort, OutputPort
from niflow.layout import (
    V_SPACING,
    X_SPACING,
    Y_SPACING,
    apply_layout,
    compute_layout,
    place,
)


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
        (0.0, V_SPACING),
        (0.0, 2 * V_SPACING),
    ]


def test_branches_fan_out_on_the_cross_axis():
    flow = Flow("Branchy")
    src, a, b, sink = _proc("Src"), _proc("A"), _proc("B"), _proc("Sink")
    flow.add_processor(src, a, b, sink)
    flow.add_connection(src >> a, src >> b, a >> sink, b >> sink)
    pos = compute_layout(flow)
    assert pos[id(a)] == (X_SPACING, 0.0)
    assert pos[id(b)] == (X_SPACING, Y_SPACING)
    # Sink ranks after the longest path through either branch and is
    # centered between the two branches that feed it.
    assert pos[id(sink)] == (2 * X_SPACING, Y_SPACING / 2)


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
    assert positions == [(0.0, 0.0), (0.0, V_SPACING), (0.0, 2 * V_SPACING)]


def test_apply_layout_materialises_positions():
    flow = _chain("horizontal")
    apply_layout(flow)
    assert all(p.position is not None for p in flow.processors)


# --- place(): the generic core niflow tidy / the webgui button run live ------


def test_place_barycenter_untangles_crossed_branches():
    # Two parallel two-stage chains, second-stage nodes listed in the order
    # that would cross: barycenter puts each next to what feeds it.
    pos = place(
        ["a", "b", "y", "x"],
        [("a", "x"), ("b", "y")],
    )
    assert pos["x"][1] == pos["a"][1]
    assert pos["y"][1] == pos["b"][1]


def test_place_ignores_foreign_endpoints_and_self_loops():
    pos = place(["a", "b"], [("a", "b"), ("a", "a"), ("ghost", "b")])
    assert pos["a"] == (0.0, 0.0)
    assert pos["b"] == (X_SPACING, 0.0)


def test_place_positions_are_unique_and_cover_everything():
    nodes = ["src", "a", "b", "sink", "island"]
    edges = [("src", "a"), ("src", "b"), ("a", "sink"), ("b", "sink")]
    pos = place(nodes, edges, "vertical", loose=["note"])
    assert set(pos) == {*nodes, "note"}
    assert len(set(pos.values())) == len(pos)  # nothing overlaps


def test_place_sizes_stretch_the_rank():
    # A double-wide node pushes the next horizontal rank past its real edge
    # (700 wide + the 248px label gap), not to a fixed 600px grid step.
    pos = place(["big", "b"], [("big", "b")], sizes={"big": (700.0, 128.0)})
    assert pos["b"][0] == 700.0 + (X_SPACING - 352.0)


def test_place_sizes_stretch_the_lane():
    # Two parallel sources, the first triple-height: the second starts below
    # its real bottom edge plus the lane gap, so tall nodes can't be overlapped.
    pos = place(
        ["tall", "b", "sink"],
        [("tall", "sink"), ("b", "sink")],
        sizes={"tall": (352.0, 400.0)},
    )
    assert pos["b"][1] == 400.0 + (Y_SPACING - 128.0)


def test_place_keeps_a_chain_dead_straight_through_odd_sizes():
    # A funnel-sized node mid-chain stays centered on the line, and the next
    # processor returns to the chain's axis instead of drifting to lane 0.
    sizes = {"mid": (48.0, 48.0)}
    pos = place(["a", "mid", "z"], [("a", "mid"), ("mid", "z")], sizes=sizes)
    a_center = pos["a"][1] + 128.0 / 2
    assert pos["mid"][1] + 48.0 / 2 == a_center
    assert pos["z"][1] + 128.0 / 2 == a_center
