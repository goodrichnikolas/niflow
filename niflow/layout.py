"""Automatic canvas layout: place components along the connection graph.

Any component with an explicit ``position`` keeps it. Everything else is
ranked by its longest path from the graph's sources, then walked rank by rank
along the group's ``layout`` axis — ``"horizontal"`` ranks march right,
``"vertical"`` ranks march down. Parallel branches that share a rank fan out
on the cross axis. Components that aren't wired into any connection (plus
labels) are spread in a spare lane past the connected ones so nothing stacks
at the canvas origin.
"""
from __future__ import annotations

from typing import Dict, Hashable, Iterable, List, Optional, Sequence, Tuple

from niflow.core import NiFiComponent, ProcessGroup

# A processor box is ~352x128 px and NiFi draws the relationship label
# (~224x80 px) at the connection midpoint, so the gap between ranks must
# clear the label along the flow axis. For processor-sized nodes that makes
# the steps below; nodes with other sizes stretch their rank/lane to fit.
PROC_W, PROC_H = 352.0, 128.0
X_SPACING = 600.0  # horizontal rank step / vertical lane step
Y_SPACING = 250.0  # horizontal lane step
V_SPACING = 350.0  # vertical rank step (label height + margin below a box)
_GAPS = {  # layout -> (gap along the flow axis, gap across it)
    "horizontal": (X_SPACING - PROC_W, Y_SPACING - PROC_H),
    "vertical": (V_SPACING - PROC_H, X_SPACING - PROC_W),
}


def place(
    nodes: Sequence[Hashable],
    edges: Iterable[Tuple[Hashable, Hashable]],
    layout: str = "horizontal",
    loose: Sequence[Hashable] = (),
    sizes: Optional[Dict[Hashable, Tuple[float, float]]] = None,
) -> Dict[Hashable, Tuple[float, float]]:
    """Rank-and-align placement for an arbitrary directed graph.

    ``nodes`` lists every placeable id in preferred tie-break order; ``edges``
    are ``(source, destination)`` pairs (self-loops and endpoints not in
    ``nodes`` are ignored); ``loose`` ids (labels) always land in the spare
    lane; ``sizes`` maps ids to ``(width, height)`` px — anything unlisted is
    processor-sized. Every node gets a top-left coordinate — callers that
    respect hand-placed positions filter afterwards.

    Ranks come from the longest path from the graph's sources (back edges in
    cycles are tolerated) and advance by the tallest/widest node in the rank,
    so nothing can overlap. Across each rank, a node is centered on the mean
    center of its predecessors (the barycenter heuristic, used for both order
    and coordinate), then nudged just far enough to clear its left/upper
    neighbour — chains stay in a dead-straight line, branches fan out.
    """
    known = set(nodes)
    preds: Dict[Hashable, List[Hashable]] = {n: [] for n in nodes}
    for src, dst in edges:
        if src in known and dst in known and src != dst:
            preds[dst].append(src)

    rank: Dict[Hashable, int] = {}

    def _rank_of(nid: Hashable, path: frozenset) -> int:
        if nid in rank:
            return rank[nid]
        best = 0
        for p in preds[nid]:
            if p in path:
                continue  # back edge in a cycle: ignore it
            best = max(best, _rank_of(p, path | {nid}) + 1)
        rank[nid] = best
        return best

    connected = {nid for nid, ps in preds.items() if ps}
    connected.update(p for ps in preds.values() for p in ps)

    by_rank: Dict[int, List[Hashable]] = {}
    for n in nodes:
        if n in connected:
            by_rank.setdefault(_rank_of(n, frozenset({n})), []).append(n)

    along_gap, cross_gap = _GAPS.get(layout, _GAPS["horizontal"])

    def extents(n: Hashable) -> Tuple[float, float]:
        """(extent along the flow axis, extent across it) for one node."""
        w, h = (sizes or {}).get(n, (PROC_W, PROC_H))
        return (h, w) if layout == "vertical" else (w, h)

    placed: Dict[Hashable, Tuple[float, float]] = {}  # (along, cross) top-left
    center: Dict[Hashable, float] = {}  # cross-axis center, for alignment
    along = 0.0
    cross_end = 0.0  # rightmost/lowest edge across all ranks (spare lane)
    for r in sorted(by_rank):
        row = by_rank[r]

        def barycenter(n: Hashable) -> float:
            cs = [center[p] for p in preds[n] if p in center]
            # Only back-edge preds (not yet placed): keep input order, after
            # anything with a real barycenter.
            return sum(cs) / len(cs) if cs else float("inf")

        if r:
            row = sorted(row, key=barycenter)  # stable: ties keep input order
        row_depth = 0.0
        prev_end: Optional[float] = None
        for n in row:
            a_ext, c_ext = extents(n)
            want = barycenter(n)
            # Centered on its feeders, but never into the neighbour.
            start = want - c_ext / 2 if want != float("inf") else 0.0
            if prev_end is not None:
                start = max(start, prev_end + cross_gap)
            placed[n] = (along, start)
            center[n] = start + c_ext / 2
            prev_end = start + c_ext
            cross_end = max(cross_end, prev_end)
            row_depth = max(row_depth, a_ext)
        along += row_depth + along_gap

    # Unwired nodes (and labels) get a spare lane past everything, stacked
    # down the flow axis by their real sizes.
    spare = cross_end + cross_gap if placed else 0.0
    stray_along = 0.0
    for n in [n for n in nodes if n not in connected] + list(loose):
        a_ext, _ = extents(n)
        placed[n] = (stray_along, spare)
        stray_along += a_ext + along_gap

    if layout == "vertical":
        return {n: (c, a) for n, (a, c) in placed.items()}
    return {n: (a, c) for n, (a, c) in placed.items()}


def compute_layout(group: ProcessGroup) -> Dict[int, Tuple[float, float]]:
    """Return ``{id(component): (x, y)}`` for the group's direct children.

    Covers every canvas component a connection can touch — processors, ports,
    funnels, child process groups — plus labels. A connection ending on a
    nested group's port counts as ending on that nested group, so child groups
    rank correctly within the parent. Cycles (retry loops) are tolerated.

    Components with an explicit ``position`` participate in ranking but are
    omitted from the result; their coordinates always win.
    """
    nodes: List[NiFiComponent] = [
        *group.input_ports,
        *group.processors,
        *group.funnels,
        *group.process_groups,
        *group.output_ports,
    ]

    # Resolve connection endpoints to direct children: a port owned by a
    # nested group stands in for the nested group itself.
    endpoint: Dict[int, NiFiComponent] = {id(n): n for n in nodes}
    for child in group.process_groups:
        for port in [*child.input_ports, *child.output_ports]:
            endpoint.setdefault(id(port), child)

    edges: List[Tuple[int, int]] = []
    for conn in group.connections:
        src = endpoint.get(id(conn.source))
        dst = endpoint.get(id(conn.target))
        if src is not None and dst is not None:
            edges.append((id(src), id(dst)))

    positions = place(
        [id(n) for n in nodes], edges, group.layout, [id(lb) for lb in group.labels]
    )
    return {
        id(n): positions[id(n)]
        for n in [*nodes, *group.labels]
        if n.position is None and id(n) in positions
    }


def apply_layout(group: ProcessGroup) -> ProcessGroup:
    """Materialise computed coordinates onto components lacking a position.

    Mutates *group* in place (recursing into nested groups) and returns it.
    This is what the emitters do implicitly; call it directly when you want
    the resolved coordinates visible on the model itself.
    """
    auto = compute_layout(group)
    components = [
        *group.input_ports,
        *group.processors,
        *group.funnels,
        *group.process_groups,
        *group.output_ports,
        *group.labels,
    ]
    for comp in components:
        if comp.position is None:
            comp.position = auto.get(id(comp))
    for child in group.process_groups:
        apply_layout(child)
    return group
