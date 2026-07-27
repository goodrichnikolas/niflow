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

from typing import Dict, List, Tuple

from niflow.core import NiFiComponent, ProcessGroup

# One step between ranks / lanes. A processor is ~352x128 px and NiFi draws
# the relationship label (~224x80 px) at the connection midpoint, so the gap
# between components must clear the label or it overlaps its endpoints.
X_SPACING = 600.0
Y_SPACING = 250.0


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

    preds: Dict[int, List[int]] = {id(n): [] for n in nodes}
    for conn in group.connections:
        src = endpoint.get(id(conn.source))
        dst = endpoint.get(id(conn.target))
        if src is None or dst is None or src is dst:
            continue  # foreign endpoint or self-loop: irrelevant to ranking
        preds[id(dst)].append(id(src))

    rank: Dict[int, int] = {}

    def _rank_of(nid: int, path: frozenset) -> int:
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

    positions: Dict[int, Tuple[float, float]] = {}
    next_lane: Dict[int, int] = {}
    max_lane = 0
    for node in nodes:
        nid = id(node)
        if nid not in connected:
            continue
        r = _rank_of(nid, frozenset({nid}))
        lane = next_lane.get(r, 0)
        next_lane[r] = lane + 1
        max_lane = max(max_lane, lane)
        positions[nid] = _coords(group.layout, r, lane)

    # Unwired components (and labels) get a spare lane of their own.
    spare_lane = max_lane + 1 if positions else 0
    loose = [n for n in nodes if id(n) not in connected] + list(group.labels)
    for i, node in enumerate(loose):
        positions[id(node)] = _coords(group.layout, i, spare_lane)

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


def _coords(layout: str, rank: int, lane: int) -> Tuple[float, float]:
    if layout == "vertical":
        return lane * X_SPACING, rank * Y_SPACING
    return rank * X_SPACING, lane * Y_SPACING
