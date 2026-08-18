"""Identity-collision safety (torture-flow round one, P0 findings).

Pushing a flow whose components hash to the same deterministic identifier
made NiFi silently merge or drop them. These tests lock in the fix: loud
rejection for same-kind name duplicates, and genuinely distinct identifiers
for everything that is legal (parallel edges, cross-group wiring, exact
duplicate connections).
"""
from __future__ import annotations

import json

import pytest

from niflow import Flow, InputPort, OutputPort
from niflow.core import Funnel, Processor, find_identity_collisions
from niflow.formats.json_format import IdentityCollisionError
from niflow.validate import validate_flow

UPDATE_ATTR = "org.apache.nifi.processors.attributes.UpdateAttribute"
ROUTE_ATTR = "org.apache.nifi.processors.standard.RouteOnAttribute"


def _proc(name: str, **props) -> Processor:
    return Processor(name=name, type=UPDATE_ATTR, properties=props,
                     auto_terminate=["success"])


# --- duplicates are rejected loudly ------------------------------------------


def test_duplicate_processors_rejected():
    flow = Flow("F")
    flow.add_processor(_proc("Worker", lane="one"), _proc("Worker", lane="two"))

    assert find_identity_collisions(flow)
    with pytest.raises(IdentityCollisionError, match="processor 'Worker'"):
        flow.to_json()


def test_duplicate_sibling_groups_rejected():
    flow = Flow("F")
    for lane in ("one", "two"):
        with flow.process_group("Stage") as stage:
            stage.add(_proc("W", lane=lane))

    assert find_identity_collisions(flow)
    with pytest.raises(IdentityCollisionError, match="process group"):
        flow.to_json()


def test_duplicate_ports_rejected():
    flow = Flow("F")
    with flow.process_group("Stage") as stage:
        stage.add(InputPort("in"), InputPort("in"))

    with pytest.raises(IdentityCollisionError, match="input port 'in'"):
        flow.to_json()


def test_validate_reports_duplicates():
    flow = Flow("F")
    flow.add_processor(_proc("Worker"), _proc("Worker"))
    issues = validate_flow(flow)
    assert any("Worker" in i["message"] and "rename" in i["message"] for i in issues)


# --- legal same-name shapes still pass ----------------------------------------


def test_same_name_different_kinds_is_fine():
    """A processor may share its name with a port — kinds are distinct."""
    flow = Flow("F")
    with flow.process_group("Stage") as stage:
        inp, out = InputPort("in"), OutputPort("out")
        work = _proc("in")  # deliberately named like the port
        stage.add(inp, out, work)
        stage.add_connection(inp >> work, work >> out)

    assert find_identity_collisions(flow) == []
    flow.to_json()  # must not raise


def test_same_name_in_different_groups_is_fine():
    flow = Flow("F")
    with flow.process_group("A") as a:
        a.add(_proc("Worker"))
    with flow.process_group("B") as b:
        b.add(_proc("Worker"))
    assert find_identity_collisions(flow) == []
    flow.to_json()


# --- connection identity --------------------------------------------------------


def _connection_ids(flow: Flow) -> list:
    contents = json.loads(flow.to_json())["flowContents"]
    ids = [c["identifier"] for c in contents.get("connections", [])]
    for child in contents.get("processGroups", []):
        ids += [c["identifier"] for c in child.get("connections", [])]
    return ids


def test_parallel_edges_distinct_relationships_all_survive():
    """16 unnamed edges between the same endpoints, one relationship each —
    round one dropped 14 of them to seed collisions."""
    flow = Flow("F")
    fanout = Processor(
        name="Fanout", type=ROUTE_ATTR,
        properties={f"r{i:02d}": f"${{fan:equals('{i}')}}" for i in range(16)},
        auto_terminate=["unmatched"],
    )
    funnel = Funnel()
    flow.add_processor(fanout)
    flow.add_funnel(funnel)
    flow.add_connection(
        *[fanout.to(funnel, relationships=[f"r{i:02d}"]) for i in range(16)]
    )

    ids = _connection_ids(flow)
    assert len(ids) == 16
    assert len(set(ids)) == 16


def test_exact_duplicate_connections_pair_by_occurrence():
    flow = Flow("F")
    a, b = _proc("A"), _proc("B")
    flow.add_processor(a, b)
    flow.add_connection(a >> b, a >> b)  # same name/endpoints/relationships

    ids = _connection_ids(flow)
    assert len(set(ids)) == 2
    # and deterministically stable across emits
    assert flow.to_json() == flow.to_json()


def test_cross_group_connections_use_real_endpoint_ids():
    """Two child-group out ports wired to one funnel used to collide: the
    child ports had no identifier yet when root connections were seeded."""
    flow = Flow("F")
    funnel = Funnel()
    flow.add_funnel(funnel)
    outs = []
    for name in ("A", "B"):
        with flow.process_group(name) as g:
            out = OutputPort("out")
            g.add(out, _proc("W"))
            g.add_connection(g.processors[0] >> out)
            outs.append(out)
    flow.add_connection(outs[0] >> funnel, outs[1] >> funnel)

    ids = _connection_ids(flow)
    assert len(ids) == 4  # 2 root + 1 in each child
    assert len(set(ids)) == 4
