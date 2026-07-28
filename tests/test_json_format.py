"""Round-trip tests for niflow.formats.json_format + python_format.

The three converters (from_json / to_json / to_python) form a triangle. Each
test below exercises one edge; together they lock the format contract without
needing a running NiFi.
"""
from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

from niflow import Flow
from niflow.core import Port
from niflow.layout import apply_layout

FIXTURE = Path(__file__).parent / "fixtures" / "nested_groups.flow.json"


def _normalize_port_rels(group):
    """Collapse `["success"]` -> `[]` on port-source connections.

    ``from_json`` does this on the way in (NiFi emits `[""]` for port sources,
    which we want as `[]`); Python-built flows often leave the model default
    ``["success"]`` in place even when the source is a port. The deployer
    ignores them either way, but to compare model dumps we have to normalise.
    """
    for c in group.connections:
        if isinstance(c.source, Port):
            c.relationships = []
    for child in group.process_groups:
        _normalize_port_rels(child)


def _load_example_flow(name: str) -> Flow:
    """Import ``examples/<name>.py`` and return its module-level ``flow``."""
    path = Path(__file__).parent.parent / "examples" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod.flow


# --- from_json --------------------------------------------------------------
def test_from_json_parses_fixture_shape():
    flow = Flow.from_json(FIXTURE)
    assert flow.name == "NestedFlow"
    assert {p.name for p in flow.processors} == {"Ingest", "Output"}
    assert [g.name for g in flow.process_groups] == ["Transform"]
    sub = flow.process_groups[0]
    assert [p.name for p in sub.processors] == ["Tag"]
    assert [p.name for p in sub.input_ports] == ["in"]
    assert [p.name for p in sub.output_ports] == ["out"]
    # Two conns at root (Ingest -> in, out -> Output), two in the subgroup.
    assert len(flow.connections) == 2
    assert len(sub.connections) == 2


# --- to_json round-trip -----------------------------------------------------
def test_to_json_is_byte_stable():
    """Same input must produce the same JSON output (deterministic UUID5)."""
    flow = Flow.from_json(FIXTURE)
    assert flow.to_json() == flow.to_json()


def test_json_round_trip_preserves_structure():
    """JSON -> Flow -> JSON -> Flow must give equivalent Flow."""
    f1 = Flow.from_json(FIXTURE)
    f2 = Flow.from_json(f1.to_json())
    d1 = f1.model_dump(exclude={"nifi_id", "nifi_entity"})
    d2 = f2.model_dump(exclude={"nifi_id", "nifi_entity"})
    assert d1 == d2


# --- to_python round-trip ---------------------------------------------------
def test_to_python_is_runnable():
    """Emitted source must parse and exec back into an equivalent Flow."""
    flow = Flow.from_json(FIXTURE)
    src = flow.to_python()
    ast.parse(src)  # syntactically valid

    ns: dict = {}
    exec(compile(src, "<test>", "exec"), ns)
    f2 = ns["flow"]
    assert isinstance(f2, Flow)
    assert f2.name == flow.name
    assert {p.name for p in f2.processors} == {p.name for p in flow.processors}


def test_to_python_to_python_idempotent():
    """to_python(exec(to_python(f))) == to_python(f) up to model dump."""
    flow = Flow.from_json(FIXTURE)
    src1 = flow.to_python()
    ns: dict = {}
    exec(compile(src1, "<test>", "exec"), ns)
    f2 = ns["flow"]
    src2 = f2.to_python()
    assert src1 == src2


# --- triangle: Python-built flow -> JSON -> Flow ----------------------------
def test_python_example_round_trips_through_json():
    """Build a Flow in code, serialise via to_json, parse back, compare."""
    original = _load_example_flow("nested_groups")
    parsed = Flow.from_json(original.to_json())

    _normalize_port_rels(original)
    _normalize_port_rels(parsed)

    # to_json materialises auto-layout coordinates for unpositioned
    # components; resolve them on the original so the dumps line up.
    apply_layout(original)

    d_orig = original.model_dump(exclude={"nifi_id", "nifi_entity"})
    d_back = parsed.model_dump(exclude={"nifi_id", "nifi_entity"})

    # The group itself still defaults to NiFi's (0,0) when unpositioned.
    _normalise_positions(d_orig)
    _normalise_positions(d_back)

    assert d_orig == d_back


def _normalise_positions(node):
    if isinstance(node, dict):
        if "position" in node and node["position"] is None:
            node["position"] = (0.0, 0.0)
        for v in node.values():
            _normalise_positions(v)
    elif isinstance(node, list):
        for v in node:
            _normalise_positions(v)


# --- service-ref resolution (NiFi 1.x quirk) --------------------------------
def test_1x_service_ref_resolves_by_instance_identifier():
    """NiFi 1.24's /download flags service-ref descriptors as
    ``identifiesControllerService: false`` and writes the service's *instance*
    id as the property value. Resolution is by value, so the link survives."""
    from niflow.core import ControllerService

    snapshot = {
        "flowContents": {
            "name": "OneX",
            "controllerServices": [{
                "identifier": "svc-versioned-id",
                "instanceIdentifier": "1111aaaa-0000-1000-8000-instanceuuid",
                "name": "Reader",
                "type": "org.apache.nifi.json.JsonTreeReader",
                "properties": {},
            }],
            "processors": [{
                "identifier": "proc-versioned-id",
                "name": "Convert",
                "type": "org.apache.nifi.processors.standard.ConvertRecord",
                "properties": {"Record Reader": "1111aaaa-0000-1000-8000-instanceuuid"},
                "propertyDescriptors": {
                    "Record Reader": {
                        "name": "Record Reader",
                        "identifiesControllerService": False,  # the 1.x lie
                    },
                },
            }],
        }
    }
    flow = Flow.from_json(snapshot)
    ref = flow.processors[0].properties["Record Reader"]
    assert isinstance(ref, ControllerService)
    assert ref is flow.controller_services[0]


def test_service_to_service_ref_resolves():
    """A controller service referencing another (e.g. a pool) resolves too."""
    from niflow.core import ControllerService

    snapshot = {
        "flowContents": {
            "name": "SvcRefs",
            "controllerServices": [
                {
                    "identifier": "pool-id",
                    "name": "Pool",
                    "type": "org.apache.nifi.dbcp.DBCPConnectionPool",
                    "properties": {},
                },
                {
                    "identifier": "lookup-id",
                    "name": "Lookup",
                    "type": "org.apache.nifi.lookup.db.DatabaseRecordLookupService",
                    "properties": {"Database Connection Pooling Service": "pool-id"},
                },
            ],
            "processors": [],
        }
    }
    flow = Flow.from_json(snapshot)
    ref = flow.controller_services[1].properties["Database Connection Pooling Service"]
    assert isinstance(ref, ControllerService)
    assert ref is flow.controller_services[0]


# --- lossy-pull warnings ----------------------------------------------------
def test_unrepresentable_components_produce_pull_warnings():
    snapshot = {
        "externalControllerServiceReferences": {
            "abc-123": {"identifier": "abc-123", "name": "SharedPool"},
        },
        "flowContents": {
            "name": "Lossy",
            "processors": [],
            "processGroups": [{
                "name": "Sub",
                "remoteProcessGroups": [{"targetUris": "https://other:8443/nifi"}],
            }],
        },
    }
    flow = Flow.from_json(snapshot)
    assert len(flow.pull_warnings) == 2
    joined = "\n".join(flow.pull_warnings)
    assert "Sub: remote process group -> https://other:8443/nifi" in joined
    assert "SharedPool" in joined


def test_clean_snapshot_has_no_pull_warnings():
    flow = Flow.from_json(FIXTURE)
    assert flow.pull_warnings == []
