"""Parse/emit tests against REAL NiFi snapshots (no live server needed).

The fixtures under ``tests/fixtures/real/`` are genuine ``VersionedFlowSnapshot``
responses captured from live NiFi 1.24.0 and 2.7.2 servers hosting the
kitchen-sink flow (``scripts/capture_fixtures.py`` refreshes them). Unlike
hand-built dicts, they contain whatever the server actually returns — which
is where every emitter/parser bug so far has hidden (1.x service refs by
``instanceIdentifier``, 2.x property-name migration, port field NPEs, ...).

Real-world quirk these fixtures pin down: 2.x *migrates* property names on
ingest — ConvertRecord pushed with 1.x keys (``record-reader``) snapshots
back with 2.x keys (``Record Reader``). Service-ref resolution must therefore
key off ``identifiesControllerService`` descriptors / by-value matching, never
off hardcoded property names.
"""
import json
from pathlib import Path

import pytest

from niflow.core import ControllerService, Flow
from niflow.formats import from_json
from niflow.plan import diff_flows

FIXTURES = sorted(
    (Path(__file__).parent / "fixtures" / "real").glob("nifi-*.snapshot.json")
)


@pytest.fixture(params=FIXTURES, ids=lambda p: p.name.split(".snapshot")[0])
def snapshot(request) -> dict:
    return json.loads(request.param.read_text())


@pytest.fixture
def flow(snapshot) -> Flow:
    return from_json(snapshot)


def test_fixtures_exist_for_both_major_versions():
    majors = {p.name.split("-")[1].split(".")[0] for p in FIXTURES}
    assert majors >= {"1", "2"}, "capture fixtures from both NiFi 1.x and 2.x"


def test_structure_parses_completely(flow):
    assert {p.name for p in flow.processors} == {"Ingest", "Stamp", "Route", "Audit"}
    assert {s.name for s in flow.controller_services} == {"CsvReader", "JsonWriter"}
    assert len(flow.funnels) == 1 and len(flow.labels) == 1
    assert len(flow.connections) == 5

    (enrich,) = flow.process_groups
    assert [p.name for p in enrich.input_ports] == ["in"]
    assert [p.name for p in enrich.output_ports] == ["out"]
    assert [p.name for p in enrich.processors] == ["ToJson"]

    (dedup,) = enrich.process_groups
    assert [p.name for p in dedup.processors] == ["Tag"]
    assert len(dedup.connections) == 2


def test_service_references_resolve_across_groups(flow):
    tojson = flow.process_groups[0].processors[0]
    services = {
        v.name for v in tojson.properties.values() if isinstance(v, ControllerService)
    }
    assert services == {"CsvReader", "JsonWriter"}
    # ... and they are the SAME objects as the root-scope definitions.
    root = {id(s) for s in flow.controller_services}
    assert all(
        id(v) in root
        for v in tojson.properties.values()
        if isinstance(v, ControllerService)
    )


def test_connection_settings_survive_the_server(flow):
    by_ends = {
        (getattr(c.source, "name", ""), getattr(c.target, "name", "")): c
        for c in flow.connections
    }
    assert by_ends[("Ingest", "Stamp")].back_pressure_object_threshold == 500
    assert by_ends[("Ingest", "Stamp")].back_pressure_data_size_threshold == "100 MB"
    assert by_ends[("Stamp", "Route")].flowfile_expiration == "1 hour"
    assert by_ends[("Route", "in")].prioritizers == [
        "org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer"
    ]
    assert by_ends[("Route", "in")].relationships == ["urgent"]


def test_parameter_context_parses_with_sensitive_flag(flow):
    ctx = flow.parameter_context
    assert ctx is not None and ctx.name == "kitchen-sink-params"
    params = {p.name: p for p in ctx.parameters}
    assert params["route.match"].value == "urgent"
    assert params["api.token"].sensitive is True
    assert params["api.token"].value is None  # NiFi never exports secrets


def test_live_component_ids_are_captured(flow):
    assert flow.nifi_id  # root's own instanceIdentifier
    for proc in flow.processors:
        assert proc.nifi_id, f"{proc.name} lost its instanceIdentifier"
    assert all(c.nifi_id for c in flow.connections)
    assert flow.process_groups[0].nifi_id


def test_roundtrip_emit_parse_is_lossless(flow):
    reparsed = from_json(json.loads(flow.to_json()))
    assert diff_flows(flow, reparsed) == []
    assert diff_flows(reparsed, flow) == []


def test_emitted_json_keeps_2x_required_fields(flow):
    """2.x's synchronizer NPEs without these — regression-pinned for good."""
    emitted = json.loads(flow.to_json())

    def walk(group):
        yield group
        for child in group.get("processGroups", []):
            yield from walk(child)

    for group in walk(emitted["flowContents"]):
        for port in group.get("inputPorts", []) + group.get("outputPorts", []):
            assert port["concurrentlySchedulableTaskCount"] >= 1
            assert port["scheduledState"] in ("ENABLED", "DISABLED", "RUNNING")
        for conn in group.get("connections", []):
            assert conn["source"]["groupId"]
            assert conn["destination"]["groupId"]


def test_pull_is_not_lossy_for_the_kitchen_sink(flow):
    assert flow.pull_warnings == []
