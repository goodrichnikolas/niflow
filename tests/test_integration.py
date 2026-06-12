"""End-to-end deployment tests against a running NiFi (marker: integration).

Auto-skipped when NiFi isn't reachable (see conftest.py's ``nifi`` fixture).
Run with: ``make test-integration`` (after ``make nifi-up && make nifi-wait``).
"""
import pytest

from niflow import Flow
from niflow.processors.standard import ConvertRecord, GetFile, PutFile
from niflow.services.standard import JsonRecordSetWriter, JsonTreeReader

pytestmark = pytest.mark.integration

FLOW_NAME = "NiFlowIntegrationTest"


def _cleanup(nipyapi, name):
    root_id = nipyapi.canvas.get_root_pg_id()
    for pg in nipyapi.canvas.list_all_process_groups(root_id):
        if pg.component.name == name and pg.component.parent_group_id == root_id:
            try:
                nipyapi.canvas.schedule_process_group(pg.id, False)
            except Exception:
                pass
            nipyapi.canvas.delete_process_group(pg, force=True, refresh=True)


@pytest.fixture
def clean_flow(nifi):
    import nipyapi

    _cleanup(nipyapi, FLOW_NAME)
    yield nipyapi
    _cleanup(nipyapi, FLOW_NAME)


def _names(entities):
    return {e.component.name for e in entities}


def test_deploy_simple_flow(clean_flow):
    nipyapi = clean_flow
    flow = Flow(FLOW_NAME, parent_pg="root")
    get = GetFile("Ingest", properties={"Input Directory": "/in", "File Filter": ".*"})
    put = PutFile("Output", properties={"Directory": "/out"})
    flow.add_processor(get, put)
    flow.add_connection(get >> put)

    flow.deploy()

    assert flow.nifi_id, "deploy should populate the flow's NiFi id"
    processors = nipyapi.canvas.list_all_processors(flow.nifi_id)
    assert _names(processors) == {"Ingest", "Output"}
    connections = nipyapi.canvas.list_all_connections(flow.nifi_id)
    assert len(connections) == 1


def test_deploy_is_delete_and_recreate(clean_flow):
    nipyapi = clean_flow
    root_id = nipyapi.canvas.get_root_pg_id()

    def deploy_once():
        flow = Flow(FLOW_NAME, parent_pg="root")
        get, put = GetFile("Ingest"), PutFile("Output")
        flow.add_processor(get, put)
        flow.add_connection(get >> put)
        flow.deploy()
        return flow

    deploy_once()
    deploy_once()  # second deploy must replace, not duplicate

    matches = [
        pg for pg in nipyapi.canvas.list_all_process_groups(root_id)
        if pg.component.name == FLOW_NAME and pg.component.parent_group_id == root_id
    ]
    assert len(matches) == 1, "re-deploying should not create a duplicate process group"


def test_deploy_record_flow_with_controller_services(clean_flow):
    nipyapi = clean_flow
    flow = Flow(FLOW_NAME, parent_pg="root")
    reader = JsonTreeReader("Reader")
    writer = JsonRecordSetWriter("Writer")
    get = GetFile("ReadJSON", properties={"Input Directory": "/in", "File Filter": r".*\.json"})
    convert = ConvertRecord("Convert", properties={"Record Reader": reader, "Record Writer": writer})
    put = PutFile("WriteJSON", properties={"Directory": "/out"})
    flow.add_controller_service(reader, writer)
    flow.add_processor(get, convert, put)
    flow.add_connection(get >> convert)
    flow.add_connection(convert >> put)

    flow.deploy()

    # Both services should exist and be enabled.
    services = nipyapi.canvas.list_all_controllers(flow.nifi_id)
    assert _names(services) == {"Reader", "Writer"}
    assert all(s.component.state == "ENABLED" for s in services)

    # ConvertRecord's reader/writer properties should point at the service UUIDs.
    convert_proc = next(p for p in nipyapi.canvas.list_all_processors(flow.nifi_id)
                        if p.component.name == "Convert")
    props = convert_proc.component.config.properties
    assert props.get("Record Reader") == reader.nifi_id
    assert props.get("Record Writer") == writer.nifi_id
