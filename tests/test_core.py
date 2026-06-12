"""Unit tests for the declarative models — no NiFi required."""
from niflow import (
    Connection,
    Flow,
    InputPort,
    OutputPort,
    ProcessGroup,
)
from niflow.processors.standard import ConvertRecord, GetFile, PutFile, UpdateAttribute
from niflow.services.standard import JsonRecordSetWriter, JsonTreeReader


def test_positional_name():
    assert Flow("MyFlow").name == "MyFlow"
    assert InputPort("in").name == "in"
    assert GetFile("ingest").name == "ingest"


def test_rshift_builds_connection_with_identity():
    get = GetFile("get")
    put = PutFile("put")
    conn = get >> put
    assert isinstance(conn, Connection)
    assert conn.source is get  # same object, so deploy can share nifi_entity
    assert conn.target is put
    assert conn.relationships == ["success"]


def test_to_with_explicit_relationships():
    a = GetFile("a")
    b = PutFile("b")
    conn = a.to(b, relationships=["failure"])
    assert conn.source is a and conn.target is b
    assert conn.relationships == ["failure"]


def test_add_methods_register_components():
    flow = Flow("F")
    get, put = GetFile("g"), PutFile("p")
    flow.add_processor(get, put)
    flow.add_connection(get >> put)
    assert flow.processors == [get, put]
    assert len(flow.connections) == 1


def test_port_routing_by_type():
    flow = Flow("F")
    flow.add_port(InputPort("in"), OutputPort("out"))
    assert [p.name for p in flow.input_ports] == ["in"]
    assert [p.name for p in flow.output_ports] == ["out"]
    assert flow.input_ports[0].port_type == "INPUT_PORT"
    assert flow.output_ports[0].port_type == "OUTPUT_PORT"


def test_nested_process_group_context_manager():
    flow = Flow("F")
    with flow.process_group("Child") as child:
        assert isinstance(child, ProcessGroup)
        child.add_processor(UpdateAttribute("tag"))
    assert flow.process_groups[0] is child
    assert child.name == "Child"
    assert child.processors[0].name == "tag"


def test_controller_service_as_property_value_kept_as_object():
    reader = JsonTreeReader("r")
    writer = JsonRecordSetWriter("w")
    convert = ConvertRecord("c", properties={"Record Reader": reader, "Record Writer": writer})
    # Until deploy, the property holds the service instance (UUID substitution is deploy-time).
    assert convert.properties["Record Reader"] is reader
    assert convert.properties["Record Writer"] is writer


def test_smart_add_dispatch():
    flow = Flow("F")
    get, put = GetFile("g"), PutFile("p")
    reader = JsonTreeReader("r")
    flow.add(get, put, reader, get >> put, InputPort("in"))
    assert flow.processors == [get, put]
    assert flow.controller_services == [reader]
    assert len(flow.connections) == 1
    assert len(flow.input_ports) == 1


def test_processor_defaults_merge_with_overrides():
    # Factory defaults present, user override wins.
    get = GetFile("g", properties={"Input Directory": "/custom"})
    assert get.properties["Input Directory"] == "/custom"
    assert get.properties["Keep Source File"] == "false"  # default retained
    assert get.type == "org.apache.nifi.processors.standard.GetFile"


def test_update_attribute_uses_attributes_package():
    assert UpdateAttribute("u").type == "org.apache.nifi.processors.attributes.UpdateAttribute"
