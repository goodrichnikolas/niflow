"""Deployment orchestration tests using a fake nipyapi — no NiFi required.

These assert the *behaviour* of the deployer (ordering, controller-service UUID
substitution, auto-termination, delete-and-recreate) without a live server.
"""
import sys
from types import SimpleNamespace

import pytest

from niflow import Flow
from niflow.processors.standard import ConvertRecord, GetFile, PutFile
from niflow.services.standard import JsonRecordSetWriter, JsonTreeReader


class _Rel:
    def __init__(self, name):
        self.name = name


def _entity(eid, name, parent_id=None, relationships=None):
    component = SimpleNamespace(
        id=eid, name=name, parent_group_id=parent_id, relationships=relationships
    )
    return SimpleNamespace(id=eid, component=component)


class FakeNiFi:
    """A stand-in for the nipyapi top module that records operations."""

    def __init__(self, existing_pgs=None):
        self.calls = []  # list of (op, payload)
        self.proc_configs = {}  # processor name -> ProcessorConfigDTO
        self.conn_args = []  # (source_id, target_id, relationships)
        self._existing = existing_pgs or []
        self._counter = 0

        self.config = SimpleNamespace(nifi_config=SimpleNamespace(host=None, verify_ssl=None))
        self.security = SimpleNamespace(
            set_ssl_warning_suppression=lambda *_: None,
            get_service_access_status=lambda *a, **k: True,
        )
        self.utils = SimpleNamespace(set_endpoint=self._set_endpoint)
        self.nifi = SimpleNamespace(
            ProcessorConfigDTO=lambda **kw: SimpleNamespace(**kw),
            ControllerServiceDTO=lambda **kw: SimpleNamespace(**kw),
        )
        self.canvas = SimpleNamespace(
            get_root_pg_id=lambda: "root-id",
            get_process_group=self._get_process_group,
            list_all_process_groups=lambda pid: list(self._existing),
            schedule_process_group=lambda pid, scheduled: self.calls.append(("schedule_pg", pid, scheduled)),
            delete_process_group=lambda pg, **kw: self.calls.append(("delete_pg", pg.id)),
            create_process_group=self._create_pg,
            get_controller_type=lambda t: _dtype(t),
            create_controller=self._create_controller,
            update_controller=lambda e, dto: (self.calls.append(("update_controller", e.id, dto.properties)) or e),
            schedule_controller=lambda e, s, **k: self.calls.append(("enable_controller", e.id)),
            create_port=self._create_port,
            get_processor_type=lambda t: _dtype(t),
            create_processor=self._create_processor,
            update_processor=self._update_processor,
            create_connection=self._create_connection,
        )

    def _next(self, prefix):
        self._counter += 1
        return f"{prefix}-{self._counter}"

    def _set_endpoint(self, *a, **k):
        self.calls.append(("set_endpoint", a[0] if a else k.get("endpoint")))
        return True

    def _get_process_group(self, identifier, identifier_type="name"):
        return _entity(identifier, "root", parent_id=None)

    def _create_pg(self, parent, name, location, comment=""):
        eid = self._next("pg")
        self.calls.append(("create_pg", name))
        return _entity(eid, name, parent_id=parent.id)

    def _create_controller(self, parent, controller, name=None):
        eid = self._next("cs")
        self.calls.append(("create_controller", name))
        return _entity(eid, name)

    def _create_port(self, pg_id, port_type, name, state, position=None):
        eid = self._next("port")
        self.calls.append(("create_port", port_type, name, state))
        return _entity(eid, name)

    def _create_processor(self, parent, processor, location, name=None, config=None):
        eid = self._next("proc")
        self.calls.append(("create_processor", name))
        return _entity(eid, name, relationships=[_Rel("success"), _Rel("failure"), _Rel("original")])

    def _update_processor(self, entity, update):
        self.proc_configs[entity.component.name] = update
        self.calls.append(("update_processor", entity.component.name))

    def _create_connection(self, source, target, relationships=None, name=None):
        eid = self._next("conn")
        self.calls.append(("create_connection", source.id, target.id, relationships))
        self.conn_args.append((source.id, target.id, relationships))
        return _entity(eid, name or "conn")

    # convenience -------------------------------------------------------------
    def index(self, op):
        return next(i for i, c in enumerate(self.calls) if c[0] == op)


def _dtype(type_string):
    return SimpleNamespace(type=type_string)


@pytest.fixture
def fake_nifi(monkeypatch):
    def _install(existing_pgs=None):
        fake = FakeNiFi(existing_pgs=existing_pgs)
        monkeypatch.setitem(sys.modules, "nipyapi", fake)
        return fake

    return _install


def _record_flow():
    flow = Flow("RecordConvert")
    reader = JsonTreeReader("Reader")
    writer = JsonRecordSetWriter("Writer")
    get = GetFile("Get")
    convert = ConvertRecord("Convert", properties={"Record Reader": reader, "Record Writer": writer})
    put = PutFile("Put")
    flow.add_controller_service(reader, writer)
    flow.add_processor(get, convert, put)
    flow.add_connection(get >> convert)
    flow.add_connection(convert >> put)
    return flow, reader, writer, convert


def test_deploy_orders_services_then_processors_then_connections(fake_nifi):
    fake = fake_nifi()
    flow, *_ = _record_flow()
    flow.deploy()

    # Controller services enabled before any processor is created,
    # and all connections come after processors.
    assert fake.index("enable_controller") < fake.index("create_processor")
    assert fake.index("create_processor") < fake.index("create_connection")


def test_controller_service_property_substituted_with_uuid(fake_nifi):
    fake = fake_nifi()
    flow, reader, writer, convert = _record_flow()
    flow.deploy()

    props = fake.proc_configs["Convert"].properties
    # The service instances were replaced by their assigned NiFi ids.
    assert props["Record Reader"] == reader.nifi_id
    assert props["Record Writer"] == writer.nifi_id
    assert reader.nifi_id and reader.nifi_id.startswith("cs-")


def test_unused_relationships_are_auto_terminated(fake_nifi):
    fake = fake_nifi()
    flow, *_ = _record_flow()
    flow.deploy()

    # Get -> Convert consumes "success"; failure/original must be auto-terminated.
    get_cfg = fake.proc_configs["Get"]
    assert "success" not in get_cfg.auto_terminated_relationships
    assert "failure" in get_cfg.auto_terminated_relationships
    # Put consumes nothing, so everything is auto-terminated.
    assert "success" in fake.proc_configs["Put"].auto_terminated_relationships


def test_auto_terminate_unused_disabled(fake_nifi):
    fake = fake_nifi()
    flow = Flow("F")
    get, put = GetFile("Get"), PutFile("Put")
    flow.add_processor(get, put)
    flow.add_connection(get >> put)
    flow.deploy(auto_terminate_unused=False)
    # With the behaviour off, nothing is auto-terminated unless explicit.
    assert fake.proc_configs["Get"].auto_terminated_relationships == []


def test_delete_and_recreate_removes_existing(fake_nifi):
    existing = _entity("old-pg", "RecordConvert", parent_id="root-id")
    fake = fake_nifi(existing_pgs=[existing])
    flow, *_ = _record_flow()
    flow.deploy()

    assert ("schedule_pg", "old-pg", False) in fake.calls
    assert ("delete_pg", "old-pg") in fake.calls
    # Deletion happens before the new group is created.
    assert fake.index("delete_pg") < fake.index("create_pg")


def test_port_source_connection_has_no_relationships(fake_nifi):
    from niflow import InputPort, OutputPort
    from niflow.processors.standard import UpdateAttribute

    fake = fake_nifi()
    flow = Flow("F")
    with flow.process_group("Child") as child:
        inp, outp = InputPort("in"), OutputPort("out")
        tag = UpdateAttribute("Tag")
        child.add_port(inp, outp)
        child.add_processor(tag)
        child.add_connection(inp >> tag)  # port source -> no relationships
        child.add_connection(tag >> outp)
    flow.deploy()

    # Find the connection whose source is the input port: relationships is None.
    port_conn = next(c for c in fake.conn_args if c[0] == inp.nifi_id)
    assert port_conn[2] is None
