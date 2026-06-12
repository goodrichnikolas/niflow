"""Integration tests for the pull/edit/push loop against a live NiFi.

Version-agnostic: run against 2.x (default) or 1.24 via::

    NIFLOW_NIFI_HOST=https://localhost:8444/nifi-api pytest -m integration tests/test_client_integration.py

(or ``make test-integration-v1``).
"""
import pytest

from niflow import Flow, Funnel, Label, Parameter, ParameterContext, Processor

pytestmark = pytest.mark.integration

NAME = "niflow-client-it"


def _build_flow() -> Flow:
    ctx = ParameterContext(
        f"{NAME}-params",
        parameters=[
            Parameter("greeting", value="hello"),
            Parameter("token", sensitive=True),
        ],
    )
    flow = Flow(NAME)
    flow.parameter_context = ctx
    gen = Processor(
        name="Generate",
        type="org.apache.nifi.processors.standard.GenerateFlowFile",
        properties={"Custom Text": "#{greeting}"},
        scheduling_period="10 sec",
        auto_terminate=[],
    )
    log = Processor(
        name="Log",
        type="org.apache.nifi.processors.standard.LogAttribute",
        auto_terminate=["success"],
        comments="end of the line",
    )
    merge = Funnel()
    note = Label("integration test flow")
    flow.add(gen, log, merge, note)
    flow.add_connection(gen.to(merge, back_pressure_object_threshold=123))
    flow.add_connection(merge >> log)
    with flow.process_group("Nested") as child:
        inner = Processor(
            name="Inner",
            type="org.apache.nifi.processors.standard.UpdateAttribute",
            auto_terminate=["success"],
        )
        child.add_processor(inner)
    return flow


@pytest.fixture()
def cleanup(nifi_client):
    yield
    for path, comp in list(nifi_client.walk_groups()):
        if comp["name"].startswith(NAME):
            nifi_client.delete_group(comp["id"])


def test_push_pull_copy_lifecycle(nifi_client, cleanup):
    client = nifi_client
    flow = _build_flow()

    # --- push (create) -------------------------------------------------------
    pg_id = client.push_flow(flow, secrets={"token": "sekrit-value"})
    assert flow.nifi_id == pg_id

    # --- pull back and compare the bones --------------------------------------
    pulled = client.pull_flow(pg_id)
    assert pulled.name == NAME
    assert {p.name for p in pulled.processors} == {"Generate", "Log"}
    assert len(pulled.funnels) == 1
    assert len(pulled.labels) == 1
    assert pulled.process_groups[0].name == "Nested"

    # parameter context round-trips; sensitive value stays hidden but applied
    params = {p.name: p for p in pulled.parameter_context.parameters}
    assert params["greeting"].value == "hello"
    assert params["token"].sensitive and params["token"].value is None

    # queue settings survive
    funnel_conn = [c for c in pulled.connections if c.back_pressure_object_threshold == 123]
    assert funnel_conn, "custom back-pressure threshold lost on round-trip"

    # --- edit + push (replace) -------------------------------------------------
    pulled_gen = [p for p in pulled.processors if p.name == "Generate"][0]
    pulled_gen.properties["Custom Text"] = "#{greeting} again"
    new_id = client.push_flow(pulled)
    assert new_id != pg_id  # delete-and-recreate
    again = client.pull_flow(new_id)
    gen = [p for p in again.processors if p.name == "Generate"][0]
    assert gen.properties["Custom Text"] == "#{greeting} again"

    # --- copy ------------------------------------------------------------------
    copy_id = client.copy_group(new_id, new_name=f"{NAME}-copy")
    copied = client.pull_flow(copy_id)
    assert copied.name == f"{NAME}-copy"


def test_python_codegen_of_live_flow_is_executable(nifi_client, cleanup):
    client = nifi_client
    flow = _build_flow()
    client.push_flow(flow)

    pulled = client.pull_flow(NAME)
    source = pulled.to_python()
    namespace = {"__name__": "_it"}
    exec(compile(source, "<pulled>", "exec"), namespace)
    rebuilt = namespace["flow"]
    assert rebuilt.name == NAME
    assert {p.name for p in rebuilt.processors} == {"Generate", "Log"}
