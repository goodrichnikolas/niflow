"""Live version-control-aware push: pushing to a group already under NiFi
Registry control rebuilds it *in place* — same group id, registry link intact,
showing as local changes — instead of delete-and-recreate.

Version-agnostic: the in-place vehicle is templates on 1.x and copy/paste on
2.x, but the observable contract is identical, so this runs against either::

    make test-integration        # 2.x  (localhost:8443 + registry  :18080)
    make test-integration-v1     # 1.24 (localhost:8444 + registry1 :18081)

Needs a NiFi Registry alongside NiFi (``docker compose up`` brings one up).
Registry URLs are derived from the NiFi version but can be overridden with
``NIFLOW_REGISTRY_URL`` (as NiFi reaches it) and ``NIFLOW_REGISTRY_HOST_URL``
(as the test host reaches it).
"""
import os

import pytest
import requests

from niflow import Flow, Processor
from niflow.core import ControllerService

pytestmark = pytest.mark.integration

NAME = "niflow-vc-it"


def _registry_urls(client):
    """(url as NiFi reaches the registry, url as this host reaches it)."""
    if client._major_version() >= 2:
        default_nifi, default_host = "http://registry:18080", "http://localhost:18080"
    else:
        default_nifi, default_host = "http://registry1:18080", "http://localhost:18081"
    return (
        os.getenv("NIFLOW_REGISTRY_URL", default_nifi),
        os.getenv("NIFLOW_REGISTRY_HOST_URL", default_host),
    )


def _bucket(reg_host: str, name: str) -> str:
    """Create (or reuse + clear) a registry bucket; return its id."""
    base = f"{reg_host}/nifi-registry-api"
    for b in requests.get(f"{base}/buckets", timeout=5).json():
        if b["name"] == name:
            for f in requests.get(f"{base}/buckets/{b['identifier']}/flows", timeout=5).json():
                requests.delete(
                    f"{base}/buckets/{b['identifier']}/flows/{f['identifier']}", timeout=5
                ).raise_for_status()
            return b["identifier"]
    r = requests.post(f"{base}/buckets", json={"name": name}, timeout=5)
    r.raise_for_status()
    return r.json()["identifier"]


def _convert_flow(name: str, proc_name: str) -> Flow:
    """A flow with two group-level services and a processor wired to both —
    the case paste can't carry on its own (services must be recreated)."""
    reader = ControllerService(name="Reader", type="org.apache.nifi.json.JsonTreeReader")
    writer = ControllerService(name="Writer", type="org.apache.nifi.json.JsonRecordSetWriter")
    flow = Flow(name)
    flow.add_controller_service(reader, writer)
    flow.add_processor(Processor(
        name=proc_name,
        type="org.apache.nifi.processors.standard.ConvertRecord",
        properties={"Record Reader": reader, "Record Writer": writer},
        auto_terminate=["success", "failure"],
    ))
    return flow


@pytest.fixture()
def versioned(nifi_client):
    client = nifi_client
    reg_from_nifi, reg_from_host = _registry_urls(client)
    try:
        requests.get(f"{reg_from_host}/nifi-registry-api/buckets", timeout=3)
    except requests.RequestException:
        pytest.skip(f"NiFi Registry not reachable at {reg_from_host}")

    reg_id = client.create_registry_client(f"{NAME}-registry", reg_from_nifi)
    bucket_id = _bucket(reg_from_host, f"{NAME}-bucket")
    try:
        yield client, reg_id, bucket_id
    finally:
        for path, comp in list(client.walk_groups()):
            if comp["name"].startswith(NAME):
                client.delete_group(comp["id"])
        _bucket(reg_from_host, f"{NAME}-bucket")  # clear flows for the next run


def test_in_place_push_preserves_version_control(versioned):
    client, reg_id, bucket_id = versioned

    # --- push v1 and place under version control -----------------------------
    pg_id = client.push_flow(_convert_flow(f"{NAME}-flow", "ConvertA"))
    client.start_version_control(pg_id, reg_id, bucket_id, f"{NAME}-flow", "v1")
    assert client.version_control_state(pg_id) == "UP_TO_DATE"

    # --- edit + push again: must rebuild IN PLACE ----------------------------
    flow2 = _convert_flow(f"{NAME}-flow", "ConvertB")
    log = Processor(name="Log",
                    type="org.apache.nifi.processors.standard.LogAttribute",
                    auto_terminate=["success"])
    flow2.add_processor(log)
    flow2.add_connection(flow2.processors[0].to(log, relationships=["success"]))

    new_id = client.push_flow(flow2)
    assert new_id == pg_id, "push must rebuild in place, not delete-and-recreate"
    assert client.version_control_state(pg_id) == "LOCALLY_MODIFIED"

    # --- the new contents round-trip -----------------------------------------
    pulled = client.pull_flow(pg_id)
    assert {p.name for p in pulled.processors} == {"ConvertB", "Log"}
    assert len(pulled.connections) == 1
    assert {s.name for s in pulled.controller_services} == {"Reader", "Writer"}

    # --- the processor is actually wired to the recreated services -----------
    # Checked against the *live* flow, not the pulled snapshot: NiFi 1.x's
    # /download reports service-ref properties differently from 2.x (descriptor
    # flag off, value not matching the service identifier), so the live config
    # is the version-agnostic source of truth for wiring.
    svc_ids = {s["id"] for s in client._group_owned_services(pg_id)}
    live = client._get_json(
        f"/flow/process-groups/{pg_id}"
    )["processGroupFlow"]["flow"]
    convert = next(p["component"] for p in live["processors"]
                   if p["component"]["name"] == "ConvertB")
    refs = {k: v for k, v in convert["config"]["properties"].items()
            if k in ("Record Reader", "Record Writer")}
    assert refs and all(v in svc_ids for v in refs.values()), \
        "processor must be wired to the recreated group-level services"


def test_push_to_unversioned_group_still_recreates(nifi_client):
    """Sanity: without version control, the same push deletes-and-recreates
    (new id) — the in-place path is gated strictly on registry control."""
    client = nifi_client
    name = f"{NAME}-plain"
    try:
        first = client.push_flow(_convert_flow(name, "A"))
        second = client.push_flow(_convert_flow(name, "B"))
        assert first != second, "unversioned push should delete-and-recreate"
    finally:
        for path, comp in list(client.walk_groups()):
            if comp["name"] == name:
                client.delete_group(comp["id"])


def test_commit_saves_local_changes_as_a_new_version(versioned):
    client, reg_id, bucket_id = versioned

    pg_id = client.push_flow(_convert_flow(f"{NAME}-commit", "ConvertA"))
    client.start_version_control(pg_id, reg_id, bucket_id, f"{NAME}-commit", "v1")

    flow2 = _convert_flow(f"{NAME}-commit", "ConvertA")
    flow2.processors[0].scheduling_period = "5 sec"
    client.push_flow(flow2)
    assert client.version_control_state(pg_id) == "LOCALLY_MODIFIED"

    info = client.commit_version(pg_id, "tweaked scheduling via niflow")
    assert str(info.get("version")) not in ("1", "None")
    assert client.version_control_state(pg_id) == "UP_TO_DATE"


def test_commit_refuses_unversioned_group(nifi_client):
    pg_id = nifi_client.push_flow(_convert_flow(f"{NAME}-plain", "ConvertA"))
    try:
        with pytest.raises(ValueError, match="not under version control"):
            nifi_client.commit_version(pg_id)
    finally:
        nifi_client.delete_group(pg_id)
