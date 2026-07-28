"""Client-certificate auth against the local mTLS NiFi (make nifi-mtls-up).

Marked integration; skipped unless the generated cert config exists.
"""
from pathlib import Path

import pytest

from niflow import Flow, Processor
from niflow.client import NiFiClient
from niflow.config import NiFiConfig
from niflow.doctor import FAIL, run_checks

CONFIG = Path("certs/mtls/niflow.env")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not CONFIG.is_file(), reason="run `make nifi-mtls-up` first"),
]


@pytest.fixture(scope="module")
def config() -> NiFiConfig:
    return NiFiConfig.from_env(str(CONFIG))


@pytest.fixture(scope="module", autouse=True)
def _root_policies(config):
    """Grant the cert identity root-group policies (idempotent).

    NiFi's initial admin gets tenant/policy/controller rights but NOT
    view/modify on the root process group — exactly what a work admin has
    to grant your identity too. We hold the /policies write policy, so we
    can do it over the API.
    """
    from niflow.client import NiFiApiError

    client = NiFiClient(config)
    root = client.root_id()
    users = client._get_json("/tenants/search-results?q=").get("users", [])
    user_id = next(
        u["id"] for u in users
        if u.get("component", {}).get("identity") == "CN=niflow-client"
    )
    for action in ("read", "write"):
        for resource in (f"/process-groups/{root}", f"/data/process-groups/{root}"):
            try:
                client._request("POST", "/policies", json={
                    "revision": {"version": 0},
                    "component": {
                        "resource": resource,
                        "action": action,
                        "users": [{"revision": {"version": 0}, "id": user_id}],
                    },
                })
            except NiFiApiError:
                pass  # already exists from a prior run


def test_doctor_all_green_over_cert_auth(config):
    checks = run_checks(config)
    problems = [c for c in checks if c.status == FAIL]
    assert not problems, problems
    assert any("cert" in c.detail for c in checks if c.title == "configuration")


def test_push_pull_round_trip_over_cert_auth(config):
    client = NiFiClient(config)
    flow = Flow("MtlsProbe")
    flow.add_processor(Processor(
        name="Gen", type="org.apache.nifi.processors.standard.GenerateFlowFile",
        auto_terminate=["success"],
    ))
    try:
        client.push_flow(flow)
        pulled = client.pull_flow("MtlsProbe")
        assert [p.name for p in pulled.processors] == ["Gen"]
        _, _, changes = client.plan_flow(pulled)
        assert changes == []
    finally:
        client.delete_group("MtlsProbe")
