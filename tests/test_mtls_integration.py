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
