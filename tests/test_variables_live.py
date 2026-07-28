"""1.x variable-registry incremental apply against a real NiFi.

Skipped on 2.x, which removed the variable registry entirely.
"""
import pytest

from niflow import Flow, Processor

pytestmark = pytest.mark.integration

GROUP = "NiflowVarsE2E"


def test_variable_changes_apply_incrementally(nifi_client):
    if nifi_client._major_version() >= 2:
        pytest.skip("NiFi 2.x has no variable registry")

    flow = Flow(GROUP)
    flow.variables = {"env": "dev", "old": "x"}
    flow.add_processor(Processor(
        name="Gen", type="org.apache.nifi.processors.standard.GenerateFlowFile",
        auto_terminate=["success"],
    ))
    nifi_client.push_flow(flow)
    try:
        pulled = nifi_client.pull_flow(GROUP)
        assert pulled.variables == {"env": "dev", "old": "x"}

        pulled.variables = {"env": "prod", "new": "y"}  # change + add + delete
        changes = nifi_client.push_update(pulled)
        assert any(c.kind == "group_settings" for c in changes)

        live = nifi_client.pull_flow(GROUP)
        assert live.variables == {"env": "prod", "new": "y"}
        _, _, converged = nifi_client.plan_flow(live)
        assert converged == []
    finally:
        nifi_client.delete_group(GROUP)
