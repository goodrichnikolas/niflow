"""Live dry-run validation: NiFi's own verdict on a flow, via a sandbox."""
import pytest

from niflow import Flow, Processor

pytestmark = pytest.mark.integration


def test_live_dry_run_flags_only_the_invalid_processor(nifi_client):
    flow = Flow("NiflowValidateE2E")
    flow.add_processor(Processor(
        name="BadPut", type="org.apache.nifi.processors.standard.PutFile",
        auto_terminate=["success", "failure"],  # missing required 'Directory'
    ))
    flow.add_processor(Processor(
        name="Gen", type="org.apache.nifi.processors.standard.GenerateFlowFile",
        auto_terminate=["success"],
    ))
    errors = nifi_client.validate_flow_live(flow)
    by_name = {e["name"]: e for e in errors}
    assert "BadPut" in by_name and "Gen" not in by_name
    assert any("Directory" in msg for msg in by_name["BadPut"]["errors"])
    # The sandbox is gone again.
    with pytest.raises(ValueError):
        nifi_client.resolve_group("NiflowValidateE2E (niflow-validate)")
