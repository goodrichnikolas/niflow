"""The flow-test harness against a REAL NiFi: inject data, watch it transform.

Runs the kitchen-sink example's own test cases — CSV in at two different
entry points (a mid-flow processor and a nested group's input port), JSON out
at the audit tap after crossing three group levels and a record conversion.
This is the "does this flow actually DO the right thing to a file" loop,
fully automated.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "examples"))

from kitchen_sink import build_flow, tests as kitchen_sink_cases  # noqa: E402

from niflow.testing import FlowTester, TestCase, as_cases, format_results  # noqa: E402

pytestmark = pytest.mark.integration


def test_kitchen_sink_cases_pass_live(nifi_client):
    flow = build_flow("NiflowHarnessE2E")
    with FlowTester(nifi_client, flow, sandbox_name="NiflowHarnessE2E-box") as tester:
        results = tester.run(as_cases(list(kitchen_sink_cases)))
    assert all(r.passed for r in results), "\n" + format_results(results)
    # Sandbox is gone again.
    with pytest.raises(ValueError):
        nifi_client.resolve_group("NiflowHarnessE2E-box")


def test_wrong_expectation_fails_cleanly_live(nifi_client):
    flow = build_flow("NiflowHarnessNeg")
    bad = TestCase(
        name="expects the wrong mime type",
        inject_at="Stamp",
        content="id,priority\n1,urgent\n",
        expect_at="Audit",
        expect_attributes={"mime.type": "text/csv"},
        timeout=45,
    )
    with FlowTester(nifi_client, flow, sandbox_name="NiflowHarnessNeg-box") as tester:
        (result,) = tester.run([bad])
    assert not result.passed
    assert any("mime.type" in f for f in result.failures)
    # The FlowFile itself DID arrive — only the expectation was wrong.
    assert result.flowfiles and result.flowfiles[0]["attributes"]["priority"] == "urgent"
