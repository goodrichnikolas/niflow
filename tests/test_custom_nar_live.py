"""Live tests against a REAL custom NAR — the last untested T7h axis.

niflow's rulebooks are harvested from stock Apache containers, so every type
they know is a type Apache ships. Work runs its own NARs, and "what does niflow
do with a type it has never seen" could only be answered by reading the code
until this existed.

``scripts/custom-nar/build.sh`` compiles one processor against the nifi-api jar
taken out of the NiFi image and packages it as a NAR;
``make nifi1-up`` builds it and mounts ``.nifi-nars/`` into the 1.24
container's ``extensions/`` directory, which NiFi 1.9+ hot-loads. The whole
module skips when the type is not on the server, so it costs nothing elsewhere.

    make nifi1-up nifi1-wait
    NIFLOW_NIFI_HOST=https://localhost:8444/nifi-api pytest -m integration \\
        tests/test_custom_nar_live.py

What it found: a custom NAR's **sensitive property drifted forever** — the
catalogs are the only thing that knew which properties NiFi will not read back,
and they have never seen a custom type, so every plan re-proposed the password
and ``niflow drift`` failed in CI for good. The server knew all along: the
snapshot's own ``propertyDescriptors`` say ``sensitive: true``, and
``from_json`` now reads them.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "flows"))

from niflow import Flow  # noqa: E402
from niflow.core import Processor  # noqa: E402
from niflow.follow import FlowFollower  # noqa: E402
from niflow.plan import only_unknowable  # noqa: E402
from niflow.validate import unchecked_types, validate_flow  # noqa: E402

pytestmark = pytest.mark.integration

CUSTOM = "com.niflow.test.NiflowStamp"
GEN = "org.apache.nifi.processors.standard.GenerateFlowFile"
LOG = "org.apache.nifi.processors.standard.LogAttribute"
GROUP = "NiflowCustomNarLive"


@pytest.fixture(scope="module")
def custom(nifi_client):
    """A client whose server actually runs the test NAR, or a skip."""
    types = {t["type"] for t in
             nifi_client._get_json("/flow/processor-types")["processorTypes"]}
    if CUSTOM not in types:
        pytest.skip(
            f"{CUSTOM} is not on {nifi_client.base} — build and mount it with "
            "'make nifi1-up' (scripts/custom-nar/build.sh)")
    return nifi_client


def _flow(name=GROUP, secret=None, stamp="from-niflow"):
    flow = Flow(name)
    gen = Processor(name="Gen", type=GEN,
                    properties={"File Size": "0B",
                                "generate-ff-custom-text": "custom"},
                    scheduling_period="60 sec")
    props = {"Stamp Value": stamp, "extra.dynamic": "yes"}
    if secret is not None:
        props["Stamp Secret"] = secret
    stamper = Processor(name="Stamp", type=CUSTOM, properties=props,
                        auto_terminate=["failure"])
    sink = Processor(name="Sink", type=LOG, properties={"Log Level": "info"},
                     auto_terminate=["success"])
    flow.add_processor(gen, stamper, sink)
    flow.add_connection(gen >> stamper, stamper >> sink)
    return flow


@pytest.fixture()
def deployed(custom):
    flow = _flow()
    custom.push_flow(flow)
    yield custom, flow
    custom.delete_group(GROUP)


# ------------------------------------------------------------ it lands right


def test_a_custom_types_properties_land_on_real_descriptors(deployed):
    """The failure this whole axis was about: an inert dynamic property.

    When niflow does not know a type it sends the property keys untranslated.
    If those keys were wrong, NiFi would file them away as *dynamic*
    properties, the real ones would run at their defaults, and nothing would
    say so. They are not wrong — a custom NAR's properties are already in the
    server's own namespace — and this pins that.
    """
    client, _ = deployed
    pg_id = client.resolve_group(GROUP)
    proc = next(p for p in client.find_processors(group=pg_id)
                if p["name"] == "Stamp")
    config = client._get_json(f"/processors/{proc['id']}")["component"]["config"]

    assert config["properties"]["Stamp Value"] == "from-niflow"
    assert config["descriptors"]["Stamp Value"]["dynamic"] is False
    # And a genuinely dynamic property still lands as one.
    assert config["descriptors"]["extra.dynamic"]["dynamic"] is True
    assert config["properties"]["extra.dynamic"] == "yes"


def test_a_flow_with_a_custom_type_converges(deployed):
    client, flow = deployed
    _, _, changes = client.plan_flow(flow)
    assert changes == [], f"plan did not converge on a custom type: {changes}"


def test_a_custom_type_round_trips_through_python(deployed, tmp_path):
    """pull → to_python → import → plan, on a type no catalog knows."""
    from niflow.formats.python_format import to_python

    client, _ = deployed
    pulled = client.pull_flow(GROUP)
    assert any(p.type == CUSTOM for p in pulled.processors)

    module = tmp_path / "pulled_custom.py"
    module.write_text(to_python(pulled), encoding="utf-8")
    namespace = {}
    exec(compile(module.read_text(), str(module), "exec"), namespace)  # noqa: S102
    reimported = namespace["flow"]

    assert {p.type for p in reimported.processors} == {p.type for p in pulled.processors}
    _, _, changes = client.plan_flow(reimported)
    assert changes == []


# ------------------------------------------------- the secret it cannot read


def test_a_custom_types_sensitive_property_is_not_eternal_drift(custom):
    """The bug: `niflow drift` failed forever on any custom processor with a
    password, which is most of them.

    NiFi never reads a sensitive value back, so the model's value differs from
    the live flow on every single run. For types Apache ships, the catalog
    knows which properties those are; for a custom NAR nothing did — until
    from_json started reading the snapshot's own descriptors.
    """
    flow = _flow(name="NiflowCustomSecretLive", secret="hunter2")
    custom.push_flow(flow)
    try:
        _, live, changes = custom.plan_flow(flow)

        stamp = next(p for p in live.processors if p.name == "Stamp")
        assert stamp.sensitive_keys == ["Stamp Secret"], (
            "the server's own descriptors should say which key is sensitive")

        assert len(changes) == 1, changes
        change = changes[0]
        assert change.unknowable == ("properties[Stamp Secret]",)
        # This is what stops `niflow drift` crying wolf in CI for good.
        assert only_unknowable(change) is True
        assert "never returns Stamp Secret" in (change.note or "")
    finally:
        custom.delete_group("NiflowCustomSecretLive")


def test_the_secret_is_never_read_back_from_the_server(custom):
    """Belt and braces: the reason the change is unknowable is real."""
    flow = _flow(name="NiflowCustomSecretRead", secret="hunter2")
    pg_id = custom.push_flow(flow)
    try:
        proc = next(p for p in custom.find_processors(group=pg_id)
                    if p["name"] == "Stamp")
        config = custom._get_json(f"/processors/{proc['id']}")["component"]["config"]
        assert config["descriptors"]["Stamp Secret"]["sensitive"] is True
        assert config["properties"].get("Stamp Secret") in (None, "********")
    finally:
        custom.delete_group(pg_id)


# ------------------------------------------------------- saying "I can't tell"


def test_validate_says_the_custom_type_was_not_checked(custom):
    """"No issues" and "I could not look" are different answers."""
    flow = _flow(name="NiflowCustomValidate")
    assert validate_flow(flow) == [], "no false positives on an unknown type"

    unchecked = unchecked_types(flow)
    assert [entry["type"] for entry in unchecked] == [CUSTOM]
    assert unchecked[0]["component"].endswith("/Stamp")


def test_the_push_warns_that_it_cannot_translate_the_type(custom, caplog):
    """The 1.x push warning that names a custom NAR, and what to do about it."""
    from niflow.rest.flows import _warn_untranslatable_types

    flow = _flow(name="NiflowCustomWarn")
    blind = _warn_untranslatable_types(flow, major=1, host=custom.base)
    assert blind == [CUSTOM]


def test_harvesting_the_server_teaches_niflow_the_custom_type(custom):
    """The advice the warning gives has to actually work.

    "Harvest that server once" is niflow's answer to a custom NAR; if the
    harvest could not see the type, the advice would be a dead end. One type,
    so this is a couple of REST calls rather than the full sweep.
    """
    from niflow.codegen import _harvest_rules

    types = [t for t in custom._get_json("/flow/processor-types")["processorTypes"]
             if t["type"] == CUSTOM]
    harvested = _harvest_rules(custom, types)

    assert CUSTOM in harvested, "the harvest did not see the custom type"
    rules = harvested[CUSTOM]
    assert sorted(rules["relationships"]) == ["failure", "success"]
    assert rules["descriptors"]["Stamp Secret"]["sensitive"] is True
    assert rules["descriptors"]["Stamp Value"]["default"] == "stamped"
    assert rules["descriptors"]["Stamp Value"]["required"] is True


# ------------------------------------------------------- it still steps


def test_the_stepper_follows_a_flowfile_through_a_custom_processor(deployed):
    """The debugger has no opinion about who wrote the processor."""
    client, _ = deployed
    follower = FlowFollower(client, GROUP, poll_timeout=10.0)
    follower.quiesce()

    follower.inject("Stamp", content="custom-fixture", attributes={"case": "x"})
    try:
        outcome = follower.step()
        assert outcome["status"] == "advanced", outcome.get("message")
        hop = outcome["hops"][0]
        assert hop["component"] == "Stamp"
        # The attribute the custom processor's own onTrigger writes.
        assert hop["attributes"]["niflow.stamp"] == "from-niflow"
        assert hop["attributes"]["case"] == "x"
    finally:
        follower.cleanup_injector()
