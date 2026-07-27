"""Bundle (NAR) coordinate resolution for emitted flows.

NiFi resolves a component by ``(type, bundle)`` together. The bug these guard
against: emitting ``nifi-standard-nar`` for a type that lives elsewhere (notably
``UpdateAttribute`` in ``nifi-update-attribute-nar``) makes NiFi reject a real
type as "not a valid processor type".
"""
import json
from types import SimpleNamespace

from niflow.core import Flow, Processor
from niflow.formats.json_format import from_json, to_json
from niflow.processors.bundles import default_bundle

UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"
GET_FILE = "org.apache.nifi.processors.standard.GetFile"


def _proc_bundle(flow: Flow, name: str) -> dict:
    snapshot = json.loads(to_json(flow))
    for proc in snapshot["flowContents"]["processors"]:
        if proc["name"] == name:
            return proc["bundle"]
    raise AssertionError(f"no processor named {name!r} in snapshot")


def test_update_attribute_emits_its_own_nar_not_standard():
    flow = Flow("b")
    flow.add_processor(Processor(name="Tag", type=UPDATE_ATTRIBUTE))
    bundle = _proc_bundle(flow, "Tag")
    assert bundle["artifact"] == "nifi-update-attribute-nar"
    assert bundle["group"] == "org.apache.nifi"


def test_standard_processor_still_emits_standard_nar():
    flow = Flow("b")
    flow.add_processor(Processor(name="Get", type=GET_FILE))
    assert _proc_bundle(flow, "Get")["artifact"] == "nifi-standard-nar"


def test_non_standard_bundle_round_trips_to_none():
    """A bundle-less UpdateAttribute emits its NAR, then collapses back to None.

    Round-trip stability: a Python-built processor has ``bundle=None``; emit
    fills the correct NAR; parse must recognise it as the default and restore
    ``None`` rather than pinning an explicit bundle.
    """
    flow = Flow("b")
    flow.add_processor(Processor(name="Tag", type=UPDATE_ATTRIBUTE))
    back = from_json(to_json(flow))
    assert back.processors[0].bundle is None


def test_default_bundle_falls_back_to_standard_for_unknown_type():
    assert default_bundle("com.example.Whatever")["artifact"] == "nifi-standard-nar"


def test_default_bundle_uses_curated_override():
    assert default_bundle(UPDATE_ATTRIBUTE)["artifact"] == "nifi-update-attribute-nar"


def test_codegen_emits_importable_bundles_map():
    """The generated BUNDLES block is valid Python mapping type -> coordinates."""
    from niflow.codegen import _emit_bundles

    named = [
        ("UpdateAttribute", SimpleNamespace(
            type=UPDATE_ATTRIBUTE,
            bundle=SimpleNamespace(group="org.apache.nifi",
                                   artifact="nifi-update-attribute-nar",
                                   version="2.7.2"))),
    ]
    ns: dict = {}
    exec(_emit_bundles(named), ns)
    assert ns["BUNDLES"][UPDATE_ATTRIBUTE] == {
        "group": "org.apache.nifi",
        "artifact": "nifi-update-attribute-nar",
        "version": "2.7.2",
    }
