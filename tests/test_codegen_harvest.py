"""Codegen rulebook harvest: instantiate-read-delete + RELATIONSHIPS emission."""
from types import SimpleNamespace

from niflow.codegen import _emit_relationships, _harvest_rules, _trim_descriptors


def _dt(type_str, artifact="nifi-standard-nar"):
    return SimpleNamespace(
        type=type_str,
        bundle=SimpleNamespace(group="org.apache.nifi", artifact=artifact, version="2.7.2"),
    )


class FakeClient:
    """Just enough of NiFiClient for _harvest_rules: a temp group + create + delete."""

    def __init__(self):
        self.created = []        # processor types we instantiated
        self.deleted_group = None

    def root_id(self):
        return "root-id"

    def _pg_entity(self, pg_id):
        return {"revision": {"version": 9}}

    def _request(self, method, path, **kw):
        if method == "POST" and path == "/process-groups/root-id/process-groups":
            return _Resp({"id": "tmp-1"})
        if method == "POST" and path == "/process-groups/tmp-1/processors":
            ptype = kw["json"]["component"]["type"]
            self.created.append(ptype)
            # GenerateFlowFile only has 'success'; LogAttribute has 'success'+'failure'.
            rels = ["success"] if "Generate" in ptype else ["success", "failure"]
            return _Resp({"component": {
                "relationships": [{"name": r} for r in rels],
                "config": {"descriptors": {
                    "Log Level": {"required": True, "defaultValue": None},
                    "Note": {"required": False},  # trimmed away (no signal)
                }}}})
        if method == "DELETE" and path == "/process-groups/tmp-1":
            assert kw["params"]["version"] == 9  # used the live revision
            self.deleted_group = "tmp-1"
            return _Resp({})
        raise AssertionError(f"unexpected {method} {path}")


class _Resp:
    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body


def test_harvest_instantiates_each_type_and_deletes_the_temp_group():
    client = FakeClient()
    types = [
        _dt("org.apache.nifi.processors.standard.GenerateFlowFile"),
        _dt("org.apache.nifi.processors.standard.LogAttribute"),
    ]
    rules = _harvest_rules(client, types)

    assert client.created == [t.type for t in types]
    assert client.deleted_group == "tmp-1"  # cleaned up even on the happy path
    assert rules[types[0].type]["relationships"] == ["success"]
    assert rules[types[1].type]["relationships"] == ["success", "failure"]


def test_harvest_skips_uninstantiable_types_and_still_deletes_group():
    client = FakeClient()

    bad = _dt("org.apache.nifi.Restricted")
    # Make the create call blow up for the restricted type only.
    original = client._request

    def flaky(method, path, **kw):
        if path.endswith("/processors") and kw["json"]["component"]["type"] == bad.type:
            raise RuntimeError("403 restricted")
        return original(method, path, **kw)

    client._request = flaky
    rules = _harvest_rules(client, [bad, _dt("org.apache.nifi.processors.standard.LogAttribute")])

    assert bad.type not in rules                 # skipped, not fatal
    assert "org.apache.nifi.processors.standard.LogAttribute" in rules
    assert client.deleted_group == "tmp-1"       # group still torn down


def test_trim_descriptors_keeps_only_actionable_facts():
    descriptors = {
        "Directory": {"required": True, "defaultValue": None},
        "Free Text": {"required": False, "defaultValue": None},   # dropped (no signal)
        "Conflict": {"required": True, "defaultValue": "fail"},
        "Codec": {"allowableValues": [
            {"allowableValue": {"value": "none"}},
            {"allowableValue": {"value": "gzip"}}]},
        "Reader": {"identifiesControllerService": "o.a.n.RecordReaderFactory"},
        "Pattern": {"required": True, "dependencies": [
            {"propertyName": "Codec", "dependentValues": ["gzip"]}]},
    }
    trimmed = _trim_descriptors(descriptors)
    assert "Free Text" not in trimmed
    assert trimmed["Directory"] == {"required": True}
    assert trimmed["Conflict"] == {"required": True, "default": "fail"}
    assert trimmed["Codec"] == {"allowable": ["none", "gzip"]}
    assert trimmed["Reader"] == {"service": "o.a.n.RecordReaderFactory"}
    assert trimmed["Pattern"] == {
        "required": True,
        "dependencies": [{"property": "Codec", "values": ["gzip"]}],
    }


def test_harvest_captures_descriptors_alongside_relationships():
    client = FakeClient()
    rules = _harvest_rules(client, [_dt("org.apache.nifi.processors.standard.LogAttribute")])
    rule = rules["org.apache.nifi.processors.standard.LogAttribute"]
    assert rule["relationships"] == ["success", "failure"]
    assert rule["descriptors"] == {"Log Level": {"required": True}}


def test_emit_relationships_renders_importable_sorted_map():
    rules = {
        "b.Type": {"relationships": ["failure", "success"]},
        "a.Type": {"relationships": ["success"]},
    }
    ns: dict = {}
    exec(_emit_relationships(rules), ns)
    assert ns["RELATIONSHIPS"] == {
        "a.Type": ["success"],
        "b.Type": ["failure", "success"],
    }
    # Keys are emitted in sorted order for stable diffs.
    assert list(ns["RELATIONSHIPS"]) == ["a.Type", "b.Type"]


# --- relationship probes ----------------------------------------------------
# A processor's relationship set is not a per-type constant: a property value
# can switch one on (UpdateAttribute's "Store State" -> "set state fail", which
# NiFi 1.24 then refuses to start the processor without), and some types turn
# every dynamic property into a relationship. Neither is readable off the create
# response, and NiFi 1.x has no /flow/processor-definition endpoint to ask, so
# both are probed by PUTting the property and reading the answer back.

from niflow.codegen import (  # noqa: E402
    _DYNAMIC_PROBE_PROPERTY,
    _emit_conditional_relationships,
    _emit_dynamic_relationships,
    _emit_type_set,
    _probe_relationships,
)


class ProbeClient:
    """A processor whose relationships depend on 'Mode' and dynamic properties."""

    def __init__(self, *, dynamic=False, refuse=()):
        self.dynamic = dynamic
        self.refuse = set(refuse)
        self.puts = []
        self.properties = {}

    def _request(self, method, path, **kw):
        assert (method, path) == ("PUT", "/processors/p1")
        props = kw["json"]["component"]["config"]["properties"]
        self.puts.append(dict(props))
        for key, value in props.items():
            if value in self.refuse:
                raise RuntimeError("NiFi refused that value")
            if value is None:
                self.properties.pop(key, None)
            else:
                self.properties[key] = value
        rels = ["success"]
        if self.properties.get("Mode") == "split":
            rels = ["left", "right"]
        if self.dynamic:
            rels += [k for k in self.properties if k not in ("Mode", "Plain")]
        return _Resp({"revision": {"version": 1},
                      "component": {"relationships": [{"name": r} for r in sorted(rels)]}})

    def _get_json(self, path):
        return {"revision": {"version": 1}}


_CREATED = {"component": {"id": "p1"}, "revision": {"version": 0}}
_DESCRIPTORS = {
    "Mode": {"defaultValue": "whole", "allowableValues": [
        {"allowableValue": {"value": "whole"}}, {"allowableValue": {"value": "split"}}]},
    "Plain": {},  # no allowable values -> never probed
}


def test_probe_finds_a_relationship_a_property_switches_on():
    client = ProbeClient()
    dynamic, conditional = _probe_relationships(
        client, _CREATED, ["success"], _DESCRIPTORS)
    assert dynamic is False
    assert conditional == {"Mode": {"split": ["left", "right"]}}


def test_probe_does_not_record_a_value_that_changes_nothing():
    client = ProbeClient()
    _, conditional = _probe_relationships(
        client, _CREATED, ["success"],
        {"Mode": {"defaultValue": "whole", "allowableValues": [
            {"allowableValue": {"value": "whole"}}]}})
    assert conditional == {}


def test_probe_detects_dynamic_relationship_types():
    client = ProbeClient(dynamic=True)
    dynamic, _ = _probe_relationships(client, _CREATED, ["success"], {})
    assert dynamic is True
    # The probe property is removed again, so it can't pollute later probes.
    assert client.puts[-1] == {_DYNAMIC_PROBE_PROPERTY: None}


def test_probe_restores_the_default_between_properties():
    client = ProbeClient()
    _probe_relationships(client, _CREATED, ["success"], _DESCRIPTORS)
    assert {"Mode": None} in client.puts


def test_probe_survives_a_value_nifi_refuses():
    client = ProbeClient(refuse={"split"})
    dynamic, conditional = _probe_relationships(
        client, _CREATED, ["success"], _DESCRIPTORS)
    assert (dynamic, conditional) == (False, {})


def test_probe_skips_controller_service_properties():
    client = ProbeClient()
    _probe_relationships(client, _CREATED, ["success"], {
        "Reader": {"identifiesControllerService": "x", "allowableValues": [
            {"allowableValue": {"value": "abc-uuid"}}]}})
    assert client.puts == [{_DYNAMIC_PROBE_PROPERTY: "niflow"},
                           {_DYNAMIC_PROBE_PROPERTY: None}]


def test_probe_is_a_no_op_without_an_instance_id():
    assert _probe_relationships(None, {"component": {}}, ["success"], {}) == (False, {})


# --- emission ---------------------------------------------------------------

def test_emit_conditional_relationships_is_sorted_and_importable():
    rendered = _emit_conditional_relationships({
        "b.T": {"conditional_relationships": {"Mode": {"split": ["right", "left"]}}},
        "a.T": {"conditional_relationships": {}},
    })
    ns = {}
    exec(rendered, ns)
    assert ns["CONDITIONAL_RELATIONSHIPS"] == {"b.T": {"Mode": {"split": ("left", "right")}}}


def test_emit_dynamic_relationships_lists_only_confirmed_types():
    rendered = _emit_dynamic_relationships({
        "b.T": {"dynamic_relationships": True},
        "a.T": {"dynamic_relationships": False},
    })
    ns = {}
    exec(rendered, ns)
    assert ns["DYNAMIC_RELATIONSHIPS"] == frozenset({"b.T"})


def test_emit_type_set_records_types_with_no_properties_at_all():
    """Otherwise 'harvested, has nothing' reads as 'never harvested'."""
    rendered = _emit_type_set({"z.T": {"properties": []}, "a.T": {"properties": ["x"]}})
    ns = {}
    exec(rendered, ns)
    assert ns["TYPES"] == frozenset({"a.T", "z.T"})


def test_service_reference_allowable_values_are_not_recorded():
    """They are live instance UUIDs: fresh every run, so the catalog churned."""
    trimmed = _trim_descriptors({
        "Reader": {"identifiesControllerService": "org.apache.nifi.RecordReader",
                   "allowableValues": [
                       {"allowableValue": {"value": "1c43e0c1-01a0-1000-6337-7f5cad0afe9f"}}]},
        "Mode": {"allowableValues": [{"allowableValue": {"value": "whole"}}]},
    })
    assert "allowable" not in trimmed["Reader"]
    assert trimmed["Reader"]["service"] == "org.apache.nifi.RecordReader"
    assert trimmed["Mode"]["allowable"] == ["whole"]  # a real enum still kept
