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
