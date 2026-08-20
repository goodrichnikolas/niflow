"""The 1.x side of the rulebook: relationships, primary-node pinning, and the
version-aware differ.

niflow's catalogs are harvested from 2.x; the estate this tool exists for runs
1.24. Every gap between those two used to fail *silently on the server* — the
push succeeds and the flow is simply wrong — so each fact here is pinned:

* relationships are per NiFi line, and some are switched on by a property
  value (``UpdateAttribute``'s ``set state fail``);
* ``@PrimaryNodeOnly`` covers three types that exist only on 1.x;
* the differ reads defaults from the *target* line, so a property that only
  exists there is not mistaken for an unwanted extra;
* a type the 1.x harvest saw but which has no properties at all is *known*,
  not a hole in the compat data.

No live NiFi anywhere in this file — everything below reads the committed
catalogs, or a synthetic table where the point is the rule and not the data.
"""
from __future__ import annotations

import pytest

from niflow.core import ControllerService, Flow, Processor
from niflow.plan import _infer_target_major, diff_flows
from niflow.processors import rules
from niflow.validate import validate_flow

UPDATE_ATTRIBUTE = "org.apache.nifi.processors.attributes.UpdateAttribute"
ROUTE_ON_ATTRIBUTE = "org.apache.nifi.processors.standard.RouteOnAttribute"
CONSUME_AMQP = "org.apache.nifi.amqp.processors.ConsumeAMQP"
CONSUME_JMS = "org.apache.nifi.jms.processors.ConsumeJMS"
QUERY_RECORD = "org.apache.nifi.processors.standard.QueryRecord"
FORK_ENRICHMENT = "org.apache.nifi.processors.standard.ForkEnrichment"
LIST_HDFS = "org.apache.nifi.processors.hadoop.ListHDFS"
PUT_FILE = "org.apache.nifi.processors.standard.PutFile"


# --- conditional relationships ---------------------------------------------

def test_store_state_switches_on_a_relationship():
    """The live-proved case: NiFi 1.24 refuses to start the processor without it."""
    base = rules.relationships_for(UPDATE_ATTRIBUTE, None, 1)
    stateful = rules.relationships_for(
        UPDATE_ATTRIBUTE, {"Store State": "Store state locally"}, 1)
    assert "set state fail" not in base
    assert "set state fail" in stateful


def test_conditional_relationship_is_flagged_by_validate():
    flow = Flow("f")
    proc = Processor(name="U", type=UPDATE_ATTRIBUTE,
                     properties={"Store State": "Store state locally"})
    proc.auto_terminate = ["success"]
    flow.add_processor(proc)
    messages = [i["message"] for i in validate_flow(flow)]
    assert any("set state fail" in m for m in messages)


def test_handling_the_conditional_relationship_is_clean():
    flow = Flow("f")
    proc = Processor(name="U", type=UPDATE_ATTRIBUTE,
                     properties={"Store State": "Store state locally"})
    proc.auto_terminate = ["success", "set state fail"]
    flow.add_processor(proc)
    assert validate_flow(flow) == []


def test_unset_conditional_property_does_not_invent_a_relationship():
    flow = Flow("f")
    proc = Processor(name="U", type=UPDATE_ATTRIBUTE)
    proc.auto_terminate = ["success"]
    flow.add_processor(proc)
    assert validate_flow(flow) == []


def test_a_conditional_override_replaces_the_base_set():
    """RouteOnAttribute's non-default strategies collapse to matched/unmatched."""
    default = rules.relationships_for(ROUTE_ON_ATTRIBUTE, {"hot": "x"}, 1)
    switched = rules.relationships_for(
        ROUTE_ON_ATTRIBUTE,
        {"Routing Strategy": "Route to 'match' if all match", "hot": "x"}, 1)
    assert default == ["unmatched"]
    assert sorted(switched) == ["matched", "unmatched"]


def test_dynamic_relationships_were_harvested_not_only_curated():
    from niflow.processors import catalog

    assert ROUTE_ON_ATTRIBUTE in catalog.DYNAMIC_RELATIONSHIPS
    assert PUT_FILE not in catalog.DYNAMIC_RELATIONSHIPS
    assert rules.supports_dynamic_relationships(ROUTE_ON_ATTRIBUTE)
    assert not rules.supports_dynamic_relationships(PUT_FILE)


def test_curated_dynamic_types_survive_a_harvest_that_missed_them():
    """RouteHL7 is curated and the 1.24 probe does not confirm it — keep it."""
    assert rules.supports_dynamic_relationships(
        "org.apache.nifi.processors.hl7.RouteHL7")


def test_relationship_lookup_prefers_the_target_line(monkeypatch):
    monkeypatch.setattr(rules, "_catalog_table",
                        lambda name: {"T": ["success"]} if name == "RELATIONSHIPS" else None)
    monkeypatch.setattr(rules, "_compat_table",
                        lambda name: {"T": ["success", "v1 only"]}
                        if name == "RELATIONSHIPS" else None)
    assert rules.relationships_for("T", None, 1) == ["success", "v1 only"]
    assert rules.relationships_for("T", None, 2) == ["success"]
    assert rules.relationships_for("T") == ["success"]


def test_a_relationship_only_the_other_line_has_is_not_called_nonexistent():
    """Auto-terminating a 2.x relationship must not be flagged on a 1.x target."""
    flow = Flow("f")
    proc = Processor(name="U", type=UPDATE_ATTRIBUTE,
                     properties={"Store State": "Store state locally"})
    proc.auto_terminate = ["success", "set state fail"]
    flow.add_processor(proc)
    messages = [i["message"] for i in validate_flow(flow, target_version="2.7")]
    assert not any("does not exist" in m for m in messages)


# --- primary-node-only, 1.x twin -------------------------------------------

@pytest.mark.parametrize("type_str", [
    LIST_HDFS,
    "org.apache.nifi.processors.standard.GetJMSTopic",
    "org.apache.nifi.processors.azure.storage.ListAzureBlobStorage",
])
def test_one_x_only_primary_node_types_are_known(type_str):
    from niflow.processors import catalog

    assert type_str not in catalog.PRIMARY_NODE_ONLY, "2.x does not ship this type"
    assert rules.primary_node_only(type_str), "the 1.x twin must still answer"


def test_primary_node_only_is_still_false_for_ordinary_types():
    assert not rules.primary_node_only(PUT_FILE)


def test_the_two_lines_agree_on_every_shared_primary_node_type():
    """The union is only safe while neither line pins a type the other doesn't."""
    from niflow.processors import catalog, compat_v1

    shared = set(catalog.TYPES) & set(compat_v1.TYPES)
    assert ((catalog.PRIMARY_NODE_ONLY ^ compat_v1.PRIMARY_NODE_ONLY) & shared) == set()


# --- the compat "hole" that was really a zero-property type -----------------

@pytest.mark.parametrize("type_str", [
    FORK_ENRICHMENT,
    "org.apache.nifi.processors.email.ExtractEmailAttachments",
    "org.apache.nifi.lookup.SimpleKeyValueLookupService",
])
def test_a_property_less_type_is_known_not_a_hole(type_str):
    assert rules.harvested_on_v1(type_str)
    # "Nothing to rename", not "no idea" — the latter silently disabled the
    # whole cross-version translation for these types.
    assert rules.property_renames_for(type_str) == {}


def test_a_type_absent_from_one_x_is_still_reported_as_unknown():
    """DeleteSFTP does not exist on 1.24 at all; that is a different answer."""
    assert not rules.harvested_on_v1("org.apache.nifi.processors.standard.DeleteSFTP")
    assert rules.property_renames_for(
        "org.apache.nifi.processors.standard.DeleteSFTP") is None


# --- the version-aware differ ----------------------------------------------

def _amqp_pair():
    live = Flow("G")
    live.add_processor(Processor(
        name="A", type=CONSUME_AMQP,
        properties={"ssl-client-auth": "NONE", "auto.acknowledge": "false"}))
    desired = Flow("G")
    desired.add_processor(Processor(
        name="A", type=CONSUME_AMQP,
        properties={"Auto-Acknowledge Messages": "false"}))
    return live, desired


def test_a_one_x_only_property_at_its_default_is_not_drift():
    live, desired = _amqp_pair()
    assert diff_flows(live, desired) == []


def test_a_one_x_only_property_the_user_really_changed_is_still_drift():
    live, desired = _amqp_pair()
    live.processors[0].properties["ssl-client-auth"] = "REQUIRED"
    fields = diff_flows(live, desired)[0].fields
    assert fields["properties[ssl-client-auth]"] == ("REQUIRED", None)


def test_the_target_line_is_inferred_from_the_live_snapshot():
    live, _ = _amqp_pair()
    assert _infer_target_major(live) == 1


def test_a_two_x_snapshot_infers_nothing_and_is_judged_as_before():
    live = Flow("G")
    live.add_processor(Processor(
        name="A", type=CONSUME_AMQP,
        properties={"Auto-Acknowledge Messages": "false", "Batch Size": "10"}))
    assert _infer_target_major(live) is None


def test_a_type_that_exists_only_on_one_x_identifies_the_line():
    live = Flow("G")
    live.add_processor(Processor(name="A", type=LIST_HDFS,
                                 properties={"Recurse Subdirectories": "true"}))
    assert _infer_target_major(live) == 1


def test_descriptors_for_target_prefers_the_targets_own_default():
    fetch = "org.apache.nifi.processors.gcp.drive.FetchGoogleDrive"
    key = "Google Spreadsheet Export Type"
    assert rules.descriptors_for_target(fetch)[key]["default"] == "application/pdf"
    assert rules.descriptors_for_target(fetch, 1)[key]["default"] == "text/csv"


def test_descriptors_for_target_adds_properties_only_the_target_has():
    assert "cache-schema" not in rules.descriptors_for_target(QUERY_RECORD)
    assert rules.descriptors_for_target(QUERY_RECORD, 1)["cache-schema"]["default"] == "true"


def test_a_service_reference_still_diffs_across_lines():
    """The version-aware defaults must not swallow a real service change."""
    svc = ControllerService(name="SSL", type="org.apache.nifi.ssl.StandardSSLContextService")
    live = Flow("G")
    live.add_controller_service(svc)
    live.add_processor(Processor(name="A", type=CONSUME_AMQP,
                                 properties={"ssl-client-auth": "NONE"}))
    desired = Flow("G")
    desired.add_controller_service(svc)
    desired.add_processor(Processor(name="A", type=CONSUME_AMQP,
                                    properties={"SSL Context Service": svc}))
    changes = diff_flows(live, desired)
    assert any("SSL Context Service" in key
               for change in changes for key in change.fields)


def test_explicit_target_beats_inference():
    """Callers that know the server version (push does) can just say so."""
    live = Flow("G")
    live.add_processor(Processor(name="A", type=CONSUME_JMS,
                                 properties={"Session Cache size": "1"}))
    desired = Flow("G")
    desired.add_processor(Processor(name="A", type=CONSUME_JMS))
    assert diff_flows(live, desired, 1) == []
    # Told it is a 2.x server, the same 1.x-only key IS an unwanted extra.
    assert diff_flows(live, desired, 2) != []
