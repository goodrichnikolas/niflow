"""Pre-push static validation of flows (unhandled relationships, etc.)."""
import pytest

from niflow.core import Flow, Processor
from niflow.validate import validate_flow

# A type the catalog hasn't harvested -> exercises the heuristic fallback.
UNKNOWN = "org.apache.nifi.processors.standard.X"


def _proc(name, type=UNKNOWN):
    return Processor(name=name, type=type)


@pytest.fixture()
def known_relationships(monkeypatch):
    """Pretend the catalog harvested relationships for type 'Known'."""
    table = {"Known": ["success", "failure"]}
    monkeypatch.setattr(
        "niflow.validate.relationships_for", lambda t: table.get(t)
    )


@pytest.fixture()
def known_descriptors(monkeypatch):
    """Pretend the catalog harvested property descriptors for type 'Known'."""
    table = {"Known": {
        "Directory": {"required": True},
        "Compression": {"allowable": ["none", "gzip"]},
        "Conflict": {"required": True, "default": "fail"},
        "Pattern": {"required": True, "dependencies": [
            {"property": "Compression", "values": ["gzip"]}]},
    }}
    # All 'Known' processors have their relationships handled in these tests.
    monkeypatch.setattr("niflow.validate.relationships_for", lambda t: None)
    monkeypatch.setattr("niflow.validate.descriptors_for", lambda t: table.get(t))


def test_dangling_terminal_processor_is_flagged():
    # Gen -> Log; Log has no outgoing connection and nothing auto-terminated.
    flow = Flow("f")
    gen, log = _proc("Gen"), _proc("Log")
    flow.add_processor(gen, log)
    flow.add_connection(gen >> log)
    issues = validate_flow(flow)
    assert [i["component"] for i in issues] == ["f/Log"]
    assert "unhandled" in issues[0]["message"]


def test_fully_handled_flow_is_clean():
    flow = Flow("f")
    gen, sink = _proc("Gen"), _proc("Sink")
    sink.auto_terminate = ["success", "failure"]
    flow.add_processor(gen, sink)
    flow.add_connection(gen >> sink)
    assert validate_flow(flow) == []


def test_relationship_both_connected_and_auto_terminated_is_flagged():
    flow = Flow("f")
    gen, sink = _proc("Gen"), _proc("Sink")
    # 'success' is wired to the sink AND listed as auto-terminated on the source.
    gen.auto_terminate = ["success"]
    sink.auto_terminate = ["success", "failure"]
    flow.add_processor(gen, sink)
    flow.add_connection(gen >> sink)
    issues = validate_flow(flow)
    assert any(i["component"] == "f/Gen" and "both connected" in i["message"]
               for i in issues)


def test_validation_recurses_into_child_groups():
    flow = Flow("f")
    with flow.process_group("Child") as child:
        child.add_processor(_proc("Lonely"))  # no connections, no auto-terminate
    issues = validate_flow(flow)
    assert [i["component"] for i in issues] == ["f/Child/Lonely"]


def test_flow_validate_method_matches_module():
    flow = Flow("f")
    flow.add_processor(_proc("Lonely"))
    assert flow.validate() == validate_flow(flow)


# --- precise checks once a type's relationships are harvested ---------------

def test_dangling_failure_caught_even_when_success_is_wired(known_relationships):
    # The heuristic misses this (success IS handled); the harvested set catches it.
    flow = Flow("f")
    src, dst = _proc("Src", type="Known"), _proc("Dst")
    flow.add_processor(src, dst)
    flow.add_connection(src >> dst)  # wires 'success' only
    issues = [i for i in validate_flow(flow) if i["component"] == "f/Src"]
    assert len(issues) == 1
    assert "'failure' is not connected or auto-terminated" in issues[0]["message"]


def test_known_type_fully_handled_is_clean(known_relationships):
    flow = Flow("f")
    src, dst = _proc("Src", type="Known"), _proc("Dst")
    src.auto_terminate = ["failure"]
    flow.add_processor(src, dst)
    flow.add_connection(src >> dst)  # success wired, failure terminated
    assert [i for i in validate_flow(flow) if i["component"] == "f/Src"] == []


def test_auto_terminating_a_nonexistent_relationship_is_flagged(known_relationships):
    flow = Flow("f")
    p = _proc("Src", type="Known")
    p.auto_terminate = ["success", "failure", "bogus"]
    flow.add_processor(p)
    msgs = [i["message"] for i in validate_flow(flow)]
    assert any("'bogus' does not exist" in m for m in msgs)


# --- property checks from harvested descriptors -----------------------------

def _known(name, **props):
    return Processor(name=name, type="Known", properties=props)


def test_missing_required_property_is_flagged(known_descriptors):
    flow = Flow("f")
    flow.add_processor(_known("P", Compression="gzip"))  # no 'Directory'
    msgs = [i["message"] for i in validate_flow(flow)]
    assert "required property 'Directory' is not set" in msgs


def test_required_property_with_default_is_satisfied(known_descriptors):
    flow = Flow("f")
    flow.add_processor(_known("P", Directory="/out"))  # 'Conflict' has a default
    msgs = [i["message"] for i in validate_flow(flow)]
    assert not any("Conflict" in m for m in msgs)


def test_value_outside_allowable_set_is_flagged(known_descriptors):
    flow = Flow("f")
    flow.add_processor(_known("P", Directory="/out", Compression="zstd"))
    msgs = [i["message"] for i in validate_flow(flow)]
    assert any("'Compression' = 'zstd' is not one of" in m for m in msgs)


def test_expression_language_value_is_not_judged(known_descriptors):
    flow = Flow("f")
    flow.add_processor(_known("P", Directory="/out", Compression="${codec}"))
    assert not any("Compression" in i["message"] for i in validate_flow(flow))


def test_dependent_required_property_inactive_until_dependency_met(known_descriptors):
    # 'Pattern' is only required when Compression == 'gzip'.
    flow = Flow("f")
    flow.add_processor(_known("P", Directory="/out", Compression="none"))
    assert not any("Pattern" in i["message"] for i in validate_flow(flow))


def test_dependent_required_property_enforced_when_dependency_met(known_descriptors):
    flow = Flow("f")
    flow.add_processor(_known("P", Directory="/out", Compression="gzip"))
    msgs = [i["message"] for i in validate_flow(flow)]
    assert "required property 'Pattern' is not set" in msgs


# --- targeted value rules (durations, cron, numerics) -----------------------

def test_invalid_time_duration_is_flagged():
    flow = Flow("f")
    flow.add_processor(Processor(name="P", type=UNKNOWN, scheduling_period="5 sekonds",
                                 auto_terminate=["x"]))
    msgs = [i["message"] for i in validate_flow(flow)]
    assert any("scheduling period '5 sekonds' is not a valid time duration" in m
               for m in msgs)


def test_valid_durations_pass():
    flow = Flow("f")
    p = Processor(name="P", type=UNKNOWN, scheduling_period="60 sec",
                  penalty_duration="30 sec", yield_duration="1 sec",
                  max_backoff_period="10 mins", auto_terminate=["x"])
    flow.add_processor(p)
    assert not any("duration" in i["message"] for i in validate_flow(flow))


def test_cron_field_count_is_checked():
    flow = Flow("f")
    p = Processor(name="P", type=UNKNOWN, scheduling_strategy="CRON_DRIVEN",
                  scheduling_period="* * *", auto_terminate=["x"])
    flow.add_processor(p)
    assert any("not a valid CRON" in i["message"] for i in validate_flow(flow))


def test_parameter_reference_in_period_is_not_judged():
    flow = Flow("f")
    p = Processor(name="P", type=UNKNOWN, scheduling_period="#{schedule}",
                  auto_terminate=["x"])
    flow.add_processor(p)
    assert not any("scheduling period" in i["message"] for i in validate_flow(flow))


def test_zero_concurrent_tasks_is_flagged():
    flow = Flow("f")
    p = Processor(name="P", type=UNKNOWN, concurrent_tasks=0, auto_terminate=["x"])
    flow.add_processor(p)
    assert any("concurrent tasks must be at least 1" in i["message"]
               for i in validate_flow(flow))


def test_primary_node_with_incoming_connection_is_flagged():
    # NiFi rejects Primary Node Only on non-source processors — the torture
    # flow's Cron 'audit' repro (funnel -> PRIMARY-scheduled logger).
    flow = Flow("f")
    gen = _proc("Gen")
    audit = Processor(name="Audit", type=UNKNOWN, execution_node="PRIMARY",
                      auto_terminate=["success"])
    flow.add_processor(gen, audit)
    flow.add_connection(gen >> audit)
    issues = [i for i in validate_flow(flow) if "PRIMARY" in i["message"]]
    assert [i["component"] for i in issues] == ["f/Audit"]
    assert "incoming connections" in issues[0]["message"]


def test_primary_node_on_source_processor_is_fine():
    flow = Flow("f")
    p = Processor(name="Gen", type=UNKNOWN, execution_node="PRIMARY",
                  auto_terminate=["success"])
    flow.add_processor(p)
    assert not any("PRIMARY" in i["message"] for i in validate_flow(flow))


def test_primary_node_incoming_from_funnel_counts():
    from niflow.core import Funnel

    flow = Flow("f")
    gen = _proc("Gen")
    audit = Processor(name="Audit", type=UNKNOWN, execution_node="PRIMARY",
                      auto_terminate=["success"])
    fun = Funnel()
    flow.add_processor(gen, audit)
    flow.add_funnel(fun)
    flow.add_connection(gen >> fun, fun >> audit)
    assert any("PRIMARY" in i["message"] and i["component"] == "f/Audit"
               for i in validate_flow(flow))
