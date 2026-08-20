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
    # validate now asks for the set on a specific NiFi line, and for the
    # property values actually set (relationships can be conditional).
    monkeypatch.setattr(
        "niflow.validate.relationships_for",
        lambda t, props=None, major_version=None: table.get(t),
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
    monkeypatch.setattr("niflow.validate.relationships_for",
                        lambda t, props=None, major_version=None: None)
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


# --- dynamic relationships (RouteOnAttribute and friends) --------------------
# These use the real committed catalog: RouteOnAttribute is harvested with the
# static set ['unmatched'] and a 'Routing Strategy' descriptor whose default is
# per-property routing.

ROUTE_ATTR = "org.apache.nifi.processors.standard.RouteOnAttribute"


def _router(name="Router", *, auto=(), strategy=None, **props):
    if strategy is not None:
        props["Routing Strategy"] = strategy
    return Processor(name=name, type=ROUTE_ATTR, properties=props,
                     auto_terminate=list(auto))


def test_dynamic_relationships_connected_validate_clean():
    # Dynamic properties 'hot'/'cold' ARE relationships — wiring them is valid.
    flow = Flow("f")
    router = _router(auto=["unmatched"],
                     hot="${x:equals('h')}", cold="${x:equals('c')}")
    sink = _proc("Sink")
    sink.auto_terminate = ["success"]
    flow.add_processor(router, sink)
    flow.add_connection(router.to(sink, relationships=["hot"]),
                        router.to(sink, relationships=["cold"]))
    assert [i for i in validate_flow(flow) if i["component"] == "f/Router"] == []


def test_dynamic_relationship_auto_terminated_validates_clean():
    flow = Flow("f")
    flow.add_processor(_router(auto=["unmatched", "hot"], hot="${x}"))
    assert validate_flow(flow) == []


def test_unhandled_dynamic_relationship_is_flagged_like_static_ones():
    # 'cold' creates a relationship that is neither wired nor auto-terminated.
    flow = Flow("f")
    flow.add_processor(_router(auto=["unmatched", "hot"],
                               hot="${x}", cold="${y}"))
    msgs = [i["message"] for i in validate_flow(flow)]
    assert "relationship 'cold' is not connected or auto-terminated" in msgs


def test_static_relationships_still_required_on_dynamic_types():
    flow = Flow("f")
    flow.add_processor(_router(auto=["hot"], hot="${x}"))  # 'unmatched' dangles
    msgs = [i["message"] for i in validate_flow(flow)]
    assert "relationship 'unmatched' is not connected or auto-terminated" in msgs


def test_non_default_routing_strategy_disables_dynamic_checks():
    # With matched-routing the dynamic properties are NOT relationships and the
    # live set becomes matched/unmatched — unknowable from the harvest, so both
    # the inverse check and the existence checks must stay quiet.
    flow = Flow("f")
    router = _router(auto=["unmatched", "matched"],
                     strategy="Route to 'match' if all match", hot="${x}")
    flow.add_processor(router)
    assert validate_flow(flow) == []


def test_dynamic_relationship_names_do_not_leak_to_other_types(known_relationships):
    # 'Known' is not a dynamic-relationship type: an unknown relationship name
    # is still an error even when a same-named dynamic property exists.
    flow = Flow("f")
    src = Processor(name="Src", type="Known", properties={"hot": "${x}"},
                    auto_terminate=["success", "failure"])
    dst = _proc("Dst")
    dst.auto_terminate = ["success"]
    flow.add_processor(src, dst)
    flow.add_connection(src.to(dst, relationships=["hot"]))
    msgs = [i["message"] for i in validate_flow(flow)]
    assert any("uses relationship 'hot' that does not exist" in m for m in msgs)


def test_torture_flow_only_flags_the_intentional_primary_node_error():
    # flows/torture.py is the live repro: 24 dynamic fanout relationships plus
    # wired 'hot'/'cold'/'give-up' must be clean, while the deliberately
    # invalid Cron 'audit' (PRIMARY node + incoming connection) stays flagged.
    import importlib.util
    from pathlib import Path

    path = Path(__file__).resolve().parent.parent / "flows" / "torture.py"
    spec = importlib.util.spec_from_file_location("torture_flow", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    issues = validate_flow(module.flow)
    assert [i["component"] for i in issues] == ["NiflowTorture/Cron 'audit'"]
    assert "PRIMARY" in issues[0]["message"]


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


# --- keys that will silently become dynamic properties -----------------------

MERGE = "org.apache.nifi.processors.standard.MergeContent"


def _merge(properties):
    return Processor(name="Merge", type=MERGE, properties=properties,
                     auto_terminate=["merged", "original", "failure"])


def test_kebab_cased_key_that_is_no_property_on_either_line_is_flagged():
    """The `max-bin-age` repro: valid-looking, inert on the server.

    Both 2.7.2 and 1.24 key it `Max Bin Age`, so NiFi files the written key
    under dynamic properties, does nothing with it, and marks the processor
    invalid — after the push, on the server.
    """
    flow = Flow("F")
    flow.add(_merge({"max-bin-age": "10 sec"}))
    messages = [i["message"] for i in validate_flow(flow)]
    assert any("'max-bin-age'" in m and "'Max Bin Age'" in m for m in messages)


def test_the_real_key_passes():
    flow = Flow("F")
    flow.add(_merge({"Max Bin Age": "10 sec"}))
    assert validate_flow(flow) == []


def test_the_1x_spelling_of_a_renamed_property_passes():
    """`Header File` is 1.24's name for 2.x's `Header` — a real property, not a typo."""
    flow = Flow("F")
    flow.add(_merge({"Header File": "/tmp/h"}))
    assert validate_flow(flow) == []


def test_a_genuine_dynamic_property_is_not_flagged():
    flow = Flow("F")
    flow.add(_merge({"my.own.attribute": "x"}))
    assert validate_flow(flow) == []


def test_two_spellings_of_one_property_are_left_to_the_reader():
    """Ambiguity gets no guess — the flow, not the catalog, needs reading."""
    flow = Flow("F")
    flow.add(_merge({"max-bin-age": "10 sec", "max_bin_age": "20 sec"}))
    assert validate_flow(flow) == []


def test_unharvested_types_are_never_guessed_at():
    flow = Flow("F")
    flow.add(Processor(name="X", type=UNKNOWN, properties={"any-key": "1"},
                       auto_terminate=["success"]))
    messages = [i["message"] for i in validate_flow(flow)]
    assert not any("did you mean" in m for m in messages)


def test_controller_service_properties_are_validated_too():
    from niflow.core import ControllerService

    flow = Flow("F")
    flow.add(ControllerService(
        name="Pool", type="org.apache.nifi.dbcp.DBCPConnectionPool",
        properties={"database-connection-url": "jdbc:h2:mem:x"}))
    messages = [i["message"] for i in validate_flow(flow)]
    assert any("did you mean" in m for m in messages), messages


# --- wired but never added ---------------------------------------------------

def test_a_service_used_as_a_property_but_never_added_is_flagged():
    from niflow.core import ControllerService

    flow = Flow("F")
    reader = ControllerService(name="Reader", type="org.x.Reader")
    flow.add(Processor(name="Q", type=UNKNOWN, properties={"Record Reader": reader},
                       auto_terminate=["success"]))
    messages = [i["message"] for i in validate_flow(flow)]
    assert any("never added to the flow" in m and "'Reader'" in m for m in messages)


def test_a_connection_endpoint_that_is_not_in_the_flow_is_flagged():
    flow = Flow("F")
    a = Processor(name="A", type=UNKNOWN, auto_terminate=["success"])
    b = Processor(name="B", type=UNKNOWN, auto_terminate=["success"])
    flow.add(a)  # B is wired but never added
    flow.add_connection(a >> b)
    messages = [i["message"] for i in validate_flow(flow)]
    assert any("connection destination 'B' is not part of the flow" in m
               for m in messages)


def test_a_service_registered_in_an_ancestor_group_is_fine():
    """NiFi resolves a service from any ancestor — registration is tree-wide."""
    from niflow.core import ControllerService

    flow = Flow("F")
    reader = ControllerService(name="Reader", type="org.x.Reader")
    flow.add(reader)
    with flow.process_group("Child") as child:
        child.add_processor(Processor(name="Q", type=UNKNOWN,
                                      properties={"Record Reader": reader},
                                      auto_terminate=["success"]))
    messages = [i["message"] for i in validate_flow(flow)]
    assert not any("never added" in m for m in messages)


# --- structural checks NiFi enforces at push time ----------------------------

def test_a_connection_between_two_child_groups_is_flagged():
    """NiFi needs a port to cross a group boundary; the push fails otherwise."""
    flow = Flow("F")
    with flow.process_group("A") as a:
        a.add_processor(Processor(name="PA", type=UNKNOWN))
    with flow.process_group("B") as b:
        b.add_processor(Processor(name="PB", type=UNKNOWN, auto_terminate=["success"]))
    pa = flow.process_groups[0].processors[0]
    pb = flow.process_groups[1].processors[0]
    flow.process_groups[0].connections.append(pa >> pb)
    messages = [i["message"] for i in validate_flow(flow)]
    assert any("cross a group boundary" in m and "'F/B'" in m for m in messages)


def test_a_connection_into_a_child_groups_port_is_fine():
    from niflow.core import InputPort

    flow = Flow("F")
    src = Processor(name="Src", type=UNKNOWN)
    flow.add(src)
    with flow.process_group("Child") as child:
        port = InputPort(name="In")
        child.add(port)
        child.add_processor(Processor(name="Inner", type=UNKNOWN,
                                      auto_terminate=["success"]))
        child.add_connection(port >> child.processors[0])
    flow.add_connection(src >> flow.process_groups[0].input_ports[0])
    messages = [i["message"] for i in validate_flow(flow)]
    assert not any("cross a group boundary" in m for m in messages)


def test_a_parameter_reference_with_no_context_bound_is_flagged():
    flow = Flow("F")
    flow.add(Processor(name="P", type=UNKNOWN, properties={"k": "#{db.password}"},
                       auto_terminate=["success"]))
    messages = [i["message"] for i in validate_flow(flow)]
    assert any("'db.password'" in m and "no parameter context is bound" in m
               for m in messages)


def test_a_bound_context_on_an_ancestor_satisfies_a_child():
    from niflow.core import Parameter, ParameterContext

    flow = Flow("F")
    flow.parameter_context = ParameterContext(
        name="Ctx", parameters=[Parameter(name="db.password", value="x")])
    with flow.process_group("Child") as child:
        child.add_processor(Processor(name="P", type=UNKNOWN,
                                      properties={"k": "#{db.password}"},
                                      auto_terminate=["success"]))
    messages = [i["message"] for i in validate_flow(flow)]
    assert not any("parameter context" in m for m in messages)


def test_the_escaped_parameter_syntax_is_not_a_reference():
    """`##{x}` is the literal text "#{x}" — flows/torture.py ships one."""
    flow = Flow("F")
    flow.add(Processor(name="P", type=UNKNOWN, properties={"k": "text ##{not.a.param}"},
                       auto_terminate=["success"]))
    assert validate_flow(flow) == []
