"""Case generation: what flows the harness builds, and how it stays reproducible.

A :class:`Case` is a ``(kind, spec)`` pair — a tiny JSON-serialisable
description of one generated flow — and :func:`build_case_flow` turns it back
into a :class:`~niflow.core.Flow`, deterministically. That indirection is what
makes a finding replayable: the case id is a hash of the spec, so the same case
always has the same id on every machine, and a repro file is four lines.

Kinds: ``solo`` (one processor of a type, all relationships handled), ``props``
(one type with a property variant — harvested defaults, allowable values,
display names, 1.x legacy keys, Python ints/bools, hostile strings, expression
language, dynamic properties), ``pair`` (``A -> B`` on one of A's
relationships), ``service`` (a processor wired to a controller service),
``svc`` (a controller service **on its own**, with the same property variants —
services were exercised only as a processor's referenced type, which is exactly
the blind spot that let every 2.x service key land on 1.24 as an inert dynamic
property), ``params`` (a parameter context, including a **sensitive**
parameter, referenced from a property — the one kind of value that must never
appear in anything niflow emits), and ``shape`` (structural adversaries:
funnels, ports, nesting, parallel edges, self-loops, duplicate names, dangling
endpoints, ...).
"""
from __future__ import annotations

import hashlib
import json
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from niflow.core import (
    ControllerService,
    Flow,
    Funnel,
    InputPort,
    Label,
    OutputPort,
    Parameter,
    ParameterContext,
    ProcessGroup,
    Processor,
)

# --- classifications ---------------------------------------------------------
PASSED = "passed"
NIFLOW_BUG = "niflow_bug"
NIFI_REJECTED = "nifi_rejected"

#: Order matters — the worst classification a case earns is the case's status.
_SEVERITY = {PASSED: 0, NIFI_REJECTED: 1, NIFLOW_BUG: 2}

DEFAULT_OUT_DIR = Path(".niflow-fuzz")

#: Every live case names its sandbox group with this prefix, so a sweep can
#: sweep up after itself — including groups a *failed* push left half-created.
SANDBOX_PREFIX = "niflow-fuzz "

# Strings that have historically broken emitters: quoting, escaping, unicode,
# whitespace significance, and NiFi's own escape syntax ($${..} / ##{..}).
HOSTILE_VALUES: Tuple[str, ...] = (
    "quote'inside",
    'double"quote',
    "back\\slash \\d+",
    "line\nbreak\nthird",
    "trailing space   ",
    "emoji 🔥 unicode ünï",
    "$${escaped.el} and ##{escaped.param}",
    "<angle> & ampersand",
    "",
    "   ",
    "tab\there",
    "a" * 300,
)

#: Names that are legal in NiFi but hostile to code generation / identity.
HOSTILE_NAMES: Tuple[str, ...] = (
    "plain",
    "with space",
    "quote'name",
    'double"name',
    "back\\slash",
    "emoji 🔥",
    "ünïcode",
    "class",          # Python keyword after sanitisation
    "flow",           # collides with the emitted module's flow variable
    "123start",
    "  padded  ",
    "dot.name",
    "a" * 120,
)

SHAPES: Tuple[str, ...] = (
    "self_loop",
    "parallel_edges",
    "named_parallel_edges",
    "funnel_chain",
    "funnel_reverse_declaration",
    "nested_input_port",
    "child_output_port",
    "sibling_same_named_ports",
    "deep_nesting",
    "labels",
    "queue_settings",
    "cron_primary",
    "disabled_and_running",
    "parameter_context",
    "group_variables",
    "hostile_names",
    "duplicate_names",
    "multi_relationship",
    "unregistered_service",
    "service_chain",
    "dangling_endpoint",
    "cross_group_connection",
    "empty_child_group",
    "wide_group",
    "port_shares_processor_name",
)

KINDS: Tuple[str, ...] = (
    "solo", "props", "pair", "service", "svc", "params", "shape")

#: The value a ``params`` case gives its sensitive parameter. A sweep asserts
#: this string appears in **nothing** niflow emits — snapshot, template, or
#: generated Python — because a secret in a flow file is a secret in git.
SECRET_VALUE = "fuzz-secret-do-not-emit-1f0e3d"


# =============================================================================
# catalog access
# =============================================================================


def _catalog():
    from niflow.processors import catalog

    return catalog


def _service_catalog():
    from niflow.services import catalog

    return catalog


def _compat_v1():
    try:
        from niflow.processors import compat_v1

        return compat_v1
    except Exception:  # pragma: no cover - compat table is optional
        return None


def processor_types(pattern: Optional[str] = None) -> List[str]:
    """Harvested processor types (those with a known relationship set), filtered."""
    catalog = _catalog()
    types = sorted(getattr(catalog, "RELATIONSHIPS", None) or {})
    if not types:  # pragma: no cover - catalog always has relationships today
        types = sorted(getattr(catalog, "TYPES", []))
    if pattern:
        rx = re.compile(pattern)
        types = [t for t in types if rx.search(t)]
    return types


def service_types(pattern: Optional[str] = None) -> List[str]:
    types = sorted(getattr(_service_catalog(), "TYPES", []))
    if pattern:
        rx = re.compile(pattern)
        types = [t for t in types if rx.search(t)]
    return types


def _relationships(type_str: str) -> List[str]:
    from niflow.processors.rules import relationships_for

    return sorted(relationships_for(type_str) or ["success"])


def _descriptors(type_str: str) -> Dict[str, dict]:
    from niflow.processors.rules import descriptors_for

    return dict(descriptors_for(type_str) or {})


# =============================================================================
# cases
# =============================================================================


@dataclass
class Case:
    """One generated flow plus everything needed to rebuild it exactly."""

    kind: str
    spec: Dict[str, Any]

    @property
    def case_id(self) -> str:
        """Stable id derived from the spec — the same case always has the same id."""
        blob = json.dumps({"kind": self.kind, "spec": self.spec},
                          sort_keys=True, ensure_ascii=False)
        digest = hashlib.sha1(blob.encode("utf-8")).hexdigest()[:10]
        return f"{self.kind}-{digest}"

    def build(self, name: Optional[str] = None) -> Flow:
        return build_case_flow(self.kind, self.spec, name=name or "Fuzz")

    def to_dict(self) -> Dict[str, Any]:
        return {"case_id": self.case_id, "kind": self.kind, "spec": self.spec}


def build_case_flow(kind: str, spec: Dict[str, Any], name: str = "Fuzz") -> Flow:
    """Build the :class:`~niflow.core.Flow` a case describes.

    Pure and deterministic: the same ``(kind, spec)`` always yields the same
    flow, which is what makes a repro file a one-liner.
    """
    builder = _BUILDERS.get(kind)
    if builder is None:
        raise ValueError(f"unknown fuzz case kind {kind!r}")
    return builder(spec, name)


def _proc(name: str, type_str: str, props: Optional[dict] = None,
          terminate: Optional[Sequence[str]] = None, **settings: Any) -> Processor:
    """A processor with every relationship handled unless told otherwise."""
    rels = _relationships(type_str) if terminate is None else terminate
    return Processor(name=name, type=type_str, properties=dict(props or {}),
                     auto_terminate=sorted(set(rels)), **settings)


def _build_solo(spec: Dict[str, Any], name: str) -> Flow:
    flow = Flow(name)
    flow.add_processor(_proc("A", spec["type"]))
    return flow


def _build_props(spec: Dict[str, Any], name: str) -> Flow:
    flow = Flow(name)
    flow.add_processor(_proc("A", spec["type"], spec.get("properties")))
    return flow


def _build_pair(spec: Dict[str, Any], name: str) -> Flow:
    source_type, target_type = spec["source"], spec["target"]
    rel = spec["relationship"]
    flow = Flow(name)
    source = _proc("A", source_type,
                   terminate=[r for r in _relationships(source_type) if r != rel])
    target = _proc("B", target_type)
    flow.add_processor(source, target)
    flow.add_connection(source.to(target, relationships=[rel]))
    return flow


def _build_service(spec: Dict[str, Any], name: str) -> Flow:
    flow = Flow(name)
    service = ControllerService(name="Svc", type=spec["service_type"])
    flow.add_controller_service(service)
    flow.add_processor(_proc("A", spec["type"], {spec["property"]: service}))
    return flow


def _build_svc(spec: Dict[str, Any], name: str) -> Flow:
    """One controller service, alone. No processor, nothing referencing it.

    A service is a first-class component of a flow — it round-trips, it gets
    diffed, its property keys are translated per NiFi line — and until this
    kind existed the harness only ever saw one as the far end of a processor's
    property.
    """
    flow = Flow(name)
    flow.add_controller_service(ControllerService(
        name="Svc", type=spec["service_type"],
        properties=dict(spec.get("properties") or {})))
    return flow


def _build_params(spec: Dict[str, Any], name: str) -> Flow:
    """A parameter context — plain and sensitive — referenced from a property.

    The sensitive parameter carries a real value in the *model*, which is what
    a hand-written flow looks like before the value is moved into a secrets
    file. Everything niflow emits from this flow has to leave it out.
    """
    flow = Flow(name)
    context = ParameterContext(
        # Prefixed like a sandbox group: parameter contexts are **global** and
        # survive the group that used them, so a sweep that leaves one behind
        # poisons the next (a stale sensitive parameter made a 1.24 group
        # unexportable — 500 on /download — for thirteen later cases).
        name=f"{SANDBOX_PREFIX}context",
        description="fuzz",
        parameters=[
            Parameter(name="fuzz.plain", value=spec.get("plain", "plain-value")),
            Parameter(name="fuzz.secret",
                      value=spec.get("secret", SECRET_VALUE), sensitive=True),
        ],
    )
    if spec.get("inherited"):
        context.inherited_contexts = list(spec["inherited"])
    flow.parameter_context = context
    properties = {}
    descriptors = _descriptors(spec["type"])
    # Two rules NiFi enforces and niflow now checks, so generating flows that
    # break them would only re-find the same refusal:
    #   * a controller-service reference must hold a service, not a parameter
    #     (on 1.24 the group then fails to download at all);
    #   * only a *sensitive* property may reference a sensitive parameter.
    plain_keys = [k for k in sorted(descriptors)
                  if not (descriptors[k] or {}).get("service")
                  and not (descriptors[k] or {}).get("sensitive")]
    secret_keys = [k for k in sorted(descriptors)
                   if (descriptors[k] or {}).get("sensitive")]
    properties[plain_keys[0] if plain_keys else "fuzz.dynamic"] = "#{fuzz.plain}"
    if spec.get("reference_secret", True) and secret_keys:
        properties[secret_keys[0]] = "#{fuzz.secret}"
    flow.add_processor(_proc("A", spec["type"], properties))
    return flow


def _build_shape(spec: Dict[str, Any], name: str) -> Flow:
    shape = spec["shape"]
    source_type = spec["source"]
    target_type = spec["target"]
    flow = Flow(name)
    rels = _relationships(source_type)
    rel = rels[0]

    if shape == "self_loop":
        a = _proc("A", source_type, terminate=[r for r in rels if r != rel])
        flow.add_processor(a)
        flow.add_connection(a.to(a, relationships=[rel]))

    elif shape in ("parallel_edges", "named_parallel_edges"):
        a = _proc("A", source_type, terminate=[r for r in rels if r != rel])
        b = _proc("B", target_type)
        flow.add_processor(a, b)
        names = ("", "") if shape == "parallel_edges" else ("one", "two")
        for conn_name in names:
            flow.add_connection(a.to(b, relationships=[rel], name=conn_name))

    elif shape in ("funnel_chain", "funnel_reverse_declaration"):
        a = _proc("A", source_type, terminate=[r for r in rels if r != rel])
        b = _proc("B", target_type)
        funnels = [Funnel(), Funnel(), Funnel()]
        flow.add_processor(a, b)
        # Declaration order is the interesting axis: funnel identity must key
        # on topology, not list position.
        flow.add_funnel(*(funnels if shape == "funnel_chain" else list(reversed(funnels))))
        flow.add_connection(a.to(funnels[0], relationships=[rel]))
        flow.add_connection(funnels[0] >> funnels[1])
        flow.add_connection(funnels[1] >> funnels[2])
        flow.add_connection(funnels[2] >> b)

    elif shape == "nested_input_port":
        child = flow.process_group("Child")
        port = InputPort("in")
        inner = _proc("Inner", target_type)
        child.add(port, inner, port >> inner)
        a = _proc("A", source_type, terminate=[r for r in rels if r != rel])
        flow.add_processor(a)
        flow.add_connection(a.to(port, relationships=[rel]))

    elif shape == "child_output_port":
        child = flow.process_group("Child")
        port = OutputPort("out")
        inner = _proc("Inner", source_type,
                      terminate=[r for r in rels if r != rel])
        child.add(port, inner, inner.to(port, relationships=[rel]))
        sink = _proc("Sink", target_type)
        flow.add_processor(sink)
        flow.add_connection(port >> sink)

    elif shape == "sibling_same_named_ports":
        a = _proc("A", source_type, terminate=[r for r in rels if r != rel])
        flow.add_processor(a)
        for child_name in ("One", "Two"):
            child = flow.process_group(child_name)
            port = InputPort("in")
            inner = _proc("Inner", target_type)
            child.add(port, inner, port >> inner)
            flow.add_connection(a.to(port, relationships=[rel]))

    elif shape == "deep_nesting":
        group: ProcessGroup = flow
        for level in range(spec.get("depth", 6)):
            group = group.process_group(f"L{level}")
        group.add(_proc("Deep", target_type))

    elif shape == "labels":
        flow.add_processor(_proc("A", source_type))
        for text in HOSTILE_VALUES[:4]:
            flow.add_label(Label(text, width=180.0, height=90.0))

    elif shape == "queue_settings":
        a = _proc("A", source_type, terminate=[r for r in rels if r != rel])
        b = _proc("B", target_type)
        flow.add_processor(a, b)
        flow.add_connection(a.to(
            b, relationships=[rel],
            back_pressure_object_threshold=0,
            back_pressure_data_size_threshold="10 MB",
            flowfile_expiration="60 sec",
            prioritizers=["org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer"],
            load_balance_strategy="PARTITION_BY_ATTRIBUTE",
            partitioning_attribute="shard",
            load_balance_compression="COMPRESS_ATTRIBUTES_ONLY",
        ))

    elif shape == "cron_primary":
        flow.add_processor(_proc(
            "A", source_type,
            scheduling_strategy="CRON_DRIVEN", scheduling_period="0 0 * * * ?",
            execution_node="PRIMARY", concurrent_tasks=3, run_duration_millis=25,
            penalty_duration="45 sec", yield_duration="2 sec",
            bulletin_level="DEBUG", comments="cron\nprimary 🔥",
        ))

    elif shape == "disabled_and_running":
        a = _proc("A", source_type, terminate=[r for r in rels if r != rel],
                  scheduled_state="RUNNING", retry_count=3,
                  retried_relationships=[rel], backoff_mechanism="YIELD_PROCESSOR",
                  max_backoff_period="5 mins")
        b = _proc("B", target_type, scheduled_state="DISABLED")
        flow.add_processor(a, b)
        flow.add_connection(a.to(b, relationships=[rel]))

    elif shape == "parameter_context":
        context = ParameterContext(
            name="FuzzCtx",
            description="generated",
            parameters=[
                Parameter("plain", "value"),
                Parameter("secret", None, sensitive=True),
                Parameter("hostile", HOSTILE_VALUES[3]),
            ],
        )
        flow.parameter_context = context
        flow.add_processor(_proc("A", source_type, {"fuzz.param": "#{plain}"}))

    elif shape == "group_variables":
        flow.variables = {"var": "value", "hostile": HOSTILE_VALUES[0]}
        child = flow.process_group("Child")
        child.variables = {"child": "v"}
        child.add(_proc("A", source_type))

    elif shape == "hostile_names":
        for index, hostile in enumerate(HOSTILE_NAMES):
            flow.add_processor(_proc(f"{hostile}", source_type))
            if index >= 5:
                break

    elif shape == "multi_relationship":
        a = _proc("A", source_type, terminate=[])
        b = _proc("B", target_type)
        flow.add_processor(a, b)
        flow.add_connection(a.to(b, relationships=list(rels)))

    elif shape == "unregistered_service":
        # A service instance referenced by a processor but never added to any
        # group — an easy mistake to make by hand; it must fail loudly, not
        # crash or emit a snapshot with a dangling reference.
        service = ControllerService(name="Orphan", type=spec.get(
            "service_type", "org.apache.nifi.ssl.StandardSSLContextService"))
        flow.add_processor(_proc("A", source_type, {"Some Service": service}))

    elif shape == "service_chain":
        base = ControllerService(name="Base", type=spec.get(
            "service_type", "org.apache.nifi.ssl.StandardSSLContextService"))
        wrapper = ControllerService(name="Wrapper", type=spec.get(
            "service_type", "org.apache.nifi.ssl.StandardSSLContextService"),
            properties={"Delegate": base})
        flow.add_controller_service(base, wrapper)
        flow.add_processor(_proc("A", source_type, {"Some Service": wrapper}))

    elif shape == "dangling_endpoint":
        # The target is wired but never registered in the group.
        a = _proc("A", source_type, terminate=[r for r in rels if r != rel])
        b = _proc("B", target_type)
        flow.add_processor(a)
        flow.add_connection(a.to(b, relationships=[rel]))

    elif shape == "cross_group_connection":
        # Two processors in *different* child groups wired directly: NiFi
        # requires ports for that, so niflow should refuse, not emit rubbish.
        one = flow.process_group("One")
        two = flow.process_group("Two")
        a = _proc("A", source_type, terminate=[r for r in rels if r != rel])
        b = _proc("B", target_type)
        one.add(a)
        two.add(b)
        flow.add_connection(a.to(b, relationships=[rel]))

    elif shape == "empty_child_group":
        flow.process_group("Empty")
        flow.process_group("AlsoEmpty").process_group("Deeper")
        flow.add_processor(_proc("A", source_type))

    elif shape == "wide_group":
        for index in range(24):
            flow.add_processor(_proc(f"P{index:02d}", source_type))

    elif shape == "port_shares_processor_name":
        # Legal in NiFi (kinds are distinct namespaces) and must stay legal.
        flow.add_processor(_proc("shared", source_type))
        flow.add_port(InputPort("shared"), OutputPort("shared"))

    elif shape == "duplicate_names":
        # Legal in NiFi, rejected by niflow on purpose — the harness asserts
        # the rejection is loud and consistent, never a silent merge.
        flow.add_processor(_proc("Twin", source_type), _proc("Twin", target_type))

    else:  # pragma: no cover - guarded by the generator
        raise ValueError(f"unknown shape {shape!r}")
    return flow


_BUILDERS: Dict[str, Callable[[Dict[str, Any], str], Flow]] = {
    "solo": _build_solo,
    "props": _build_props,
    "pair": _build_pair,
    "service": _build_service,
    "svc": _build_svc,
    "params": _build_params,
    "shape": _build_shape,
}


# =============================================================================
# generation
# =============================================================================


def _rng(seed: int, kind: str) -> random.Random:
    """A generator per kind, so adding a kind never shifts another's cases."""
    mixed = hashlib.sha1(f"{seed}:{kind}".encode("utf-8")).hexdigest()[:12]
    return random.Random(int(mixed, 16))


def _property_variants(type_str: str, rng: random.Random) -> List[Tuple[str, dict]]:
    """Property dicts worth pushing through the pipeline for one type.

    Each variant targets a documented failure mode: harvested defaults,
    allowable values, display-name keys, 1.x legacy keys, real Python
    ints/bools (NiFi property values are strings — a mismatch here is
    permanent phantom drift), hostile strings, expression language, and
    genuinely dynamic properties.
    """
    from niflow.processors.rules import LEGACY_PROPERTY_ALIASES

    descriptors = _descriptors(type_str)
    plain = {name: entry for name, entry in descriptors.items() if not entry.get("service")}
    variants: List[Tuple[str, dict]] = []

    defaults = {name: entry["default"] for name, entry in plain.items()
                if entry.get("default") is not None}
    if defaults:
        variants.append(("defaults", defaults))

    allowable = {name: rng.choice(entry["allowable"]) for name, entry in plain.items()
                 if entry.get("allowable")}
    if allowable:
        variants.append(("allowable", allowable))

    display = {entry["display"]: (entry.get("default") or "fuzz")
               for name, entry in plain.items()
               if entry.get("display") and entry["display"] != name}
    if display:
        variants.append(("display_names", display))

    legacy = {old: "fuzz" for old, new in LEGACY_PROPERTY_ALIASES.items()
              if new in plain and old not in descriptors}
    if legacy:
        variants.append(("legacy_keys", legacy))

    # Real ints/bools where the property *looks* numeric or boolean. NiFi keys
    # properties by string; anything else can only round-trip as drift.
    typed: Dict[str, Any] = {}
    for name, entry in plain.items():
        default = entry.get("default")
        if isinstance(default, str) and default.strip().isdigit():
            typed[name] = int(default)
        elif isinstance(default, str) and default.strip().lower() in ("true", "false"):
            typed[name] = default.strip().lower() == "true"
    if typed:
        variants.append(("python_typed_values", typed))

    names = sorted(plain)
    if names:
        chosen = names[: 3]
        variants.append(("hostile_values",
                         {name: rng.choice(HOSTILE_VALUES) for name in chosen}))
        variants.append(("expression_language",
                         {name: "${fuzz.attr:trim()}" for name in chosen[:1]}
                         | {name: "#{fuzz.param}" for name in chosen[1:2]}))

    variants.append(("dynamic_properties", {
        "fuzz.dynamic": "value",
        "dynamic with space": "value",
        "🔥": "value",
    }))
    return variants


def generate_cases(
    *,
    seed: int = 0,
    kinds: Sequence[str] = KINDS,
    type_pattern: Optional[str] = None,
    count: int = 0,
) -> List[Case]:
    """The deterministic case list for a run.

    Same ``(seed, kinds, type_pattern, catalog)`` — same cases, same ids, same
    order. ``count`` truncates the (round-robin interleaved) stream so a small
    run is a representative sample of a big one rather than its first slice.
    """
    types = processor_types(type_pattern)
    if not types:
        return []
    per_kind: Dict[str, List[Case]] = {}

    if "solo" in kinds:
        per_kind["solo"] = [Case("solo", {"type": t}) for t in types]

    if "props" in kinds:
        rng = _rng(seed, "props")
        cases = []
        for type_str in types:
            for variant, props in _property_variants(type_str, rng):
                cases.append(Case("props", {"type": type_str, "variant": variant,
                                            "properties": props}))
        per_kind["props"] = cases

    if "pair" in kinds:
        rng = _rng(seed, "pair")
        cases = []
        # Four sampled partners per type: exhaustive pairing is ~85k cases and
        # adds nothing once the emitter has seen each type on both ends.
        for type_str in types:
            for _ in range(4):
                target = rng.choice(types)
                rel = rng.choice(_relationships(type_str))
                cases.append(Case("pair", {"source": type_str, "target": target,
                                           "relationship": rel}))
        per_kind["pair"] = cases

    if "service" in kinds:
        available = set(service_types())
        cases = []
        for type_str in types:
            for prop, entry in sorted(_descriptors(type_str).items()):
                wanted = entry.get("service")
                if not wanted:
                    continue
                implementations = [s for s in sorted(available)
                                   if s.rsplit(".", 1)[-1] in wanted or wanted in s]
                service_type = implementations[0] if implementations else wanted
                cases.append(Case("service", {"type": type_str, "property": prop,
                                              "service_type": service_type}))
                break  # one service-bearing property per type is enough
        per_kind["service"] = cases

    if "svc" in kinds:
        rng = _rng(seed, "svc")
        cases = []
        for service_type in service_types(type_pattern):
            cases.append(Case("svc", {"service_type": service_type}))
            for variant, props in _property_variants(service_type, rng):
                cases.append(Case("svc", {"service_type": service_type,
                                          "variant": variant, "properties": props}))
        per_kind["svc"] = cases

    if "params" in kinds:
        rng = _rng(seed, "params")
        cases = []
        # A context is a property of the flow, not of a type, so a sample of
        # types is enough — what varies is the shape of the context itself.
        for _ in range(12):
            type_str = rng.choice(types)
            cases.append(Case("params", {"type": type_str}))
        cases.append(Case("params", {"type": rng.choice(types),
                                     "reference_secret": False}))
        cases.append(Case("params", {"type": rng.choice(types),
                                     "inherited": ["Parent Context"]}))
        cases.append(Case("params", {"type": rng.choice(types),
                                     "plain": "value with spaces and 'quotes'"}))
        per_kind["params"] = cases

    if "shape" in kinds:
        rng = _rng(seed, "shape")
        cases = []
        for shape in SHAPES:
            for _ in range(8):
                cases.append(Case("shape", {
                    "shape": shape,
                    "source": rng.choice(types),
                    "target": rng.choice(types),
                }))
        per_kind["shape"] = cases

    ordered = _interleave([per_kind[k] for k in kinds if k in per_kind])
    # Ids are content-derived, so the same case can be generated twice (two
    # types sharing a shape sample); keep the first occurrence only.
    seen: set = set()
    unique: List[Case] = []
    for case in ordered:
        if case.case_id in seen:
            continue
        seen.add(case.case_id)
        unique.append(case)
    return unique[:count] if count else unique


def _interleave(buckets: List[List[Case]]) -> List[Case]:
    out: List[Case] = []
    index = 0
    while any(index < len(bucket) for bucket in buckets):
        for bucket in buckets:
            if index < len(bucket):
                out.append(bucket[index])
        index += 1
    return out


