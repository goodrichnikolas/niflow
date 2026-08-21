"""Semantic diff between two Flow trees: the "what will change" plan.

``diff_flows(live, desired)`` compares the live state of a group (as pulled
from NiFi, components carrying their canvas ids) against the desired model
(built in Python) and returns an ordered list of :class:`Change` records —
adds, removes, and field-level updates keyed by group path and component
name. The plan drives two consumers:

* ``format_plan`` renders it for humans (``niflow plan``, the GUI preview);
* ``NiFiClient.apply_plan`` executes it with targeted REST calls so a push
  touches only what changed.

Identity is name-based within each group (NiFi allows duplicate names; when
they occur, same-keyed components pair by field similarity — an exact twin
always wins — and the rest become adds/removes; funnels, which have no name,
match by connection topology). Positions and layout are deliberately NOT diffed —
moving things on the canvas is cosmetic and must never force an update.
Sensitive processor properties never appear in live snapshots, so a desired
literal value for one always registers as an update; reference them through
parameters (``#{...}``) to keep plans quiet.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from niflow.core import (
    Connection,
    ControllerService,
    Funnel,
    Label,
    NiFiComponent,
    Port,
    ProcessGroup,
    Processor,
    nifi_property_value,
)

# Processor fields worth diffing (position/bundle/name/type excluded: the
# first two are cosmetic or instance-aligned, the last two are identity).
_PROCESSOR_FIELDS = (
    "scheduling_period",
    "scheduling_strategy",
    "concurrent_tasks",
    "auto_terminate",
    "penalty_duration",
    "yield_duration",
    "bulletin_level",
    "run_duration_millis",
    "execution_node",
    "scheduled_state",
    "retry_count",
    "retried_relationships",
    "backoff_mechanism",
    "max_backoff_period",
    "comments",
)
_SERVICE_FIELDS = ("enabled", "comments")
_CONNECTION_FIELDS = (
    "name",
    "relationships",
    "back_pressure_object_threshold",
    "back_pressure_data_size_threshold",
    "flowfile_expiration",
    "prioritizers",
    "load_balance_strategy",
    "partitioning_attribute",
    "load_balance_compression",
)


@dataclass
class Change:
    """One planned mutation.

    ``path`` is the group path relative to the diffed root (``()`` means the
    root group itself). For updates, ``fields`` maps field name to a
    ``(live, desired)`` pair; property changes appear as ``properties[Key]``.
    """

    op: str  # "add" | "remove" | "update"
    kind: str  # "processor" | "controller_service" | "connection" | ...
    path: Tuple[str, ...]
    name: str
    fields: Dict[str, Tuple[Any, Any]] = field(default_factory=dict)
    desired: Optional[Any] = None
    live: Optional[Any] = None
    note: Optional[str] = None  # e.g. probable-rename warning
    #: Field names in this change whose live value NiFi will not disclose —
    #: sensitive properties. They are still applied (the model's intent is the
    #: only thing that can be), but they cannot be *compared*, so a change made
    #: only of these is not evidence that anything drifted.
    unknowable: Tuple[str, ...] = ()

    @property
    def location(self) -> str:
        return "/".join(self.path) or "."


def diff_flows(
    live: ProcessGroup,
    desired: ProcessGroup,
    target_major: Optional[int] = None,
) -> List[Change]:
    """Return the ordered change plan turning ``live`` into ``desired``.

    ``target_major`` is the NiFi major version the live side came from. Property
    emission has been server-version-aware since the cross-version fidelity fix;
    the diff side was not, so a property that exists only on the target's line —
    ``QueryRecord``'s ``cache-schema``, ``ConsumeJMS``'s ``Session Cache size`` —
    had no descriptor to read a default from, was judged against ``None``, and
    planned as an unset on every run forever. Left ``None`` it is inferred from
    the live snapshot's own property keys (:func:`_infer_target_major`), so
    callers that never knew the version get the fix too.
    """
    if target_major is None:
        target_major = _infer_target_major(live)
    changes: List[Change] = []
    _diff_group(live, desired, (), changes, target_major)
    _annotate_renames(changes)
    _annotate_sensitive(changes, target_major)
    return changes


def _infer_target_major(live: ProcessGroup) -> Optional[int]:
    """Which NiFi line the live snapshot was pulled from, read off its own keys.

    A live component carries its server's full materialised property map, and a
    pull already rewrites every *renamed* key into the catalog's namespace. So a
    key that survives canonicalization while belonging to the 1.x namespace and
    not the 2.x one had no 2.x counterpart to be rewritten into — which only a
    1.x server can have produced. That residue is the evidence; the 2.x-looking
    keys are not, because canonicalization manufactures them on both lines.

    Only "this is 1.x" is ever concluded. ``None`` means "judge with the catalog
    alone", which is both the old behaviour and the right answer for 2.x, so
    there is nothing to gain from guessing further. Evidence is pooled across
    the tree — one server serves the whole snapshot — at the cost of one
    far-fetched corner: a *dynamic* property on a 2.x server whose name happens
    to equal a 1.x-only real property of the same type would read as 1.x.
    """
    from niflow.processors.rules import (
        _compat_entry, canonical_properties, property_names_for,
    )

    def judge(type_str: str, props: Dict[str, Any]) -> bool:
        if not props:
            return False
        catalog_names = property_names_for(type_str)
        v1_names = _compat_entry("PROPERTY_NAMES", type_str)
        if v1_names is None:
            return False
        if catalog_names is None:
            # The 1.x harvest knows this type and the 2.x catalog does not, so
            # only a 1.x server can be running it (ListHDFS, GetJMSTopic, ...).
            # That is the strongest evidence there is, and every one of its
            # properties would otherwise be judged against no descriptor at all.
            return True
        catalog_set, v1_set = set(catalog_names), set(v1_names)
        return any(
            key in v1_set and key not in catalog_set
            for key in canonical_properties(type_str, props)
        )

    def visit(group: ProcessGroup) -> bool:
        return (
            any(judge(p.type, p.properties or {}) for p in group.processors)
            or any(judge(s.type, s.properties or {})
                   for s in group.controller_services)
            or any(visit(child) for child in group.process_groups)
        )

    return 1 if visit(live) else None


# ---------------------------------------------------------------- internals


def _diff_group(
    live: ProcessGroup,
    desired: ProcessGroup,
    path: Tuple[str, ...],
    changes: List[Change],
    target_major: Optional[int] = None,
) -> None:
    # Group-level settings (variables, comment, parameter-context binding).
    settings: Dict[str, Tuple[Any, Any]] = {}
    if live.variables != desired.variables:
        settings["variables"] = (live.variables, desired.variables)
    if (live.comment or "") != (desired.comment or ""):
        settings["comment"] = (live.comment, desired.comment)
    live_ctx = live.parameter_context.name if live.parameter_context else None
    desired_ctx = desired.parameter_context.name if desired.parameter_context else None
    if live_ctx != desired_ctx:
        settings["parameter_context"] = (live_ctx, desired_ctx)
    if settings:
        changes.append(
            Change("update", "group_settings", path, live.name or ".",
                   fields=settings, desired=desired, live=live)
        )

    # A service the *import* created (NiFi 2.x makes an
    # AWSCredentialsProviderControllerService for the AWS processors and wires
    # it into their required credentials property) is on the canvas without
    # ever having been in the model. Removing it is not "cleaning up": it is
    # deleting the thing the processor requires, on every plan, forever. It is
    # not ours to manage, so it is not diffed — write it in the flow and it
    # becomes a normal, fully diffed service again.
    _diff_named(
        _managed_services(live, desired, target_major), desired.controller_services,
        "controller_service", path, changes,
        lambda a, b: _diff_service_fields(a, b, target_major),
    )
    _diff_named(live.input_ports, desired.input_ports, "input_port", path, changes)
    _diff_named(live.output_ports, desired.output_ports, "output_port", path, changes)
    _diff_named(
        live.processors, desired.processors,
        "processor", path, changes,
        lambda a, b: _diff_processor_fields(a, b, target_major),
    )

    # Funnels are anonymous, so they're identified by connection topology
    # (what feeds them / what they feed) rather than list position — the
    # server lists funnels in arbitrary order. Unmatched surplus becomes
    # add/remove; a matched pair has nothing diffable besides position.
    funnel_pairs = match_funnels(live, desired)
    matched_live = set(funnel_pairs.values())
    for j, funnel in enumerate(live.funnels):
        if j not in matched_live:
            changes.append(Change("remove", "funnel", path, f"funnel[{j}]", live=funnel))
    for i, funnel in enumerate(desired.funnels):
        if i not in funnel_pairs:
            changes.append(Change("add", "funnel", path, f"funnel[{i}]", desired=funnel))

    # Labels: match by text (duplicates pair in order).
    _diff_keyed(
        live.labels, desired.labels, lambda l: l.text, "label", path, changes
    )

    # Connections: identity is (source endpoint, target endpoint); the
    # relationship set and queue settings are updatable fields. Endpoint keys
    # are built per side — funnels through the topology pairing above, ports
    # qualified by owning child group — so a live tree (with ids) and a
    # desired tree (without) still line up.
    live_key = _endpoint_keyer(
        live, funnel_ordinals={j: i for i, j in funnel_pairs.items()}
    )
    desired_key = _endpoint_keyer(desired)
    _diff_keyed(
        live.connections, desired.connections, None,
        "connection", path, changes, _diff_connection_fields,
        namer=_connection_name,
        key_fn_live=lambda c: (live_key(c.source), live_key(c.target)),
        key_fn_desired=lambda c: (desired_key(c.source), desired_key(c.target)),
    )

    # Child groups: recurse on name matches; whole-subtree add/remove else.
    live_children = {g.name: g for g in live.process_groups}
    desired_children = {g.name: g for g in desired.process_groups}
    for name, child in live_children.items():
        if name not in desired_children:
            changes.append(Change("remove", "process_group", path, name, live=child))
    for name, child in desired_children.items():
        if name in live_children:
            _diff_group(live_children[name], child, path + (name,), changes,
                        target_major)
        else:
            changes.append(Change("add", "process_group", path, name, desired=child))


def _managed_services(
    live: ProcessGroup, desired: ProcessGroup, target_major: Optional[int]
) -> List[Any]:
    """Live services minus the ones this NiFi line creates for itself on import.

    Only services the desired model does not name are ever dropped from the
    comparison, and only when their type is one the harvested
    ``IMPORT_SERVICES`` table says the import creates (see
    ``python -m niflow.codegen --import-defaults``).
    """
    from niflow.processors.rules import import_created_service_types

    created = import_created_service_types(target_major)
    if not created:
        return live.controller_services
    named = {service.name for service in desired.controller_services}
    return [
        service for service in live.controller_services
        if service.name in named or service.type not in created
    ]


def _diff_named(live_items, desired_items, kind, path, changes, field_differ=None):
    _diff_keyed(
        live_items, desired_items, lambda c: c.name, kind, path, changes, field_differ
    )


def _diff_keyed(live_items, desired_items, key_fn, kind, path, changes,
                field_differ=None, namer=None,
                key_fn_live=None, key_fn_desired=None):
    """Generic matcher: same key pairs up (in order for duplicates)."""
    key_fn_live = key_fn_live or key_fn
    key_fn_desired = key_fn_desired or key_fn
    namer = namer or (lambda c: key_fn_desired(c))
    live_by_key: Dict[Any, List[Any]] = {}
    for item in live_items:
        live_by_key.setdefault(key_fn_live(item), []).append(item)

    for item in desired_items:
        bucket = live_by_key.get(key_fn_desired(item))
        if bucket:
            live_item = bucket.pop(_closest_index(bucket, item, field_differ))
            if field_differ is not None:
                fields = field_differ(live_item, item)
                if fields:
                    changes.append(
                        Change("update", kind, path, namer(item),
                               fields=fields, desired=item, live=live_item)
                    )
        else:
            changes.append(Change("add", kind, path, namer(item), desired=item))

    for bucket in live_by_key.values():
        for leftover in bucket:
            changes.append(Change("remove", kind, path, namer(leftover), live=leftover))


def _closest_index(bucket: List[Any], desired: Any, field_differ) -> int:
    """Index of the live candidate most similar to ``desired``.

    Same-key duplicates (parallel edges between one endpoint pair, notably)
    all land in one bucket; popping the first listed candidate paired them
    arbitrarily, so consecutive plans "rotated" the clones and re-applied
    forever. Choosing the candidate with the fewest differing fields means
    an exact twin always pairs at cost zero — which is exactly what makes
    plan -> apply -> plan converge. Ties keep listed order (deterministic).
    """
    if len(bucket) == 1 or field_differ is None:
        return 0
    costs = [len(field_differ(candidate, desired)) for candidate in bucket]
    return costs.index(min(costs))


def _diff_processor_fields(
    live: Processor, desired: Processor, target_major: Optional[int] = None
) -> Dict[str, Tuple[Any, Any]]:
    fields = _diff_properties(
        live.properties, desired.properties, desired.type, target_major)
    for name in _PROCESSOR_FIELDS:
        a, b = getattr(live, name), getattr(desired, name)
        if _normalise_field(name, a, desired.type) != _normalise_field(name, b, desired.type):
            # A live RUNNING processor is not drift against a model that never
            # said anything about run state: the live read now reports RUNNING
            # (it used to be sanitised to ENABLED), and proposing to *stop*
            # every running processor because the field defaults to ENABLED
            # would turn a plan into an outage. Written down — either value —
            # it is an assertion again, and stays diffed.
            if (name == "scheduled_state" and a == "RUNNING" and b == "ENABLED"
                    and "scheduled_state" not in desired.model_fields_set):
                continue
            fields[name] = (a, b)
    return fields


def _diff_service_fields(
    live: ControllerService,
    desired: ControllerService,
    target_major: Optional[int] = None,
) -> Dict[str, Tuple[Any, Any]]:
    """Service drift. ``enabled`` counts only when the model actually states it.

    NiFi imports every controller service of a pushed flow DISABLED — the
    snapshot's ``scheduledState`` is ignored on create — so the model's
    ``enabled=True`` *default* is a promise no push keeps, and diffing it
    reported drift on every service-bearing flow immediately after a clean
    push, forever. Same rule as :func:`_diff_properties`: a side that states
    nothing takes the other's value instead of proposing a change. An
    explicit ``enabled=True``/``False`` (pulled flows always carry one) is an
    assertion, stays diffed, and ``push --update`` enables/disables to match.
    The live side is only as good as the pull: NiFi's flow-definition download
    sanitises run state (services always read DISABLED, processors never read
    RUNNING), so a stated ``enabled=True`` re-plans until the live read is
    taught to ask the controller-services endpoint for the real state.
    """
    fields = _diff_properties(
        live.properties, desired.properties, desired.type, target_major)
    for name in _SERVICE_FIELDS:
        if name == "enabled" and "enabled" not in desired.model_fields_set:
            continue
        a, b = getattr(live, name), getattr(desired, name)
        if a != b:
            fields[name] = (a, b)
    return fields


def _diff_connection_fields(live: Connection, desired: Connection) -> Dict[str, Tuple[Any, Any]]:
    fields: Dict[str, Tuple[Any, Any]] = {}
    for name in _CONNECTION_FIELDS:
        a, b = getattr(live, name), getattr(desired, name)
        if name == "relationships":
            # Port/funnel sources carry no relationships; ignore the model
            # default ["success"] on either side.
            if isinstance(live.source, (Port, Funnel)):
                continue
            a, b = sorted(a), sorted(b)
        if a != b:
            fields[name] = (getattr(live, name), getattr(desired, name))
    return fields


def _diff_properties(
    live: Dict[str, Any],
    desired: Dict[str, Any],
    type_str: str = "",
    target_major: Optional[int] = None,
) -> Dict[str, Tuple[Any, Any]]:
    """Property drift, judged on *effective* values, in the target's namespace.

    Both sides are canonicalized first (display-name keys -> server keys), and
    a side that leaves a property unset effectively holds the descriptor
    default — NiFi materialises defaults on the live side, so comparing raw
    dicts would propose unsetting every default back to ``None`` forever.

    ``target_major`` decides *whose* defaults those are. Read from the 2.x
    catalog alone, a property that exists only on a 1.x server has no descriptor
    at all, so its materialised value was compared against ``None`` and planned
    as an unset on every run — the diff-side twin of the cross-version emission
    bug. With the target's own descriptors, a 1.x-only property sitting at its
    1.x default is what the model asking for nothing *means* there, and no
    change is proposed. A value that is NOT the default is still real drift: a
    property the user actually removed from a pulled flow still plans as an
    unset, which is the distinction this has to keep.
    """
    from niflow.processors.rules import (
        canonical_properties, descriptors_for_target, import_created_properties,
        unsupported_properties,
    )

    live = canonical_properties(type_str, live)
    desired = canonical_properties(type_str, desired)
    descriptors = descriptors_for_target(type_str, target_major)
    fields: Dict[str, Tuple[Any, Any]] = {}
    server_managed = import_created_properties(type_str, target_major)
    # Properties the target line does not have at all. The emitter already
    # omits them from the snapshot and says so loudly (and `validate` fails on
    # them against the baseline), so re-reporting them as drift on every plan
    # is the "cries wolf" pattern: the plan would never converge and the real
    # changes would be buried under an intent that cannot land there.
    impossible = set(unsupported_properties(type_str, desired, target_major)) \
        if target_major is not None else set()
    for key in sorted(set(live) | set(desired)):
        a, b = live.get(key), desired.get(key)
        if key in server_managed and b is None and a is not None:
            # The import wired a service it created into this property; the
            # model saying nothing is not a request to unset it.
            continue
        if key in impossible and a is None:
            continue
        allowable = (descriptors.get(key) or {}).get("allowable")
        default = (descriptors.get(key) or {}).get("default")
        a_eff = _effective_prop(a, default, allowable)
        b_eff = _effective_prop(b, default, allowable)
        if a_eff != b_eff:
            fields[f"properties[{key}]"] = (_display_prop(a), _display_prop(b))
    return fields


def _effective_prop(value: Any, default: Any, allowable: Any) -> Any:
    """What a property is really worth on one side of the diff.

    Unset means the descriptor default. And an **empty string means unset**
    when the descriptor has no default: NiFi materialises "no value" as ``""``
    for some properties (``DetectDuplicate``'s ``FlowFile Description``), so a
    live ``""`` against a model that says nothing was read as drift and
    ``properties[FlowFile Description]: '' -> None`` re-planned forever.
    A ``""`` written against a descriptor that *does* have a default is still
    a real assertion — the user overriding the default with nothing — and
    stays diffed.
    """
    if value is None:
        return default
    normalised = _normalise_prop(value, allowable)
    if normalised == "" and default is None:
        return None
    return normalised


def _normalise_prop(value: Any, allowable: Any = None) -> Any:
    """Compare property values the way NiFi stores them: as strings.

    A model built through :class:`~niflow.core.Processor` is already
    normalised, but one whose ``properties`` dict was edited in place after
    construction is not — and NiFi returns ``"10"`` for a Python ``10``
    either way, so the differ does its own coercion rather than trusting it.
    """
    if isinstance(value, ControllerService):
        return f"@service:{value.name}"
    return nifi_property_value(value, allowable)


def _display_prop(value: Any) -> Any:
    if isinstance(value, ControllerService):
        return f"service {value.name!r}"
    return value


def _normalise_field(name: str, value: Any, type_str: str = "") -> Any:
    """The comparable form of a component field.

    Relationship lists are sets to NiFi, so order can't be drift. And
    ``execution_node`` has a per-type effective default: NiFi forces
    ``PRIMARY`` on ``@PrimaryNodeOnly`` types (ListFTP, ListSFTP, ...) and
    rejects anything else, so the model's ``ALL`` default — and even an
    explicit ``ALL`` — means PRIMARY there. Without this, every flow holding
    one of those types planned ``execution_node: 'PRIMARY' -> 'ALL'`` after a
    clean push, forever. The primary-node-only set is harvested from the live
    server (``ProcessorDTO.executionNodeRestricted``) into the catalog.
    """
    if isinstance(value, list):
        return sorted(value)
    if name == "execution_node" and type_str:
        from niflow.processors.rules import primary_node_only

        if primary_node_only(type_str):
            return "PRIMARY"
    return value


def _endpoint_keyer(group: ProcessGroup, funnel_ordinals: Optional[Dict[int, int]] = None):
    """Key connection endpoints of ``group`` structurally (no live ids).

    Funnels key by ordinal within the group — remapped through
    ``funnel_ordinals`` when given, so a live funnel keys as its matched
    desired twin (unmatched ones get a negative key that never collides).
    Ports belonging to a direct child group are qualified by the child's
    name (two children may both expose an ``out`` port).
    """
    funnel_ordinal = {id(f): i for i, f in enumerate(group.funnels)}
    own_ports = {id(p) for p in group.input_ports + group.output_ports}
    port_owner: Dict[int, str] = {}
    for child in group.process_groups:
        for port in child.input_ports + child.output_ports:
            port_owner[id(port)] = child.name

    def key(component: NiFiComponent) -> Tuple[str, Any, str]:
        if isinstance(component, Funnel):
            i = funnel_ordinal.get(id(component), -1)
            if funnel_ordinals is not None:
                i = funnel_ordinals.get(i, -1 - i)
            return ("funnel", i, "")
        kind = type(component).__name__.lower()
        owner = ""
        if isinstance(component, Port) and id(component) not in own_ports:
            owner = port_owner.get(id(component), "?")
        return (kind, owner, getattr(component, "name", "") or "")

    return key


def match_funnels(live: ProcessGroup, desired: ProcessGroup) -> Dict[int, int]:
    """Pair funnels across two groups: desired ordinal -> live ordinal.

    Funnels have no name, so identity comes from connection topology: same-
    signature funnels pair in listed order (they are indistinguishable), and
    leftovers — rewired funnels — pair by best endpoint overlap so an
    equal-count rewire stays connection churn instead of funnel churn. Also
    used by the applier, so plan and apply resolve the same live funnel.
    """
    live_sigs = funnel_signatures(live)
    unclaimed: Dict[Any, List[int]] = {}
    for j, sig in enumerate(live_sigs):
        unclaimed.setdefault(sig, []).append(j)

    pairs: Dict[int, int] = {}
    desired_sigs = funnel_signatures(desired)
    leftover: List[int] = []
    for i, sig in enumerate(desired_sigs):
        bucket = unclaimed.get(sig)
        if bucket:
            pairs[i] = bucket.pop(0)
        else:
            leftover.append(i)

    spare = sorted(j for bucket in unclaimed.values() for j in bucket)
    for i in leftover:
        if not spare:
            break
        best = max(spare, key=lambda j: _signature_overlap(desired_sigs[i], live_sigs[j]))
        spare.remove(best)
        pairs[i] = best
    return pairs


def funnel_signatures(group: ProcessGroup) -> List[Tuple[Any, Any]]:
    """Topology signature per funnel: (what feeds it, what it feeds).

    Neighbour endpoints use the structural key — funnel neighbours collapse
    to a wildcard, so funnel-to-funnel chains stay ordinal within their
    signature bucket — and inbound edges carry their relationships so two
    funnels fed by the same processor on different relationships differ.
    """
    key = _endpoint_keyer(group)

    def neighbour(component: NiFiComponent) -> Tuple[str, Any, str]:
        if isinstance(component, Funnel):
            return ("funnel", "*", "")
        return key(component)

    signatures: List[Tuple[Any, Any]] = []
    for funnel in group.funnels:
        ins = sorted(
            (neighbour(c.source), tuple(sorted(c.relationships or [])))
            for c in group.connections
            if c.target is funnel
        )
        outs = sorted(
            neighbour(c.target) for c in group.connections if c.source is funnel
        )
        signatures.append((tuple(ins), tuple(outs)))
    return signatures


def _signature_overlap(a: Tuple[Any, Any], b: Tuple[Any, Any]) -> int:
    """How many inbound/outbound endpoint entries two signatures share."""
    return sum(
        sum((Counter(a[side]) & Counter(b[side])).values()) for side in (0, 1)
    )


def _connection_name(conn: Connection) -> str:
    def end(c: NiFiComponent) -> str:
        if isinstance(c, Funnel):
            return "(funnel)"
        return getattr(c, "name", "?") or "?"

    rels = ",".join(conn.relationships) if not isinstance(conn.source, (Port, Funnel)) else ""
    arrow = f" -[{rels}]-> " if rels else " -> "
    return f"{end(conn.source)}{arrow}{end(conn.target)}"


# Kinds whose add/remove pairs may really be a rename. Identity is name-based,
# so renaming a component turns into remove+add — which destroys processor
# state and every FlowFile queued on the component's connections. The plan
# can't know intent, but it can shout when the shape looks like a rename.
_RENAMEABLE_KINDS = ("processor", "controller_service", "input_port", "output_port", "process_group")


def _annotate_sensitive(changes: List[Change], target_major: Optional[int]) -> None:
    """Mark property changes whose live value NiFi refuses to disclose.

    A sensitive property — a DBCP pool's ``Password``, an API token, a
    keystore passphrase — reads back as ``None`` (or the literal ``********``)
    however it was set, so a model that states one differs from the live side
    *forever*. The change is kept, because sending the model's value is the
    only way an intended change can ever land, but it is labelled: an eternal
    "1 to change" with no explanation is how people learn to ignore a plan.

    Two sources, because one of them cannot cover work: the harvested catalogs
    know every type Apache ships, and the live snapshot's own
    ``propertyDescriptors`` know the rest — a **custom NAR**'s password is
    invisible to any catalog, and its flow would otherwise re-plan that change
    on every run forever (measured against a real custom NAR on 1.24.0).

    :func:`niflow.plan.only_unknowable` then lets the callers that answer
    "has anything drifted?" — ``niflow drift``, the fuzz convergence checks —
    tell this apart from real drift.
    """
    from niflow.processors.rules import sensitive_properties

    for change in changes:
        if change.op != "update" or not change.fields:
            continue
        type_str = getattr(change.desired, "type", "") or getattr(change.live, "type", "")
        if not type_str:
            continue
        secret = set(sensitive_properties(type_str, target_major))
        # The server is the authority for anything the catalog has never seen.
        secret.update(getattr(change.live, "sensitive_keys", None) or ())
        secret.update(getattr(change.desired, "sensitive_keys", None) or ())
        if not secret:
            continue
        unknowable = tuple(
            name for name in change.fields
            if name.startswith("properties[") and name.endswith("]")
            and name[len("properties["):-1] in secret
            and change.fields[name][0] in (None, "", "********")
        )
        if not unknowable:
            continue
        change.unknowable = unknowable
        listed = ", ".join(n[len("properties["):-1] for n in unknowable)
        note = (f"sensitive: NiFi never returns {listed}, so this cannot be "
                f"compared — it is applied as written and will re-plan on every "
                f"run whether or not the server already has it")
        change.note = f"{change.note} / {note}" if change.note else note


def only_unknowable(change: Change) -> bool:
    """True when every field of *change* is one NiFi will not disclose.

    Such a change is an assertion, not evidence: nothing about it says the live
    flow drifted from the model.
    """
    return bool(change.unknowable) and set(change.fields) == set(change.unknowable)


def _annotate_renames(changes: List[Change]) -> None:
    """Flag add/remove pairs that look like renames (same group, same type).

    Processors and services pair only when the component type matches (in
    listed order for multiples). Ports and child groups carry no type, so
    they pair only when the match is unambiguous: exactly one add and one
    remove of that kind in the group.
    """
    by_bucket: Dict[Tuple, Dict[str, List[Change]]] = {}
    for change in changes:
        if change.kind not in _RENAMEABLE_KINDS or change.op not in ("add", "remove"):
            continue
        comp = change.desired if change.op == "add" else change.live
        ctype = getattr(comp, "type", None)
        bucket = by_bucket.setdefault((change.path, change.kind, ctype), {"add": [], "remove": []})
        bucket[change.op].append(change)

    for (path, kind, ctype), bucket in by_bucket.items():
        adds, removes = bucket["add"], bucket["remove"]
        if ctype is None and (len(adds) != 1 or len(removes) != 1):
            continue  # untyped kinds: only flag the unambiguous 1:1 case
        for add, remove in zip(adds, removes):
            noun = kind.replace("_", " ")
            add.note = (
                f"looks like a RENAME of {noun} {remove.name!r} — identity is "
                f"name-based, so this applies as remove+add: "
                + ("everything inside the old group is destroyed, including queued data. "
                   if kind == "process_group"
                   else "component state and FlowFiles queued on its connections are LOST. ")
                + "Keep the old name (or rename it on the NiFi canvas first) if that matters."
            )
            remove.note = f"may be a rename to {add.name!r} — see the matching add above/below"


# ---------------------------------------------------------------- rendering


_OP_MARK = {"add": "+", "remove": "-", "update": "~"}


def format_plan(changes: List[Change]) -> str:
    """Human-readable plan, one line per change (terraform-plan flavour)."""
    if not changes:
        return "No changes. Live flow matches the model."
    lines: List[str] = []
    for change in changes:
        mark = _OP_MARK[change.op]
        head = f"{mark} {change.kind.replace('_', ' ')} {change.location}: {change.name}"
        if change.op == "add" and isinstance(change.desired, Processor):
            head += f" ({change.desired.type.rsplit('.', 1)[-1]})"
        lines.append(head)
        if change.note:
            lines.append(f"    ! {change.note}")
        for fname, (old, new) in change.fields.items():
            suffix = "   (sensitive — not comparable)" if fname in change.unknowable else ""
            lines.append(f"    {fname}: {old!r} -> {new!r}{suffix}")
    counts = {"add": 0, "remove": 0, "update": 0}
    for change in changes:
        counts[change.op] += 1
    lines.append(
        f"Plan: {counts['add']} to add, {counts['update']} to change, "
        f"{counts['remove']} to remove."
    )
    renames = sum(1 for c in changes if c.op == "add" and c.note)
    if renames:
        lines.append(
            f"WARNING: {renames} probable rename(s) detected — renames apply as "
            f"remove+add and destroy state/queued data (see ! lines above)."
        )
    return "\n".join(lines)
