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
they occur, same-named components are paired in listed order and the rest
become adds/removes). Positions and layout are deliberately NOT diffed —
moving things on the canvas is cosmetic and must never force an update.
Sensitive processor properties never appear in live snapshots, so a desired
literal value for one always registers as an update; reference them through
parameters (``#{...}``) to keep plans quiet.
"""
from __future__ import annotations

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

    @property
    def location(self) -> str:
        return "/".join(self.path) or "."


def diff_flows(live: ProcessGroup, desired: ProcessGroup) -> List[Change]:
    """Return the ordered change plan turning ``live`` into ``desired``."""
    changes: List[Change] = []
    _diff_group(live, desired, (), changes)
    _annotate_renames(changes)
    return changes


# ---------------------------------------------------------------- internals


def _diff_group(
    live: ProcessGroup,
    desired: ProcessGroup,
    path: Tuple[str, ...],
    changes: List[Change],
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

    _diff_named(
        live.controller_services, desired.controller_services,
        "controller_service", path, changes, _diff_service_fields,
    )
    _diff_named(live.input_ports, desired.input_ports, "input_port", path, changes)
    _diff_named(live.output_ports, desired.output_ports, "output_port", path, changes)
    _diff_named(
        live.processors, desired.processors,
        "processor", path, changes, _diff_processor_fields,
    )

    # Funnels are anonymous (nothing diffable besides position): match by
    # ordinal, surplus becomes add/remove.
    for i in range(len(desired.funnels), len(live.funnels)):
        changes.append(Change("remove", "funnel", path, f"funnel[{i}]", live=live.funnels[i]))
    for i in range(len(live.funnels), len(desired.funnels)):
        changes.append(Change("add", "funnel", path, f"funnel[{i}]", desired=desired.funnels[i]))

    # Labels: match by text (duplicates pair in order).
    _diff_keyed(
        live.labels, desired.labels, lambda l: l.text, "label", path, changes
    )

    # Connections: identity is (source endpoint, target endpoint); the
    # relationship set and queue settings are updatable fields. Endpoint keys
    # are built per side — funnels by ordinal within their group, ports
    # qualified by owning child group — so a live tree (with ids) and a
    # desired tree (without) still line up.
    live_key = _endpoint_keyer(live)
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
            _diff_group(live_children[name], child, path + (name,), changes)
        else:
            changes.append(Change("add", "process_group", path, name, desired=child))


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
            live_item = bucket.pop(0)
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


def _diff_processor_fields(live: Processor, desired: Processor) -> Dict[str, Tuple[Any, Any]]:
    fields = _diff_properties(live.properties, desired.properties, desired.type)
    for name in _PROCESSOR_FIELDS:
        a, b = getattr(live, name), getattr(desired, name)
        if _normalise_field(name, a) != _normalise_field(name, b):
            fields[name] = (a, b)
    return fields


def _diff_service_fields(
    live: ControllerService, desired: ControllerService
) -> Dict[str, Tuple[Any, Any]]:
    fields = _diff_properties(live.properties, desired.properties, desired.type)
    for name in _SERVICE_FIELDS:
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
    live: Dict[str, Any], desired: Dict[str, Any], type_str: str = ""
) -> Dict[str, Tuple[Any, Any]]:
    """Property drift, judged on *effective* values.

    Both sides are canonicalized first (display-name keys -> server keys), and
    a side that leaves a property unset effectively holds the descriptor
    default — NiFi materialises defaults on the live side, so comparing raw
    dicts would propose unsetting every default back to ``None`` forever.
    """
    from niflow.processors.rules import canonical_properties, descriptors_for

    live = canonical_properties(type_str, live)
    desired = canonical_properties(type_str, desired)
    descriptors = descriptors_for(type_str) or {}
    fields: Dict[str, Tuple[Any, Any]] = {}
    for key in sorted(set(live) | set(desired)):
        a, b = live.get(key), desired.get(key)
        default = (descriptors.get(key) or {}).get("default")
        a_eff = default if a is None else _normalise_prop(a)
        b_eff = default if b is None else _normalise_prop(b)
        if a_eff != b_eff:
            fields[f"properties[{key}]"] = (_display_prop(a), _display_prop(b))
    return fields


def _normalise_prop(value: Any) -> Any:
    if isinstance(value, ControllerService):
        return f"@service:{value.name}"
    return value


def _display_prop(value: Any) -> Any:
    if isinstance(value, ControllerService):
        return f"service {value.name!r}"
    return value


def _normalise_field(name: str, value: Any) -> Any:
    if isinstance(value, list):
        return sorted(value)
    return value


def _endpoint_keyer(group: ProcessGroup):
    """Key connection endpoints of ``group`` structurally (no live ids).

    Funnels key by ordinal within the group; ports belonging to a direct
    child group are qualified by the child's name (two children may both
    expose an ``out`` port).
    """
    funnel_ordinal = {id(f): i for i, f in enumerate(group.funnels)}
    own_ports = {id(p) for p in group.input_ports + group.output_ports}
    port_owner: Dict[int, str] = {}
    for child in group.process_groups:
        for port in child.input_ports + child.output_ports:
            port_owner[id(port)] = child.name

    def key(component: NiFiComponent) -> Tuple[str, Any, str]:
        if isinstance(component, Funnel):
            return ("funnel", funnel_ordinal.get(id(component), -1), "")
        kind = type(component).__name__.lower()
        owner = ""
        if isinstance(component, Port) and id(component) not in own_ports:
            owner = port_owner.get(id(component), "?")
        return (kind, owner, getattr(component, "name", "") or "")

    return key


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
            lines.append(f"    {fname}: {old!r} -> {new!r}")
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
