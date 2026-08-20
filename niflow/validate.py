"""Local, pre-push validation of a :class:`~niflow.core.Flow`.

NiFi refuses to run a processor whose relationships aren't all *handled* — each
must either feed a connection or be auto-terminated — and reports the familiar
*"Relationship 'success' is not connected to any component and is not
auto-terminated"*. Catching the obvious cases *before* a delete-and-recreate
push saves a round trip and avoids replacing a working live group with a broken
one.

When the processor type's rulebook has been harvested into the catalog
(``make catalog``, see :mod:`niflow.processors.rules`), this checks:

* **relationships**, precisely — catching ``failure`` left dangling even while
  ``success`` is wired, and relationships you didn't know existed. The set is
  the one the *target* NiFi line has (an explicit ``target_version``, else the
  declared baseline), and it accounts for relationships a property switches on:
  ``UpdateAttribute`` with ``Store State`` = "Store state locally" grows a
  ``set state fail`` relationship, and NiFi will not start the processor until
  it is handled. Types whose dynamic properties create relationships
  (RouteOnAttribute and friends) have those counted as real relationships —
  valid to connect, and flagged when left unhandled; and
* **properties** — required properties left unset (honouring defaults and
  ``dependencies``), and values outside a property's allowable set.

For types not yet harvested it falls back to a relationship heuristic: a
processor with **nothing** handled is always invalid, but subtler gaps can't be
seen and fall to NiFi's own validation (the Processor Errors panel after a
push). Property values that use Expression Language or parameters are left for
NiFi, since they can't be judged statically.
"""
from __future__ import annotations

import re
from typing import Dict, List, Set, Tuple

from niflow.core import (
    Flow, Port, ProcessGroup, find_identity_collisions,
    find_unregistered_components,
)
from niflow.processors.rules import (
    canonical_properties,
    descriptors_for,
    dynamic_relationships_for,
    relationships_for,
    supports_dynamic_relationships,
)

# Time-duration units NiFi accepts (FormatUtils). Generous on spelling so we
# never flag a *valid* duration — the goal is catching obvious typos like
# "5 sekonds", not policing exact wording.
_TIME_UNITS = {
    "ns", "nano", "nanos", "nanosecond", "nanoseconds",
    "ms", "milli", "millis", "millisecond", "milliseconds",
    "s", "sec", "secs", "second", "seconds",
    "m", "min", "mins", "minute", "minutes",
    "h", "hr", "hrs", "hour", "hours",
    "d", "day", "days",
    "w", "wk", "wks", "week", "weeks",
}
_DURATION_RE = re.compile(r"^\s*\d+(\.\d+)?\s*([A-Za-z]+)\s*$")


# ``#{param}`` — the reference syntax; whether a context is BOUND is static
# even though the value is not. NiFi escapes a literal ``#`` by doubling it,
# so ``##{x}`` is the text "#{x}" and not a reference at all (flows/torture.py
# has one on purpose) — an *odd* run of ``#`` before the brace is the real
# thing, an even run is escaped.
_PARAMETER_REF = re.compile(r"(#+)\{([^}]+)\}")


def _parameter_references(value: str) -> List[str]:
    return [name for hashes, name in _PARAMETER_REF.findall(value)
            if len(hashes) % 2 == 1]


def _is_expression(value: object) -> bool:
    """Property values using EL (``${...}``) or parameters (``#{...}``) can't be
    judged statically — skip value checks for them."""
    return isinstance(value, str) and ("${" in value or "#{" in value)


def _dependency_active(entry: dict, props: dict) -> bool:
    """Whether a descriptor's rules apply, given its ``dependencies``.

    A property is only required/validated when each property it depends on holds
    one of the listed values. We stay conservative: if a dependency can't be
    confirmed satisfied, treat the rule as inactive (don't flag) to avoid false
    positives.
    """
    for dep in entry.get("dependencies") or []:
        current = props.get(dep["property"])
        allowed = dep.get("values") or []
        if allowed:
            if not (isinstance(current, str) and current in allowed):
                return False
        elif current in (None, ""):
            return False
    return True


def _is_time_duration(value: object) -> bool:
    if _is_expression(value) or not isinstance(value, str):
        return _is_expression(value)  # EL is unjudgeable -> treat as OK
    match = _DURATION_RE.match(value)
    return bool(match) and match.group(2).lower() in _TIME_UNITS


def _targeted_issues(proc, label: str) -> List[dict]:
    """Model-level value checks NiFi only enforces in code (durations, cron, ...).

    These need no harvested catalog — the fields live on every processor — so
    they run universally and offline.
    """
    out: List[dict] = []

    period = proc.scheduling_period
    if proc.scheduling_strategy == "CRON_DRIVEN":
        if isinstance(period, str) and not _is_expression(period) and len(period.split()) not in (6, 7):
            out.append({"component": label,
                        "message": f"scheduling period {period!r} is not a valid CRON "
                                   f"expression (expected 6 or 7 fields)"})
    elif not _is_time_duration(period):
        out.append({"component": label,
                    "message": f"scheduling period {period!r} is not a valid time "
                               f"duration (e.g. '30 sec')"})

    for field, value in (("penalty duration", proc.penalty_duration),
                         ("yield duration", proc.yield_duration),
                         ("max backoff period", proc.max_backoff_period)):
        if not _is_time_duration(value):
            out.append({"component": label,
                        "message": f"{field} {value!r} is not a valid time duration"})

    if proc.concurrent_tasks < 1:
        out.append({"component": label,
                    "message": f"concurrent tasks must be at least 1 "
                               f"(got {proc.concurrent_tasks})"})
    if proc.run_duration_millis < 0:
        out.append({"component": label,
                    "message": f"run duration must be 0 or more "
                               f"(got {proc.run_duration_millis})"})
    return out


def _near_miss_issues(component, label: str) -> List[dict]:
    """Keys that will land as an inert dynamic property instead of the real one.

    This is the check that was missing when ``max-bin-age`` reached a work
    1.24: NiFi takes any unrecognised key as a dynamic property, so nothing
    rejects it locally, the value does nothing, and the processor goes invalid
    on a server you then have to go and read. See
    :func:`niflow.processors.rules.near_miss_properties` for what qualifies.
    """
    from niflow.processors.rules import near_miss_properties

    return [
        {"component": label,
         "message": f"property {key!r} is not a property of this type — "
                    f"did you mean {suggestion!r}? As written NiFi keeps it as "
                    f"a dynamic property, the value has no effect, and the "
                    f"component is invalid"}
        for key, suggestion in sorted(
            near_miss_properties(component.type, component.properties or {}).items())
    ]


def _property_issues(proc, label: str) -> List[dict]:
    """Required-property and allowable-value checks from harvested descriptors."""
    descriptors = descriptors_for(proc.type)
    if not descriptors:
        return []
    # Display-name keys ("Custom Text") count as setting the canonical
    # property — the emitter rewrites them the same way at push time.
    props = canonical_properties(proc.type, proc.properties or {})
    out: List[dict] = []
    for name, entry in descriptors.items():
        if not _dependency_active(entry, props):
            continue
        value = props.get(name)
        unset = value in (None, "")
        if entry.get("required") and unset and "default" not in entry:
            out.append({"component": label,
                        "message": f"required property '{name}' is not set"})
            continue
        allowable = entry.get("allowable")
        if (allowable and isinstance(value, str) and value
                and not _is_expression(value) and value not in allowable):
            out.append({"component": label,
                        "message": f"property '{name}' = {value!r} is not one of "
                                   f"{allowable}"})
    return out


def _structural_issues(flow: Flow) -> List[dict]:
    """Two things NiFi rejects at push time that are visible right here.

    * **A connection that crosses a group boundary without a port.** NiFi
      requires an input/output port to leave a process group; niflow emitted
      the connection happily and the push failed with "Connection has a source
      with identifier … but no component could be found in the Process Group",
      which reads like a niflow bug and costs a whole push to discover. Legal
      endpoints for a connection owned by group *g* are members of *g* itself,
      or a **port** of one of its direct children (the parent-to-child hop
      niflow already supports).
    * **A parameter reference with no parameter context bound.** The validator
      deliberately does not judge ``#{...}`` *values* — they are resolved on
      the server — but whether any context is bound up the tree is static, and
      NiFi refuses the component outright: "references one or more Parameters
      but no Parameter Context is currently set on the Process Group".
    """
    owners: Dict[int, Tuple[ProcessGroup, str]] = {}

    def index(group: ProcessGroup, path: str) -> None:
        for member in (list(group.processors) + list(group.input_ports)
                       + list(group.output_ports) + list(group.funnels)
                       + list(group.process_groups)):
            owners[id(member)] = (group, path)
        for child in group.process_groups:
            index(child, f"{path}/{child.name}")

    index(flow, flow.name or ".")
    issues: List[dict] = []

    def visit(group: ProcessGroup, path: str, context_bound: bool) -> None:
        bound = context_bound or group.parameter_context is not None
        children = {id(child) for child in group.process_groups}
        for conn in group.connections:
            for role, end in (("source", conn.source), ("destination", conn.target)):
                owner = owners.get(id(end))
                if owner is None:
                    continue  # not in the flow at all — reported separately
                owner_group, owner_path = owner
                if owner_group is group:
                    continue
                if isinstance(end, Port) and id(owner_group) in children:
                    continue  # the parent-to-child-port hop, which is legal
                issues.append({
                    "component": f"{path}/{_connection_label(conn)}",
                    "message": f"connection {role} {end.name!r} lives in "
                               f"{owner_path!r}, not in {path!r} — NiFi needs an "
                               f"input/output port to cross a group boundary",
                })
        if not bound:
            for component in list(group.processors) + list(group.controller_services):
                referenced = sorted({
                    ref for value in (component.properties or {}).values()
                    if isinstance(value, str)
                    for ref in _parameter_references(value)
                })
                if referenced:
                    issues.append({
                        "component": f"{path}/{component.name}",
                        "message": f"references parameter(s) "
                                   f"{', '.join(repr(r) for r in referenced)} but no "
                                   f"parameter context is bound to this group or any "
                                   f"ancestor — NiFi refuses to run the component",
                    })
        for child in group.process_groups:
            visit(child, f"{path}/{child.name}", bound)

    visit(flow, flow.name or ".", False)
    return issues


def _connection_label(conn) -> str:
    return f"{getattr(conn.source, 'name', '?')} -> {getattr(conn.target, 'name', '?')}"


def validate_flow(
    flow: Flow, target_version: object = None, *, baseline: bool = True
) -> List[dict]:
    """Return a list of ``{"component", "message"}`` issues, empty if clean.

    Two cross-version modes, and they are mutually exclusive:

    ``target_version`` ("1.24", "1", 1, ...) is the *ad-hoc* check: judge this
    flow against that NiFi line, using the generated cross-version map
    (:mod:`niflow.compat`) — properties and types that do not exist there,
    values outside that line's allowable set, properties it makes mandatory.

    With no ``target_version``, the flow is checked against the declared
    **compatibility baseline** instead (``NIFLOW_MIN_NIFI_VERSION`` in
    ``.niflow.env``, default 1.24). That is on by default on purpose: the
    oldest line the estate runs is a standing requirement, not something to
    remember to pass a flag for, and a property that cannot land there fails
    silently on the server. Pass ``baseline=False`` (or set the baseline to
    ``none``) when only the newest line matters.

    Either way this is the check that catches, on your laptop, the property
    that would have silently become an inert dynamic property at work.
    """
    from niflow.compat import baseline_major, parse_major

    # Which NiFi line will actually run this flow. Relationships are judged
    # against *that* line's harvested set, not the catalog's: a 1.24 server has
    # relationships 2.x does not (``UpdateAttribute``'s ``set state fail``) and
    # it is the server that refuses to start the processor. With no explicit
    # target the declared baseline (default 1.24) answers, for the same reason
    # the cross-version property check uses it.
    if target_version is not None:
        target_major = parse_major(str(target_version))
    elif baseline:
        target_major = baseline_major()
    else:
        target_major = None

    issues: List[dict] = [
        # Name-based identity means same-kind duplicates in one group would
        # silently merge or clobber each other on push — always an error.
        {"component": where, "message": message}
        for where, message in find_identity_collisions(flow)
    ] + [
        # A service used as a property value but never added, or a connection
        # endpoint that is not in the flow: the emitter has no identifier for
        # it and raised a bare KeyError mid-push. Statically visible here.
        {"component": where, "message": message}
        for where, message in find_unregistered_components(flow)
    ] + _structural_issues(flow)

    def visit(group: ProcessGroup, prefix: str) -> None:
        path = f"{prefix}/{group.name}" if prefix else group.name

        # Relationships each component actually feeds out, by source identity,
        # and which components have anything feeding *in*.
        used: Dict[int, Set[str]] = {}
        has_incoming: Set[int] = set()
        for conn in group.connections:
            has_incoming.add(id(conn.target))
            for rel in conn.relationships:
                used.setdefault(id(conn.source), set()).add(rel)

        for proc in group.processors:
            label = f"{path}/{proc.name}"
            connected = used.get(id(proc), set())
            auto = set(proc.auto_terminate or [])

            # NiFi only allows Primary-Node-Only scheduling on *source*
            # processors; one with an incoming connection is live-invalid
            # ("'Execution Node' is invalid because Processors with incoming
            # connections cannot be scheduled for Primary Node Only.").
            if proc.execution_node == "PRIMARY" and id(proc) in has_incoming:
                issues.append({
                    "component": label,
                    "message": "execution node PRIMARY requires a source "
                               "processor — NiFi rejects Primary Node Only on "
                               "processors with incoming connections",
                })

            for rel in sorted(connected & auto):
                issues.append({
                    "component": label,
                    "message": f"relationship '{rel}' is both connected and "
                               f"auto-terminated (NiFi rejects this)",
                })

            props = proc.properties or {}
            known = relationships_for(proc.type, props, target_major)
            if known is not None:
                # Precise: we know the full relationship set for this type, on
                # the target line and for the property values actually set.
                # Dynamic-relationship types (RouteOnAttribute, ...) extend it
                # with one relationship per dynamic property; those must be
                # handled just like the static ones.
                dynamic = dynamic_relationships_for(proc.type, props, target_major)
                # "Must be handled" is the target line's set; "exists at all" is
                # the union of both lines, so validating against 1.24 never
                # calls a relationship that 2.x really has non-existent.
                known_set = set(known) | set(dynamic or ()) | set(
                    relationships_for(proc.type, props) or ())
                handled = connected | auto
                for rel in list(known) + sorted(set(dynamic or ()) - set(known)):
                    if rel not in handled:
                        issues.append({
                            "component": label,
                            "message": f"relationship '{rel}' is not connected "
                                       f"or auto-terminated",
                        })
                # A dynamic-relationship type whose per-property routing can't
                # be confirmed active (strategy switched or set via EL) has an
                # unknowable relationship set — skip existence checks rather
                # than risk false positives.
                if dynamic is not None or not supports_dynamic_relationships(
                        proc.type, target_major):
                    for rel in sorted(auto - known_set):
                        issues.append({
                            "component": label,
                            "message": f"auto-terminated relationship '{rel}' does "
                                       f"not exist on this processor type",
                        })
                    for rel in sorted(connected - known_set):
                        issues.append({
                            "component": label,
                            "message": f"a connection uses relationship '{rel}' that "
                                       f"does not exist on this processor type",
                        })
            elif not connected and not auto:
                # Heuristic fallback for un-harvested types.
                issues.append({
                    "component": label,
                    "message": "no relationship is connected or auto-terminated "
                               "— NiFi will flag its relationships as unhandled",
                })

            issues.extend(_property_issues(proc, label))
            issues.extend(_near_miss_issues(proc, label))
            issues.extend(_targeted_issues(proc, label))

        # Controller services were never checked here at all, which is the
        # same blind spot the cross-version work found: a service's properties
        # go through the identical descriptor tables (``_typed_entry`` covers
        # both catalogs), and a mistyped key on a service is *harder* to spot
        # on the canvas than one on a processor.
        for service in group.controller_services:
            label = f"{path}/{service.name}"
            issues.extend(_property_issues(service, label))
            issues.extend(_near_miss_issues(service, label))

        for child in group.process_groups:
            visit(child, path)

    visit(flow, "")

    if target_version is not None:
        from niflow.compat import flow_issues, parse_major

        target_major = parse_major(str(target_version))
        if target_major is not None:
            issues.extend(flow_issues(flow, target_major))
    elif baseline:
        from niflow.compat import baseline_issues

        issues.extend(baseline_issues(flow))
    return issues
