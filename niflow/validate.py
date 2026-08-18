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
  ``success`` is wired, and relationships you didn't know existed. Types whose
  dynamic properties create relationships (RouteOnAttribute and friends, see
  ``rules.DYNAMIC_RELATIONSHIP_TYPES``) have those counted as real
  relationships — valid to connect, and flagged when left unhandled; and
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
from typing import Dict, List, Set

from niflow.core import Flow, ProcessGroup, find_identity_collisions
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


def validate_flow(flow: Flow) -> List[dict]:
    """Return a list of ``{"component", "message"}`` issues, empty if clean."""
    issues: List[dict] = [
        # Name-based identity means same-kind duplicates in one group would
        # silently merge or clobber each other on push — always an error.
        {"component": where, "message": message}
        for where, message in find_identity_collisions(flow)
    ]

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

            known = relationships_for(proc.type)
            if known is not None:
                # Precise: we know the full relationship set for this type.
                # Dynamic-relationship types (RouteOnAttribute, ...) extend it
                # with one relationship per dynamic property; those must be
                # handled just like the static ones.
                dynamic = dynamic_relationships_for(proc.type, proc.properties or {})
                known_set = set(known) | set(dynamic or ())
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
                if dynamic is not None or not supports_dynamic_relationships(proc.type):
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
            issues.extend(_targeted_issues(proc, label))

        for child in group.process_groups:
            visit(child, path)

    visit(flow, "")
    return issues
