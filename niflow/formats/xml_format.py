"""Round-trip a :class:`~niflow.core.Flow` to/from a NiFi 1.x template XML.

NiFi 1.x exports flows as ``<template><snippet>...</snippet></template>`` XML:
the snippet wraps repeated ``<processors>``, ``<connections>``,
``<controllerServices>``, ``<inputPorts>``, ``<outputPorts>``, and
``<processGroups>`` children. This module converts that representation into our
in-memory :class:`Flow` model and back again — purely in stdlib, with no
nipyapi import, so it works in environments without NiFi.

The two pure functions are:

* :func:`from_xml` — parse a template (str / bytes / Path) into a :class:`Flow`.
* :func:`to_xml`  — emit a :class:`Flow` as a deterministic template string.

Determinism note: :func:`to_xml` assigns identifiers via UUID5 keyed on each
component's path within the flow — the same scheme as :mod:`json_format`. The
template ``<timestamp>`` is fixed too, so re-emitting an unchanged flow is
byte-stable across runs and machines.

This module is the vehicle for the **NiFi 1.x in-place push**: a group under
Registry version control is rebuilt by uploading :func:`to_xml` as a template
and instantiating it into the existing group (2.x uses copy/paste instead), so
anything this emitter drops is silently lost from the user's live flow. It is
therefore held to the same fidelity bar as :mod:`json_format`; the element set
below was checked field-by-field against a template downloaded from a live
NiFi 1.24 (see ``_TEMPLATE_CANNOT_CARRY``).

Caveats (the wiki fixture doesn't exercise these so they're documented here):

* We don't model the full set of *available* relationships on a
  :class:`Processor`, only the auto-terminated / retried ones and the ones
  referenced by outgoing connections. :func:`to_xml` emits ``<relationships>``
  elements for exactly that union (with ``autoTerminate``/``retry`` flags set
  per entry). On re-parse the available-set re-shrinks accordingly.
* Bundle coordinates are emitted with sensible defaults
  (``nifi-standard-nar``/``nifi-standard-services-api-nar``) because the model
  doesn't carry NAR coordinates yet. NiFi resolves the actual NAR at import.
* ControllerService ``enabled`` round-trips through ``<state>``
  (``ENABLED``/``DISABLED``).
* A template's ``<snippet>`` has no DTO for the group the contents land *in*,
  and NiFi's own template export drops parameter-context bindings entirely —
  see :func:`template_limitations`. XML is an export format (``niflow
  convert``), not a push vehicle: the 1.x in-place push moved to the snippet
  API on 2026-08-19, which loses none of this.
"""
from __future__ import annotations

import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from niflow.core import (
    Connection,
    ControllerService,
    Flow,
    Funnel,
    InputPort,
    Label,
    NiFiComponent,
    OutputPort,
    Port,
    Processor,
    ProcessGroup,
)
from niflow.layout import compute_layout
from niflow.processors.bundles import default_bundle

# Standard UUID DNS namespace — used as the root for deterministic UUID5s so
# generated identifiers don't collide with random NiFi-assigned ones.
_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

# Sensible-default bundle coordinates for emitted components. NiFi accepts these
# and resolves the actual NAR on import; we only emit them so the template is
# well-formed.
_DEFAULT_PROCESSOR_BUNDLE = {
    "group": "org.apache.nifi",
    "artifact": "nifi-standard-nar",
    "version": "1.27.0",
}
_DEFAULT_SERVICE_BUNDLE = {
    "group": "org.apache.nifi",
    "artifact": "nifi-standard-services-api-nar",
    "version": "1.27.0",
}

# A fixed timestamp so re-emits are byte-stable. NiFi only uses this for display.
_FIXED_TIMESTAMP = "1970-01-01T00:00:00.000Z"

# NiFi's ProcessorDTO ``<state>`` vocabulary vs the model's ``scheduled_state``.
# "not running but enabled" is STOPPED to NiFi and ENABLED to us.
_SCHEDULED_STATE_OUT = {"ENABLED": "STOPPED", "DISABLED": "DISABLED", "RUNNING": "RUNNING"}
_SCHEDULED_STATE_IN = {"STOPPED": "ENABLED", "DISABLED": "DISABLED", "RUNNING": "RUNNING"}


# =============================================================================
# from_xml
# =============================================================================


def from_xml(source: Union[str, bytes, Path]) -> Flow:
    """Parse a NiFi 1.x ``<template>`` XML document into a :class:`Flow`.

    ``source`` can be a :class:`Path` to a ``.xml`` file, a path-like string, a
    raw XML document string, or raw bytes. The resulting :class:`Flow` carries
    the same nesting, components, and wiring; runtime fields
    (``nifi_id``/``nifi_entity``) are left unset since this is an offline parse.
    """
    root = _coerce_to_element(source)
    if root.tag != "template":
        raise ValueError(
            f"Expected <template> root element, got <{root.tag}>"
        )

    name = _text(root.find("name")) or "Flow"
    flow = Flow(name=name, parent_pg="root")
    flow.comment = _text(root.find("description")) or ""

    snippet = root.find("snippet")
    if snippet is None:
        return flow

    # Two-pass build: first materialise every component so connections (which
    # may reference cross-group endpoints) can resolve ``source.id`` and
    # ``destination.id`` to live model instances. We also remember each
    # property-bearing component's descriptor block so we can resolve
    # service-ref properties once all services exist. Services are in that set
    # too — one service referencing another (a lookup pointing at a pool) is
    # just as common as a processor referencing one.
    by_identifier: Dict[str, NiFiComponent] = {}
    ref_holders: List[Tuple[Any, Optional[ET.Element]]] = []
    pending_connections: List[Tuple[ProcessGroup, ET.Element]] = []

    _populate_group(flow, snippet, by_identifier, ref_holders, pending_connections)

    # Resolve service-ref properties: any property whose descriptor flags
    # ``identifiesControllerService`` and whose value matches a known service
    # identifier is rewritten to hold the service instance itself.
    for component, descriptors_elem in ref_holders:
        if descriptors_elem is None:
            continue
        for entry in descriptors_elem.findall("entry"):
            key_elem = entry.find("key")
            value_elem = entry.find("value")
            if key_elem is None or value_elem is None:
                continue
            key = key_elem.text or ""
            if value_elem.find("identifiesControllerService") is None:
                continue
            current = component.properties.get(key)
            if isinstance(current, str):
                resolved = by_identifier.get(current)
                if isinstance(resolved, ControllerService):
                    component.properties[key] = resolved

    # Build connections once all endpoints exist.
    for group, conn_elem in pending_connections:
        connection = _build_connection(conn_elem, by_identifier)
        if connection is not None:
            group.connections.append(connection)

    return flow


def _coerce_to_element(source: Union[str, bytes, Path]) -> ET.Element:
    """Parse *source* into an :class:`~xml.etree.ElementTree.Element`."""
    if isinstance(source, Path):
        return ET.fromstring(source.read_text(encoding="utf-8"))
    if isinstance(source, bytes):
        return ET.fromstring(source)
    if isinstance(source, str):
        # Heuristic: an XML document starts with ``<`` (possibly after BOM /
        # whitespace). A path-like string almost always lacks that prefix.
        stripped = source.lstrip("﻿").lstrip()
        if stripped.startswith("<"):
            return ET.fromstring(source)
        return ET.fromstring(Path(source).read_text(encoding="utf-8"))
    raise TypeError(f"Unsupported source type for from_xml: {type(source).__name__}")


def _populate_group(
    group: ProcessGroup,
    container: ET.Element,
    by_identifier: Dict[str, NiFiComponent],
    ref_holders: List[Tuple[Any, Optional[ET.Element]]],
    pending_connections: List[Tuple[ProcessGroup, ET.Element]],
) -> None:
    """Fill ``group`` from ``container`` (a ``<snippet>`` or ``<contents>``)."""
    # Controller services first so processor service-refs resolve in one sweep
    # at the top.
    for service_elem in container.findall("controllerServices"):
        service = ControllerService(
            name=_text(service_elem.find("name")) or "",
            type=_text(service_elem.find("type")) or "",
            properties=_parse_properties(service_elem.find("properties")),
            enabled=(_text(service_elem.find("state")) or "ENABLED") == "ENABLED",
            comments=_text(service_elem.find("comments")) or "",
        )
        identifier = _text(service_elem.find("id"))
        if identifier:
            by_identifier[identifier] = service
        group.controller_services.append(service)
        ref_holders.append((service, service_elem.find("descriptors")))

    for funnel_elem in container.findall("funnels"):
        funnel = Funnel(position=_parse_position(funnel_elem.find("position")))
        identifier = _text(funnel_elem.find("id"))
        if identifier:
            by_identifier[identifier] = funnel
        group.funnels.append(funnel)

    for label_elem in container.findall("labels"):
        group.labels.append(
            Label(
                text=_text(label_elem.find("label")) or "",
                position=_parse_position(label_elem.find("position")),
                width=_parse_float(label_elem.find("width"), 150.0),
                height=_parse_float(label_elem.find("height"), 150.0),
            )
        )

    for port_elem in container.findall("inputPorts"):
        port = InputPort(
            name=_text(port_elem.find("name")) or "",
            position=_parse_position(port_elem.find("position")),
        )
        identifier = _text(port_elem.find("id"))
        if identifier:
            by_identifier[identifier] = port
        group.input_ports.append(port)

    for port_elem in container.findall("outputPorts"):
        port = OutputPort(
            name=_text(port_elem.find("name")) or "",
            position=_parse_position(port_elem.find("position")),
        )
        identifier = _text(port_elem.find("id"))
        if identifier:
            by_identifier[identifier] = port
        group.output_ports.append(port)

    for proc_elem in container.findall("processors"):
        config = proc_elem.find("config")
        # Auto-terminated relationships can appear in two places:
        #   (a) the wiki-style ``<relationships><autoTerminate>true</...>...``
        #       sub-elements directly on the processor, OR
        #   (b) the newer ``<config><autoTerminatedRelationships>NAME``
        #       repeated text elements.
        # We honour both so the same parser works for old and new templates.
        auto_term: List[str] = []
        for rel in proc_elem.findall("relationships"):
            flag = _text(rel.find("autoTerminate"))
            rel_name = _text(rel.find("name"))
            if rel_name and flag and flag.lower() == "true":
                auto_term.append(rel_name)
        if config is not None:
            for at in config.findall("autoTerminatedRelationships"):
                if at.text and at.text not in auto_term:
                    auto_term.append(at.text)

        # Retried relationships, like auto-terminated ones, live in two places:
        # ``<config><retriedRelationships>`` and the per-relationship
        # ``<relationships><retry>true`` flag. Honour both.
        retried: List[str] = []
        for rel in proc_elem.findall("relationships"):
            flag = _text(rel.find("retry"))
            rel_name = _text(rel.find("name"))
            if rel_name and flag and flag.lower() == "true":
                retried.append(rel_name)

        scheduling_period = "0 sec"
        scheduling_strategy = "TIMER_DRIVEN"
        concurrent_tasks = 1
        properties: Dict[str, Any] = {}
        fidelity: Dict[str, Any] = {}
        if config is not None:
            scheduling_period = _text(config.find("schedulingPeriod")) or scheduling_period
            scheduling_strategy = _text(config.find("schedulingStrategy")) or scheduling_strategy
            concurrent_tasks = _parse_int(
                config.find("concurrentlySchedulableTaskCount"), concurrent_tasks
            )
            properties = _parse_properties(config.find("properties"))
            for rr in config.findall("retriedRelationships"):
                if rr.text and rr.text not in retried:
                    retried.append(rr.text)
            # Only override the model defaults when the template says something
            # — an older/hand-written template shouldn't blank these out.
            fidelity = _present(
                comments=_text(config.find("comments")),
                penalty_duration=_text(config.find("penaltyDuration")),
                yield_duration=_text(config.find("yieldDuration")),
                bulletin_level=_text(config.find("bulletinLevel")),
                execution_node=_text(config.find("executionNode")),
                backoff_mechanism=_text(config.find("backoffMechanism")),
                max_backoff_period=_text(config.find("maxBackoffPeriod")),
            )
            fidelity["run_duration_millis"] = _parse_int(config.find("runDurationMillis"), 0)
            fidelity["retry_count"] = _parse_int(config.find("retryCount"), 10)

        processor = Processor(
            name=_text(proc_elem.find("name")) or "",
            type=_text(proc_elem.find("type")) or "",
            properties=properties,
            scheduling_period=scheduling_period,
            scheduling_strategy=scheduling_strategy,  # type: ignore[arg-type]
            concurrent_tasks=concurrent_tasks,
            auto_terminate=auto_term,
            position=_parse_position(proc_elem.find("position")),
            # NiFi's ``<state>`` is RUNNING/STOPPED/DISABLED; the model's is
            # ENABLED/DISABLED/RUNNING — a stopped-but-enabled processor is
            # simply ENABLED to us.
            scheduled_state=_SCHEDULED_STATE_IN.get(
                _text(proc_elem.find("state")) or "STOPPED", "ENABLED"
            ),
            retried_relationships=retried,
            **fidelity,
        )
        identifier = _text(proc_elem.find("id"))
        if identifier:
            by_identifier[identifier] = processor
        group.processors.append(processor)
        ref_holders.append((processor, config.find("descriptors") if config is not None else None))

    # Defer connections to the second pass — but record their owning group.
    for conn_elem in container.findall("connections"):
        pending_connections.append((group, conn_elem))

    # Recurse into nested process groups. Their components live under
    # ``<contents>``, which has the same shape as ``<snippet>``.
    for pg_elem in container.findall("processGroups"):
        child = ProcessGroup(
            name=_text(pg_elem.find("name")) or "",
            comment=_text(pg_elem.find("comments")) or "",
            position=_parse_position(pg_elem.find("position")),
            variables={
                k: v or "" for k, v in _parse_properties(pg_elem.find("variables")).items()
            },
        )
        identifier = _text(pg_elem.find("id"))
        if identifier:
            by_identifier[identifier] = child
        group.process_groups.append(child)
        contents = pg_elem.find("contents")
        if contents is not None:
            _populate_group(child, contents, by_identifier, ref_holders, pending_connections)


def _text(elem: Optional[ET.Element]) -> Optional[str]:
    if elem is None:
        return None
    return elem.text if elem.text is not None else ""


def _present(**values: Optional[str]) -> Dict[str, Any]:
    """Keep only the keys whose element was actually present (non-``None``)."""
    return {k: v for k, v in values.items() if v}


def _parse_int(elem: Optional[ET.Element], default: int) -> int:
    text = _text(elem)
    if not text:
        return default
    try:
        return int(text)
    except ValueError:
        return default


def _parse_float(elem: Optional[ET.Element], default: float) -> float:
    text = _text(elem)
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def _parse_position(elem: Optional[ET.Element]) -> Optional[Tuple[float, float]]:
    if elem is None:
        return None
    x = _text(elem.find("x"))
    y = _text(elem.find("y"))
    if x is None or y is None or x == "" or y == "":
        return None
    try:
        return (float(x), float(y))
    except ValueError:
        return None


def _parse_properties(elem: Optional[ET.Element]) -> Dict[str, Any]:
    """Collect ``<entry><key>K</key><value>V</value></entry>`` into a dict.

    A missing ``<value>`` element (as opposed to an empty ``<value/>``) means
    the property was never set — we represent that as ``None`` so re-emit can
    drop the value element in turn.
    """
    out: Dict[str, Any] = {}
    if elem is None:
        return out
    for entry in elem.findall("entry"):
        key_elem = entry.find("key")
        if key_elem is None or key_elem.text is None:
            continue
        key = key_elem.text
        value_elem = entry.find("value")
        if value_elem is None:
            out[key] = None
        else:
            out[key] = value_elem.text if value_elem.text is not None else ""
    return out


def _build_connection(
    elem: ET.Element, by_identifier: Dict[str, NiFiComponent]
) -> Optional[Connection]:
    source_elem = elem.find("source")
    dest_elem = elem.find("destination")
    if source_elem is None or dest_elem is None:
        return None
    source_id = _text(source_elem.find("id"))
    target_id = _text(dest_elem.find("id"))
    source = by_identifier.get(source_id) if source_id else None
    target = by_identifier.get(target_id) if target_id else None
    if source is None or target is None:
        # Dangling reference — skip silently; we don't fabricate endpoints.
        return None

    raw_relationships = [r.text for r in elem.findall("selectedRelationships") if r.text]
    if isinstance(source, (Port, Funnel)):
        # Port and funnel sources have no named relationships.
        relationships: List[str] = []
    else:
        relationships = [r for r in raw_relationships if r]
        if not relationships:
            relationships = ["success"]

    settings = _present(
        back_pressure_data_size_threshold=_text(elem.find("backPressureDataSizeThreshold")),
        flowfile_expiration=_text(elem.find("flowFileExpiration")),
        load_balance_strategy=_text(elem.find("loadBalanceStrategy")),
        load_balance_compression=_text(elem.find("loadBalanceCompression")),
    )
    return Connection(
        name=_text(elem.find("name")) or "",
        source=source,
        target=target,
        relationships=relationships,
        back_pressure_object_threshold=_parse_int(
            elem.find("backPressureObjectThreshold"), 10000
        ),
        prioritizers=[p.text for p in elem.findall("prioritizers") if p.text],
        # NiFi's connection DTO calls this ``loadBalancePartitionAttribute``;
        # the versioned (JSON) schema calls the same field
        # ``partitioningAttribute``.
        partitioning_attribute=_text(elem.find("loadBalancePartitionAttribute")) or "",
        **settings,
    )


# =============================================================================
# template_limitations
# =============================================================================

# State a NiFi 1.x template cannot carry into a group, verified against a live
# 1.24.0 (see tests/test_xml_format.py). This is a property of *templates*, and
# it is why the 1.x in-place push stopped using them: the push now imports the
# JSON snapshot and moves it with the snippet API, which carries load-balance
# compression and parameter references natively. What remains here is advice
# for anyone instantiating a ``niflow convert``-produced template by hand.
_TEMPLATE_CANNOT_CARRY = {
    "parameter_context": (
        "parameter-context binding to {value!r} — a template has no element "
        "for it (NiFi's own template export drops it too)"
    ),
    "variables": (
        "variables {value!r} — a template's <snippet> has no DTO for the group "
        "its contents land in, so the target group's own variables are lost "
        "(nested groups keep theirs)"
    ),
    "comment": (
        "group comment {value!r} — a template's <snippet> has no DTO for the "
        "group its contents land in"
    ),
    "load_balance_compression": (
        "load-balance compression {value!r} — the template carries it but NiFi "
        "1.x ignores it when instantiating a snippet"
    ),
    "parameter_reference": (
        "parameter references in {value} — NiFi 1.x *escapes* every '#{{' it "
        "finds while instantiating a snippet (a working '#{{param}}' lands as "
        "the literal '##{{param}}'), so the property stops resolving"
    ),
}
_TEMPLATE_REPAIR = "set it over the REST API after instantiating the template"


def template_limitations(flow: Flow) -> List[Dict[str, str]]:
    """Flow state a NiFi 1.x template cannot carry, as ``{where, message}``.

    Advice for the ``niflow convert`` XML path — what to expect if you
    instantiate the template by hand, and what to set over the REST API
    afterwards. The 1.x in-place *push* no longer goes through a template
    (``rest/flows.py::_move_snapshot_into_group``): the snippet move carries
    load-balance compression and, crucially, parameter references, which a
    template instantiation on 1.24 escapes into dead ``##{param}`` literals.
    """
    out: List[Dict[str, str]] = []

    def note(where: str, kind: str, value: Any) -> None:
        out.append({
            "where": where,
            "message": _TEMPLATE_CANNOT_CARRY[kind].format(value=value),
            "repair": _TEMPLATE_REPAIR,
        })

    def visit(group: ProcessGroup, path: str, root: bool) -> None:
        if group.parameter_context is not None:
            note(path, "parameter_context", group.parameter_context.name)
        if root:
            # Only the *target* group is affected: nested groups travel as
            # ProcessGroupDTOs, which do carry variables and comments.
            if group.variables:
                note(path, "variables", group.variables)
            if group.comment:
                note(path, "comment", group.comment)
        for connection in group.connections:
            if connection.load_balance_compression != "DO_NOT_COMPRESS":
                note(
                    f"{path}/{connection.name or '(unnamed connection)'}",
                    "load_balance_compression",
                    connection.load_balance_compression,
                )
        for component in list(group.processors) + list(group.controller_services):
            keys = [
                key for key, value in component.properties.items()
                if isinstance(value, str) and "#{" in value
            ]
            if keys:
                note(
                    f"{path}/{component.name}",
                    "parameter_reference",
                    ", ".join(repr(k) for k in sorted(keys)),
                )
        for child in group.process_groups:
            visit(child, f"{path}/{child.name}", root=False)

    visit(flow, flow.name or ".", root=True)
    return out


# =============================================================================
# to_xml
# =============================================================================


def to_xml(flow: Flow, *, indent: int = 4) -> str:
    """Emit ``flow`` as a NiFi 1.x ``<template>`` XML string."""
    # Pre-walk the tree to assign deterministic identifiers (UUID5 over a
    # path-derived name). Store them in an ``id() -> str`` map keyed on Python
    # object identity so connection emission can resolve endpoints later.
    identifiers: Dict[int, str] = {}
    # Two passes: every component across the whole tree first, then connections
    # — a connection's seed embeds its endpoint identifiers, and an endpoint may
    # live in a child group (cross-group wiring to a port).
    _assign_identifiers(flow, parent_path=(), identifiers=identifiers)
    _assign_connection_identifiers(flow, (flow.name,), identifiers)
    # Connection endpoints carry the identifier of the group that *owns* them,
    # not the group that owns the connection — they differ for cross-group
    # wiring (a processor connected to a child group's input port).
    owners: Dict[int, str] = {}
    _assign_owners(flow, identifiers, owners)

    # Pre-compute, per processor, the relationships referenced by outgoing
    # connections — so we can emit `<relationships>` blocks for the union of
    # (auto-terminated + retried + referenced) names. Without round-tripping
    # NiFi's canonical relationship list, this is the best we can do.
    outgoing_relationships: Dict[int, List[str]] = {}
    _collect_outgoing_relationships(flow, outgoing_relationships)

    root = ET.Element("template")
    _sub_text(root, "description", flow.comment or "")
    _sub_text(root, "id", identifiers[id(flow)])
    _sub_text(root, "name", flow.name)
    snippet = ET.SubElement(root, "snippet")
    _emit_group_contents(flow, snippet, identifiers, owners, outgoing_relationships)
    _sub_text(root, "timestamp", _FIXED_TIMESTAMP)

    ET.indent(root, space=" " * indent)
    body = ET.tostring(root, encoding="unicode")
    return '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' + body


def _assign_identifiers(
    group: ProcessGroup,
    parent_path: Tuple[str, ...],
    identifiers: Dict[int, str],
) -> None:
    """Recursively assign UUID5 identifiers to every component in ``group``.

    The seed for each UUID5 is a slash-joined string ``<kind>:<g1>/.../<own>``
    where ``g1..gn`` are the group-name chain from root and ``<own>`` is the
    component's own name. Re-emits with no model changes are byte-stable.
    """
    own_path = parent_path + (group.name,)
    identifiers[id(group)] = _det_uuid("process_group", own_path)

    for service in group.controller_services:
        identifiers[id(service)] = _det_uuid("controller_service", own_path + (service.name,))
    for port in group.input_ports:
        identifiers[id(port)] = _det_uuid("input_port", own_path + (port.name,))
    for port in group.output_ports:
        identifiers[id(port)] = _det_uuid("output_port", own_path + (port.name,))
    for processor in group.processors:
        identifiers[id(processor)] = _det_uuid("processor", own_path + (processor.name,))
    # Funnels and labels have no name to seed on; use their list position.
    for index, funnel in enumerate(group.funnels):
        identifiers[id(funnel)] = _det_uuid("funnel", own_path + (str(index),))
    for index, label in enumerate(group.labels):
        identifiers[id(label)] = _det_uuid("label", own_path + (str(index),))

    for child in group.process_groups:
        _assign_identifiers(child, own_path, identifiers)


def _assign_connection_identifiers(
    group: ProcessGroup, own_path: Tuple[str, ...], identifiers: Dict[int, str]
) -> None:
    """Seed connection identifiers, disambiguating exact-duplicate parallel edges.

    The seed is (name, source id, target id, relationships) — parallel edges
    between the same pair are legal as long as one of those differs. Exact
    duplicates get an occurrence suffix so the template can't carry two
    components with the same id (NiFi would merge or drop one). Same scheme as
    :mod:`json_format`.
    """
    seeds = [
        (
            connection.name or "",
            identifiers.get(id(connection.source), ""),
            identifiers.get(id(connection.target), ""),
            ",".join(sorted(connection.relationships or [])),
        )
        for connection in group.connections
    ]
    counts: Dict[Tuple[str, ...], int] = {}
    for seed in seeds:
        counts[seed] = counts.get(seed, 0) + 1
    occurrence: Dict[Tuple[str, ...], int] = {}
    for connection, seed in zip(group.connections, seeds):
        if counts[seed] > 1:
            n = occurrence.get(seed, 0)
            occurrence[seed] = n + 1
            seed = seed + (f"#{n}",)
        identifiers[id(connection)] = _det_uuid("connection", own_path + seed)

    for child in group.process_groups:
        _assign_connection_identifiers(child, own_path + (child.name,), identifiers)


def _assign_owners(
    group: ProcessGroup, identifiers: Dict[int, str], owners: Dict[int, str]
) -> None:
    """Map every connectable component to the identifier of its owning group."""
    gid = identifiers[id(group)]
    for component in (
        list(group.processors)
        + list(group.input_ports)
        + list(group.output_ports)
        + list(group.funnels)
        + list(group.process_groups)
    ):
        owners[id(component)] = gid
    for child in group.process_groups:
        _assign_owners(child, identifiers, owners)


def _det_uuid(kind: str, path: Tuple[str, ...]) -> str:
    name = f"{kind}:" + "/".join(path)
    return str(uuid.uuid5(_NS, name))


def _collect_outgoing_relationships(
    group: ProcessGroup, out: Dict[int, List[str]]
) -> None:
    """Record, per processor id(), the union of relationships used by outgoing
    connections — preserving first-seen order. Recurses into nested groups."""
    for connection in group.connections:
        if isinstance(connection.source, Processor):
            bucket = out.setdefault(id(connection.source), [])
            for rel in connection.relationships:
                if rel and rel not in bucket:
                    bucket.append(rel)
    for child in group.process_groups:
        _collect_outgoing_relationships(child, out)


def _emit_group_contents(
    group: ProcessGroup,
    container: ET.Element,
    identifiers: Dict[int, str],
    owners: Dict[int, str],
    outgoing_relationships: Dict[int, List[str]],
) -> None:
    """Emit ``group``'s components into ``container`` (snippet or contents).

    Element ordering follows the NiFi 1.x convention seen in the wiki fixture:
    connections, processors, then everything else. We're conservative and emit
    each child kind in a single block so the output reads as a flat list per
    type. Components with no explicit position get auto-layout coordinates —
    the same treatment :mod:`json_format` gives them, so an in-place 1.x push
    doesn't stack the whole flow at the canvas origin.
    """
    parent_identifier = identifiers[id(group)]
    auto = compute_layout(group)

    for connection in group.connections:
        _emit_connection(container, connection, parent_identifier, identifiers, owners)
    for service in group.controller_services:
        _emit_service(container, service, parent_identifier, identifiers)
    for port in group.input_ports:
        _emit_port(container, port, parent_identifier, identifiers, auto,
                   "inputPorts", "INPUT_PORT")
    for port in group.output_ports:
        _emit_port(container, port, parent_identifier, identifiers, auto,
                   "outputPorts", "OUTPUT_PORT")
    for processor in group.processors:
        _emit_processor(container, processor, parent_identifier, identifiers, auto,
                        outgoing_relationships)
    for funnel in group.funnels:
        _emit_funnel(container, funnel, parent_identifier, identifiers, auto)
    for label in group.labels:
        _emit_label(container, label, parent_identifier, identifiers, auto)
    for child in group.process_groups:
        _emit_process_group(container, child, parent_identifier, identifiers, auto,
                            owners, outgoing_relationships)


def _emit_process_group(
    container: ET.Element,
    group: ProcessGroup,
    parent_identifier: str,
    identifiers: Dict[int, str],
    auto: Dict[int, Tuple[float, float]],
    owners: Dict[int, str],
    outgoing_relationships: Dict[int, List[str]],
) -> None:
    elem = ET.SubElement(container, "processGroups")
    _sub_text(elem, "id", identifiers[id(group)])
    _sub_text(elem, "parentGroupId", parent_identifier)
    _emit_position(elem, group.position or auto.get(id(group)))
    _sub_text(elem, "name", group.name)
    _sub_text(elem, "comments", group.comment or "")
    # The 1.x variable registry. (A group's parameter-context binding has no
    # template equivalent — see :func:`template_limitations`.)
    if group.variables:
        _emit_entry_map(elem, "variables", group.variables)
    contents = ET.SubElement(elem, "contents")
    _emit_group_contents(group, contents, identifiers, owners, outgoing_relationships)


def _emit_funnel(
    container: ET.Element,
    funnel: Funnel,
    parent_identifier: str,
    identifiers: Dict[int, str],
    auto: Dict[int, Tuple[float, float]],
) -> None:
    elem = ET.SubElement(container, "funnels")
    _sub_text(elem, "id", identifiers[id(funnel)])
    _sub_text(elem, "parentGroupId", parent_identifier)
    _emit_position(elem, funnel.position or auto.get(id(funnel)))


def _emit_label(
    container: ET.Element,
    label: Label,
    parent_identifier: str,
    identifiers: Dict[int, str],
    auto: Dict[int, Tuple[float, float]],
) -> None:
    elem = ET.SubElement(container, "labels")
    _sub_text(elem, "id", identifiers[id(label)])
    _sub_text(elem, "parentGroupId", parent_identifier)
    _emit_position(elem, label.position or auto.get(id(label)))
    _sub_text(elem, "height", _fmt_float(label.height))
    _sub_text(elem, "label", label.text)
    ET.SubElement(elem, "style")
    _sub_text(elem, "width", _fmt_float(label.width))
    _sub_text(elem, "zIndex", "0")


def _emit_service(
    container: ET.Element,
    service: ControllerService,
    parent_identifier: str,
    identifiers: Dict[int, str],
) -> None:
    elem = ET.SubElement(container, "controllerServices")
    _sub_text(elem, "id", identifiers[id(service)])
    _sub_text(elem, "parentGroupId", parent_identifier)
    _emit_position(elem, None)
    _emit_bundle(elem, _bundle_for(service, service.type, _DEFAULT_SERVICE_BUNDLE, service=True))
    _sub_text(elem, "comments", service.comments or "")
    _emit_descriptors(elem, service.properties)
    _emit_properties(elem, service.properties, identifiers)
    _sub_text(elem, "name", service.name)
    _sub_text(elem, "state", "ENABLED" if service.enabled else "DISABLED")
    _sub_text(elem, "type", service.type)


def _emit_port(
    container: ET.Element,
    port: Port,
    parent_identifier: str,
    identifiers: Dict[int, str],
    auto: Dict[int, Tuple[float, float]],
    tag: str,
    port_type: str,
) -> None:
    elem = ET.SubElement(container, tag)
    _sub_text(elem, "id", identifiers[id(port)])
    _sub_text(elem, "parentGroupId", parent_identifier)
    _emit_position(elem, port.position or auto.get(id(port)))
    _sub_text(elem, "comments", "")
    _sub_text(elem, "concurrentlySchedulableTaskCount", "1")
    _sub_text(elem, "name", port.name)
    _sub_text(elem, "state", "STOPPED")
    _sub_text(elem, "type", port_type)


def _emit_processor(
    container: ET.Element,
    processor: Processor,
    parent_identifier: str,
    identifiers: Dict[int, str],
    auto: Dict[int, Tuple[float, float]],
    outgoing_relationships: Dict[int, List[str]],
) -> None:
    elem = ET.SubElement(container, "processors")
    _sub_text(elem, "id", identifiers[id(processor)])
    _sub_text(elem, "parentGroupId", parent_identifier)
    _emit_position(elem, processor.position or auto.get(id(processor)))
    _emit_bundle(elem, _bundle_for(processor, processor.type, _DEFAULT_PROCESSOR_BUNDLE))

    # Config block. Every field here is one NiFi honours on template import —
    # hard-coding them (as this emitter once did) silently reset the live
    # processor's scheduling, bulletins and retry policy on every 1.x push.
    config = ET.SubElement(elem, "config")
    _sub_text(config, "backoffMechanism", processor.backoff_mechanism)
    _sub_text(config, "bulletinLevel", processor.bulletin_level)
    _sub_text(config, "comments", processor.comments or "")
    _sub_text(config, "concurrentlySchedulableTaskCount", str(processor.concurrent_tasks))
    _emit_descriptors(config, processor.properties)
    _sub_text(config, "executionNode", processor.execution_node)
    _sub_text(config, "lossTolerant", "false")
    _sub_text(config, "maxBackoffPeriod", processor.max_backoff_period)
    _sub_text(config, "penaltyDuration", processor.penalty_duration)
    _emit_properties(config, processor.properties, identifiers)
    for rel in processor.retried_relationships:
        _sub_text(config, "retriedRelationships", rel)
    _sub_text(config, "retryCount", str(processor.retry_count))
    _sub_text(config, "runDurationMillis", str(processor.run_duration_millis))
    _sub_text(config, "schedulingPeriod", processor.scheduling_period)
    _sub_text(config, "schedulingStrategy", processor.scheduling_strategy)
    _sub_text(config, "yieldDuration", processor.yield_duration)
    # Emit the newer-style auto-terminated list too so 2.x importers see it.
    for rel in processor.auto_terminate:
        _sub_text(config, "autoTerminatedRelationships", rel)

    _sub_text(elem, "name", processor.name)

    # Relationships: emit the union of (auto-terminated + retried + referenced
    # by outgoing connections). Without round-tripping NiFi's canonical
    # relationship list, this is the best we can do.
    seen: List[str] = []
    for rel in (
        list(processor.auto_terminate)
        + list(processor.retried_relationships)
        + outgoing_relationships.get(id(processor), [])
    ):
        if rel not in seen:
            seen.append(rel)
    for rel in seen:
        rel_elem = ET.SubElement(elem, "relationships")
        _sub_text(rel_elem, "autoTerminate", "true" if rel in processor.auto_terminate else "false")
        _sub_text(rel_elem, "name", rel)
        _sub_text(rel_elem, "retry", "true" if rel in processor.retried_relationships else "false")

    _sub_text(elem, "state", _SCHEDULED_STATE_OUT.get(processor.scheduled_state, "STOPPED"))
    ET.SubElement(elem, "style")
    _sub_text(elem, "type", processor.type)


def _emit_connection(
    container: ET.Element,
    connection: Connection,
    parent_identifier: str,
    identifiers: Dict[int, str],
    owners: Dict[int, str],
) -> None:
    elem = ET.SubElement(container, "connections")
    _sub_text(elem, "id", identifiers[id(connection)])
    _sub_text(elem, "parentGroupId", parent_identifier)
    _sub_text(
        elem, "backPressureDataSizeThreshold", connection.back_pressure_data_size_threshold
    )
    _sub_text(elem, "backPressureObjectThreshold", str(connection.back_pressure_object_threshold))
    _emit_endpoint(elem, "destination", connection.target, parent_identifier, identifiers, owners)
    _sub_text(elem, "flowFileExpiration", connection.flowfile_expiration)
    _sub_text(elem, "labelIndex", "0")
    _sub_text(elem, "loadBalanceCompression", connection.load_balance_compression)
    # NiFi's connection DTO spells the partitioning attribute
    # ``loadBalancePartitionAttribute`` (the versioned JSON schema calls the
    # same field ``partitioningAttribute``).
    _sub_text(elem, "loadBalancePartitionAttribute", connection.partitioning_attribute)
    _sub_text(elem, "loadBalanceStrategy", connection.load_balance_strategy)
    _sub_text(elem, "name", connection.name or "")
    for prioritizer in connection.prioritizers:
        _sub_text(elem, "prioritizers", prioritizer)
    # Per the contract: omit ``<selectedRelationships>`` entirely when the
    # source is a port or funnel (neither has named relationships); otherwise
    # emit one element per relationship.
    if not isinstance(connection.source, (Port, Funnel)):
        for rel in connection.relationships:
            _sub_text(elem, "selectedRelationships", rel)
    _emit_endpoint(elem, "source", connection.source, parent_identifier, identifiers, owners)
    _sub_text(elem, "zIndex", "0")


def _emit_endpoint(
    parent: ET.Element,
    tag: str,
    component: NiFiComponent,
    group_identifier: str,
    identifiers: Dict[int, str],
    owners: Dict[int, str],
) -> None:
    elem = ET.SubElement(parent, tag)
    # The endpoint's own group, which is not the connection's group for
    # cross-group wiring (processor -> a child group's input port).
    _sub_text(elem, "groupId", owners.get(id(component), group_identifier))
    _sub_text(elem, "id", identifiers[id(component)])
    _sub_text(elem, "type", _endpoint_type(component))


def _endpoint_type(component: NiFiComponent) -> str:
    if isinstance(component, Processor):
        return "PROCESSOR"
    if isinstance(component, InputPort):
        return "INPUT_PORT"
    if isinstance(component, OutputPort):
        return "OUTPUT_PORT"
    if isinstance(component, Funnel):
        return "FUNNEL"
    if isinstance(component, ProcessGroup):
        return "PROCESS_GROUP"
    return "PROCESSOR"  # defensive fallback; shouldn't happen in valid flows


def _emit_position(parent: ET.Element, position: Optional[Tuple[float, float]]) -> None:
    elem = ET.SubElement(parent, "position")
    if position is None:
        _sub_text(elem, "x", "0.0")
        _sub_text(elem, "y", "0.0")
    else:
        _sub_text(elem, "x", _fmt_float(position[0]))
        _sub_text(elem, "y", _fmt_float(position[1]))


def _fmt_float(value: float) -> str:
    # Render integral floats as ``123.0`` (matches NiFi's own output) and keep
    # fractional ones at their natural string form.
    if float(value).is_integer():
        return f"{float(value):.1f}"
    return repr(float(value))


def _bundle_for(
    component: Any, type_str: str, default: Dict[str, str], *, service: bool = False
) -> Dict[str, str]:
    """Resolve a component's NAR coordinates for the XML template.

    Honours an explicit ``component.bundle`` first; otherwise resolves the
    correct artifact per type (so e.g. UpdateAttribute lands in
    nifi-update-attribute-nar, not nifi-standard-nar). The template's own
    version string is kept for byte-stability — NiFi resolves the NAR from
    group+artifact regardless of version.
    """
    bundle = getattr(component, "bundle", None)
    if bundle is not None:
        return {"group": bundle.group, "artifact": bundle.artifact, "version": bundle.version}
    resolved = default_bundle(type_str, service=service)
    return {"group": resolved["group"], "artifact": resolved["artifact"], "version": default["version"]}


def _emit_bundle(parent: ET.Element, bundle: Dict[str, str]) -> None:
    elem = ET.SubElement(parent, "bundle")
    _sub_text(elem, "artifact", bundle["artifact"])
    _sub_text(elem, "group", bundle["group"])
    _sub_text(elem, "version", bundle["version"])


def _emit_properties(
    parent: ET.Element, props: Dict[str, Any], identifiers: Dict[int, str]
) -> None:
    """Emit ``<properties><entry>...</entry></properties>``.

    A ``None`` value emits ``<entry><key>K</key></entry>`` (no ``<value>``);
    a :class:`ControllerService` value becomes its assigned identifier.
    """
    elem = ET.SubElement(parent, "properties")
    for key, value in props.items():
        entry = ET.SubElement(elem, "entry")
        _sub_text(entry, "key", key)
        if value is None:
            continue
        if isinstance(value, ControllerService):
            _sub_text(entry, "value", identifiers[id(value)])
        else:
            _sub_text(entry, "value", str(value))


def _emit_entry_map(parent: ET.Element, tag: str, values: Dict[str, str]) -> None:
    """Emit a NiFi ``<tag><entry><key/><value/></entry>...</tag>`` string map."""
    elem = ET.SubElement(parent, tag)
    for key, value in values.items():
        entry = ET.SubElement(elem, "entry")
        _sub_text(entry, "key", key)
        _sub_text(entry, "value", value)


def _emit_descriptors(parent: ET.Element, props: Dict[str, Any]) -> None:
    """Emit ``<descriptors>`` with one ``<entry>`` per property.

    Service-ref descriptors carry an ``<identifiesControllerService>`` child
    holding the service's Java FQCN — that's how NiFi reconnects the property
    to a service on re-import. Plain descriptors carry just ``<name>`` so the
    set of properties matches the ``<properties>`` block.
    """
    elem = ET.SubElement(parent, "descriptors")
    for key, value in props.items():
        entry = ET.SubElement(elem, "entry")
        _sub_text(entry, "key", key)
        value_elem = ET.SubElement(entry, "value")
        _sub_text(value_elem, "name", key)
        if isinstance(value, ControllerService):
            _sub_text(value_elem, "identifiesControllerService", value.type)


def _sub_text(parent: ET.Element, tag: str, text: str) -> ET.Element:
    """Append a child element with text content (returns the element)."""
    elem = ET.SubElement(parent, tag)
    elem.text = text
    return elem
