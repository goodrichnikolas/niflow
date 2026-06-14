"""Round-trip a :class:`~niflow.core.Flow` to/from a NiFi 2.x flow-definition JSON.

NiFi 2.x exports flows as ``VersionedFlowSnapshot`` JSON: an envelope wrapping a
``VersionedProcessGroup`` tree (``flowContents``) with processors, controller
services, ports, connections, and nested process groups. This module turns that
representation into our in-memory :class:`Flow` model and back again — purely
in stdlib, with no nipyapi import, so it works in environments without NiFi.

The two pure functions are:

* :func:`from_json` — parse a snapshot (str / dict / Path) into a :class:`Flow`.
* :func:`to_json`  — emit a :class:`Flow` as a deterministic snapshot string.

Determinism note: :func:`to_json` assigns identifiers via UUID5 keyed on each
component's path within the flow (component-kind + group-name chain + own name).
Re-emitting an unchanged flow is therefore byte-stable across runs and machines,
which is what we want for git-tracked exports.
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from niflow.core import (
    Bundle,
    Connection,
    ControllerService,
    Flow,
    Funnel,
    InputPort,
    Label,
    NiFiComponent,
    OutputPort,
    Parameter,
    ParameterContext,
    Port,
    Processor,
    ProcessGroup,
)
from niflow.layout import compute_layout
from niflow.processors.bundles import default_bundle

# Standard UUID DNS namespace — used as the root for deterministic UUID5s so
# generated identifiers don't collide with random NiFi-assigned ones.
_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

# Bundle (NAR) coordinates for emitted components are resolved per-type by
# niflow.processors.bundles.default_bundle — most live in nifi-standard-nar, but
# some (e.g. UpdateAttribute) ship in their own NAR, and emitting the wrong one
# makes NiFi reject an otherwise-valid type. push() has the final say, rewriting
# these from the target instance's installed NARs.


# =============================================================================
# from_json
# =============================================================================


def from_json(source: Union[str, dict, Path]) -> Flow:
    """Parse a NiFi ``VersionedFlowSnapshot`` into a :class:`Flow`.

    ``source`` can be a JSON string, an already-parsed dict, or a :class:`Path`
    pointing at a ``.json`` file. The resulting :class:`Flow` carries the same
    nesting, components, and wiring; runtime fields (``nifi_id``/``nifi_entity``)
    are left unset since this is an offline parse.
    """
    data = _coerce_to_dict(source)
    contents = data["flowContents"]

    # Parameter contexts live in the snapshot envelope, keyed by name; groups
    # reference them via ``parameterContextName``. Sharing one instance per
    # name preserves the "same context bound to several groups" shape.
    contexts = _parse_parameter_contexts(data.get("parameterContexts"))

    # Two-pass build: first materialise every component so connections (which
    # may reference cross-group endpoints) can resolve ``source.id`` and
    # ``destination.id`` to live model instances. We also remember each
    # processor's raw DTO so we can resolve service-ref properties once all
    # services exist.
    by_identifier: Dict[str, NiFiComponent] = {}
    processor_dtos: List[Tuple[Processor, dict]] = []
    pending_connections: List[Tuple[ProcessGroup, dict]] = []

    flow = Flow(name=contents.get("name", "Flow"))
    _populate_group(flow, contents, by_identifier, processor_dtos, pending_connections, contexts)

    # Resolve service-ref properties: any property whose descriptor flags
    # ``identifiesControllerService`` and whose value matches a known service
    # identifier is rewritten to hold the service instance itself.
    for proc, dto in processor_dtos:
        descriptors = dto.get("propertyDescriptors") or {}
        for key, descriptor in descriptors.items():
            if not descriptor or not descriptor.get("identifiesControllerService"):
                continue
            current = proc.properties.get(key)
            if isinstance(current, str):
                resolved = by_identifier.get(current)
                if isinstance(resolved, ControllerService):
                    proc.properties[key] = resolved

    # Build connections once everything is in place.
    for group, conn_dto in pending_connections:
        connection = _build_connection(conn_dto, by_identifier)
        if connection is not None:
            group.connections.append(connection)

    return flow


def _coerce_to_dict(source: Union[str, dict, Path]) -> dict:
    if isinstance(source, dict):
        return source
    if isinstance(source, Path):
        return json.loads(source.read_text())
    if isinstance(source, str):
        # Heuristic: a path-like string with no leading brace is a file; a
        # JSON document starts with whitespace + ``{``.
        stripped = source.lstrip()
        if stripped.startswith("{"):
            return json.loads(source)
        return json.loads(Path(source).read_text())
    raise TypeError(f"Unsupported source type for from_json: {type(source).__name__}")


def _parse_parameter_contexts(raw: Any) -> Dict[str, ParameterContext]:
    """Parse the envelope's ``parameterContexts`` map into shared instances.

    Handles both shapes NiFi produces: the versioned form (parameters as flat
    dicts) and the REST-entity form (parameters wrapped in ``{"parameter": …}``).
    """
    contexts: Dict[str, ParameterContext] = {}
    for name, ctx_dto in (raw or {}).items():
        params = []
        for p in ctx_dto.get("parameters") or []:
            p = p.get("parameter", p)  # unwrap REST-entity shape
            params.append(
                Parameter(
                    name=p.get("name", ""),
                    value=p.get("value"),
                    sensitive=bool(p.get("sensitive", False)),
                    description=p.get("description") or "",
                )
            )
        contexts[name] = ParameterContext(
            name=ctx_dto.get("name", name),
            description=ctx_dto.get("description") or "",
            parameters=params,
            inherited_contexts=list(ctx_dto.get("inheritedParameterContexts") or []),
        )
    return contexts


def _parse_bundle(raw: Any, type_str: str = "", *, service: bool = False) -> Optional[Bundle]:
    if not raw:
        return None
    group = raw.get("group", "org.apache.nifi")
    artifact = raw.get("artifact", "")
    # If the bundle is just what we'd emit by default for this type, collapse it
    # back to None so Python-built flows (bundle=None) round-trip stable. We
    # ignore the version: the default's version is an instance-specific
    # placeholder, and NiFi resolves the NAR by group+artifact regardless.
    default = default_bundle(type_str, service=service)
    if group == default["group"] and artifact == default["artifact"]:
        return None
    return Bundle(group=group, artifact=artifact, version=raw.get("version", ""))


def _populate_group(
    group: ProcessGroup,
    dto: dict,
    by_identifier: Dict[str, NiFiComponent],
    processor_dtos: List[Tuple[Processor, dict]],
    pending_connections: List[Tuple[ProcessGroup, dict]],
    contexts: Dict[str, ParameterContext],
) -> None:
    """Fill ``group`` from ``dto`` (a ``VersionedProcessGroup``) in place."""
    group.comment = dto.get("comments", "") or ""
    group.position = _parse_position(dto.get("position"))
    group.variables = dict(dto.get("variables") or {})

    ctx_name = dto.get("parameterContextName")
    if ctx_name:
        # A referenced-but-undeclared context still round-trips by name.
        group.parameter_context = contexts.setdefault(
            ctx_name, ParameterContext(name=ctx_name)
        )

    # Controller services first so processor service-refs can resolve in one
    # sweep at the top.
    for service_dto in dto.get("controllerServices") or []:
        service = ControllerService(
            name=service_dto["name"],
            type=service_dto.get("type", ""),
            properties=_clean_properties(service_dto.get("properties")),
            # Older exports don't carry scheduledState on services; assume
            # enabled unless explicitly DISABLED.
            enabled=service_dto.get("scheduledState") != "DISABLED",
            comments=service_dto.get("comments") or "",
            bundle=_parse_bundle(
                service_dto.get("bundle"), service_dto.get("type", ""), service=True
            ),
        )
        identifier = service_dto.get("identifier")
        if identifier:
            by_identifier[identifier] = service
        group.controller_services.append(service)

    for funnel_dto in dto.get("funnels") or []:
        funnel = Funnel(position=_parse_position(funnel_dto.get("position")))
        identifier = funnel_dto.get("identifier")
        if identifier:
            by_identifier[identifier] = funnel
        group.funnels.append(funnel)

    for label_dto in dto.get("labels") or []:
        group.labels.append(
            Label(
                text=label_dto.get("label") or "",
                position=_parse_position(label_dto.get("position")),
                width=float(label_dto.get("width", 150.0)),
                height=float(label_dto.get("height", 150.0)),
            )
        )

    for port_dto in dto.get("inputPorts") or []:
        port = InputPort(
            name=port_dto["name"],
            position=_parse_position(port_dto.get("position")),
        )
        identifier = port_dto.get("identifier")
        if identifier:
            by_identifier[identifier] = port
        group.input_ports.append(port)

    for port_dto in dto.get("outputPorts") or []:
        port = OutputPort(
            name=port_dto["name"],
            position=_parse_position(port_dto.get("position")),
        )
        identifier = port_dto.get("identifier")
        if identifier:
            by_identifier[identifier] = port
        group.output_ports.append(port)

    for proc_dto in dto.get("processors") or []:
        scheduled_state = proc_dto.get("scheduledState") or "ENABLED"
        processor = Processor(
            name=proc_dto["name"],
            type=proc_dto.get("type", ""),
            properties=_clean_properties(proc_dto.get("properties")),
            scheduling_period=proc_dto.get("schedulingPeriod", "0 sec"),
            scheduling_strategy=proc_dto.get("schedulingStrategy", "TIMER_DRIVEN"),
            concurrent_tasks=proc_dto.get("concurrentlySchedulableTaskCount", 1),
            auto_terminate=list(proc_dto.get("autoTerminatedRelationships") or []),
            position=_parse_position(proc_dto.get("position")),
            comments=proc_dto.get("comments") or "",
            penalty_duration=proc_dto.get("penaltyDuration") or "30 sec",
            yield_duration=proc_dto.get("yieldDuration") or "1 sec",
            bulletin_level=proc_dto.get("bulletinLevel") or "WARN",
            run_duration_millis=proc_dto.get("runDurationMillis") or 0,
            execution_node=proc_dto.get("executionNode") or "ALL",
            scheduled_state=scheduled_state,
            retry_count=proc_dto.get("retryCount", 10),
            retried_relationships=list(proc_dto.get("retriedRelationships") or []),
            backoff_mechanism=proc_dto.get("backoffMechanism") or "PENALIZE_FLOWFILE",
            max_backoff_period=proc_dto.get("maxBackoffPeriod") or "10 mins",
            bundle=_parse_bundle(proc_dto.get("bundle"), proc_dto.get("type", "")),
        )
        identifier = proc_dto.get("identifier")
        if identifier:
            by_identifier[identifier] = processor
        group.processors.append(processor)
        processor_dtos.append((processor, proc_dto))

    # Defer connections to the second pass — but still record which group they
    # belong to so we attach them correctly later.
    for conn_dto in dto.get("connections") or []:
        pending_connections.append((group, conn_dto))

    # Recurse into children.
    for child_dto in dto.get("processGroups") or []:
        child = ProcessGroup(name=child_dto.get("name", ""))
        group.process_groups.append(child)
        _populate_group(child, child_dto, by_identifier, processor_dtos, pending_connections, contexts)


def _parse_position(raw: Any) -> Optional[Tuple[float, float]]:
    if not raw:
        return None
    return (float(raw.get("x", 0.0)), float(raw.get("y", 0.0)))


def _clean_properties(raw: Any) -> dict:
    """Drop ``None``-valued properties — they're NiFi defaults, not user state.

    Keeping them would force round-trips to re-emit a wall of defaults that the
    original Python flow never set, making diffs unreadable.
    """
    if not raw:
        return {}
    return {k: v for k, v in raw.items() if v is not None}


def _build_connection(
    dto: dict, by_identifier: Dict[str, NiFiComponent]
) -> Optional[Connection]:
    source_ref = dto.get("source") or {}
    target_ref = dto.get("destination") or {}
    source = by_identifier.get(source_ref.get("id"))
    target = by_identifier.get(target_ref.get("id"))
    if source is None or target is None:
        # Dangling reference — skip silently; from_json shouldn't fail on a
        # snapshot that NiFi itself produced, but we don't fabricate endpoints.
        return None

    raw_relationships = list(dto.get("selectedRelationships") or [])
    # Port and funnel sources have no named relationships; NiFi emits a single
    # empty string as a placeholder. Collapse it to ``[]`` so our model carries
    # the truth.
    if isinstance(source, (Port, Funnel)):
        relationships: List[str] = []
    else:
        relationships = [r for r in raw_relationships if r != ""]
        if not relationships:
            relationships = ["success"]

    return Connection(
        name=dto.get("name") or "",
        source=source,
        target=target,
        relationships=relationships,
        back_pressure_object_threshold=int(dto.get("backPressureObjectThreshold", 10000)),
        back_pressure_data_size_threshold=dto.get("backPressureDataSizeThreshold") or "1 GB",
        flowfile_expiration=dto.get("flowFileExpiration") or "0 sec",
        prioritizers=list(dto.get("prioritizers") or []),
        load_balance_strategy=dto.get("loadBalanceStrategy") or "DO_NOT_LOAD_BALANCE",
        partitioning_attribute=dto.get("partitioningAttribute") or "",
        load_balance_compression=dto.get("loadBalanceCompression") or "DO_NOT_COMPRESS",
    )


# =============================================================================
# to_json
# =============================================================================


def to_json(flow: Flow, *, indent: int = 2) -> str:
    """Emit ``flow`` as a NiFi ``VersionedFlowSnapshot`` JSON string."""
    # Pre-walk the tree to assign deterministic identifiers (UUID5 over a
    # path-derived name). We store them in an id() -> str map keyed on the
    # Python object identity so connection emission can look them up later.
    identifiers: Dict[int, str] = {}
    _assign_identifiers(flow, parent_path=(), identifiers=identifiers)

    flow_contents = _emit_group(
        flow,
        parent_identifier=None,
        identifiers=identifiers,
    )

    envelope = {
        "flowEncodingVersion": "1.0",
        "flowContents": flow_contents,
        "externalControllerServices": {},
        "parameterContexts": _emit_parameter_contexts(flow),
        "parameterProviders": {},
        "latest": False,
    }
    return json.dumps(envelope, indent=indent, sort_keys=True)


def _emit_parameter_contexts(flow: Flow) -> dict:
    """Collect every context bound anywhere in the tree, keyed by name."""
    out: Dict[str, dict] = {}

    def visit(group: ProcessGroup) -> None:
        ctx = group.parameter_context
        if ctx is not None and ctx.name not in out:
            out[ctx.name] = {
                "name": ctx.name,
                "description": ctx.description or "",
                "parameters": [
                    {
                        "name": p.name,
                        "description": p.description or "",
                        "sensitive": p.sensitive,
                        # NiFi never exports sensitive values; we never emit them
                        # either so secrets can't leak into git-tracked files.
                        "value": None if p.sensitive else p.value,
                    }
                    for p in ctx.parameters
                ],
                "inheritedParameterContexts": list(ctx.inherited_contexts),
            }
        for child in group.process_groups:
            visit(child)

    visit(flow)
    return out


def _assign_identifiers(
    group: ProcessGroup,
    parent_path: Tuple[str, ...],
    identifiers: Dict[int, str],
) -> None:
    """Recursively assign UUID5 identifiers to every component in ``group``.

    The seed for each UUID5 is a slash-joined string: ``"<kind>:<g1>/.../<own>"``
    where ``g1..gn`` are the group-name chain from root and ``<own>`` is the
    component's own name. This keeps re-emits byte-stable.
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
    # Funnels and labels have no (unique) name; seed on their index instead.
    for i, funnel in enumerate(group.funnels):
        identifiers[id(funnel)] = _det_uuid("funnel", own_path + (str(i),))
    for i, label in enumerate(group.labels):
        identifiers[id(label)] = _det_uuid("label", own_path + (str(i),))
    for connection in group.connections:
        # A connection's seed includes the endpoints so two connections with
        # the same (empty) name between different pairs don't collide.
        seed = (
            connection.name or "",
            identifiers.get(id(connection.source), ""),
            identifiers.get(id(connection.target), ""),
        )
        identifiers[id(connection)] = _det_uuid("connection", own_path + seed)

    for child in group.process_groups:
        _assign_identifiers(child, own_path, identifiers)


def _det_uuid(kind: str, path: Tuple[str, ...]) -> str:
    name = f"{kind}:" + "/".join(path)
    return str(uuid.uuid5(_NS, name))


def _emit_group(
    group: ProcessGroup,
    parent_identifier: Optional[str],
    identifiers: Dict[int, str],
    auto_position: Optional[Tuple[float, float]] = None,
) -> dict:
    identifier = identifiers[id(group)]
    # Auto-layout fills in coordinates for components without an explicit
    # position; explicit positions always win.
    auto = compute_layout(group)

    dto: Dict[str, Any] = {
        "componentType": "PROCESS_GROUP",
        "identifier": identifier,
        "name": group.name,
        "comments": group.comment or "",
        "position": _emit_position(group.position or auto_position),
        "controllerServices": [
            _emit_service(svc, identifier, identifiers) for svc in group.controller_services
        ],
        "inputPorts": [
            _emit_port(p, identifier, identifiers, "INPUT_PORT", auto.get(id(p)))
            for p in group.input_ports
        ],
        "outputPorts": [
            _emit_port(p, identifier, identifiers, "OUTPUT_PORT", auto.get(id(p)))
            for p in group.output_ports
        ],
        "processors": [
            _emit_processor(proc, identifier, identifiers, auto.get(id(proc)))
            for proc in group.processors
        ],
        "connections": [
            _emit_connection(conn, identifier, identifiers) for conn in group.connections
        ],
        "funnels": [
            {
                "componentType": "FUNNEL",
                "identifier": identifiers[id(f)],
                "groupIdentifier": identifier,
                "position": _emit_position(f.position or auto.get(id(f))),
            }
            for f in group.funnels
        ],
        "labels": [
            {
                "componentType": "LABEL",
                "identifier": identifiers[id(lbl)],
                "groupIdentifier": identifier,
                "label": lbl.text,
                "position": _emit_position(lbl.position or auto.get(id(lbl))),
                "width": float(lbl.width),
                "height": float(lbl.height),
                "zIndex": 0,
            }
            for lbl in group.labels
        ],
        "processGroups": [
            _emit_group(child, identifier, identifiers, auto.get(id(child)))
            for child in group.process_groups
        ],
    }
    if group.variables:
        dto["variables"] = dict(group.variables)
    if group.parameter_context is not None:
        dto["parameterContextName"] = group.parameter_context.name
    if parent_identifier is not None:
        dto["groupIdentifier"] = parent_identifier
    return dto


def _emit_position(position: Optional[Tuple[float, float]]) -> dict:
    if position is None:
        return {"x": 0.0, "y": 0.0}
    return {"x": float(position[0]), "y": float(position[1])}


def _emit_bundle(bundle: Any, type_str: str, *, service: bool = False) -> dict:
    if bundle is None:
        return default_bundle(type_str, service=service)
    return {"group": bundle.group, "artifact": bundle.artifact, "version": bundle.version}


def _emit_service(
    service: ControllerService,
    group_identifier: str,
    identifiers: Dict[int, str],
) -> dict:
    return {
        "componentType": "CONTROLLER_SERVICE",
        "identifier": identifiers[id(service)],
        "groupIdentifier": group_identifier,
        "name": service.name,
        "type": service.type,
        "bundle": _emit_bundle(service.bundle, service.type, service=True),
        "comments": service.comments or "",
        "scheduledState": "ENABLED" if service.enabled else "DISABLED",
        "properties": _emit_properties(service.properties, identifiers),
        "propertyDescriptors": _emit_service_ref_descriptors(service.properties),
    }


def _emit_port(
    port: Port,
    group_identifier: str,
    identifiers: Dict[int, str],
    component_type: str,
    auto_position: Optional[Tuple[float, float]] = None,
) -> dict:
    return {
        "componentType": component_type,
        "type": component_type,
        "identifier": identifiers[id(port)],
        "groupIdentifier": group_identifier,
        "name": port.name,
        "position": _emit_position(port.position or auto_position),
    }


def _emit_processor(
    processor: Processor,
    group_identifier: str,
    identifiers: Dict[int, str],
    auto_position: Optional[Tuple[float, float]] = None,
) -> dict:
    return {
        "componentType": "PROCESSOR",
        "identifier": identifiers[id(processor)],
        "groupIdentifier": group_identifier,
        "name": processor.name,
        "type": processor.type,
        "bundle": _emit_bundle(processor.bundle, processor.type),
        "position": _emit_position(processor.position or auto_position),
        "properties": _emit_properties(processor.properties, identifiers),
        "propertyDescriptors": _emit_service_ref_descriptors(processor.properties),
        "schedulingPeriod": processor.scheduling_period,
        "schedulingStrategy": processor.scheduling_strategy,
        "concurrentlySchedulableTaskCount": processor.concurrent_tasks,
        "autoTerminatedRelationships": list(processor.auto_terminate),
        "comments": processor.comments or "",
        "penaltyDuration": processor.penalty_duration,
        "yieldDuration": processor.yield_duration,
        "bulletinLevel": processor.bulletin_level,
        "runDurationMillis": processor.run_duration_millis,
        "executionNode": processor.execution_node,
        "scheduledState": processor.scheduled_state,
        "retryCount": processor.retry_count,
        "retriedRelationships": list(processor.retried_relationships),
        "backoffMechanism": processor.backoff_mechanism,
        "maxBackoffPeriod": processor.max_backoff_period,
    }


def _emit_properties(props: dict, identifiers: Dict[int, str]) -> dict:
    """Render properties: ControllerService instances become their identifier."""
    out: Dict[str, Any] = {}
    for key, value in props.items():
        if isinstance(value, ControllerService):
            out[key] = identifiers[id(value)]
        else:
            out[key] = value
    return out


def _emit_service_ref_descriptors(props: dict) -> dict:
    """Emit ``propertyDescriptors`` entries only for service-ref keys.

    NiFi can rebuild plain descriptors on import, but it needs to know which
    properties identify a controller service (and the expected service type) so
    it can wire the reference back up.
    """
    out: Dict[str, Any] = {}
    for key, value in props.items():
        if isinstance(value, ControllerService):
            # NiFi's VersionedPropertyDescriptor.identifiesControllerService is a
            # *boolean* flag; the expected service type is resolved from the
            # referenced service (the property value is its identifier), not the
            # descriptor. Emitting the type string here is rejected on the JSON
            # import path ("not of required type boolean").
            out[key] = {
                "name": key,
                "displayName": key,
                "identifiesControllerService": True,
            }
    return out


def _emit_connection(
    connection: Connection,
    group_identifier: str,
    identifiers: Dict[int, str],
) -> dict:
    source_ref = _emit_endpoint(connection.source, identifiers)
    target_ref = _emit_endpoint(connection.target, identifiers)

    # Ports/funnels have no named relationships; NiFi expects ``[""]`` as a
    # placeholder.
    if isinstance(connection.source, (Port, Funnel)):
        relationships: List[str] = [""]
    else:
        relationships = list(connection.relationships)

    return {
        "componentType": "CONNECTION",
        "identifier": identifiers[id(connection)],
        "groupIdentifier": group_identifier,
        "name": connection.name or "",
        "source": source_ref,
        "destination": target_ref,
        "selectedRelationships": relationships,
        "backPressureObjectThreshold": connection.back_pressure_object_threshold,
        "backPressureDataSizeThreshold": connection.back_pressure_data_size_threshold,
        "flowFileExpiration": connection.flowfile_expiration,
        "prioritizers": list(connection.prioritizers),
        "loadBalanceStrategy": connection.load_balance_strategy,
        "partitioningAttribute": connection.partitioning_attribute,
        "loadBalanceCompression": connection.load_balance_compression,
        "bends": [],
        "labelIndex": 0,
        "zIndex": 0,
    }


def _emit_endpoint(component: NiFiComponent, identifiers: Dict[int, str]) -> dict:
    if isinstance(component, Processor):
        kind = "PROCESSOR"
    elif isinstance(component, InputPort):
        kind = "INPUT_PORT"
    elif isinstance(component, OutputPort):
        kind = "OUTPUT_PORT"
    elif isinstance(component, Funnel):
        kind = "FUNNEL"
    elif isinstance(component, ProcessGroup):
        kind = "PROCESS_GROUP"
    else:
        kind = "PROCESSOR"  # defensive fallback; shouldn't happen in valid flows
    return {
        "id": identifiers[id(component)],
        "name": component.name,
        "type": kind,
    }
