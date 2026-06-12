"""Push a :class:`~niflow.core.Flow` to a running NiFi instance via nipyapi.

Strategy: **delete-and-recreate**. If a process group with the flow's name
already exists under the target parent, it is stopped and deleted, then the
whole tree is rebuilt. This keeps deploys predictable during iteration.

Within each process group, components are created in dependency order:
controller services (enabled) -> ports -> processors -> nested groups ->
connections. Component objects carry their assigned ``nifi_entity``/``nifi_id``
after creation, so connections (which reference the same objects) resolve
their endpoints directly.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from niflow.config import NiFiConfig, connect
from niflow.core import (
    Connection,
    ControllerService,
    Flow,
    Port,
    Processor,
    ProcessGroup,
)
from niflow.utils import get_logger, grid_position

logger = get_logger()


def deploy(
    flow: Flow,
    config: Optional[NiFiConfig] = None,
    start: bool = False,
    auto_terminate_unused: bool = True,
) -> Flow:
    """Deploy ``flow`` to NiFi and return it (with runtime ids populated)."""
    import nipyapi

    connect(config)

    parent_entity = _resolve_parent(nipyapi, flow.parent_pg)
    _delete_existing(nipyapi, parent_entity, flow.name)

    logger.info("Deploying flow %r under parent %r", flow.name, flow.parent_pg)
    _deploy_pg(nipyapi, flow, parent_entity, auto_terminate_unused)

    if start:
        logger.info("Starting flow %r", flow.name)
        nipyapi.canvas.schedule_process_group(flow.nifi_id, True)

    logger.info("Deploy complete: %r (id=%s)", flow.name, flow.nifi_id)
    return flow


# --- parent resolution & cleanup --------------------------------------------
def _resolve_parent(nipyapi: Any, parent_pg: str) -> Any:
    if parent_pg == "root":
        root_id = nipyapi.canvas.get_root_pg_id()
        return nipyapi.canvas.get_process_group(root_id, identifier_type="id")
    parent = nipyapi.canvas.get_process_group(parent_pg, identifier_type="name")
    if parent is None:
        raise RuntimeError(f"Parent process group {parent_pg!r} not found in NiFi.")
    if isinstance(parent, list):
        raise RuntimeError(
            f"Parent process group name {parent_pg!r} is ambiguous ({len(parent)} matches)."
        )
    return parent


def _delete_existing(nipyapi: Any, parent_entity: Any, name: str) -> None:
    """Stop and delete any direct child of ``parent_entity`` named ``name``."""
    parent_id = parent_entity.id
    for pg in nipyapi.canvas.list_all_process_groups(parent_id):
        if pg.component.name == name and pg.component.parent_group_id == parent_id:
            logger.info("Existing flow %r found (id=%s); stopping and deleting it", name, pg.id)
            try:
                nipyapi.canvas.schedule_process_group(pg.id, False)
            except Exception as exc:  # already stopped / nothing to stop
                logger.debug("Stop before delete reported: %s", exc)
            nipyapi.canvas.delete_process_group(pg, force=True, refresh=True)


# --- recursive deploy --------------------------------------------------------
def _deploy_pg(
    nipyapi: Any, pg: ProcessGroup, parent_entity: Any, auto_terminate_unused: bool
) -> None:
    location = pg.position or (0.0, 0.0)
    pg_entity = nipyapi.canvas.create_process_group(
        parent_entity, pg.name, location, comment=getattr(pg, "comment", "") or ""
    )
    pg.nifi_entity = pg_entity
    pg.nifi_id = pg_entity.id
    logger.info("Created process group %r (id=%s)", pg.name, pg.nifi_id)

    _deploy_controller_services(nipyapi, pg, pg_entity)
    _deploy_ports(nipyapi, pg, pg_entity)
    _deploy_processors(nipyapi, pg, pg_entity, auto_terminate_unused)

    for child in pg.process_groups:
        _deploy_pg(nipyapi, child, pg_entity, auto_terminate_unused)

    _deploy_connections(nipyapi, pg)


def _deploy_controller_services(nipyapi: Any, pg: ProcessGroup, pg_entity: Any) -> None:
    for svc in pg.controller_services:
        dtype = _resolve_type(nipyapi.canvas.get_controller_type(svc.type), svc.type, "controller service")
        entity = nipyapi.canvas.create_controller(pg_entity, dtype, name=svc.name)
        if svc.properties:
            entity = nipyapi.canvas.update_controller(
                entity, nipyapi.nifi.ControllerServiceDTO(properties=_stringify(svc.properties))
            )
        # Enable the service so processors that reference it are valid (unless
        # the service is explicitly created-but-disabled, e.g. by the catalog test).
        if svc.enabled:
            nipyapi.canvas.schedule_controller(entity, True, refresh=True)
        svc.nifi_entity = entity
        svc.nifi_id = entity.id
        logger.info(
            "Created %s controller service %r (id=%s)",
            "+ enabled" if svc.enabled else "(disabled)",
            svc.name, svc.nifi_id,
        )


def _deploy_ports(nipyapi: Any, pg: ProcessGroup, pg_entity: Any) -> None:
    # Lay input ports down the left edge and output ports down the right edge.
    for column, ports in ((0.0, pg.input_ports), (800.0, pg.output_ports)):
        for i, port in enumerate(ports):
            position = port.position or (column, float(i * 150))
            entity = nipyapi.canvas.create_port(
                pg_entity.id, port.port_type, port.name, "STOPPED", position=position
            )
            port.nifi_entity = entity
            port.nifi_id = entity.id
            logger.info("Created %s %r (id=%s)", port.port_type, port.name, port.nifi_id)


def _deploy_processors(
    nipyapi: Any, pg: ProcessGroup, pg_entity: Any, auto_terminate_unused: bool
) -> None:
    for i, proc in enumerate(pg.processors):
        dtype = _resolve_type(nipyapi.canvas.get_processor_type(proc.type), proc.type, "processor")
        location = proc.position or grid_position(i)
        entity = nipyapi.canvas.create_processor(pg_entity, dtype, location, name=proc.name)
        proc.nifi_entity = entity
        proc.nifi_id = entity.id

        auto_terminated = _auto_terminated_relationships(pg, proc, entity, auto_terminate_unused)
        config = nipyapi.nifi.ProcessorConfigDTO(
            properties=_stringify(proc.properties),
            scheduling_period=proc.scheduling_period,
            scheduling_strategy=proc.scheduling_strategy,
            concurrently_schedulable_task_count=proc.concurrent_tasks,
            auto_terminated_relationships=auto_terminated,
        )
        nipyapi.canvas.update_processor(entity, config)
        logger.info(
            "Created processor %r (%s, id=%s); auto-terminate=%s",
            proc.name, proc.type.rsplit(".", 1)[-1], proc.nifi_id, auto_terminated,
        )


def _deploy_connections(nipyapi: Any, pg: ProcessGroup) -> None:
    for conn in pg.connections:
        source = _require_entity(conn.source, conn, "source")
        target = _require_entity(conn.target, conn, "target")
        # Ports have no named relationships; processors use the connection's set.
        relationships = None if isinstance(conn.source, Port) else conn.relationships
        entity = nipyapi.canvas.create_connection(
            source, target, relationships=relationships, name=conn.name or None
        )
        conn.nifi_entity = entity
        conn.nifi_id = entity.id
        logger.info(
            "Connected %r -> %r %s",
            conn.source.name, conn.target.name,
            f"on {relationships}" if relationships else "",
        )


# --- helpers -----------------------------------------------------------------
def _auto_terminated_relationships(
    pg: ProcessGroup, proc: Processor, entity: Any, auto_terminate_unused: bool
) -> List[str]:
    """Relationships to auto-terminate: explicit ones plus, optionally, any not
    consumed by an outgoing connection (so the processor deploys valid)."""
    available = {r.name for r in (entity.component.relationships or [])}
    used = {
        rel
        for conn in pg.connections
        if conn.source is proc
        for rel in conn.relationships
    }
    terminated = set(proc.auto_terminate)
    if auto_terminate_unused:
        terminated |= (available - used)
    # Only return relationships the processor actually exposes.
    return sorted(terminated & available) if available else sorted(terminated)


def _require_entity(component: Any, conn: Connection, role: str) -> Any:
    if getattr(component, "nifi_entity", None) is None:
        raise RuntimeError(
            f"Connection {conn.source.name!r} -> {conn.target.name!r}: the {role} "
            f"{component.name!r} has no deployed entity. Make sure it was added to the "
            "flow (add_processor/add_port/...) and lives in or under the same flow."
        )
    return component.nifi_entity


def _resolve_type(candidates: Any, type_string: str, kind: str) -> Any:
    """Pick the exact DocumentedTypeDTO matching ``type_string`` from a lookup result."""
    if candidates is None:
        raise RuntimeError(
            f"Unknown {kind} type {type_string!r}. It is not available on this NiFi "
            "instance (check the type string and that the relevant NAR is installed)."
        )
    if isinstance(candidates, list):
        exact = [c for c in candidates if getattr(c, "type", None) == type_string]
        if exact:
            return exact[0]
        if len(candidates) == 1:
            return candidates[0]
        options = ", ".join(sorted(getattr(c, "type", "?") for c in candidates))
        raise RuntimeError(
            f"Ambiguous {kind} type {type_string!r}; matches: {options}. "
            "Use the fully-qualified class name."
        )
    return candidates


def _stringify(properties: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """NiFi property values are strings. Convert, and swap controller-service
    references for their assigned NiFi UUID."""
    out: Dict[str, Optional[str]] = {}
    for key, value in properties.items():
        if isinstance(value, ControllerService):
            if not value.nifi_id:
                raise RuntimeError(
                    f"Property {key!r} references controller service {value.name!r}, "
                    "but it has not been deployed yet. Add it with add_controller_service(...) "
                    "so it is created before the component that uses it."
                )
            out[key] = value.nifi_id
        elif isinstance(value, bool):
            out[key] = "true" if value else "false"
        elif value is None:
            out[key] = None
        else:
            out[key] = str(value)
    return out
