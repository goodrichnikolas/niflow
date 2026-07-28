"""Execute a change plan against live NiFi with targeted REST calls.

``PlanApplier`` walks the :mod:`niflow.plan` changes for a group and applies
each with the narrowest possible operation — stop/update/restart one
processor, drain/delete one connection, create one missing component — so a
push touches only what changed. Queues, processor state, and the group id
all survive. Works identically on NiFi 1.x and 2.x (only version-agnostic
endpoints are used).

Ordering matters and is fixed here, not in the plan: services first (new
ones exist before anything references them), then connection removals
(components can't be deleted while wired), component removals, subtree and
component additions, connection additions, processor updates, and finally
restarts of whatever we had to stop.

Known limitation: NiFi 1.x *variable registry* changes are reported in the
plan but skipped by the applier (the async update-request dance isn't
implemented) — a warning is logged; use parameter contexts instead.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Tuple

from niflow.core import (
    Connection,
    ControllerService,
    Flow,
    Funnel,
    InputPort,
    Label,
    NiFiComponent,
    Port,
    ProcessGroup,
    Processor,
)
from niflow.plan import Change
from niflow.utils import get_logger

logger = get_logger()

_POLL_INTERVAL_S = 0.25
_POLL_TIMEOUT_S = 30.0

# Execution order: (op, kind) -> stage. Lower runs first.
_STAGE = {
    ("update", "group_settings"): 0,
    ("add", "controller_service"): 1,
    ("update", "controller_service"): 2,
    ("remove", "connection"): 3,
    ("update", "connection"): 4,
    ("remove", "processor"): 5,
    ("remove", "input_port"): 5,
    ("remove", "output_port"): 5,
    ("remove", "funnel"): 5,
    ("remove", "label"): 5,
    ("remove", "process_group"): 6,
    ("remove", "controller_service"): 7,
    ("add", "process_group"): 8,
    ("add", "input_port"): 9,
    ("add", "output_port"): 9,
    ("add", "funnel"): 9,
    ("add", "processor"): 10,
    ("add", "label"): 10,
    ("add", "connection"): 11,
    ("update", "processor"): 12,
}

_PORT_ENDPOINT = {"input_port": "input-ports", "output_port": "output-ports"}


class PlanApplier:
    """Applies a plan produced by :func:`niflow.plan.diff_flows`."""

    def __init__(
        self, client: Any, root_pg_id: str, live: ProcessGroup, desired: ProcessGroup
    ):
        self.client = client
        self.live = live
        self.desired = desired
        # path -> live group id, seeded from the pulled model and extended as
        # groups are created.
        self.group_ids: Dict[Tuple[str, ...], str] = {(): root_pg_id}
        # (path, name) -> live controller-service id.
        self.service_ids: Dict[Tuple[Tuple[str, ...], str], str] = {}
        # (path, kind, name-or-funnel-ordinal) -> live component id, so
        # desired-side connection endpoints resolve to their live twins.
        self.component_ids: Dict[Tuple[Tuple[str, ...], str, Any], str] = {}
        # desired ControllerService object identity -> (path, name), so
        # property values referencing a service resolve to its live id.
        self.desired_service_paths: Dict[int, Tuple[Tuple[str, ...], str]] = {}
        # Processors/ports stopped by us that were running: (kind, id).
        self.to_restart: List[Tuple[str, str]] = []

        self._index_group(live, ())
        self._index_desired(desired, ())

    # ------------------------------------------------------------- indexing

    def _index_group(self, group: ProcessGroup, path: Tuple[str, ...]) -> None:
        if group.nifi_id:
            self.group_ids[path] = group.nifi_id
        for service in group.controller_services:
            if service.nifi_id:
                self.service_ids[(path, service.name)] = service.nifi_id
        for processor in group.processors:
            if processor.nifi_id:
                self.component_ids[(path, "processor", processor.name)] = processor.nifi_id
        for port in group.input_ports:
            if port.nifi_id:
                self.component_ids[(path, "input_port", port.name)] = port.nifi_id
        for port in group.output_ports:
            if port.nifi_id:
                self.component_ids[(path, "output_port", port.name)] = port.nifi_id
        for i, funnel in enumerate(group.funnels):
            if funnel.nifi_id:
                self.component_ids[(path, "funnel", i)] = funnel.nifi_id
        for child in group.process_groups:
            self._index_group(child, path + (child.name,))

    def _index_desired(self, group: ProcessGroup, path: Tuple[str, ...]) -> None:
        for service in group.controller_services:
            self.desired_service_paths[id(service)] = (path, service.name)
        for child in group.process_groups:
            self._index_desired(child, path + (child.name,))

    # -------------------------------------------------------------- driving

    def apply(self, changes: List[Change]) -> None:
        ordered = sorted(
            changes, key=lambda c: (_STAGE.get((c.op, c.kind), 99), len(c.path))
        )
        for change in ordered:
            handler = getattr(self, f"_{change.op}_{change.kind}", None)
            if handler is None:
                logger.warning(
                    "No applier for %s %s (%s) — skipped", change.op, change.kind, change.name
                )
                continue
            logger.info("%s %s %s: %s", change.op, change.kind, change.location, change.name)
            handler(change)
        self._restart_stopped()

    # ------------------------------------------------------- state plumbing

    def _get(self, path: str) -> dict:
        return self.client._get_json(path)

    def _put(self, path: str, component: dict) -> dict:
        entity = self._get(path)
        return self.client._request(
            "PUT", path, json={"revision": entity["revision"], "component": component}
        ).json()

    def _post(self, path: str, component: dict) -> dict:
        return self.client._request(
            "POST",
            path,
            json={"revision": {"version": 0, "clientId": "niflow"}, "component": component},
        ).json()

    def _await(self, path: str, ok, what: str) -> dict:
        deadline = time.monotonic() + _POLL_TIMEOUT_S
        while True:
            entity = self._get(path)
            if ok(entity):
                return entity
            if time.monotonic() > deadline:
                raise TimeoutError(f"timed out waiting for {what}")
            time.sleep(_POLL_INTERVAL_S)

    def _stop_processor(self, proc_id: str) -> None:
        entity = self._get(f"/processors/{proc_id}")
        if entity["component"].get("state") == "RUNNING":
            self.client.stop_processor(proc_id)
            self.to_restart.append(("processors", proc_id))
        self._await(
            f"/processors/{proc_id}",
            lambda e: e["component"].get("state") != "RUNNING"
            and (e.get("status", {}).get("aggregateSnapshot", {}).get("activeThreadCount", 0) == 0),
            f"processor {proc_id} to stop",
        )

    def _stop_port(self, kind: str, port_id: str) -> None:
        entity = self._get(f"/{kind}/{port_id}")
        if entity["component"].get("state") == "RUNNING":
            self.client._request(
                "PUT",
                f"/{kind}/{port_id}/run-status",
                json={"revision": entity["revision"], "state": "STOPPED",
                      "disconnectedNodeAcknowledged": False},
            )
            self.to_restart.append((kind, port_id))

    def _stop_endpoint(self, ref: dict) -> None:
        ref_type, ref_id = ref.get("type"), ref.get("id")
        if ref_type == "PROCESSOR":
            self._stop_processor(ref_id)
        elif ref_type == "INPUT_PORT":
            self._stop_port("input-ports", ref_id)
        elif ref_type == "OUTPUT_PORT":
            self._stop_port("output-ports", ref_id)
        # funnels have no run state

    def _restart_stopped(self) -> None:
        for kind, comp_id in reversed(self.to_restart):
            try:
                entity = self._get(f"/{kind}/{comp_id}")
                self.client._request(
                    "PUT",
                    f"/{kind}/{comp_id}/run-status",
                    json={"revision": entity["revision"], "state": "RUNNING",
                          "disconnectedNodeAcknowledged": False},
                )
            except Exception as exc:  # deleted since, or invalid — best effort
                logger.warning("Could not restart %s %s: %s", kind, comp_id, exc)
        self.to_restart.clear()

    # ------------------------------------------------------------ resolvers

    def _group_id(self, path: Tuple[str, ...]) -> str:
        if path in self.group_ids:
            return self.group_ids[path]
        # A group created this run by subtree instantiation: resolve by name.
        parent = self._group_id(path[:-1])
        for child in self.client._child_groups(parent):
            if child["name"] == path[-1]:
                self.group_ids[path] = child["id"]
                return child["id"]
        raise KeyError(f"no live group at path {'/'.join(path)!r}")

    def _service_id(self, service: ControllerService) -> str:
        key = self.desired_service_paths.get(id(service))
        if key is None:
            # A live-side instance (already resolved on pull) — use its id.
            if service.nifi_id:
                return service.nifi_id
            raise KeyError(f"unknown controller service {service.name!r}")
        if key not in self.service_ids:
            # Created inside a subtree instantiation: resolve via listing.
            gid = self._group_id(key[0])
            listing = self._get(f"/flow/process-groups/{gid}/controller-services")
            for svc in listing.get("controllerServices", []):
                comp = svc.get("component", {})
                if comp.get("name") == key[1] and comp.get("parentGroupId") == gid:
                    self.service_ids[key] = svc["id"]
                    break
        return self.service_ids[key]

    def _endpoint_ref(self, component: NiFiComponent, conn_path: Tuple[str, ...]) -> dict:
        """Build a connection endpoint ``{id, groupId, type}`` for a desired
        component: either in the connection's group or a direct child."""
        owner_path, kind = self._owner_of(component, conn_path)
        gid = self._group_id(owner_path)
        comp_id = self._live_component_id(component, owner_path, kind, gid)
        type_name = {
            "processor": "PROCESSOR",
            "input_port": "INPUT_PORT",
            "output_port": "OUTPUT_PORT",
            "funnel": "FUNNEL",
        }[kind]
        return {"id": comp_id, "groupId": gid, "type": type_name}

    def _owner_of(
        self, component: NiFiComponent, conn_path: Tuple[str, ...]
    ) -> Tuple[Tuple[str, ...], str]:
        group = self._desired_group(conn_path)
        kind = _kind_of(component)
        candidates = [(conn_path, group)] + [
            (conn_path + (child.name,), child) for child in group.process_groups
        ]
        for path, g in candidates:
            members = {
                "processor": g.processors,
                "input_port": g.input_ports,
                "output_port": g.output_ports,
                "funnel": g.funnels,
            }[kind]
            if any(m is component for m in members):
                return path, kind
        raise KeyError(
            f"connection endpoint {getattr(component, 'name', kind)!r} not found "
            f"in group {'/'.join(conn_path) or '.'!r} or its children"
        )

    def _desired_group(self, path: Tuple[str, ...]) -> ProcessGroup:
        group: ProcessGroup = self.desired
        for name in path:
            group = next(g for g in group.process_groups if g.name == name)
        return group

    def _live_component_id(
        self, component: NiFiComponent, owner_path: Tuple[str, ...], kind: str, gid: str
    ) -> str:
        if component.nifi_id:
            return component.nifi_id
        # Live twin recorded from the pulled model (matched the same way the
        # plan matched: by name within the group, funnels by ordinal).
        if kind == "funnel":
            owner = self._desired_group(owner_path)
            ordinal: Any = next(i for i, f in enumerate(owner.funnels) if f is component)
            key = (owner_path, "funnel", ordinal)
        else:
            key = (owner_path, kind, getattr(component, "name", ""))
        if key in self.component_ids:
            component.nifi_id = self.component_ids[key]
            return component.nifi_id
        # Fall back to live listings — the component was created this run
        # (e.g. inside a subtree instantiation).
        if kind == "funnel":
            listing = self._get(f"/process-groups/{gid}/funnels").get("funnels", [])
            listing.sort(key=lambda f: (
                f.get("position", {}).get("y", 0.0), f.get("position", {}).get("x", 0.0)
            ))
            if ordinal < len(listing):
                component.nifi_id = listing[ordinal]["id"]
                return component.nifi_id
            raise KeyError(f"funnel #{ordinal} not found live in {gid}")
        endpoint = {"processor": "processors", "input_port": "input-ports",
                    "output_port": "output-ports"}[kind]
        key = {"processors": "processors", "input-ports": "inputPorts",
               "output-ports": "outputPorts"}[endpoint]
        listing = self._get(f"/process-groups/{gid}/{endpoint}").get(key, [])
        for entity in listing:
            if entity.get("component", {}).get("name") == getattr(component, "name", None):
                component.nifi_id = entity["id"]
                return entity["id"]
        raise KeyError(f"{kind} {getattr(component, 'name', '?')!r} not found live in {gid}")

    # ------------------------------------------------------ change handlers

    def _update_group_settings(self, change: Change) -> None:
        gid = self._group_id(change.path)
        component: dict = {"id": gid}
        if "comment" in change.fields:
            component["comments"] = change.fields["comment"][1] or ""
        if "parameter_context" in change.fields:
            ctx_name = change.fields["parameter_context"][1]
            if ctx_name is None:
                component["parameterContext"] = {"id": None}
            else:
                entity = self.client._find_context_entity(ctx_name)
                if entity is None:
                    raise KeyError(
                        f"parameter context {ctx_name!r} does not exist on this NiFi"
                    )
                component["parameterContext"] = {"id": entity["id"]}
        if "variables" in change.fields:
            logger.warning(
                "Variable registry changes on %s are not applied incrementally "
                "(use parameter contexts); skipped: %r -> %r",
                change.location, *change.fields["variables"],
            )
        if len(component) > 1:
            self._put(f"/process-groups/{gid}", component)

    # --- controller services

    def _service_component(self, service: ControllerService) -> dict:
        properties = {
            k: (self._service_id(v) if isinstance(v, ControllerService) else v)
            for k, v in service.properties.items()
        }
        return {"name": service.name, "comments": service.comments or "",
                "properties": properties}

    def _add_controller_service(self, change: Change) -> None:
        service: ControllerService = change.desired
        gid = self._group_id(change.path)
        bundle = self.client.bundle_index().get(service.type)
        component = self._service_component(service)
        component["type"] = service.type
        if bundle:
            component["bundle"] = bundle
        entity = self._post(f"/process-groups/{gid}/controller-services", component)
        self.service_ids[(change.path, service.name)] = entity["id"]
        if service.enabled:
            self._set_service_state(entity["id"], "ENABLED")

    def _update_controller_service(self, change: Change) -> None:
        service: ControllerService = change.desired
        svc_id = self.service_ids.get((change.path, service.name)) or self._service_id(service)
        entity = self._get(f"/controller-services/{svc_id}")
        running_refs = self._stop_service_references(svc_id, entity)
        was_enabled = entity["component"].get("state") == "ENABLED"
        if was_enabled:
            self._set_service_state(svc_id, "DISABLED", wait=True)
        self._put(f"/controller-services/{svc_id}", {
            "id": svc_id, **self._service_component(service)
        })
        if service.enabled:
            self._set_service_state(svc_id, "ENABLED", wait=bool(running_refs))
        for kind, ref_id in running_refs:
            self.to_restart.append((kind, ref_id))

    def _remove_controller_service(self, change: Change) -> None:
        svc_id = change.live.nifi_id or self.service_ids.get((change.path, change.name))
        if not svc_id:
            raise KeyError(f"no live id for controller service {change.name!r}")
        entity = self._get(f"/controller-services/{svc_id}")
        if entity["component"].get("state") == "ENABLED":
            self._stop_service_references(svc_id, entity)
            self._set_service_state(svc_id, "DISABLED", wait=True)
        self.client._delete_component("controller-services", svc_id)

    def _set_service_state(self, svc_id: str, state: str, wait: bool = False) -> None:
        entity = self._get(f"/controller-services/{svc_id}")
        self.client._request(
            "PUT",
            f"/controller-services/{svc_id}/run-status",
            json={"revision": entity["revision"], "state": state,
                  "disconnectedNodeAcknowledged": False},
        )
        if wait:
            self._await(
                f"/controller-services/{svc_id}",
                lambda e: e["component"].get("state") == state,
                f"service {svc_id} -> {state}",
            )

    def _stop_service_references(self, svc_id: str, entity: dict) -> List[Tuple[str, str]]:
        """Stop running components referencing the service; return them."""
        running: List[Tuple[str, str]] = []
        refs = (entity["component"].get("referencingComponents")
                or entity.get("component", {}).get("referencingComponents") or [])
        for ref in refs:
            comp = ref.get("component", {})
            if comp.get("referenceType") == "Processor" and comp.get("state") == "RUNNING":
                self._stop_processor(comp["id"])
                # _stop_processor records the restart; drop the duplicate and
                # let the service handler own it so enable happens first.
                self.to_restart.remove(("processors", comp["id"]))
                running.append(("processors", comp["id"]))
        return running

    # --- connections

    def _connection_component(self, conn: Connection) -> dict:
        component: dict = {
            "name": conn.name or "",
            "backPressureObjectThreshold": conn.back_pressure_object_threshold,
            "backPressureDataSizeThreshold": conn.back_pressure_data_size_threshold,
            "flowFileExpiration": conn.flowfile_expiration,
            "prioritizers": list(conn.prioritizers),
            "loadBalanceStrategy": conn.load_balance_strategy,
            "loadBalancePartitionAttribute": conn.partitioning_attribute,
            "loadBalanceCompression": conn.load_balance_compression,
        }
        # Port/funnel sources have no named relationships; the live REST
        # endpoint wants the field absent (the [""] placeholder is a
        # snapshot-format quirk only).
        if not isinstance(conn.source, (Port, Funnel)):
            component["selectedRelationships"] = list(conn.relationships)
        return component

    def _add_connection(self, change: Change) -> None:
        conn: Connection = change.desired
        gid = self._group_id(change.path)
        component = self._connection_component(conn)
        component["source"] = self._endpoint_ref(conn.source, change.path)
        component["destination"] = self._endpoint_ref(conn.target, change.path)
        entity = self._post(f"/process-groups/{gid}/connections", component)
        conn.nifi_id = entity["id"]

    def _update_connection(self, change: Change) -> None:
        conn_id = change.live.nifi_id
        if not conn_id:
            raise KeyError(f"no live id for connection {change.name!r}")
        entity = self._get(f"/connections/{conn_id}")
        if "relationships" in change.fields:
            self._stop_endpoint(entity["component"].get("source") or {})
        component = self._connection_component(change.desired)
        component["id"] = conn_id
        self._put(f"/connections/{conn_id}", component)

    def _remove_connection(self, change: Change) -> None:
        conn_id = change.live.nifi_id
        if not conn_id:
            raise KeyError(f"no live id for connection {change.name!r}")
        entity = self._get(f"/connections/{conn_id}")
        self._stop_endpoint(entity["component"].get("source") or {})
        self._stop_endpoint(entity["component"].get("destination") or {})
        self._drain_connection(conn_id)
        self.client._delete_component("connections", conn_id)

    def _drain_connection(self, conn_id: str) -> None:
        self.client.drain_connection(conn_id)

    # --- processors

    def _processor_config(self, proc: Processor) -> dict:
        properties = {
            k: (self._service_id(v) if isinstance(v, ControllerService) else v)
            for k, v in proc.properties.items()
        }
        return {
            "properties": properties,
            "schedulingPeriod": proc.scheduling_period,
            "schedulingStrategy": proc.scheduling_strategy,
            "concurrentlySchedulableTaskCount": proc.concurrent_tasks,
            "autoTerminatedRelationships": list(proc.auto_terminate),
            "penaltyDuration": proc.penalty_duration,
            "yieldDuration": proc.yield_duration,
            "bulletinLevel": proc.bulletin_level,
            "runDurationMillis": proc.run_duration_millis,
            "executionNode": proc.execution_node,
            "comments": proc.comments or "",
            "retryCount": proc.retry_count,
            "retriedRelationships": list(proc.retried_relationships),
            "backoffMechanism": proc.backoff_mechanism,
            "maxBackoffPeriod": proc.max_backoff_period,
        }

    def _add_processor(self, change: Change) -> None:
        proc: Processor = change.desired
        gid = self._group_id(change.path)
        x, y = proc.position or (0.0, 0.0)
        component: dict = {
            "type": proc.type,
            "name": proc.name,
            "position": {"x": x, "y": y},
            "config": self._processor_config(proc),
        }
        bundle = self.client.bundle_index().get(proc.type)
        if bundle:
            component["bundle"] = bundle
        entity = self._post(f"/process-groups/{gid}/processors", component)
        proc.nifi_id = entity["id"]
        if proc.scheduled_state == "DISABLED":
            self.client._set_processor_state(entity["id"], "DISABLED")

    def _update_processor(self, change: Change) -> None:
        proc: Processor = change.desired
        proc_id = change.live.nifi_id
        if not proc_id:
            raise KeyError(f"no live id for processor {change.name!r}")
        self._stop_processor(proc_id)
        self._put(f"/processors/{proc_id}", {
            "id": proc_id, "name": proc.name, "config": self._processor_config(proc),
        })
        if "scheduled_state" in change.fields:
            state = "DISABLED" if proc.scheduled_state == "DISABLED" else "STOPPED"
            self.client._set_processor_state(proc_id, state)
            # Don't restart a processor the model wants disabled.
            if state == "DISABLED" and ("processors", proc_id) in self.to_restart:
                self.to_restart.remove(("processors", proc_id))
        proc.nifi_id = proc_id

    def _remove_processor(self, change: Change) -> None:
        proc_id = change.live.nifi_id
        if not proc_id:
            raise KeyError(f"no live id for processor {change.name!r}")
        self._stop_processor(proc_id)
        if ("processors", proc_id) in self.to_restart:
            self.to_restart.remove(("processors", proc_id))
        self.client._delete_component("processors", proc_id)

    # --- ports / funnels / labels

    def _add_input_port(self, change: Change) -> None:
        self._add_port(change, "input-ports")

    def _add_output_port(self, change: Change) -> None:
        self._add_port(change, "output-ports")

    def _add_port(self, change: Change, endpoint: str) -> None:
        port: Port = change.desired
        gid = self._group_id(change.path)
        x, y = port.position or (0.0, 0.0)
        entity = self._post(
            f"/process-groups/{gid}/{endpoint}",
            {"name": port.name, "position": {"x": x, "y": y}},
        )
        port.nifi_id = entity["id"]

    def _remove_input_port(self, change: Change) -> None:
        self._remove_port(change, "input-ports")

    def _remove_output_port(self, change: Change) -> None:
        self._remove_port(change, "output-ports")

    def _remove_port(self, change: Change, endpoint: str) -> None:
        port_id = change.live.nifi_id
        if not port_id:
            raise KeyError(f"no live id for port {change.name!r}")
        self._stop_port(endpoint, port_id)
        self.to_restart = [r for r in self.to_restart if r != (endpoint, port_id)]
        self.client._delete_component(endpoint, port_id)

    def _add_funnel(self, change: Change) -> None:
        funnel: Funnel = change.desired
        gid = self._group_id(change.path)
        x, y = funnel.position or (0.0, 0.0)
        entity = self._post(f"/process-groups/{gid}/funnels", {"position": {"x": x, "y": y}})
        funnel.nifi_id = entity["id"]

    def _remove_funnel(self, change: Change) -> None:
        if not change.live.nifi_id:
            raise KeyError(f"no live id for {change.name}")
        self.client._delete_component("funnels", change.live.nifi_id)

    def _add_label(self, change: Change) -> None:
        label: Label = change.desired
        gid = self._group_id(change.path)
        x, y = label.position or (0.0, 0.0)
        self._post(
            f"/process-groups/{gid}/labels",
            {"label": label.text, "width": label.width, "height": label.height,
             "position": {"x": x, "y": y}},
        )

    def _remove_label(self, change: Change) -> None:
        if not change.live.nifi_id:
            raise KeyError(f"no live id for label {change.name!r}")
        self.client._delete_component("labels", change.live.nifi_id)

    # --- process groups

    def _add_process_group(self, change: Change) -> None:
        child: ProcessGroup = change.desired
        parent_gid = self._group_id(change.path)
        sub = _as_flow(child)
        snapshot = json.loads(sub.to_json())
        self.client._align_bundles(snapshot)
        x, y = child.position or (0.0, 0.0)
        new_id = self.client._create_from_snapshot(
            parent_gid, child.name, snapshot, {"x": x, "y": y}
        )
        self.group_ids[change.path + (child.name,)] = new_id
        child.nifi_id = new_id

    def _remove_process_group(self, change: Change) -> None:
        gid = change.live.nifi_id or self.group_ids.get(change.path + (change.name,))
        if not gid:
            raise KeyError(f"no live id for group {change.name!r}")
        self.client._teardown(gid)


def _kind_of(component: NiFiComponent) -> str:
    if isinstance(component, Funnel):
        return "funnel"
    if isinstance(component, InputPort):
        return "input_port"
    if isinstance(component, Port):
        return "output_port"
    return "processor"


def _as_flow(group: ProcessGroup) -> Flow:
    """Wrap a ProcessGroup as a Flow (sharing component references) so the
    existing snapshot emitter can serialise the subtree."""
    sub = Flow(name=group.name)
    for name in ("comment", "position", "layout", "processors", "controller_services",
                 "input_ports", "output_ports", "connections", "process_groups",
                 "funnels", "labels", "variables", "parameter_context"):
        object.__setattr__(sub, name, getattr(group, name))
    return sub
