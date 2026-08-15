"""The pull/plan/push engine: snapshots, in-place rebuilds, parameters."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from xml.etree import ElementTree

from niflow.core import Flow, find_identity_collisions
from niflow.rest.common import (
    _POLL_INTERVAL_S,
    _POLL_TIMEOUT_S,
    NiFiApiError,
    _iter_contexts,
    _load_env_overlay,
    _load_secrets,
    logger,
)


def _assert_identity_safe(flow: Flow) -> None:
    """Refuse to touch NiFi when the model has name-identity collisions.

    Must run before *any* live mutation: push_flow tears the old group down
    before recreating it, so an emit-time failure after teardown would lose
    the live flow outright.
    """
    collisions = find_identity_collisions(flow)
    if collisions:
        lines = "\n".join(f"  - {where}: {message}" for where, message in collisions)
        raise ValueError(
            "flow has duplicate names that break niflow's name-based identity; "
            "pushing would silently merge or drop components, so nothing was "
            f"changed:\n{lines}"
        )


class FlowsMixin:
    def validate_flow_live(self, flow: Flow, timeout: float = 30.0) -> List[dict]:
        """Dry-run ``flow`` against the live server; returns validation errors.

        Pushes a throwaway sandbox copy, waits for NiFi's (async) component
        validation to settle, collects every validation error, and deletes
        the sandbox. Catches value-level problems the static rulebook can't
        encode — bad EL expressions, unsatisfied service requirements, values
        rejected by the specific server version.
        """
        sandbox = flow.model_copy(deep=True)
        sandbox.name = f"{flow.name} (niflow-validate)"
        pg_id = self.push_flow(sandbox)
        try:
            deadline = time.monotonic() + timeout
            while True:
                statuses = [
                    comp.get("validationStatus")
                    for _, _, comp in self.walk_processors(pg_id)
                ]
                if "VALIDATING" not in statuses or time.monotonic() > deadline:
                    break
                time.sleep(_POLL_INTERVAL_S)
            return self.validation_errors(pg_id)
        finally:
            self.delete_group(pg_id)

    def download_snapshot(self, pg_id: str) -> dict:
        """A process group as a ``VersionedFlowSnapshot`` dict."""
        resp = self._request("GET", f"/process-groups/{pg_id}/download")
        return resp.json() if callable(getattr(resp, "json", None)) else json.loads(resp.text)

    def pull_flow(self, group: str) -> Flow:
        """Pull a live process group into a :class:`Flow`.

        ``parent_pg`` is set to the live parent's name (or ``"root"``) so a
        subsequent push lands the group back where it came from.
        """
        from niflow.formats import from_json

        pg_id = self.resolve_group(group)
        snapshot = self.download_snapshot(pg_id)
        flow = from_json(snapshot)

        entity = self._pg_entity(pg_id)
        parent_id = entity["component"].get("parentGroupId")
        if parent_id and parent_id != self.root_id():
            flow.parent_pg = self._pg_entity(parent_id)["component"]["name"]
        flow.nifi_id = pg_id

        # The downloaded snapshot omits sensitive *values* but parameter
        # contexts may also exist with values only NiFi knows. Pull live
        # non-sensitive values so the Python file reflects reality.
        self._refresh_parameter_values(flow)

        for warning in flow.pull_warnings:
            logger.warning("Pull of %r is lossy: %s", flow.name, warning)
        return flow

    def _refresh_parameter_values(self, flow: Flow) -> None:
        try:
            live = {
                c["component"]["name"]: c["component"]
                for c in self._get_json("/flow/parameter-contexts").get(
                    "parameterContexts", []
                )
            }
        except NiFiApiError:  # parameter contexts may be permission-restricted
            return
        for ctx in _iter_contexts(flow):
            live_ctx = live.get(ctx.name)
            if not live_ctx:
                continue
            live_params = {
                p["parameter"]["name"]: p["parameter"]
                for p in live_ctx.get("parameters", [])
            }
            for param in ctx.parameters:
                lp = live_params.get(param.name)
                if lp and not param.sensitive and lp.get("value") is not None:
                    param.value = lp["value"]

    def push_flow(
        self,
        flow: Flow,
        *,
        start: bool = False,
        secrets: Union[None, dict, str, Path] = None,
        env: Optional[str] = None,
    ) -> str:
        """Apply ``flow`` under its ``parent_pg``; returns the group id.

        Two strategies, chosen automatically:

        * **In-place rebuild** when the target group already exists *and is
          under NiFi Registry version control*. The group id and its registry
          linkage are preserved — only the contents are swapped — so the push
          shows up as *local changes* you can review and commit, instead of
          orphaning a brand-new group that has to be re-added to version
          control by hand.
        * **Delete-and-recreate** otherwise (a fresh group, or one not under
          version control). Simpler, and there's nothing to lose.
        """
        _assert_identity_safe(flow)
        parent_id = self.resolve_group(flow.parent_pg or "root")

        position = {"x": 0.0, "y": 0.0}
        existing = [c for c in self._child_groups(parent_id) if c["name"] == flow.name]
        if existing:
            pg_id = existing[0]["id"]
            position = existing[0].get("position") or position
            self._backup(pg_id, name=flow.name)
            if self._under_version_control(pg_id):
                return self._push_in_place(pg_id, flow, start=start, secrets=secrets, env=env)
            logger.info("Replacing existing group %r (%s)", flow.name, pg_id)
            self._teardown(pg_id)

        snapshot = json.loads(flow.to_json())
        self._align_bundles(snapshot)
        new_id = self._create_from_snapshot(parent_id, flow.name, snapshot, position)
        flow.nifi_id = new_id
        logger.info("Created group %r (%s) on NiFi %s", flow.name, new_id, self.version())

        self.apply_parameters(flow, secrets, env=env)

        if start:
            self.enable_services(new_id)
            self.start_group(new_id)
        return new_id

    def plan_flow(self, flow: Flow) -> Tuple[Optional[str], Flow, List[Any]]:
        """Diff ``flow`` against its live group.

        Returns ``(pg_id, live, changes)``. ``pg_id`` is ``None`` when the
        group doesn't exist yet — the plan is then "everything is an add".
        """
        from niflow.formats.json_format import from_json
        from niflow.layout import apply_layout
        from niflow.plan import diff_flows

        # Duplicate names make name-based plan matching meaningless (and the
        # apply that consumes the plan dangerous) — reject up front. Only the
        # *desired* side is checked; a live group someone built with
        # duplicates still pulls and diffs (pairing them in listed order).
        _assert_identity_safe(flow)
        # Materialise auto-layout coordinates so planned adds carry positions.
        apply_layout(flow)
        parent_id = self.resolve_group(flow.parent_pg or "root")
        existing = [c for c in self._child_groups(parent_id) if c["name"] == flow.name]
        if not existing:
            live = Flow(name=flow.name)
            return None, live, diff_flows(live, flow)
        pg_id = existing[0]["id"]
        live = from_json(self.download_snapshot(pg_id))
        live.nifi_id = pg_id
        return pg_id, live, diff_flows(live, flow)

    def push_update(
        self,
        flow: Flow,
        *,
        start: bool = False,
        secrets: Union[None, dict, str, Path] = None,
        env: Optional[str] = None,
    ) -> List[Any]:
        """Incrementally reconcile the live group with ``flow``.

        Unlike :meth:`push_flow` this never rebuilds: it computes the change
        plan and applies each change with a targeted call, so untouched
        components keep their state and queues. Returns the applied plan
        (empty list = live already matched). Creating a missing group falls
        back to a full :meth:`push_flow`.
        """
        from niflow.apply import ApplyError, PlanApplier

        pg_id, live, changes = self.plan_flow(flow)
        if pg_id is None:
            logger.info("Group %r not found — creating it in full", flow.name)
            self.push_flow(flow, start=start, secrets=secrets, env=env)
            return changes

        # Contexts first: a plan may bind a group to a brand-new context, and
        # value updates (incl. secrets) are independent of the tree diff.
        self.ensure_parameter_contexts(flow)
        self.apply_parameters(flow, secrets, env=env)

        if changes:
            backup_path = self._backup(pg_id, name=flow.name)
            try:
                PlanApplier(self, pg_id, live, flow).apply(changes)
            except ApplyError as exc:
                exc.hint = (
                    f"Restore the pre-push state with 'niflow rollback "
                    f"{flow.name}' (backup: {backup_path})."
                )
                raise
            logger.info("Applied %d change(s) to group %r (%s)", len(changes), flow.name, pg_id)
        else:
            logger.info("No changes for group %r (%s)", flow.name, pg_id)
        flow.nifi_id = pg_id

        if start:
            self.enable_services(pg_id)
            self.start_group(pg_id)
        return changes

    def ensure_parameter_contexts(self, flow: Flow) -> None:
        """Create any parameter context the flow references that NiFi lacks."""
        for ctx in _iter_contexts(flow):
            if self._find_context_entity(ctx.name) is not None:
                continue
            component: Dict[str, Any] = {
                "name": ctx.name,
                "description": ctx.description or "",
                "parameters": [],
            }
            if ctx.inherited_contexts:
                inherited = []
                for name in ctx.inherited_contexts:
                    entity = self._find_context_entity(name)
                    if entity is None:
                        logger.warning(
                            "Inherited parameter context %r not found; skipping link", name
                        )
                        continue
                    inherited.append({"id": entity["id"], "component": {"id": entity["id"]}})
                component["inheritedParameterContexts"] = inherited
            self._request(
                "POST",
                "/parameter-contexts",
                json={"revision": {"version": 0, "clientId": "niflow"}, "component": component},
            )
            logger.info("Created parameter context %r", ctx.name)

    def _under_version_control(self, pg_id: str) -> bool:
        """Is ``pg_id`` tracked by a NiFi Registry flow?"""
        component = self._pg_entity(pg_id).get("component", {})
        return bool(component.get("versionControlInformation"))

    def _push_in_place(
        self,
        pg_id: str,
        flow: Flow,
        *,
        start: bool,
        secrets: Union[None, dict, str, Path],
        env: Optional[str] = None,
    ) -> str:
        """Swap a versioned group's contents *without* deleting the group.

        The vehicle that drops a flow's components (services + wiring) straight
        *into* an existing group differs by line: NiFi 1.x uses templates
        (removed in 2.x); NiFi 2.x uses copy/paste (``PUT .../paste``), which is
        the supported template replacement. Either way the group id and its
        ``versionControlInformation`` are preserved, so the push shows up as
        *local changes* to review and commit.
        """
        logger.info("In-place rebuild of versioned group %r (%s)", flow.name, pg_id)
        self._set_group_state(pg_id, "STOPPED")
        self._empty_queues(pg_id)
        self._empty_group_contents(pg_id)
        if self._major_version() >= 2:
            self._paste_into_group(pg_id, flow)
        else:
            self._instantiate_template(pg_id, flow)
        flow.nifi_id = pg_id

        self.apply_parameters(flow, secrets, env=env)

        if start:
            self.enable_services(pg_id)
            self.start_group(pg_id)
        logger.info(
            "Rebuilt %r in place; group id and version control preserved "
            "(commit the local changes in the Registry to save a version)",
            flow.name,
        )
        return pg_id

    def _empty_group_contents(self, pg_id: str) -> None:
        """Delete everything *inside* ``pg_id`` but keep the group itself.

        Order matters: connections reference their endpoints, so they go first;
        then the canvas components; then child groups (recursively); finally the
        group-scoped controller services (disabled first). The group and its
        ``versionControlInformation`` are left untouched.
        """
        flow = self._get_json(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
        for conn in flow.get("connections", []):
            self._delete_component("connections", conn["id"])
        for proc in flow.get("processors", []):
            self._delete_component("processors", proc["id"])
        for port in flow.get("inputPorts", []):
            self._delete_component("input-ports", port["id"])
        for port in flow.get("outputPorts", []):
            self._delete_component("output-ports", port["id"])
        for funnel in flow.get("funnels", []):
            self._delete_component("funnels", funnel["id"])
        for label in flow.get("labels", []):
            self._delete_component("labels", label["id"])
        for child in flow.get("processGroups", []):
            self._teardown(child["component"]["id"])

        self._disable_services(pg_id)
        for svc in self._group_owned_services(pg_id):
            self._delete_component("controller-services", svc["id"])

    def _group_owned_services(self, pg_id: str) -> List[dict]:
        """Controller services defined *on* ``pg_id`` (not inherited from above)."""
        services = self._get_json(
            f"/flow/process-groups/{pg_id}/controller-services"
        ).get("controllerServices", [])
        return [
            s["component"]
            for s in services
            if s.get("component", {}).get("parentGroupId") == pg_id
        ]

    def _delete_component(self, kind: str, comp_id: str) -> None:
        """Delete a single component, fetching its current revision first."""
        version = self._get_json(f"/{kind}/{comp_id}")["revision"]["version"]
        self._request(
            "DELETE",
            f"/{kind}/{comp_id}",
            params={
                "version": version,
                "clientId": "niflow",
                "disconnectedNodeAcknowledged": "false",
            },
        )

    def _instantiate_template(self, pg_id: str, flow: Flow) -> None:
        """Upload ``flow`` as a template and drop its contents into ``pg_id``.

        NiFi 1.x only. The components land directly inside ``pg_id`` (no extra
        nesting), services and connections included. The temporary template is
        deleted afterwards so it doesn't linger in the template registry.
        """
        # Templates are instance-global and keyed by name, so an earlier
        # interrupted push can leave one behind and 409 the upload (and even
        # block deleting the group). Clear any same-named template first.
        self._delete_templates_named(flow.name)
        # NiFi 1.x returns the template-upload result as XML (a <templateEntity>),
        # not JSON — parse the id out of it rather than calling .json().
        upload = self._request(
            "POST",
            f"/process-groups/{pg_id}/templates/upload",
            files={"template": (f"{flow.name}.xml", flow.to_xml(), "application/xml")},
        )
        template_id = ElementTree.fromstring(upload.text).findtext("template/id")
        try:
            self._request(
                "POST",
                f"/process-groups/{pg_id}/template-instance",
                json={
                    "templateId": template_id,
                    "originX": 0.0,
                    "originY": 0.0,
                    "disconnectedNodeAcknowledged": False,
                },
            )
        finally:
            self._request("DELETE", f"/templates/{template_id}")

    def _delete_templates_named(self, name: str) -> None:
        """Delete every instance-global template whose name is ``name`` (1.x)."""
        templates = self._get_json("/flow/templates").get("templates", [])
        for entity in templates:
            if entity.get("template", {}).get("name") == name:
                self._request("DELETE", f"/templates/{entity['id']}")

    def _paste_into_group(self, pg_id: str, flow: Flow) -> None:
        """Inject ``flow``'s components into ``pg_id`` via NiFi 2.x copy/paste.

        Paste (``PUT /process-groups/{id}/paste``) is the 2.x replacement for
        templates, but it carries only *references* to a group's own controller
        services, not their definitions. So we recreate the group-level services
        first, remap every reference (processor properties and inter-service
        properties) to the new ids, and hand paste those ids as
        ``externalControllerServiceReferences`` so it wires them back up.
        Components nested in child groups bring their own services along.
        """
        snapshot = json.loads(flow.to_json())
        self._align_bundles(snapshot)
        contents = snapshot["flowContents"]

        id_map, ext_refs = self._recreate_group_services(
            pg_id, contents.get("controllerServices") or []
        )
        self._remap_service_refs(contents, id_map)

        copy_response = {
            key: contents.get(key) or []
            for key in (
                "processGroups", "processors", "inputPorts",
                "outputPorts", "connections", "labels", "funnels",
            )
        }
        copy_response["externalControllerServiceReferences"] = ext_refs
        revision = self._pg_entity(pg_id)["revision"]
        self._request(
            "PUT",
            f"/process-groups/{pg_id}/paste",
            json={
                "copyResponse": copy_response,
                "revision": {"version": revision["version"], "clientId": "niflow"},
            },
        )

    def _recreate_group_services(
        self, pg_id: str, service_dtos: List[dict]
    ) -> Tuple[Dict[str, str], Dict[str, dict]]:
        """Create ``pg_id``'s group-level controller services from the snapshot.

        Returns ``(versioned_id -> new_id, externalControllerServiceReferences)``.
        Done in two passes: create the bare services first (their properties may
        reference each other, and those ids don't exist until all are created),
        then set properties with inter-service references remapped.
        """
        id_map: Dict[str, str] = {}
        ext_refs: Dict[str, dict] = {}
        for svc in service_dtos:
            component = {"name": svc["name"], "type": svc["type"]}
            if svc.get("bundle"):
                component["bundle"] = svc["bundle"]
            created = self._request(
                "POST",
                f"/process-groups/{pg_id}/controller-services",
                json={"revision": {"version": 0, "clientId": "niflow"}, "component": component},
            ).json()
            new_id = created["component"]["id"]
            id_map[svc["identifier"]] = new_id
            ext_refs[new_id] = {"identifier": new_id, "name": svc["name"]}

        for svc in service_dtos:
            props = svc.get("properties") or {}
            if not props:
                continue
            new_id = id_map[svc["identifier"]]
            remapped = {k: id_map.get(v, v) for k, v in props.items()}
            revision = self._get_json(f"/controller-services/{new_id}")["revision"]
            self._request(
                "PUT",
                f"/controller-services/{new_id}",
                json={"revision": revision, "component": {"id": new_id, "properties": remapped}},
            )
        return id_map, ext_refs

    def _remap_service_refs(self, group: dict, id_map: Dict[str, str]) -> None:
        """Rewrite processor service-ref property values (versioned id -> new id),
        recursing into child groups. Only values matching a recreated group-level
        service are touched; services owned by nested groups are left for paste."""
        if not id_map:
            return
        for proc in group.get("processors") or []:
            props = proc.get("properties")
            if not props:
                continue
            for key, value in list(props.items()):
                if value in id_map:
                    props[key] = id_map[value]
        for child in group.get("processGroups") or []:
            self._remap_service_refs(child, id_map)

    def _create_from_snapshot(
        self, parent_id: str, name: str, snapshot: dict, position: dict
    ) -> str:
        """Create a PG from a snapshot — inline first, multipart upload fallback."""
        # NiFi (1.x at least) names the group from the snapshot's embedded
        # flowContents.name, ignoring component.name — stamp both so renames
        # (e.g. copy) actually take effect.
        if "flowContents" in snapshot:
            snapshot = dict(snapshot)
            snapshot["flowContents"] = dict(snapshot["flowContents"])
            snapshot["flowContents"]["name"] = name
        body = {
            "revision": {"version": 0, "clientId": "niflow"},
            "component": {"name": name, "position": position},
            "versionedFlowSnapshot": snapshot,
        }
        try:
            resp = self._request(
                "POST", f"/process-groups/{parent_id}/process-groups", json=body
            )
            return resp.json()["id"]
        except NiFiApiError as exc:
            if exc.status not in (400, 404, 405):
                raise
            logger.info("Inline snapshot create rejected (%s); trying multipart upload", exc.status)

        resp = self._request(
            "POST",
            f"/process-groups/{parent_id}/process-groups/upload",
            files={"file": (f"{name}.json", json.dumps(snapshot), "application/json")},
            data={
                "groupName": name,
                "positionX": str(position.get("x", 0.0)),
                "positionY": str(position.get("y", 0.0)),
                "clientId": "niflow",
            },
        )
        return resp.json()["id"]

    def bundle_index(self) -> Dict[str, dict]:
        """Map every installed ``type`` to its real NAR bundle on this instance.

        Built from ``/flow/processor-types`` + ``/flow/controller-service-types``
        (version-agnostic, so it works on 1.x and 2.x). Cached for the client's
        lifetime — the installed NAR set doesn't change mid-session.
        """
        if self._bundle_index is None:
            index: Dict[str, dict] = {}
            for endpoint, key in (
                ("/flow/processor-types", "processorTypes"),
                ("/flow/controller-service-types", "controllerServiceTypes"),
            ):
                for dto in self._get_json(endpoint).get(key, []):
                    bundle, type_str = dto.get("bundle"), dto.get("type")
                    if type_str and bundle:
                        index.setdefault(type_str, {
                            "group": bundle.get("group", ""),
                            "artifact": bundle.get("artifact", ""),
                            "version": bundle.get("version", ""),
                        })
            self._bundle_index = index
        return self._bundle_index

    def _align_bundles(self, snapshot: dict) -> None:
        """Rewrite every component's bundle to the target instance's real NAR.

        The offline emitter guesses bundle coordinates (and a placeholder
        version); the *target* is authoritative. Matching each type to the
        instance's installed NAR is what lets a flow import cleanly across NiFi
        1.x/2.x — a wrong artifact or version is exactly what yields the
        "is not a valid processor type" rejection. Types the instance doesn't
        know are left untouched (let NiFi report them honestly).
        """
        index = self.bundle_index()
        if not index:
            return

        def stamp(component: dict) -> None:
            target = index.get(component.get("type"))
            if target:
                component["bundle"] = dict(target)

        def walk(group: dict) -> None:
            for comp in group.get("processors") or []:
                stamp(comp)
            for comp in group.get("controllerServices") or []:
                stamp(comp)
            for child in group.get("processGroups") or []:
                walk(child)

        contents = snapshot.get("flowContents")
        if isinstance(contents, dict):
            walk(contents)

    def apply_parameters(
        self,
        flow: Flow,
        secrets: Union[None, dict, str, Path] = None,
        env: Optional[str] = None,
    ) -> None:
        """Make live parameter values match the model (plus secret values).

        Snapshot import creates missing contexts/parameters but never
        overwrites existing values — so after a push we submit one update
        request per context. Sensitive parameters are only sent when the
        secrets mapping provides a value.

        ``env`` selects an environment overlay: non-sensitive values from
        ``.niflow-params.<env>.env`` (same ``Context::param=value`` format as
        the secrets file) override the model's values, and
        ``.niflow-secrets.<env>.env`` becomes the default secrets file if it
        exists — one flow module, per-environment values.
        """
        if env and secrets is None:
            env_secrets = Path(f".niflow-secrets.{env}.env")
            if env_secrets.exists():
                secrets = env_secrets
        secret_map = _load_secrets(secrets)
        overlay = _load_env_overlay(env)
        for ctx in _iter_contexts(flow):
            updates = []
            for p in ctx.parameters:
                value = p.value
                if p.sensitive:
                    value = secret_map.get(f"{ctx.name}::{p.name}", secret_map.get(p.name))
                    if value is None:
                        continue  # keep whatever NiFi already has
                else:
                    value = overlay.get(f"{ctx.name}::{p.name}", overlay.get(p.name, value))
                if value is None:
                    continue
                updates.append(
                    {
                        "parameter": {
                            "name": p.name,
                            "sensitive": p.sensitive,
                            "description": p.description or "",
                            "value": value,
                        }
                    }
                )
            if updates:
                self._update_context(ctx.name, updates)

    def _find_context_entity(self, name: str) -> Optional[dict]:
        for entity in self._get_json("/flow/parameter-contexts").get(
            "parameterContexts", []
        ):
            if entity["component"]["name"] == name:
                return entity
        return None

    def _update_context(self, name: str, parameter_updates: List[dict]) -> None:
        entity = self._find_context_entity(name)
        if entity is None:
            logger.warning("Parameter context %r not found on server; skipping update", name)
            return
        ctx_id = entity["component"]["id"]
        body = {
            "revision": entity["revision"],
            "id": ctx_id,
            "component": {"id": ctx_id, "parameters": parameter_updates},
        }
        req = self._request(
            "POST", f"/parameter-contexts/{ctx_id}/update-requests", json=body
        ).json()
        req_id = req["request"]["requestId"]
        try:
            deadline = time.monotonic() + _POLL_TIMEOUT_S
            while not req["request"].get("complete"):
                if time.monotonic() > deadline:
                    raise NiFiApiError(408, f"parameter update for {name!r} timed out")
                time.sleep(_POLL_INTERVAL_S)
                req = self._get_json(
                    f"/parameter-contexts/{ctx_id}/update-requests/{req_id}"
                )
            failure = req["request"].get("failureReason")
            if failure:
                raise NiFiApiError(500, f"parameter update for {name!r} failed: {failure}")
        finally:
            self._request(
                "DELETE", f"/parameter-contexts/{ctx_id}/update-requests/{req_id}"
            )
        logger.info("Updated %d parameter(s) in context %r", len(parameter_updates), name)
