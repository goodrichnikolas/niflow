"""The pull/plan/push engine: snapshots, in-place rebuilds, parameters."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from niflow.core import (
    Flow, find_identity_collisions, find_unregistered_components,
)
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
    # Same contract, different mistake: a component that is wired but never
    # added used to surface as a bare KeyError with a memory address, thrown
    # from the middle of the emit — i.e. after teardown, on a push that had
    # already started.
    unregistered = find_unregistered_components(flow)
    if unregistered:
        lines = "\n".join(f"  - {where}: {message}" for where, message in unregistered)
        raise ValueError(
            "flow wires up components it does not contain, so the snapshot "
            f"cannot be emitted; nothing was changed:\n{lines}"
        )


def _warn_baseline(flow: Flow, live_major: int, live_version: str) -> List[dict]:
    """Warn when a flow breaks the declared baseline on a *different* line.

    The baseline (``NIFLOW_MIN_NIFI_VERSION``, default 1.24) is the oldest NiFi
    line the flows must keep working on. Pushing to a 2.x server is legitimate
    and is never blocked by it — but a 2.x-only property is still a flow that
    will not run at work, and mid-push is the cheapest moment to hear so.

    Skipped when this server *is* the baseline line: ``_warn_cross_version``
    already says it, and saying it twice trains the reader to skim.
    """
    from niflow.compat import baseline_issues, baseline_major, baseline_version

    major = baseline_major()
    if major is None or major == live_major:
        return []
    issues = baseline_issues(flow)
    if not issues:
        return []
    logger.warning(
        "%d issue(s) in %r against your compatibility baseline (NiFi %s): this "
        "push to NiFi %s is fine, but the same flow would NOT work on the "
        "baseline line",
        len(issues), flow.name, baseline_version(), live_version,
    )
    for issue in issues:
        logger.warning("  ! %s: %s", issue["component"], issue["message"])
    logger.warning(
        "  Full detail offline: niflow validate <flow.py>  (set "
        "NIFLOW_MIN_NIFI_VERSION=none if you no longer target that line)"
    )
    return issues


def _warn_untranslatable_types(flow: Flow, major: int, host: str = "") -> List[str]:
    """Types this push cannot translate, because nothing was harvested.

    Pushing to a 1.x server, ``properties_for_target`` returns identity for
    a type ``compat_v1`` has never seen — "unknown, don't translate" — so
    the properties go under their catalog (2.x) keys, 1.x files any it does
    not recognise as inert dynamic properties, and the real ones run at
    their defaults. Silently. That is the same failure the cross-version
    work chased, in the one hole it cannot close by itself.

    The stock catalogs no longer have such a hole (every type either has
    1.x data or is known 2.x-only, in which case ``flow_issues`` already
    says the push will fail). What still lands here is a **custom NAR** —
    work's own processors, which no harvest of a stock container can know —
    and a compat table generated before the type existed. Both are exactly
    the case where a silent identity translation is worst, so say it.
    """
    if major != 1:
        return []
    from niflow.processors.rules import harvested_on_v1
    from niflow.version_map import (
        PROCESSOR_TYPES_ONLY_NEW, SERVICE_TYPES_ONLY_NEW,
    )

    known_only_new = set(PROCESSOR_TYPES_ONLY_NEW) | set(SERVICE_TYPES_ONLY_NEW)
    blind: List[str] = []

    def visit(group) -> None:
        for component in list(group.processors) + list(group.controller_services):
            type_str = component.type
            if (type_str not in known_only_new
                    and not harvested_on_v1(type_str)
                    and type_str not in blind):
                blind.append(type_str)
        for child in group.process_groups:
            visit(child)

    visit(flow)
    if not blind:
        return []
    logger.warning(
        "%d type(s) in %r have no NiFi 1.x property data, so their "
        "properties are being sent under their catalog keys untranslated: "
        "%s", len(blind), flow.name, ", ".join(sorted(blind)),
    )
    logger.warning(
        "  If this server runs them (a custom NAR, or a newer 1.x line), "
        "harvest it once: NIFLOW_NIFI_HOST=%s make catalog-v1",
        host,
    )
    return blind


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
                ] + [
                    comp.get("validationStatus")
                    for _, _, comp in self.walk_services(pg_id)
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
        self._overlay_run_state(pg_id, flow)

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

    def _overlay_run_state(self, pg_id: str, flow: Flow) -> None:
        """Correct the run state that ``/download`` sanitises, in two calls.

        ``GET /process-groups/{id}/download`` is a *flow definition*, not a
        snapshot of the canvas: it reports every controller service as
        ``DISABLED`` however live it is, and every processor as ``ENABLED``
        however hard it is running (checked side by side against the live
        endpoints on 1.24 and 2.7.2). Left alone that makes ``niflow pull``
        write ``enabled=False`` for services that are enabled — a lie in the
        checked-in code that review cannot catch — and makes a *stated*
        ``enabled=True`` re-plan forever, because the live side can never agree.

        Cost is two calls for the whole subtree, however deep it is:
        ``/flow/process-groups/{id}/status?recursive=true`` carries every
        processor's ``runStatus``, and the controller-service listing takes
        ``includeDescendantGroups`` (both work on 1.x and 2.x). Best effort: if
        either read is unavailable the model keeps the snapshot's values, which
        is exactly where it was before.
        """
        from niflow.core import ProcessGroup

        status = self._recursive_status(pg_id)
        if status is None:
            logger.debug("No recursive status for %s; run state left as downloaded", pg_id)
            return

        # Walk the status tree and the model together, by group name, so every
        # group ends up paired with its live id.
        group_ids: Dict[str, ProcessGroup] = {}

        def visit(snapshot: dict, group: ProcessGroup) -> None:
            group_ids[snapshot["id"]] = group
            by_name = {p.name: p for p in group.processors}
            for wrapper in snapshot.get("processorStatusSnapshots") or []:
                live = wrapper.get("processorStatusSnapshot") or {}
                processor = by_name.get(live.get("name"))
                if processor is None:
                    continue
                run = (live.get("runStatus") or "").upper()
                # Anything that is neither running nor disabled (Stopped,
                # Validating, Invalid) is a processor that *may* run: ENABLED.
                processor.scheduled_state = (
                    "RUNNING" if run == "RUNNING"
                    else "DISABLED" if run == "DISABLED" else "ENABLED"
                )
            children = {child.name: child for child in group.process_groups}
            for wrapper in snapshot.get("processGroupStatusSnapshots") or []:
                live = wrapper.get("processGroupStatusSnapshot") or {}
                child = children.get(live.get("name"))
                if child is not None:
                    visit(live, child)

        visit(status, flow)

        try:
            listing = self._get_json(
                f"/flow/process-groups/{pg_id}/controller-services"
                "?includeAncestorGroups=false&includeDescendantGroups=true"
            )
        except Exception as exc:  # permissions, or an older endpoint signature
            logger.debug("Live controller-service state unavailable: %s", exc)
            return
        for entity in listing.get("controllerServices") or []:
            component = entity.get("component") or {}
            group = group_ids.get(component.get("parentGroupId"))
            if group is None:
                continue  # a service of an ancestor group, or a group we skipped
            for service in group.controller_services:
                if service.name == component.get("name"):
                    # ENABLING counts as enabled: it is on its way up, and a
                    # plan that proposes enabling it again would never settle.
                    service.enabled = component.get("state") in ("ENABLED", "ENABLING")

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

    def _warn_cross_version(self, flow: Flow) -> List[dict]:
        """Log every incompatibility with the *live server's* NiFi line, loudly.

        The emitter already drops unsupported keys one warning at a time while
        it renders the snapshot, which is both late (mid-push) and easy to lose
        in the log. This runs first, before any mutation, and says the whole
        thing at once: which components set properties — or use types — that
        cannot survive the crossing, and how to see the full list offline.

        Also warns when the flow would not survive the declared compatibility
        *baseline* (``NIFLOW_MIN_NIFI_VERSION``, default 1.24) even though this
        particular server is a different line. Pushing 2.x-only properties to a
        2.x server is legitimate and must not be blocked or made to look like an
        error — but if the same flow has to run on 1.24 next week, this is the
        moment it is cheapest to hear about it.

        Never fatal, in either direction: a flow with cross-version problems
        still pushes (NiFi accepts it; that is exactly the problem), so this
        informs rather than blocks. ``niflow validate`` is the gate that fails —
        it checks the baseline by default and exits non-zero.
        """
        from niflow.compat import flow_issues

        try:
            major = self._major_version()
        except Exception:  # unreachable server is the caller's problem, not ours
            return []
        _warn_baseline(flow, major, self.version())
        _warn_untranslatable_types(flow, major, getattr(self, "base", ""))
        issues = flow_issues(flow, major)
        if not issues:
            return []
        logger.warning(
            "%d cross-version issue(s) pushing %r to NiFi %s: these properties "
            "will NOT take effect on this server (NiFi stores an unknown key as "
            "an inert dynamic property and runs the real one at its default)",
            len(issues), flow.name, self.version(),
        )
        for issue in issues:
            logger.warning("  ! %s: %s", issue["component"], issue["message"])
        logger.warning(
            "  See docs/version-compat.md, or check offline with: "
            "niflow validate <flow.py> --target-version %s", self.version(),
        )
        return issues

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
        self._warn_cross_version(flow)
        self.assert_inherited_contexts_exist(flow)
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

        snapshot = json.loads(flow.to_json(target_major=self._major_version()))
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
        self._warn_cross_version(flow)
        # Materialise auto-layout coordinates so planned adds carry positions.
        apply_layout(flow)
        parent_id = self.resolve_group(flow.parent_pg or "root")
        existing = [c for c in self._child_groups(parent_id) if c["name"] == flow.name]
        if not existing:
            live = Flow(name=flow.name)
            return None, live, diff_flows(live, flow, self._major_version())
        pg_id = existing[0]["id"]
        live = from_json(self.download_snapshot(pg_id))
        # The download sanitises run state; without this a stated enabled=True
        # would re-plan forever (see :meth:`_overlay_run_state`).
        self._overlay_run_state(pg_id, live)
        live.nifi_id = pg_id
        # The server itself says which line it is; plan.diff_flows only has to
        # *infer* that (from 1.x-only property keys in the snapshot) for
        # callers with no client, and a flow whose live side happens to carry
        # no 1.x-only residue would otherwise be judged with the 2.x catalog
        # alone — the diff-side half of the cross-version fix.
        return pg_id, live, diff_flows(live, flow, self._major_version())

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

    def assert_inherited_contexts_exist(self, flow: Flow) -> None:
        """Refuse a flow that inherits a parameter context nobody has.

        NiFi's answer to an unresolvable ``inheritedParameterContexts`` entry is
        ``500 An unexpected error has occurred. Please check the logs`` on the
        *group create* — which points at niflow, not at the missing context,
        and costs a push and a log dig to work out. Found by the fuzz harness's
        parameter-context cases.
        """
        own = {ctx.name for ctx in _iter_contexts(flow)}
        missing: Dict[str, List[str]] = {}
        for ctx in _iter_contexts(flow):
            for name in ctx.inherited_contexts:
                if name in own:
                    continue
                if self._find_context_entity(name) is None:
                    missing.setdefault(ctx.name, []).append(name)
        if not missing:
            return
        lines = "\n".join(
            f"  - context {ctx!r} inherits {', '.join(repr(n) for n in names)}"
            for ctx, names in sorted(missing.items())
        )
        raise ValueError(
            "flow inherits parameter context(s) that do not exist on this "
            "server and are not part of the flow; NiFi answers a 500 with "
            f"nothing useful in it, so nothing was pushed:\n{lines}\n"
            "  Create them on the server first, or define them in the flow."
        )

    def ensure_parameter_contexts(self, flow: Flow) -> None:
        """Create any parameter context the flow references that NiFi lacks."""
        self.assert_inherited_contexts_exist(flow)
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

        Both lines follow the same three beats — emit the snapshot, pre-create
        the group's own controller services and remap every reference to them
        (:meth:`_stage_in_place_contents`), then hand the components to the
        group — and differ only in that last transport step:

        * **NiFi 2.x**: ``PUT /process-groups/{id}/paste`` (:meth:`_paste_into_group`).
        * **NiFi 1.x**: import the snapshot as a temporary child group and
          **move** its contents up with the snippet API
          (:meth:`_move_snapshot_into_group`). 1.x snapshot import always
          creates a *new* child group, which is why templates were used here
          first; the snippet move is what turns that into an in-place inject,
          and unlike a template it carries parameter references and
          load-balance compression natively.

        Either way the group id and its ``versionControlInformation`` are
        preserved, so the push shows up as *local changes* to review and commit.
        """
        logger.info("In-place rebuild of versioned group %r (%s)", flow.name, pg_id)
        on_1x = self._major_version() < 2
        # Say what no in-place vehicle carries BEFORE emptying the group:
        # afterwards the live flow is gone and the warning is too late to act on.
        self._warn_in_place_limits(pg_id, flow)
        self._set_group_state(pg_id, "STOPPED")
        self._empty_queues(pg_id)
        self._empty_group_contents(pg_id)
        vehicle = "snippet move" if on_1x else "paste"
        if on_1x:
            self._move_snapshot_into_group(pg_id, flow)
        else:
            self._paste_into_group(pg_id, flow)
        # Neither vehicle is lossless (see :meth:`_reconcile_in_place`), so the
        # rebuild always ends by diffing what landed against the model.
        self._reconcile_in_place(pg_id, flow, vehicle)
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

    # State that no in-place vehicle carries *by construction*, on either line:
    # both the snippet move and paste inject the group's **contents**, and have
    # no DTO for the group those contents land in. Everything else that a
    # vehicle mangles is discovered (not declared) by :meth:`_reconcile_in_place`.
    #
    # Narrowed on 2026-08-19 when the 1.x vehicle became the snippet move:
    # ``xml_format.template_limitations()`` also declared load-balance
    # compression and parameter references, because 1.24 dropped the former and
    # *escaped* ``#{`` in the latter while instantiating a template. The snippet
    # move carries both natively (verified live on 1.24), so warning about them
    # would now be crying wolf — templates keep the declaration for
    # ``niflow convert``, the push no longer uses it.
    _IN_PLACE_CANNOT_CARRY = (
        ("parameter_context", "parameter-context binding to {value!r}"),
        ("variables", "variables {value!r}"),
        ("comment", "group comment {value!r}"),
    )

    def _in_place_limitations(self, pg_id: str, flow: Flow) -> List[dict]:
        """The target group's own settings that this push has to *change*.

        Only the ones that actually differ from the live group: the vehicle
        never carries any of the three, but the group keeps whatever it already
        had (emptying its contents leaves its own settings alone), so declaring
        a setting that is already correct would be crying wolf.
        """
        live = self.download_snapshot(pg_id).get("flowContents") or {}
        desired = {
            "parameter_context": (
                flow.parameter_context.name if flow.parameter_context else None
            ),
            "variables": flow.variables or None,
            "comment": flow.comment or None,
        }
        current = {
            "parameter_context": live.get("parameterContextName"),
            "variables": live.get("variables") or None,
            "comment": live.get("comments") or None,
        }
        out: List[dict] = []
        for key, message in self._IN_PLACE_CANNOT_CARRY:
            value = desired[key]
            if value and value != current[key]:
                out.append({
                    "where": flow.name or ".",
                    "message": message.format(value=value),
                    "repair": "applied over the REST API once the contents land",
                })
        return out

    def _warn_in_place_limits(self, pg_id: str, flow: Flow) -> List[dict]:
        """Say what the in-place vehicle cannot carry — before anything moves.

        The in-place rebuild empties the live group first, so a warning issued
        afterwards is a post-mortem. This runs while the flow is still intact
        and names every item plus the repair that follows (see
        :meth:`_reconcile_in_place`). Not fatal: everything listed *is* applied
        over the REST API afterwards, and refusing the push would leave the
        user with no way to move a versioned flow at all.
        """
        limits = self._in_place_limitations(pg_id, flow)
        if not limits:
            return limits
        logger.warning(
            "%d setting(s) of the group %r itself cannot travel with its "
            "contents; each is applied over the REST API afterwards:",
            len(limits), flow.name,
        )
        for item in limits:
            logger.warning("  ! %s: %s (%s)", item["where"], item["message"], item["repair"])
        return limits

    # What an in-place vehicle is allowed to have got wrong, and which NiFi
    # behaviour makes the repair necessary:
    #   group_settings     — neither vehicle has a DTO for the *target* group,
    #                        so its own binding/variables/comment are re-applied
    #                        (2.x paste also drops a *child* group's binding)
    #   connection         — reserved: the 1.x template vehicle dropped
    #                        load-balance compression; the snippet move carries it
    #   processor/service  — 2.x paste lands every processor ENABLED, losing
    #                        DISABLED (the 1.x template vehicle also ESCAPED
    #                        ``#{p}`` into ``##{p}``; the snippet move does not)
    _IN_PLACE_REPAIR_KINDS = (
        "group_settings", "connection", "processor", "controller_service",
    )

    def _reconcile_in_place(self, pg_id: str, flow: Flow, vehicle: str) -> List[Any]:
        """Put back the state the in-place vehicle mangled or dropped.

        Neither vehicle round-trips a flow exactly: 1.x templates and 2.x paste
        each lose a different slice (see :data:`_IN_PLACE_REPAIR_KINDS`). Rather
        than hand-code each repair, diff the live group against the model and
        re-apply the residue with the same incremental applier
        ``push --update`` uses — one change at a time, so a single change the
        server rejects can't strand the rest.

        Only *updates* are repaired; a missing or extra component would mean the
        emitter itself lost something, which is a bug to report rather than to
        paper over.
        """
        from niflow.apply import PlanApplier
        from niflow.formats.json_format import from_json
        from niflow.plan import diff_flows

        self.ensure_parameter_contexts(flow)
        live = from_json(self.download_snapshot(pg_id))
        live.nifi_id = pg_id
        changes = diff_flows(live, flow, self._major_version())
        repairable = [
            c for c in changes
            if c.op == "update" and c.kind in self._IN_PLACE_REPAIR_KINDS
        ]
        if repairable:
            logger.info(
                "In-place %s left %d difference(s); restoring them",
                vehicle, len(repairable),
            )
            applier = PlanApplier(self, pg_id, live, flow)
            for change in repairable:
                try:
                    applier.apply([change])
                except Exception as exc:  # the contents are already in place
                    logger.warning(
                        "Could not restore %s %s %s: %s — %s; run 'niflow push "
                        "--update' to finish the job",
                        change.op, change.kind, change.location, change.name, exc,
                    )
        repaired = {id(c) for c in repairable}
        for change in changes:
            if id(change) in repaired:
                continue
            logger.warning(
                "In-place push left a difference the %s could not carry: "
                "%s %s %s: %s",
                vehicle, change.op, change.kind, change.location, change.name,
            )
        return changes

    # Name of the throwaway child group the 1.x snippet move imports into.
    # Fixed (not random) so an interrupted push leaves a *findable* group
    # rather than anonymous debris — see :meth:`_move_snapshot_into_group`.
    _STAGING_GROUP_NAME = "niflow-in-place-staging"

    def _stage_in_place_contents(
        self, pg_id: str, flow: Flow
    ) -> Tuple[dict, dict, Dict[str, dict]]:
        """Emit ``flow`` and pre-create ``pg_id``'s own controller services.

        Shared by both in-place vehicles, because both have the same hole: a
        group's *own* controller services travel as references, never as
        definitions (2.x paste has no field for them; a 1.x snippet move is
        refused outright — *"references a service that is not available in the
        destination Process Group"*). So the services are created on the target
        group first, every reference to them is remapped to the new ids, and
        the definitions are dropped from the payload. Services owned by nested
        groups travel inside those groups and are left alone.

        Returns ``(snapshot, contents, externalControllerServiceReferences)``.
        """
        snapshot = json.loads(flow.to_json(target_major=self._major_version()))
        self._align_bundles(snapshot)
        contents = snapshot["flowContents"]

        id_map, ext_refs = self._recreate_group_services(
            pg_id, contents.get("controllerServices") or []
        )
        self._remap_service_refs(contents, id_map)
        contents["controllerServices"] = []
        return snapshot, contents, ext_refs

    def _move_snapshot_into_group(self, pg_id: str, flow: Flow) -> None:
        """Inject ``flow``'s components into ``pg_id`` via the NiFi 1.x snippet API.

        NiFi 1.x has no paste and (from niflow's side) no lossless template:
        1.24 drops load-balance compression and *escapes* every ``#{`` while
        instantiating one, so parameter references land dead. What it does have
        is snapshot import plus snippets. Import always creates a **new child
        group**, so:

        1. import the snapshot as a temporary child of ``pg_id`` — its
           processors already reference the services pre-created on ``pg_id``,
           which are in scope from a child group;
        2. ``POST /snippets`` over everything the temp group holds;
        3. ``PUT /snippets/{id}`` with ``parentGroupId`` = ``pg_id`` — one
           server-side move, no re-serialisation, so property values (parameter
           references included) are byte-identical to the snapshot;
        4. delete the now-empty temp group.

        The temp group is removed on **every** exit path: a group left on the
        canvas is its own bug, and half-moved contents inside it are invisible
        to ``niflow plan``. If any step fails the caller is told that the
        versioned group is empty and how to restore it.
        """
        snapshot, contents, _ext_refs = self._stage_in_place_contents(pg_id, flow)

        # An earlier interrupted push can only have left a staging group inside
        # pg_id, which _empty_group_contents has just torn down — but never
        # import on top of one, or the move would carry a stranger's components.
        for child in self._child_groups(pg_id):
            if child["name"] == self._STAGING_GROUP_NAME:
                logger.warning("Removing a staging group left by an earlier push")
                self._teardown(child["id"])

        temp_id = self._create_from_snapshot(
            pg_id, self._STAGING_GROUP_NAME, snapshot, {"x": 0.0, "y": 0.0}
        )
        try:
            snippet_id = self._snippet_over_group(temp_id)
            self._request(
                "PUT",
                f"/snippets/{snippet_id}",
                json={
                    "snippet": {"id": snippet_id, "parentGroupId": pg_id},
                    "disconnectedNodeAcknowledged": False,
                },
            )
        except Exception as exc:
            self._discard_staging_group(temp_id)
            raise RuntimeError(
                f"In-place push of {flow.name!r} ({pg_id}) failed while moving "
                f"the new contents into the group: {exc}. The staging group was "
                f"removed, so nothing is left on the canvas — but the versioned "
                f"group is now EMPTY and still linked to its registry flow. "
                f"Restore it with 'niflow rollback {flow.name}' (the pre-push "
                f"backup), or re-run the push."
            ) from exc
        self._discard_staging_group(temp_id)

    def _snippet_over_group(self, pg_id: str) -> str:
        """Define a snippet covering everything ``pg_id`` currently holds (1.x).

        A snippet is a set of component ids plus each one's current revision;
        ``PUT /snippets/{id}`` with a different ``parentGroupId`` then moves
        exactly that set. Controller services are deliberately absent — the
        target group's own services were pre-created by
        :meth:`_stage_in_place_contents`, and nested groups carry theirs inside.
        """
        flow = self._get_json(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
        snippet: Dict[str, Any] = {"parentGroupId": pg_id}
        for key in (
            "processors", "connections", "inputPorts", "outputPorts",
            "funnels", "labels", "processGroups", "remoteProcessGroups",
        ):
            entities = flow.get(key) or []
            if entities:
                snippet[key] = {
                    entity["id"]: {
                        "clientId": "niflow",
                        "version": entity["revision"]["version"],
                    }
                    for entity in entities
                }
        created = self._request("POST", "/snippets", json={"snippet": snippet}).json()
        return created["snippet"]["id"]

    def _discard_staging_group(self, temp_id: str) -> None:
        """Delete the staging group, whether it is empty or still holds a flow.

        Never raises: this runs on the failure path too, where an exception
        would replace the *real* error with a cleanup one. A staging group that
        somehow survives is reported loudly, with its id, so it can be deleted
        by hand.
        """
        try:
            self._teardown(temp_id)
        except Exception as exc:  # pragma: no cover - defensive
            logger.error(
                "Could not delete the in-place staging group %s: %s — delete it "
                "by hand (it is a child of the group that was just pushed)",
                temp_id, exc,
            )

    def _paste_into_group(self, pg_id: str, flow: Flow) -> None:
        """Inject ``flow``'s components into ``pg_id`` via NiFi 2.x copy/paste.

        Paste (``PUT /process-groups/{id}/paste``) is the 2.x replacement for
        templates. It carries only *references* to a group's own controller
        services, so :meth:`_stage_in_place_contents` pre-creates them and
        remaps the references first; paste gets those ids as
        ``externalControllerServiceReferences`` and wires them back up.
        """
        _snapshot, contents, ext_refs = self._stage_in_place_contents(pg_id, flow)

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
        """Rewrite service-ref property values (versioned id -> new id) across the
        tree. Only values matching a recreated group-level service are touched;
        services owned by nested groups keep their own identifiers, but a nested
        component may still *reference* a parent-group service, so child groups
        are swept too (their own services included)."""
        if not id_map:
            return
        components = (group.get("processors") or []) + (group.get("controllerServices") or [])
        for component in components:
            props = component.get("properties")
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
