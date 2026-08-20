"""The checks: what "niflow got this wrong" means, and how findings are grouped.

Tier 1 (:func:`check_offline`) is pure and needs no NiFi — build, emit, re-parse,
re-emit, plan against itself, model what the server would hand back, emit for a
1.x target, round-trip through Python and XML, and prove the differ can see
every definition field (:func:`check_plan_sensitivity`). Tier 2
(:func:`check_live_validate`) adds NiFi's own verdict from a sandbox, and tier 3
(:func:`check_live_roundtrip`) adds push -> pull -> plan convergence.

Every failure carries a *signature* — the check plus either the deepest niflow
stack frame or a name-stripped message — so hundreds of failures from one root
cause collapse into one line of the report.
"""
from __future__ import annotations

import json
import re
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from niflow.core import (
    Flow, Funnel, ProcessGroup, Processor, find_identity_collisions,
    find_unregistered_components,
)
from niflow.fuzz.cases import (
    NIFI_REJECTED,
    NIFLOW_BUG,
    PASSED,
    SANDBOX_PREFIX,
    Case,
    _catalog,
    _compat_v1,
)

#: Order matters — the worst classification a case earns is the case's status.
_SEVERITY = {PASSED: 0, NIFI_REJECTED: 1, NIFLOW_BUG: 2}
# =============================================================================
# findings + signatures
# =============================================================================


@dataclass
class Finding:
    """One thing that went wrong, with a root-cause grouping key."""

    check: str
    classification: str
    message: str
    signature: str
    detail: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {"check": self.check, "classification": self.classification,
                "message": self.message, "signature": self.signature,
                "detail": self.detail}


_UUID_RE = re.compile(r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b")
_QUOTED_RE = re.compile(r"'[^']*'|\"[^\"]*\"")
_TYPE_RE = re.compile(r"\b(?:[a-z][\w]*\.){2,}[A-Za-z][\w]*\b")
_NUM_RE = re.compile(r"\b\d+\b")


def normalise_message(message: str) -> str:
    """Strip case-specific detail so two reports of one bug share a signature."""
    text = _UUID_RE.sub("<uuid>", message)
    text = _TYPE_RE.sub("<type>", text)
    text = _QUOTED_RE.sub("<x>", text)
    text = _NUM_RE.sub("<n>", text)
    return " ".join(text.split())[:160]


def _finding(check: str, message: str, *, detail: str = "",
             classification: str = NIFLOW_BUG, key: Optional[str] = None) -> Finding:
    signature = f"{check}:{key or normalise_message(message)}"
    return Finding(check, classification, message, signature, detail)


def _exception_finding(check: str, exc: BaseException, *,
                       classification: str = NIFLOW_BUG) -> Finding:
    """Group crashes by the deepest *niflow* frame — that's the root cause."""
    frames = traceback.extract_tb(exc.__traceback__)
    niflow_frames = [f for f in frames if f"{Path('niflow')}/" in f.filename.replace("\\", "/")]
    frame = (niflow_frames or frames or [None])[-1]
    where = f"{Path(frame.filename).stem}.{frame.name}:{frame.lineno}" if frame else "?"
    return Finding(
        check=check,
        classification=classification,
        message=f"{type(exc).__name__}: {exc}  (raised in {where})",
        signature=f"{check}:{type(exc).__name__}@{where}",
        detail="".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-4000:],
    )


# =============================================================================
# checks — tier 1 (offline)
# =============================================================================


@dataclass
class CaseResult:
    case: Case
    status: str = PASSED
    findings: List[Finding] = field(default_factory=list)
    observations: List[Dict[str, Any]] = field(default_factory=list)
    elapsed: float = 0.0

    def add(self, finding: Optional[Finding]) -> None:
        if finding is None:
            return
        self.findings.append(finding)
        if _SEVERITY[finding.classification] > _SEVERITY[self.status]:
            self.status = finding.classification

    def to_dict(self) -> Dict[str, Any]:
        return {
            "case_id": self.case.case_id,
            "kind": self.case.kind,
            "spec": self.case.spec,
            "status": self.status,
            "elapsed": round(self.elapsed, 4),
            "findings": [f.to_dict() for f in self.findings],
            "observations": self.observations,
        }


def _plan_detail(changes: Sequence[Any], limit: int = 2000) -> str:
    from niflow.plan import format_plan

    return format_plan(list(changes))[:limit]


def _server_normalised(snapshot: dict) -> dict:
    """The snapshot as the *server* would hand it back.

    NiFi keys component properties by string on both sides of the map, so a
    Python ``int``/``bool`` written into a flow comes back stringified. Modelling
    that offline catches the phantom-drift class without a live server.
    """
    def stringify(value: Any) -> Any:
        if value is None or isinstance(value, str):
            return value
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def visit(group: dict) -> None:
        for component in list(group.get("processors") or []) + list(
                group.get("controllerServices") or []):
            component["properties"] = {
                key: stringify(value)
                for key, value in (component.get("properties") or {}).items()
            }
        for child in group.get("processGroups") or []:
            visit(child)

    visit(snapshot["flowContents"])
    return snapshot


def check_offline(case: Case, *, target_majors: Sequence[int] = (1,)) -> CaseResult:
    """Tier 1: build, emit, re-parse, plan, and cross-version-translate a case."""
    from niflow.formats import from_json, from_xml, to_json, to_python, to_xml
    from niflow.formats.xml_format import template_limitations
    from niflow.formats.json_format import IdentityCollisionError
    from niflow.plan import diff_flows
    from niflow.validate import validate_flow

    result = CaseResult(case)
    started = time.perf_counter()
    try:
        try:
            flow = case.build()
        except Exception as exc:
            result.add(_exception_finding("build", exc))
            return result

        collisions = find_identity_collisions(flow)
        unregistered = find_unregistered_components(flow)

        # --- JSON emission + round trip -------------------------------------
        try:
            snapshot_text = to_json(flow)
        except IdentityCollisionError as exc:
            # A rejection is *correct* when the model really has duplicates;
            # rejecting a legal flow is the bug.
            if not collisions:
                result.add(_finding(
                    "identity",
                    f"to_json raised IdentityCollisionError but "
                    f"find_identity_collisions() sees no duplicate: {exc}",
                    key="unreported-collision"))
            return result
        except ValueError as exc:
            # Wiring a component the flow does not contain is the user's
            # mistake, and refusing it by name is the fix for the bare
            # KeyError-with-a-memory-address this used to raise. Only an
            # *unexplained* refusal is a niflow bug.
            if not unregistered:
                result.add(_exception_finding("emit_json", exc))
            return result
        except Exception as exc:
            result.add(_exception_finding("emit_json", exc))
            return result
        else:
            if collisions:
                result.add(_finding(
                    "identity",
                    "find_identity_collisions() reports duplicates but to_json "
                    f"emitted anyway: {collisions[0][1]}",
                    key="silent-merge"))
            if unregistered:
                result.add(_finding(
                    "wiring",
                    "find_unregistered_components() reports a component that is "
                    f"wired but not in the flow, and to_json emitted anyway: "
                    f"{unregistered[0][1]}",
                    key="unregistered-emitted"))

        try:
            reparsed = from_json(snapshot_text)
        except Exception as exc:
            result.add(_exception_finding("parse_json", exc))
            return result

        try:
            second = to_json(reparsed)
        except Exception as exc:
            result.add(_exception_finding("reemit_json", exc))
            second = None
        if second is not None and second != snapshot_text:
            result.add(_finding(
                "json_stable",
                "to_json(from_json(to_json(flow))) is not byte-identical",
                detail=_text_diff(snapshot_text, second),
                key="unstable"))

        try:
            changes = diff_flows(reparsed, flow)
        except Exception as exc:
            result.add(_exception_finding("plan", exc))
            changes = []
        if changes:
            result.add(_finding(
                "json_selfplan",
                "a flow re-parsed from its own snapshot does not plan to zero "
                f"changes ({len(changes)} op(s))",
                detail=_plan_detail(changes),
                key=_plan_signature(changes)))

        # --- what the *server* would hand back -------------------------------
        try:
            normalised = from_json(_server_normalised(json.loads(snapshot_text)))
            drift = diff_flows(normalised, flow)
        except Exception as exc:
            result.add(_exception_finding("server_normalised", exc))
            drift = []
        if drift:
            result.add(_finding(
                "server_normalised_plan",
                "the model plans non-zero against its own snapshot once NiFi "
                f"stringifies property values ({len(drift)} op(s)) — permanent drift",
                detail=_plan_detail(drift),
                key=_plan_signature(drift)))

        # --- Python emission round trip --------------------------------------
        try:
            source = to_python(flow)
            namespace: Dict[str, Any] = {}
            exec(compile(source, f"<fuzz:{case.case_id}>", "exec"), namespace)
            rebuilt = namespace.get("flow")
            if not isinstance(rebuilt, Flow):
                result.add(_finding("python_roundtrip",
                                    "to_python output does not define a top-level Flow",
                                    key="no-flow"))
            else:
                emitted = to_json(rebuilt)
                if emitted != snapshot_text:
                    result.add(_finding(
                        "python_roundtrip",
                        "to_python -> exec -> to_json does not reproduce the flow",
                        detail=_text_diff(snapshot_text, emitted),
                        key="mismatch"))
        except Exception as exc:
            result.add(_exception_finding("python_roundtrip", exc))

        # --- XML (NiFi 1.x template) emission --------------------------------
        # XML is a supported `niflow convert` export format (it stopped being
        # the 1.x in-place push vehicle on 2026-08-19), and a lossy exporter is
        # still a bug, so the round trip is held to the same bar as JSON.
        try:
            xml_text = to_xml(flow)
            xml_flow = from_xml(xml_text)
            xml_changes = diff_flows(xml_flow, flow)
            # A template's <snippet> has no DTO for the group its contents land
            # in, so the *root* group's own variables/comment/parameter-context
            # cannot cross by construction. Those are declared by
            # xml_format.template_limitations() — holding the pure format to
            # them would report a permanent false positive.
            if template_limitations(flow):
                xml_changes = [
                    c for c in xml_changes
                    if not (c.kind == "group_settings" and c.path == ()
                            and set(c.fields) <= {"variables", "comment", "parameter_context"})
                ]
            if xml_changes:
                result.add(_finding(
                    "xml_roundtrip",
                    f"to_xml -> from_xml loses {len(xml_changes)} change(s) worth of state",
                    detail=_plan_detail(xml_changes),
                    key=_plan_signature(xml_changes)))
        except Exception as exc:
            result.add(_exception_finding("emit_xml", exc))

        # --- every definition field must be diffable --------------------------
        for finding in check_plan_sensitivity(flow):
            result.add(finding)

        # --- validation must never crash --------------------------------------
        try:
            validate_flow(flow)
        except Exception as exc:
            result.add(_exception_finding("validate", exc))

        # --- cross-version property fidelity ----------------------------------
        for major in target_majors:
            _check_target_namespace(case, flow, major, result)
    finally:
        result.elapsed = time.perf_counter() - started
    return result


# --- plan sensitivity ---------------------------------------------------------
# A field the differ cannot see is a field ``push --update`` silently refuses to
# apply: you edit the flow file, niflow says "already in sync", and the live
# server keeps the old value. Each mutation below changes exactly one thing that
# the model says is part of a flow's definition; the plan MUST notice.

def _mutations() -> List[Tuple[str, Callable[[Flow], bool]]]:
    def first_processor(flow: Flow) -> Optional[Processor]:
        return flow.processors[0] if flow.processors else None

    def field_mutation(name: str, attribute: str, value: Any):
        def apply(flow: Flow) -> bool:
            processor = first_processor(flow)
            if processor is None or getattr(processor, attribute) == value:
                return False
            setattr(processor, attribute, value)
            return True

        return (name, apply)

    def connection_mutation(name: str, attribute: str, value: Any):
        def apply(flow: Flow) -> bool:
            if not flow.connections:
                return False
            connection = flow.connections[0]
            if getattr(connection, attribute) == value:
                return False
            setattr(connection, attribute, value)
            return True

        return (name, apply)

    def property_set(flow: Flow) -> bool:
        processor = first_processor(flow)
        if processor is None:
            return False
        processor.properties = dict(processor.properties)
        processor.properties["niflow.fuzz.mutation"] = "mutated"
        return True

    def property_change(flow: Flow) -> bool:
        processor = first_processor(flow)
        if processor is None or not processor.properties:
            return False
        key = sorted(k for k, v in processor.properties.items() if isinstance(v, str))
        if not key:
            return False
        processor.properties = dict(processor.properties)
        processor.properties[key[0]] = "niflow-fuzz-mutated"
        return True

    def execution_node(flow: Flow) -> bool:
        from niflow.processors.rules import primary_node_only

        processor = first_processor(flow)
        if processor is None or processor.execution_node == "PRIMARY":
            return False
        if primary_node_only(processor.type):
            # NiFi pins @PrimaryNodeOnly types to PRIMARY and refuses ALL, so
            # the model has nothing to edit here and the differ deliberately
            # ignores the field for them (niflow.plan._normalise_field).
            return False
        processor.execution_node = "PRIMARY"
        return True

    def rename(flow: Flow) -> bool:
        processor = first_processor(flow)
        if processor is None:
            return False
        processor.name = processor.name + " renamed"
        return True

    def drop_processor(flow: Flow) -> bool:
        if not flow.processors or flow.connections:
            return False
        flow.processors = flow.processors[1:]
        return True

    def group_comment(flow: Flow) -> bool:
        flow.comment = (flow.comment or "") + " mutated"
        return True

    def group_variables(flow: Flow) -> bool:
        flow.variables = dict(flow.variables, fuzz="mutated")
        return True

    def service_toggle(flow: Flow) -> bool:
        if not flow.controller_services:
            return False
        flow.controller_services[0].enabled = not flow.controller_services[0].enabled
        return True

    def service_property(flow: Flow) -> bool:
        if not flow.controller_services:
            return False
        service = flow.controller_services[0]
        service.properties = dict(service.properties, **{"niflow.fuzz": "mutated"})
        return True

    def label_text(flow: Flow) -> bool:
        if not flow.labels:
            return False
        flow.labels[0].text = flow.labels[0].text + " mutated"
        return True

    def add_funnel(flow: Flow) -> bool:
        flow.add_funnel(Funnel())
        return True

    def port_rename(flow: Flow) -> bool:
        if not flow.input_ports:
            return False
        flow.input_ports[0].name += " renamed"
        return True

    return [
        field_mutation("processor.comments", "comments", "mutated comment"),
        field_mutation("processor.scheduling_period", "scheduling_period", "7 sec"),
        field_mutation("processor.scheduling_strategy", "scheduling_strategy", "CRON_DRIVEN"),
        field_mutation("processor.concurrent_tasks", "concurrent_tasks", 7),
        field_mutation("processor.bulletin_level", "bulletin_level", "DEBUG"),
        ("processor.execution_node", execution_node),
        field_mutation("processor.penalty_duration", "penalty_duration", "77 sec"),
        field_mutation("processor.yield_duration", "yield_duration", "7 sec"),
        field_mutation("processor.run_duration_millis", "run_duration_millis", 25),
        field_mutation("processor.scheduled_state", "scheduled_state", "DISABLED"),
        field_mutation("processor.retry_count", "retry_count", 7),
        field_mutation("processor.backoff_mechanism", "backoff_mechanism", "YIELD_PROCESSOR"),
        field_mutation("processor.max_backoff_period", "max_backoff_period", "7 mins"),
        field_mutation("processor.auto_terminate", "auto_terminate", []),
        ("processor.properties[new]", property_set),
        ("processor.properties[existing]", property_change),
        ("processor.name", rename),
        ("processor.removed", drop_processor),
        connection_mutation("connection.name", "name", "mutated"),
        connection_mutation("connection.back_pressure_object_threshold",
                            "back_pressure_object_threshold", 77),
        connection_mutation("connection.back_pressure_data_size_threshold",
                            "back_pressure_data_size_threshold", "77 MB"),
        connection_mutation("connection.flowfile_expiration", "flowfile_expiration", "77 sec"),
        connection_mutation("connection.prioritizers", "prioritizers",
                            ["org.apache.nifi.prioritizer.NewestFlowFileFirstPrioritizer"]),
        connection_mutation("connection.load_balance_strategy",
                            "load_balance_strategy", "ROUND_ROBIN"),
        connection_mutation("connection.load_balance_compression",
                            "load_balance_compression", "COMPRESS_ATTRIBUTES_ONLY"),
        ("group.comment", group_comment),
        ("group.variables", group_variables),
        ("service.enabled", service_toggle),
        ("service.properties", service_property),
        ("label.text", label_text),
        ("funnel.added", add_funnel),
        ("port.name", port_rename),
    ]


def _state_deploy_intents(flow: Flow) -> None:
    """Make every *stated-intent* field an explicit assertion on this copy.

    ``ControllerService.enabled`` is a deploy intent, not observable state:
    NiFi imports every service DISABLED whatever the snapshot says, so the
    differ ignores the bare default and honours only a stated value (see
    :func:`niflow.plan._diff_service_fields`). Restating it here — assignment
    is what marks a pydantic field as set — is what makes the mutation below
    a real edit rather than a no-op, in both directions.
    """
    def visit(group) -> None:
        for service in group.controller_services:
            service.enabled = service.enabled
        for child in group.process_groups:
            visit(child)

    visit(flow)


def check_plan_sensitivity(flow: Flow) -> List[Finding]:
    """Every definition field must be visible to the differ, in both directions."""
    from niflow.plan import diff_flows

    findings: List[Finding] = []
    flow = flow.model_copy(deep=True)
    _state_deploy_intents(flow)
    for name, mutate in _mutations():
        mutated = flow.model_copy(deep=True)
        try:
            if not mutate(mutated):
                continue
        except Exception as exc:  # a mutation that cannot apply is not a finding
            findings.append(_exception_finding(f"mutate:{name}", exc))
            continue
        try:
            forward = diff_flows(flow, mutated)
            backward = diff_flows(mutated, flow)
        except Exception as exc:
            findings.append(_exception_finding("plan_mutation", exc))
            continue
        if not forward or not backward:
            direction = "live -> desired" if not forward else "desired -> live"
            findings.append(_finding(
                "plan_blind",
                f"changing {name} produces an empty plan ({direction}) — "
                f"`push --update` would silently not apply the edit",
                key=name))
    return findings


def _plan_signature(changes: Sequence[Any]) -> str:
    """Group plan drift by *which fields* drifted, not which component."""
    fields = sorted({
        re.sub(r"\[.*\]", "[…]", key)
        for change in changes for key in (change.fields or {})
    })
    kinds = sorted({f"{change.op}:{change.kind}" for change in changes})
    return "|".join(kinds + fields)[:160]


def _text_diff(left: str, right: str, limit: int = 60) -> str:
    import difflib

    lines = list(difflib.unified_diff(left.splitlines(), right.splitlines(),
                                      "expected", "actual", lineterm=""))
    return "\n".join(lines[:limit])


def _check_target_namespace(case: Case, flow: Flow, major: int, result: CaseResult) -> None:
    """Every emitted property key must exist on the target server.

    The catalog speaks 2.x; a 1.x server keys renamed properties by their old
    names and does *not* migrate — a 2.x key sent to 1.24 lands as an inert
    dynamic property while the real one stays at its default (todo.md,
    "Cross-version property fidelity"). Emitting for the target and checking
    every key against that server's own harvested namespace catches a
    regression offline.
    """
    from niflow.formats import to_json
    from niflow.processors.rules import properties_for_target

    compat = _compat_v1()
    target_names = getattr(compat, "PROPERTY_NAMES", None) if compat else None
    if major != 1 or not target_names:
        return
    try:
        snapshot = json.loads(to_json(flow, target_major=major))
    except Exception as exc:
        result.add(_exception_finding(f"emit_json_v{major}", exc))
        return

    catalog_names = getattr(_catalog(), "PROPERTY_NAMES", None) or {}

    def visit(group: dict) -> None:
        for processor in group.get("processors") or []:
            type_str = processor.get("type", "")
            known_target = set(target_names.get(type_str) or ())
            known_source = set(catalog_names.get(type_str) or ())
            if not known_target:
                continue  # no 1.x data for this type — nothing to judge
            for key in processor.get("properties") or {}:
                if key in known_target:
                    continue
                if key in known_source:
                    result.add(_finding(
                        f"target_v{major}_namespace",
                        f"property {key!r} of {type_str} is emitted under its "
                        f"2.x name to a NiFi {major}.x target — the server will "
                        f"store it as an inert dynamic property",
                        key="leaked-canonical-key"))
        for child in group.get("processGroups") or []:
            visit(child)

    visit(snapshot["flowContents"])

    # Observations (not failures on their own):
    #  * a property with no counterpart on the target is dropped on purpose,
    #    but it *is* silent value loss for the user (raw material for T13);
    #  * a type the compat harvest never saw cannot be translated at all, so
    #    any renamed property on it lands as an inert dynamic property — the
    #    live-confirmed failure mode, one harvest gap away.
    from niflow.processors.rules import harvested_on_v1

    for processor in _walk_processors(flow):
        # "No property table" is not the same as "never harvested": a type with
        # no properties at all has nothing to translate and is fully known.
        if (processor.properties and not target_names.get(processor.type)
                and not harvested_on_v1(processor.type)):
            result.observations.append({
                "kind": "no_cross_version_data",
                "target_major": major,
                "type": processor.type,
            })
            continue
        _, unsupported = properties_for_target(
            processor.type, processor.properties, major)
        if unsupported:
            result.observations.append({
                "kind": "cross_version_drop",
                "target_major": major,
                "type": processor.type,
                "properties": sorted(unsupported),
            })


def _walk_processors(group: ProcessGroup) -> Iterable[Processor]:
    for processor in group.processors:
        yield processor
    for child in group.process_groups:
        yield from _walk_processors(child)


# =============================================================================
# checks — tier 2/3 (live)
# =============================================================================

#: Live errors that are NiFi legitimately refusing a nonsensical combination
#: (or a component this server simply doesn't ship) — a pass, not a finding.
_NIFI_REJECTION_RE = re.compile(
    r"is invalid because|is not a valid processor type|Unable to find bundle|"
    r"no compatible bundle|is not known to this NiFi|unable to find a Processor|"
    r"does not exist|is required|Unable to create Controller Service|"
    r"is not a supported|Cannot create|no component could be found",
    re.IGNORECASE,
)
#: Live errors that mean *niflow* produced a bad snapshot.
_NIFLOW_FAULT_RE = re.compile(
    r"NullPointerException|not of required type|Unrecognized field|"
    r"Cannot deserialize|IllegalStateException|500 Server Error|"
    r"Internal Server Error|ClassCastException",
    re.IGNORECASE,
)


def _classify_live_error(message: str) -> str:
    if _NIFLOW_FAULT_RE.search(message):
        return NIFLOW_BUG
    if _NIFI_REJECTION_RE.search(message):
        return NIFI_REJECTED
    return NIFLOW_BUG


#: Live-validation failures ``niflow validate`` claims to catch *regardless of
#: which NiFi line the catalog was harvested from*. Everything else (a required
#: property, an allowable-value set) differs between 1.x and 2.x, so a
#: disagreement there is the documented "catalog from the other line" situation
#: (``niflow doctor`` warns about it) rather than a validator bug — those are
#: recorded as observations instead.
_VERSION_STABLE_CLAIMS: Tuple[Tuple[str, "re.Pattern[str]"], ...] = (
    ("unhandled-relationship",
     re.compile(r"Relationship .{0,80}?(?:not connected|auto-?terminated)", re.IGNORECASE)),
    ("parameter-without-context",
     re.compile(r"references one or more Parameters but no Parameter Context", re.IGNORECASE)),
)


_CATALOG_MATCH_CACHE: Dict[int, bool] = {}


def _catalog_matches(client: Any) -> bool:
    """Whether the catalog and the target server are the same NiFi line."""
    cached = _CATALOG_MATCH_CACHE.get(id(client))
    if cached is not None:
        return cached
    meta = getattr(_catalog(), "CATALOG_META", {}) or {}
    catalog_major = str(meta.get("nifi_version", "")).split(".")[0]
    try:
        server_major = str(client.version()).split(".")[0]
    except Exception:  # pragma: no cover - version is a cheap GET
        return False
    match = bool(catalog_major) and catalog_major == server_major
    _CATALOG_MATCH_CACHE[id(client)] = match
    return match


def check_live_validate(case: Case, client: Any) -> CaseResult:
    """Tier 2: NiFi's own validation of the case, versus ``niflow validate``.

    Disagreements are only a *niflow* bug when niflow could have known better:
    a flagged component the server calls valid, or a server complaint in one of
    the classes ``validate`` claims across both NiFi lines. Anything that turns
    on the harvested catalog's own NiFi version is recorded, not blamed.
    """
    from niflow.validate import validate_flow

    result = check_offline(case)
    if result.status == NIFLOW_BUG:
        return result  # offline already failed; don't burn a sandbox on it
    started = time.perf_counter()
    flow = case.build(name=f"{SANDBOX_PREFIX}{case.case_id}")
    if find_identity_collisions(flow):
        # niflow refuses to push these on purpose (name-based identity), and
        # the offline tier already checked that the refusal is consistent —
        # the guard firing is correct behaviour, not a finding.
        return result
    try:
        static = validate_flow(flow)
    except Exception as exc:
        result.add(_exception_finding("validate", exc))
        return result
    try:
        live = client.validate_flow_live(flow)
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"
        result.add(_exception_finding(
            "live_push", exc, classification=_classify_live_error(message)))
        result.elapsed += time.perf_counter() - started
        return result

    same_line = _catalog_matches(client)
    static_components = {issue["component"].rsplit("/", 1)[-1] for issue in static}
    live_components = {error["name"] for error in live}
    live_messages = {error["name"]: " ".join(error["errors"]) for error in live}

    for name in sorted(static_components - live_components):
        detail = "; ".join(i["message"] for i in static
                           if i["component"].endswith(name))[:1000]
        if same_line:
            result.add(_finding(
                "validate_false_positive",
                f"niflow validate flags {name!r} but NiFi reports it valid",
                detail=detail, key="false-positive"))
        else:
            result.observations.append({"kind": "validate_disagreement",
                                        "direction": "static_only",
                                        "component": name, "detail": detail})

    for name in sorted(live_components - static_components):
        message = live_messages[name]
        claim = next((label for label, pattern in _VERSION_STABLE_CLAIMS
                      if pattern.search(message)), None)
        if claim:
            result.add(_finding(
                "validate_false_negative",
                f"NiFi rejects {name!r} for something niflow validate claims to "
                f"cover ({claim}): {message[:200]}",
                key=claim))
        else:
            result.observations.append({"kind": "validate_disagreement",
                                        "direction": "live_only",
                                        "component": name, "detail": message[:500]})
    result.elapsed += time.perf_counter() - started
    return result


def _types_absent_from_target(flow: Flow, client: Any) -> List[str]:
    """Component types this server does not ship, so a push cannot round-trip.

    Pushing a 2.x-only type to 1.24 leaves a *ghost* component on the canvas: the
    group is created, the processor exists by name, and its properties are inert
    because NiFi has no descriptors to attach them to. The plan then never
    converges — but that is the server refusing a type it does not have, which
    niflow already warns about before the push ("type does not exist on NiFi
    1.24.0 … the push will fail"), not a niflow bug. The sweep used to blame
    niflow for it, which is how the DeleteSFTP case came to be filed as a
    compat-data gap when the type simply is not on 1.x.
    """
    from niflow.compat import type_missing_on

    try:
        major = client._major_version()
    except Exception:  # pragma: no cover - version is a cheap GET
        return []
    absent = set()
    def visit(group):
        for component in list(group.processors) + list(group.controller_services):
            if type_missing_on(component.type, major):
                absent.add(component.type)
        for child in group.process_groups:
            visit(child)
    visit(flow)
    return sorted(absent)


def check_live_roundtrip(case: Case, client: Any) -> CaseResult:
    """Tier 3: push -> pull -> plan must converge, and a re-push change nothing."""
    from niflow.formats import from_json, to_json
    from niflow.plan import diff_flows

    result = check_live_validate(case, client)
    if result.status == NIFLOW_BUG:
        return result
    started = time.perf_counter()
    flow = case.build(name=f"{SANDBOX_PREFIX}{case.case_id}")
    absent = _types_absent_from_target(flow, client)
    if absent:
        result.observations.append({
            "kind": "type_absent_from_target", "types": absent})
        result.elapsed += time.perf_counter() - started
        return result
    pg_id = None
    try:
        try:
            pg_id = client.push_flow(flow)
        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            result.add(_exception_finding(
                "live_push", exc, classification=_classify_live_error(message)))
            return result

        pulled = client.pull_flow(pg_id)
        # Judge the round trip on the line it actually happened on: the server
        # adds components of its own on import (2.x creates an AWS credentials
        # service and wires it in), and those are not drift for a model that
        # never mentioned them.
        changes = diff_flows(pulled, flow, client._major_version())
        if changes:
            result.add(_finding(
                "live_roundtrip_plan",
                f"push -> pull -> plan does not converge ({len(changes)} op(s))",
                detail=_plan_detail(changes),
                key=_plan_signature(changes)))
        snapshot = to_json(pulled)
        if to_json(from_json(snapshot)) != snapshot:
            result.add(_finding(
                "live_pull_stable",
                "a pulled flow does not re-emit byte-identically",
                key="unstable"))
        try:
            update = client.push_update(flow)
        except Exception as exc:
            result.add(_exception_finding("live_push_update", exc))
        else:
            if update:
                result.add(_finding(
                    "live_push_update",
                    f"push --update right after a clean push still applies "
                    f"{len(update)} change(s)",
                    detail=_plan_detail(update),
                    key=_plan_signature(update)))
    except Exception as exc:
        result.add(_exception_finding("live_roundtrip", exc))
    finally:
        if pg_id:
            try:
                client.delete_group(pg_id)
            except Exception:  # pragma: no cover - best-effort cleanup
                pass
        result.elapsed += time.perf_counter() - started
    return result


