"""Regenerate the processor + controller-service *catalogs* from a running NiFi.

NiFi exposes every available processor / controller-service type via its REST
API; this module pulls that list and emits two generated modules:

* ``niflow/processors/catalog.py`` — one thin factory per processor type
  (hundreds of them), plus ``ALL`` / ``TYPES`` / ``RESTRICTED`` / ``DEPRECATED``
  registries you can iterate over.
* ``niflow/services/catalog.py`` — same shape for controller services.

The hand-curated factories in :mod:`niflow.processors.standard` /
:mod:`niflow.services.standard` carry sensible default properties and stay the
*preferred* entry points; the generated catalog is the exhaustive type-only
shell — useful as a registry, for catalog tests, and so callers don't have to
hand-type fully-qualified class names.

Run it::

    python -m niflow.codegen        # needs a reachable NiFi (see niflow.config)

or ``make catalog``. The generator overwrites the catalog files in place; treat
them as build artefacts that just happen to be committed for offline imports.
"""
from __future__ import annotations

import keyword
import re
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List, Optional, Tuple

from niflow.config import NiFiConfig
from niflow.utils import get_logger

logger = get_logger()

# Where the generated files live, derived from this module's location so the
# script works regardless of CWD.
_NIFLOW_ROOT = Path(__file__).resolve().parent
PROCESSORS_CATALOG_PATH = _NIFLOW_ROOT / "processors" / "catalog.py"
SERVICES_CATALOG_PATH = _NIFLOW_ROOT / "services" / "catalog.py"
COMPAT_V1_PATH = _NIFLOW_ROOT / "processors" / "compat_v1.py"

# Temp group + client id used while harvesting the rulebook (relationships /
# descriptors). NiFi only exposes these once a type is instantiated, so we
# create one of each in a throwaway group, read them back, and delete the group.
_HARVEST_GROUP = "__niflow_harvest__"
_CLIENT_ID = "niflow-codegen"

# A dynamic property name no real processor declares, used to find out whether a
# type turns dynamic properties into *relationships* (RouteOnAttribute and
# friends). NiFi 1.x has no ``/flow/processor-definition`` endpoint to ask —
# 1.24 answers that URL with a 404 — but setting one dynamic property and
# reading the relationships back works on both lines, so the fact is harvested
# rather than curated.
_DYNAMIC_PROBE_PROPERTY = "niflow-harvest-probe"

# Cap on how many allowable values of one property get probed for conditional
# relationships. A relationship set that turns on a property does so on a small
# enum (UpdateAttribute's "Store State", RouteOnAttribute's "Routing Strategy");
# a very long allowable list is a lookup of some sort, not a mode switch, and
# probing it would cost far more than it can find.
_MAX_CONDITIONAL_PROBE_VALUES = 12


# --- name sanitisation ------------------------------------------------------

_IDENT_RE = re.compile(r"[^0-9a-zA-Z_]")


def _simple_name(fqcn: str) -> str:
    """``org.apache.nifi.processors.standard.PutFile`` -> ``PutFile``."""
    return fqcn.rsplit(".", 1)[-1]


def _artifact_stem(bundle: Any) -> str:
    """Short, identifier-safe tail of the bundle artifact (e.g. ``kafka_3``)."""
    artifact = getattr(bundle, "artifact", "") or ""
    # Strip "nifi-" prefix and "-nar" suffix that almost every NAR carries.
    trimmed = re.sub(r"^nifi-", "", artifact)
    trimmed = re.sub(r"-nar$", "", trimmed)
    return _IDENT_RE.sub("_", trimmed).strip("_") or "ext"


def _safe_identifier(raw: str) -> str:
    """Coerce *raw* into a valid (non-keyword) Python identifier."""
    out = _IDENT_RE.sub("_", raw)
    if not out or not (out[0].isalpha() or out[0] == "_"):
        out = "_" + out
    if keyword.iskeyword(out):
        out += "_"
    return out


def _unique_factory_names(items: List[Any]) -> List[Tuple[str, Any]]:
    """Return ``[(factory_name, item), ...]`` with every factory name unique.

    Strategy: try the simple class name first; on collision, suffix with the
    bundle artifact stem; if even that collides, append a counter. Items are
    paired with the same ``DocumentedTypeDTO`` they came from.

    Items already sharing a fully-qualified type (same FQCN in two bundles)
    are de-duplicated up front — only the first is emitted, and the duplicate
    fact is logged. niflow's deploy can't pick a side for an ambiguous FQCN
    anyway (``_resolve_type`` would raise), so the catalog matches reality.
    """
    by_fqcn: dict = defaultdict(list)
    for it in items:
        by_fqcn[it.type].append(it)
    for fqcn, dupes in by_fqcn.items():
        if len(dupes) > 1:
            logger.warning(
                "FQCN %r appears in %d bundles; keeping first, dropping the rest",
                fqcn, len(dupes),
            )

    deduped = [dupes[0] for dupes in by_fqcn.values()]
    deduped.sort(key=lambda it: it.type)

    used: set = set()
    out: List[Tuple[str, Any]] = []
    for it in deduped:
        base = _safe_identifier(_simple_name(it.type))
        candidate = base
        if candidate in used:
            candidate = _safe_identifier(f"{base}_{_artifact_stem(it.bundle)}")
        n = 2
        while candidate in used:
            candidate = f"{base}_{n}"
            n += 1
        used.add(candidate)
        out.append((candidate, it))
    return out


# --- file emission ----------------------------------------------------------

_HEADER = '''"""AUTO-GENERATED by ``python -m niflow.codegen`` — do not edit by hand.

Exhaustive catalog of {kind} types reported by the NiFi instance this was
generated against. Each factory is a thin shell over the type string; for
sensible defaults prefer the curated factories in :mod:`{curated}`.

Regenerate after upgrading NiFi or installing new NARs::

    python -m niflow.codegen
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from {curated} import {helper}
from {component_mod} import {component_cls}
'''


def _emit_processor_factory(factory_name: str, fqcn: str) -> str:
    return (
        f"def {factory_name}(name: str = {factory_name!r}, "
        f"properties: Optional[Dict[str, Any]] = None, **settings: Any) -> {{cls}}:\n"
        f"    return _processor({fqcn!r}, name, properties=properties, **settings)\n\n"
    ).replace("{cls}", "Processor")


def _emit_service_factory(factory_name: str, fqcn: str) -> str:
    return (
        f"def {factory_name}(name: str = {factory_name!r}, "
        f"properties: Optional[Dict[str, Any]] = None) -> ControllerService:\n"
        f"    return _service({fqcn!r}, name, properties=properties)\n\n"
    )


def _emit_registries(named: List[Tuple[str, Any]]) -> str:
    """Produce the ``ALL`` / ``TYPES`` / ``RESTRICTED`` / ``DEPRECATED`` block."""
    all_list = ",\n    ".join(name for name, _ in named)
    types_list = ",\n    ".join(repr(it.type) for _, it in named)
    restricted = sorted(it.type for _, it in named if getattr(it, "restricted", False))
    deprecated = sorted(
        it.type for _, it in named if getattr(it, "deprecation_reason", None)
    )

    out = []
    out.append("ALL = [\n    " + all_list + ",\n]\n\n")
    out.append("TYPES = [\n    " + types_list + ",\n]\n\n")
    out.append(
        "RESTRICTED = {\n    "
        + (",\n    ".join(repr(t) for t in restricted) + ",\n" if restricted else "")
        + "}\n\n"
    )
    out.append(
        "DEPRECATED = {\n    "
        + (",\n    ".join(repr(t) for t in deprecated) + ",\n" if deprecated else "")
        + "}\n"
    )
    return "".join(out)


def _emit_bundles(named: List[Tuple[str, Any]]) -> str:
    """Produce the ``BUNDLES`` map: ``type -> {group, artifact, version}``.

    This is the authoritative ``type -> NAR`` mapping for the instance this
    catalog was generated against; the formats consult it so each type emits the
    bundle it actually lives in (not every processor is in nifi-standard-nar).
    """
    lines = []
    for _, item in named:
        bundle = item.bundle
        lines.append(
            f"    {item.type!r}: "
            f"{{'group': {bundle.group!r}, 'artifact': {bundle.artifact!r}, "
            f"'version': {bundle.version!r}}},"
        )
    return "BUNDLES = {\n" + "\n".join(lines) + ("\n" if lines else "") + "}\n"


def _emit_relationships(rules: dict) -> str:
    """Produce the ``RELATIONSHIPS`` map: ``type -> [relationship names]``.

    The harvested rulebook the validator uses to check that every relationship
    is either connected or auto-terminated — completely, not heuristically.
    """
    lines = [
        f"    {type_str!r}: {sorted(rule.get('relationships', []))!r},"
        for type_str, rule in sorted(rules.items())
    ]
    return "RELATIONSHIPS = {\n" + "\n".join(lines) + ("\n" if lines else "") + "}\n"


def _emit_primary_node_only(rules: dict) -> str:
    """Produce ``PRIMARY_NODE_ONLY``: the types NiFi pins to the primary node.

    ``@PrimaryNodeOnly`` processors (ListFTP, ListSFTP, ...) come up with
    ``executionNode=PRIMARY`` however they were created, and the server will
    not accept ``ALL``; the differ reads this set so the model's ``ALL``
    default stops planning a change that can never apply (see
    :func:`niflow.plan._normalise_field`).
    """
    types = sorted(t for t, rule in rules.items() if rule.get("primary_node_only"))
    lines = [f"    {type_str!r}," for type_str in types]
    return "PRIMARY_NODE_ONLY = frozenset({\n" + "\n".join(lines) + ("\n" if lines else "") + "})\n"


def _emit_type_set(rules: dict, table: str = "TYPES") -> str:
    """Produce the set of every type the harvest actually instantiated.

    Without this, "harvested and has no properties at all" is indistinguishable
    from "never harvested": :func:`_emit_property_names` and
    :func:`_emit_descriptors` both skip a type with an empty table, so ten types
    that exist on 1.24 (``ForkEnrichment``, ``ExtractEmailAttachments``, the
    lookup services, ...) read as holes in the compat data and every
    cross-version lookup for them degraded to "unknown — don't translate".
    """
    lines = [f"    {type_str!r}," for type_str in sorted(rules)]
    return f"{table} = frozenset({{\n" + "\n".join(lines) + ("\n" if lines else "") + "})\n"


def _emit_conditional_relationships(rules: dict) -> str:
    """Produce ``CONDITIONAL_RELATIONSHIPS``: relationships a property switches on.

    A processor's relationship set is not a per-type constant. ``UpdateAttribute``
    grows a ``set state fail`` relationship the moment ``Store State`` is set to
    "Store state locally", and NiFi then refuses to start the processor until it
    is handled — a flow that ``validate`` called clean. Keyed
    ``{type: {property: {value: (relationships, ...)}}}``, each entry the FULL
    relationship set while that property holds that value; only values whose set
    differs from the type's default set are recorded.
    """
    lines = []
    for type_str, rule in sorted(rules.items()):
        conditional = rule.get("conditional_relationships") or {}
        if not conditional:
            continue
        inner = ", ".join(
            f"{prop!r}: {{"
            + ", ".join(
                f"{value!r}: {tuple(sorted(rels))!r}"
                for value, rels in sorted(by_value.items())
            )
            + "}"
            for prop, by_value in sorted(conditional.items())
        )
        lines.append(f"    {type_str!r}: {{{inner}}},")
    return ("CONDITIONAL_RELATIONSHIPS = {\n" + "\n".join(lines)
            + ("\n" if lines else "") + "}\n")


def _emit_dynamic_relationships(rules: dict) -> str:
    """Produce ``DYNAMIC_RELATIONSHIPS``: types whose dynamic properties are relationships.

    Harvested by setting one probe dynamic property and reading the relationship
    list back, which works on both NiFi lines — the 2.x-only
    ``/flow/processor-definition`` endpoint (404 on 1.24) is not needed, and this
    replaces guessing from a curated list.
    """
    types = sorted(t for t, rule in rules.items() if rule.get("dynamic_relationships"))
    lines = [f"    {type_str!r}," for type_str in types]
    return ("DYNAMIC_RELATIONSHIPS = frozenset({\n" + "\n".join(lines)
            + ("\n" if lines else "") + "})\n")


def _emit_descriptors(rules: dict, table: str = "DESCRIPTORS") -> str:
    """Produce the ``DESCRIPTORS`` map: ``type -> {prop -> {required, allowable, ...}}``.

    The harvested property rulebook the validator uses to flag missing required
    properties and out-of-range (non-allowable) values.
    """
    lines = []
    for type_str, rule in sorted(rules.items()):
        descriptors = rule.get("descriptors") or {}
        if descriptors:
            lines.append(f"    {type_str!r}: {descriptors!r},")
    return f"{table} = {{\n" + "\n".join(lines) + ("\n" if lines else "") + "}\n"


def _emit_property_names(rules: dict, table: str = "PROPERTY_NAMES") -> str:
    """Produce the ``PROPERTY_NAMES`` map: ``type -> (canonical prop names)``.

    Unlike ``DESCRIPTORS`` this is exhaustive — the differ and emitter use it
    to tell real properties from dynamic ones when canonicalizing keys.
    """
    lines = []
    for type_str, rule in sorted(rules.items()):
        names = rule.get("properties") or []
        if names:
            lines.append(f"    {type_str!r}: {tuple(names)!r},")
    return f"{table} = {{\n" + "\n".join(lines) + ("\n" if lines else "") + "}\n"


def _render(
    kind: str,
    curated_mod: str,
    helper: str,
    component_mod: str,
    component_cls: str,
    named: List[Tuple[str, Any]],
    emit_factory,
    extra: str = "",
    meta: str = "",
) -> str:
    header = _HEADER.format(
        kind=kind, curated=curated_mod, helper=helper,
        component_mod=component_mod, component_cls=component_cls,
    )
    body = "".join(emit_factory(name, item.type) for name, item in named)
    rendered = header + meta + "\n" + body + _emit_registries(named) + "\n" + _emit_bundles(named)
    return rendered + ("\n" + extra if extra else "")


def _emit_meta(version: str) -> str:
    """The provenance stamp: which NiFi wrote this catalog, and when.

    The rulebook silently went stale once (weeks of heuristic-only
    validation); `niflow doctor` now compares this against the live server.
    """
    from datetime import datetime, timezone

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return (
        "\nCATALOG_META = {\n"
        f"    'nifi_version': {version!r},\n"
        f"    'generated': {generated!r},\n"
        "}\n"
    )


# --- public entrypoints -----------------------------------------------------

def _to_documented_type(dto: dict) -> SimpleNamespace:
    """Adapt a REST ``DocumentedTypeDTO`` dict to the attribute shape we render."""
    bundle = dto.get("bundle") or {}
    return SimpleNamespace(
        type=dto.get("type", ""),
        bundle=SimpleNamespace(
            group=bundle.get("group", ""),
            artifact=bundle.get("artifact", ""),
            version=bundle.get("version", ""),
        ),
        restricted=bool(dto.get("restricted", False)),
        deprecation_reason=dto.get("deprecationReason"),
    )


def _trim_descriptors(descriptors: Optional[dict]) -> dict:
    """Keep only the descriptor facts the validator and differ can act on.

    A property is worth recording if it's required, constrains values (an enum),
    is conditionally required (dependencies), references a controller service,
    has a server-populated default, or has a display name that differs from its
    canonical name (so property keys can be canonicalized). Plain optional
    free-text properties carry none of those signals, so we drop them to keep
    the emitted catalog small.
    """
    out: dict = {}
    for name, d in (descriptors or {}).items():
        entry: dict = {}
        if d.get("required"):
            entry["required"] = True
        display = d.get("displayName")
        if display and display != name:
            entry["display"] = display
        default = d.get("defaultValue")
        if default not in (None, ""):
            entry["default"] = default
        service = d.get("identifiesControllerService")
        if service:
            entry["service"] = service
        allowable = [
            a["allowableValue"]["value"]
            for a in (d.get("allowableValues") or [])
            if a.get("allowableValue")
        ]
        # A controller-service reference's "allowable values" are the service
        # INSTANCES that happened to exist in the harvest group — live UUIDs,
        # not an enum. Recording them made every regeneration churn (a fresh id
        # per run) and gave the validator a stale id list to reject real values
        # against. The `service` key above is the fact worth keeping.
        if allowable and not service:
            entry["allowable"] = allowable
        # NiFi hands dependencies (and their values) back out of a Set, so the
        # order varies run to run. Sort both — the list is ANDed, so order
        # carries no meaning — otherwise every regeneration churns the diff.
        # Allowable values are NOT sorted: that order is the UI's.
        dependencies = sorted(
            ({"property": dep.get("propertyName"),
              "values": sorted(dep.get("dependentValues") or [])}
             for dep in (d.get("dependencies") or [])),
            key=lambda dep: dep["property"] or "",
        )
        if dependencies:
            entry["dependencies"] = dependencies
        if entry:
            out[name] = entry
    return out


def _probe_relationships(
    client: Any, created: dict, base: List[str], descriptors: dict
) -> Tuple[bool, dict]:
    """Find the relationships a *fresh* instance does not show.

    ``ProcessorDTO.relationships`` on the create response is the set for the
    default property values only, and that is not the whole truth:

    * a property value can *add* relationships — ``UpdateAttribute`` with
      ``Store State`` = "Store state locally" grows ``set state fail``, and NiFi
      then refuses to start the processor until it is handled;
    * a *dynamic* property can be a relationship in its own right
      (``RouteOnAttribute``), which NiFi 1.x exposes nowhere — its 2.x
      ``/flow/processor-definition`` endpoint answers 404 on 1.24.

    Both are recovered the same cheap way: PUT the property, read the
    relationship list off the update response. Returns
    ``(dynamic_relationships, {property: {value: [relationships]}})`` where the
    conditional map holds only values whose set differs from *base*.

    Best-effort throughout: a PUT NiFi refuses (an allowable value that needs a
    sibling property set first, say) just means that combination is not probed.
    """
    component = created.get("component") or {}
    processor_id = component.get("id")
    revision = created.get("revision")
    if not processor_id or not revision:
        return False, {}
    state = {"revision": revision}

    def put(properties: dict) -> Optional[List[str]]:
        """Set *properties*, return the resulting relationship names or None."""
        try:
            resp = client._request(
                "PUT", f"/processors/{processor_id}",
                json={"revision": state["revision"],
                      "component": {"id": processor_id,
                                    "config": {"properties": properties}}},
            ).json()
        except Exception:
            # The revision may or may not have moved; re-read it so the next
            # probe isn't doomed by a stale one.
            try:
                entity = client._get_json(f"/processors/{processor_id}")
                state["revision"] = entity["revision"]
            except Exception:
                return None
            return None
        state["revision"] = resp.get("revision") or state["revision"]
        return [r["name"] for r in (resp.get("component") or {}).get("relationships", [])]

    base_set = sorted(base)

    # 1. Dynamic properties as relationships, judged at the type's defaults.
    probed = put({_DYNAMIC_PROBE_PROPERTY: "niflow"})
    dynamic = bool(probed is not None and _DYNAMIC_PROBE_PROPERTY in probed)
    if probed is not None:
        put({_DYNAMIC_PROBE_PROPERTY: None})  # back to the default set

    # 2. Enum property values that change the set. Only properties with a small
    #    allowable list are worth probing (see _MAX_CONDITIONAL_PROBE_VALUES);
    #    a controller-service reference never gates relationships.
    conditional: dict = {}
    for name, descriptor in sorted(descriptors.items()):
        if descriptor.get("identifiesControllerService"):
            continue
        allowable = [
            a["allowableValue"]["value"]
            for a in (descriptor.get("allowableValues") or [])
            if a.get("allowableValue")
        ]
        default = descriptor.get("defaultValue")
        values = [v for v in allowable if v != default]
        if not values or len(allowable) > _MAX_CONDITIONAL_PROBE_VALUES:
            continue
        found: dict = {}
        for value in values:
            relationships = put({name: value})
            if relationships is not None and sorted(relationships) != base_set:
                found[value] = sorted(relationships)
        if found:
            conditional[name] = found
        # Restore the default so the next property is probed in isolation —
        # otherwise a leftover mode from one property would be attributed to
        # the next one.
        put({name: None})
    return dynamic, conditional


def _harvest_rules(client: Any, proc_types: List[Any]) -> dict:
    """Instantiate one of each processor type to read its relationships.

    NiFi has no "describe type X" endpoint — relationships (and property
    descriptors) only appear once a processor exists. So we spin up a throwaway
    group, create one processor per type (the create response already carries the
    component's relationships), and delete the group. Best-effort: a type that
    can't be instantiated (restricted, needs setup) is simply skipped, and the
    validator falls back to its heuristic for it.

    Returns ``{type: {"relationships": [...]}}``.
    """
    rules: dict = {}
    root = client.root_id()
    group = client._request(
        "POST", f"/process-groups/{root}/process-groups",
        json={"revision": {"version": 0, "clientId": _CLIENT_ID},
              "component": {"name": _HARVEST_GROUP, "position": {"x": 0.0, "y": 0.0}}},
    ).json()
    group_id = group["id"]
    logger.info("Harvesting rules from %d types into temp group %s", len(proc_types), group_id)
    try:
        for i, dt in enumerate(proc_types):
            try:
                resp = client._request(
                    "POST", f"/process-groups/{group_id}/processors",
                    json={"revision": {"version": 0, "clientId": _CLIENT_ID},
                          "component": {
                              "type": dt.type,
                              "bundle": {"group": dt.bundle.group,
                                         "artifact": dt.bundle.artifact,
                                         "version": dt.bundle.version},
                              "position": {"x": 0.0, "y": float(i)}}},
                ).json()
                component = resp.get("component", {})
                descriptors = (component.get("config") or {}).get("descriptors") or {}
                base_relationships = [
                    r["name"] for r in component.get("relationships", [])]
                dynamic_rel, conditional_rel = _probe_relationships(
                    client, resp, base_relationships, descriptors)
                rules[dt.type] = {
                    "relationships": base_relationships,
                    # A relationship set is not a per-type constant: some are
                    # switched on by a property value, and some types turn every
                    # dynamic property into a relationship. Both are harvested
                    # (see :func:`_probe_relationships`) because both make NiFi
                    # refuse to start a processor `validate` called clean.
                    "conditional_relationships": conditional_rel,
                    "dynamic_relationships": dynamic_rel,
                    # @PrimaryNodeOnly: NiFi forces executionNode=PRIMARY on
                    # these and refuses ALL, so the differ needs to know which
                    # types they are (ProcessorDTO reports it on both 1.x and
                    # 2.x — unlike the 2.x-only processor-definition endpoint).
                    "primary_node_only": bool(component.get("executionNodeRestricted")),
                    "descriptors": _trim_descriptors(descriptors),
                    # Full canonical key list — the differ/emitter needs to know
                    # whether a key is a real property or a dynamic one, even
                    # for properties the trimmed descriptors drop.
                    "properties": sorted(descriptors),
                    # Untrimmed facts, used only by the cross-version rulebook
                    # dump (harvest_rulebook); never emitted into a catalog.
                    "raw": _full_descriptors(descriptors),
                }
            except Exception as exc:  # restricted / un-instantiable type — skip it
                logger.warning("Skipped %s while harvesting: %s", dt.type, exc)
    finally:
        try:
            version = client._pg_entity(group_id)["revision"]["version"]
            client._request(
                "DELETE", f"/process-groups/{group_id}",
                params={"version": version, "clientId": _CLIENT_ID,
                        "disconnectedNodeAcknowledged": "false"},
            )
            logger.info("Removed temp harvest group %s", group_id)
        except Exception as exc:  # don't leave it behind silently
            logger.error("Could not delete temp harvest group %s: %s", group_id, exc)
    return rules


def _full_descriptors(descriptors: Optional[dict]) -> dict:
    """Every descriptor fact the *cross-version* diff needs, untrimmed.

    :func:`_trim_descriptors` throws away display names that match the
    canonical name, descriptions, and sensitivity — all of which the version
    map needs to pair a 2.x property with its 1.x counterpart when the key was
    renamed. This shape is only ever written to a scratch rulebook dump (see
    :func:`harvest_rulebook`), never to a committed catalog.
    """
    out: dict = {}
    for name, d in (descriptors or {}).items():
        out[name] = {
            "display": d.get("displayName") or name,
            "description": (d.get("description") or "").strip(),
            "required": bool(d.get("required")),
            "default": d.get("defaultValue"),
            "sensitive": bool(d.get("sensitive")),
            "dynamic": bool(d.get("dynamic")),
            "service": d.get("identifiesControllerService"),
            "allowable": [
                a["allowableValue"]["value"]
                for a in (d.get("allowableValues") or [])
                if a.get("allowableValue")
            ],
        }
    return out


def _harvest_service_rules(client: Any, svc_types: List[Any]) -> dict:
    """Instantiate one of each controller-service type to read its descriptors.

    The processor harvest's twin. Controller services have the same
    "descriptors only exist on an instance" problem, and the same cross-version
    property renames (a ``JsonTreeReader``'s ``schema-access-strategy`` is
    ``Schema Access Strategy`` on 2.x) — but until now nothing harvested them,
    so every service silently skipped the compat join. Note the descriptors sit
    at ``component.descriptors``, not under ``component.config`` as they do for
    processors.

    Returns ``{type: {"descriptors": {...}, "properties": [...], "raw": {...}}}``.
    """
    rules: dict = {}
    root = client.root_id()
    group = client._request(
        "POST", f"/process-groups/{root}/process-groups",
        json={"revision": {"version": 0, "clientId": _CLIENT_ID},
              "component": {"name": _HARVEST_GROUP + "_svc",
                            "position": {"x": 0.0, "y": 0.0}}},
    ).json()
    group_id = group["id"]
    logger.info(
        "Harvesting rules from %d controller-service types into temp group %s",
        len(svc_types), group_id,
    )
    try:
        for dt in svc_types:
            try:
                resp = client._request(
                    "POST", f"/process-groups/{group_id}/controller-services",
                    json={"revision": {"version": 0, "clientId": _CLIENT_ID},
                          "component": {
                              "type": dt.type,
                              "bundle": {"group": dt.bundle.group,
                                         "artifact": dt.bundle.artifact,
                                         "version": dt.bundle.version}}},
                ).json()
                component = resp.get("component", {})
                descriptors = component.get("descriptors") or {}
                rules[dt.type] = {
                    "descriptors": _trim_descriptors(descriptors),
                    "properties": sorted(descriptors),
                    "raw": _full_descriptors(descriptors),
                }
            except Exception as exc:  # restricted / un-instantiable type — skip it
                logger.warning("Skipped service %s while harvesting: %s", dt.type, exc)
    finally:
        try:
            version = client._pg_entity(group_id)["revision"]["version"]
            client._request(
                "DELETE", f"/process-groups/{group_id}",
                params={"version": version, "clientId": _CLIENT_ID,
                        "disconnectedNodeAcknowledged": "false"},
            )
            logger.info("Removed temp service-harvest group %s", group_id)
        except Exception as exc:
            logger.error("Could not delete temp harvest group %s: %s", group_id, exc)
    return rules


def _fetch(client: Any) -> Tuple[List[Any], List[Any]]:
    """Return ``(processor_types, controller_service_types)`` from the live NiFi.

    Uses the version-agnostic REST endpoints, so this works on 1.x and 2.x —
    including instances with custom NARs installed.
    """
    procs = client._get_json("/flow/processor-types").get("processorTypes", [])
    svcs = client._get_json("/flow/controller-service-types").get(
        "controllerServiceTypes", []
    )
    return (
        [_to_documented_type(d) for d in procs],
        [_to_documented_type(d) for d in svcs],
    )


def harvest_rulebook(config: Optional[NiFiConfig] = None) -> dict:
    """Harvest the COMPLETE property rulebook — processors *and* services.

    Unlike :func:`generate` this writes nothing; it returns a JSON-serialisable
    dump that :mod:`niflow.versionmap` diffs against a dump from another NiFi
    line to build the cross-version property map. Keeping it out-of-band means
    a cross-version harvest never has to overwrite a committed catalog.
    """
    from niflow.client import NiFiClient

    client = NiFiClient(config)
    nifi_version = client.version()
    logger.info("Harvesting full rulebook from NiFi %s", nifi_version)
    procs, svcs = _fetch(client)
    proc_named = _unique_factory_names(procs)
    svc_named = _unique_factory_names(svcs)
    proc_rules = _harvest_rules(client, [item for _, item in proc_named])
    svc_rules = _harvest_service_rules(client, [item for _, item in svc_named])
    from datetime import datetime, timezone

    return {
        "nifi_version": nifi_version,
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "processors": proc_rules,
        "services": svc_rules,
    }


def generate(
    config: Optional[NiFiConfig] = None, *, harvest: bool = True
) -> Tuple[int, int]:
    """Connect to NiFi, regenerate both catalog files, return ``(n_procs, n_svcs)``.

    When ``harvest`` is true (the default) each processor type is briefly
    instantiated to capture its relationships into the ``RELATIONSHIPS`` map the
    validator uses. This creates and deletes a throwaway group, so it needs
    write access; pass ``harvest=False`` for a read-only, relationship-less
    catalog.
    """
    from niflow.client import NiFiClient

    client = NiFiClient(config)
    nifi_version = client.version()
    meta = _emit_meta(nifi_version)
    logger.info("Generating catalogs from NiFi %s", nifi_version)
    procs, svcs = _fetch(client)
    logger.info("Catalog: %d processors, %d controller services on this NiFi", len(procs), len(svcs))

    proc_named = _unique_factory_names(procs)
    svc_named = _unique_factory_names(svcs)

    rules = _harvest_rules(client, [item for _, item in proc_named]) if harvest else {}
    # Controller services carry property descriptors — and the same cross-version
    # renames — as processors do; harvesting them makes the compat join work for
    # services instead of silently skipping every one of them.
    svc_rules = (
        _harvest_service_rules(client, [item for _, item in svc_named])
        if harvest else {}
    )

    PROCESSORS_CATALOG_PATH.write_text(
        _render(
            kind="processor",
            curated_mod="niflow.processors.standard",
            helper="_processor",
            component_mod="niflow.core",
            component_cls="Processor",
            named=proc_named,
            emit_factory=_emit_processor_factory,
            extra=_emit_relationships(rules)
            + "\n" + _emit_conditional_relationships(rules)
            + "\n" + _emit_dynamic_relationships(rules)
            + "\n" + _emit_primary_node_only(rules)
            + "\n" + _emit_descriptors(rules)
            + "\n" + _emit_property_names(rules),
            meta=meta,
        )
    )
    SERVICES_CATALOG_PATH.write_text(
        _render(
            kind="controller-service",
            curated_mod="niflow.services.standard",
            helper="_service",
            component_mod="niflow.core",
            component_cls="ControllerService",
            named=svc_named,
            emit_factory=_emit_service_factory,
            extra=_emit_descriptors(svc_rules) + "\n" + _emit_property_names(svc_rules),
            meta=meta,
        )
    )
    return len(proc_named), len(svc_named)


_COMPAT_HEADER = '''"""AUTO-GENERATED by ``python -m niflow.codegen --compat`` — do not edit by hand.

Property *and relationship* rulebook harvested from a NiFi *1.x* instance. The
main catalog is the authoring namespace (currently 2.x);
:mod:`niflow.processors.rules` joins these tables against it to translate
property keys when talking to a 1.x server, to normalise 1.x keys pulled from
one, and to judge a flow's relationships against the line it will actually run
on. ``TYPES``/``SERVICE_TYPES`` list every type the harvest instantiated, so a
type with no properties at all is still known rather than looking like a hole.
Regenerate against the oldest 1.x line you push to::

    NIFLOW_NIFI_HOST=https://host:8444/nifi-api python -m niflow.codegen --compat
"""
from __future__ import annotations

'''


def generate_compat(config: Optional[NiFiConfig] = None) -> int:
    """Harvest a 1.x NiFi's property rulebook into ``compat_v1.py``.

    Returns the number of processor types harvested. Refuses to run against a
    2.x server — that would record the authoring namespace as the "old" one
    and silently disable every translation.
    """
    from niflow.client import NiFiClient

    client = NiFiClient(config)
    nifi_version = client.version()
    major = client._major_version()
    if major != 1:
        raise SystemExit(
            f"--compat harvests the 1.x property namespace, but {client.config.host} "
            f"is NiFi {nifi_version}; point NIFLOW_NIFI_HOST at a 1.x instance"
        )
    logger.info("Generating 1.x compatibility table from NiFi %s", nifi_version)
    procs, svcs = _fetch(client)
    named = _unique_factory_names(procs)
    svc_named = _unique_factory_names(svcs)
    rules = _harvest_rules(client, [item for _, item in named])
    svc_rules = _harvest_service_rules(client, [item for _, item in svc_named])
    COMPAT_V1_PATH.write_text(
        _COMPAT_HEADER
        + _emit_meta(nifi_version).replace("CATALOG_META", "COMPAT_META").lstrip("\n")
        + "\n" + _emit_type_set(rules, "TYPES")
        + "\n" + _emit_type_set(svc_rules, "SERVICE_TYPES")
        + "\n" + _emit_relationships(rules)
        + "\n" + _emit_conditional_relationships(rules)
        + "\n" + _emit_dynamic_relationships(rules)
        + "\n" + _emit_primary_node_only(rules)
        + "\n" + _emit_descriptors(rules)
        + "\n" + _emit_property_names(rules)
        + "\n" + _emit_descriptors(svc_rules, "SERVICE_DESCRIPTORS")
        + "\n" + _emit_property_names(svc_rules, "SERVICE_PROPERTY_NAMES")
    )
    return len(rules) + len(svc_rules)


def main() -> None:
    import sys

    args = sys.argv[1:]
    if "--dump-rulebook" in args:
        import json

        out = Path(args[args.index("--dump-rulebook") + 1])
        book = harvest_rulebook(NiFiConfig.from_env())
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(book, indent=1, sort_keys=True))
        print(
            f"Wrote {out}: NiFi {book['nifi_version']}, "
            f"{len(book['processors'])} processors, {len(book['services'])} services"
        )
        return
    if "--compat" in args:
        n_types = generate_compat(NiFiConfig.from_env())
        print(f"Wrote {COMPAT_V1_PATH.relative_to(_NIFLOW_ROOT.parent)}: {n_types} types")
        return
    n_procs, n_svcs = generate(NiFiConfig.from_env())
    print(f"Wrote {PROCESSORS_CATALOG_PATH.relative_to(_NIFLOW_ROOT.parent)}: {n_procs} factories")
    print(f"Wrote {SERVICES_CATALOG_PATH.relative_to(_NIFLOW_ROOT.parent)}: {n_svcs} factories")


if __name__ == "__main__":
    main()
