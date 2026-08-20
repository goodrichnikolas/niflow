"""Cross-version compatibility checks, driven by the generated version map.

:mod:`niflow.version_map` is the data — a property-by-property diff of two real
NiFi lines (2.x, the namespace flows are authored in, vs 1.x, what production
runs). This module is the part that *acts* on it: given a component type, the
properties a flow sets on it, and the NiFi major version being targeted, it
says what will not survive the crossing.

The failure this exists to prevent is a silent one. Push a flow that sets
``Pretty Print`` at a 1.24 server and nothing errors: NiFi has no such property
on ``AttributesToJSON``, so it files the value away as an inert *dynamic*
property and the processor runs with the default. You find out when the output
is wrong, in production, at work. Everything here exists to move that discovery
to `niflow validate` on your laptop.

Degrades gracefully throughout: with no generated map (or one built against a
different pair of lines), every lookup returns "unknown" and callers carry on
exactly as they did before — quieter, but never wrong.
"""
from __future__ import annotations

from typing import Dict, List, Optional

from niflow.processors.rules import (
    canonical_properties,
    descriptors_for,
    property_names_for,
    unsupported_properties,
)


def map_meta() -> Optional[dict]:
    """``{new_version, old_version, generated}`` for the generated map, or ``None``."""
    try:
        from niflow.version_map import VERSION_MAP_META
    except Exception:
        return None
    return dict(VERSION_MAP_META)


def parse_major(version: str) -> Optional[int]:
    """``"1.24"`` / ``"1.24.0"`` / ``"1"`` -> ``1``. ``None`` if unparseable."""
    if version is None:
        return None
    head = str(version).strip().lstrip("vV").split(".", 1)[0]
    try:
        return int(head)
    except ValueError:
        return None


def _map_tables():
    try:
        from niflow import version_map
    except Exception:
        return None
    return version_map


def _direction(target_major: int) -> Optional[str]:
    """Which side of the map the target is: ``"old"``, ``"new"``, or ``None``.

    Determines which bucket means "unsupported here": pushing to the old line,
    ``only_new`` properties cannot land; pushing to the new line, ``only_old``
    ones cannot. ``None`` means the map says nothing about this target and every
    check below should stay silent rather than guess.
    """
    meta = map_meta()
    if not meta:
        return None
    if parse_major(meta.get("old_version", "")) == target_major:
        return "old"
    if parse_major(meta.get("new_version", "")) == target_major:
        return "new"
    return None


def _entry(type_str: str) -> Optional[dict]:
    version_map = _map_tables()
    if version_map is None:
        return None
    for table in ("PROCESSOR_DIFF", "SERVICE_DIFF"):
        diff = getattr(version_map, table, None) or {}
        if type_str in diff:
            return diff[type_str]
    for table in ("PROCESSOR_TYPES_BOTH", "SERVICE_TYPES_BOTH"):
        if type_str in (getattr(version_map, table, None) or frozenset()):
            return {}
    return None


def type_missing_on(type_str: str, target_major: int) -> Optional[str]:
    """A message if the type does not exist on the target line, else ``None``.

    Only reports types the map positively knows are one-sided; a type it never
    saw (custom NAR, un-instantiable) is left alone.
    """
    version_map = _map_tables()
    direction = _direction(target_major)
    if version_map is None or direction is None:
        return None
    meta = map_meta() or {}
    absent_side = "new" if direction == "old" else "old"
    other_version = meta.get(f"{'new' if absent_side == 'new' else 'old'}_version", "?")
    target_version = meta.get(f"{direction}_version", "?")
    for prefix in ("PROCESSOR", "SERVICE"):
        only = getattr(version_map, f"{prefix}_TYPES_ONLY_{absent_side.upper()}", None)
        if only and type_str in only:
            return (f"type does not exist on NiFi {target_version} — it is only "
                    f"available on NiFi {other_version}; the push will fail")
    return None


def unsupported_property_names(
    type_str: str, props: Dict[str, object], target_major: int
) -> List[str]:
    """Property keys set on this component that the target line does not have.

    In the old-line direction this delegates to
    :func:`niflow.processors.rules.unsupported_properties`, which is the same
    computation the push path uses to decide what to drop — so validate and push
    can never disagree.
    """
    direction = _direction(target_major)
    if direction is None or not props:
        return []
    if direction == "old":
        return unsupported_properties(type_str, props, target_major)
    entry = _entry(type_str)
    if entry is None:
        return []
    # Targeting the newer line: the flow was authored against the old namespace,
    # so its keys are already old ones and `only_old` is what cannot land.
    return sorted(set(props) & set(entry.get("only_old") or ()))


def _is_expression(value: object) -> bool:
    return isinstance(value, str) and ("${" in value or "#{" in value)


def component_issues(
    type_str: str, props: Optional[Dict[str, object]], target_major: int
) -> List[str]:
    """Every cross-version problem with one component, as plain messages.

    Covers, in the order a reader cares about them:

    1. the type not existing on the target line at all;
    2. properties whose key does not exist there (the silent-dynamic-property
       trap);
    3. values outside the target's allowable set, even though the key survives;
    4. properties the target line makes mandatory that this flow leaves unset.

    Expression Language and parameter references are never value-checked — they
    resolve at runtime and cannot be judged here.
    """
    meta = map_meta() or {}
    direction = _direction(target_major)
    if direction is None:
        return []
    target_version = meta.get(f"{direction}_version", f"{target_major}.x")
    other_version = meta.get(
        f"{'new' if direction == 'old' else 'old'}_version", "the other line"
    )
    issues: List[str] = []

    missing = type_missing_on(type_str, target_major)
    if missing:
        return [missing]

    props = props or {}
    canonical = canonical_properties(type_str, props) if direction == "old" else dict(props)

    for key in unsupported_property_names(type_str, props, target_major):
        issues.append(
            f"property '{key}' does not exist on NiFi {target_version} "
            f"(it is a NiFi {other_version} property) — NiFi would store it as "
            f"an inert dynamic property and run with the real default"
        )

    entry = _entry(type_str)
    if entry is None:
        return issues

    known = set(property_names_for(type_str) or ())
    renamed = entry.get("renamed") or {}
    for key, change in (entry.get("allowable_changed") or {}).items():
        value = canonical.get(key)
        if not isinstance(value, str) or not value or _is_expression(value):
            continue
        gone = change["only_new"] if direction == "old" else change["only_old"]
        if value in gone:
            # Deliberately no "did you mean" list: `change` holds the *difference*
            # between the two allowable sets, not the target's full set, so
            # naming those values would suggest the wrong alternatives.
            issues.append(
                f"property '{key}' = {value!r} is not an allowed value on NiFi "
                f"{target_version} — that value exists only on NiFi {other_version}"
            )

    descriptors = descriptors_for(type_str) or {}
    defaults_changed = entry.get("default_changed") or {}
    for key, change in (entry.get("required_changed") or {}).items():
        required_there = change["old"] if direction == "old" else change["new"]
        if not required_there:
            continue
        # A property with a default is never "unset" in NiFi's eyes — and it is
        # the *target's* default that decides, not the catalog's. Where the two
        # lines disagree the map records both, so prefer that; the catalog
        # descriptor is only the fallback for a default both lines share.
        if key in defaults_changed:
            target_default = defaults_changed[key]["old" if direction == "old" else "new"]
        else:
            target_default = (descriptors.get(key) or {}).get("default")
        if target_default is not None:
            continue
        if canonical.get(key) in (None, "") and (key in known or key in renamed):
            issues.append(
                f"property '{key}' is required on NiFi {target_version} "
                f"(optional on NiFi {other_version}) but is not set"
            )
    return issues


def flow_issues(flow, target_major: int) -> List[dict]:
    """Walk a flow and return ``{"component", "message"}`` cross-version issues.

    Visits controller services as well as processors — services carry exactly
    the same renames and the same silent-dynamic-property failure, and until the
    services rulebook was harvested nothing checked them at all.
    """
    if _direction(target_major) is None:
        return []
    issues: List[dict] = []

    def visit(group, prefix: str) -> None:
        path = f"{prefix}/{group.name}" if prefix else group.name
        for service in getattr(group, "controller_services", ()) or ():
            for message in component_issues(
                service.type, _plain(service.properties), target_major
            ):
                issues.append({"component": f"{path}/{service.name}", "message": message})
        for proc in group.processors:
            for message in component_issues(
                proc.type, _plain(proc.properties), target_major
            ):
                issues.append({"component": f"{path}/{proc.name}", "message": message})
        for child in group.process_groups:
            visit(child, path)

    visit(flow, "")
    return issues


def _plain(props) -> Dict[str, object]:
    """Property map with controller-service references reduced to a marker.

    A property whose value is a :class:`~niflow.core.ControllerService` is a
    *reference*; the key still matters for compatibility, the object does not.
    """
    from niflow.core import ControllerService

    return {
        key: ("<service>" if isinstance(value, ControllerService) else value)
        for key, value in (props or {}).items()
    }


def describe_target(target_major: int) -> str:
    """One line naming what a target-version check is actually comparing against."""
    meta = map_meta()
    direction = _direction(target_major)
    if not meta or direction is None:
        return (
            f"no cross-version map for NiFi {target_major}.x — "
            f"regenerate with `make version-map` against the pair you use"
        )
    return (
        f"NiFi {meta[f'{direction}_version']} "
        f"(map generated {meta.get('generated', '?')} from "
        f"{meta['new_version']} vs {meta['old_version']})"
    )


# --- the declared compatibility baseline ------------------------------------
#
# Policy, in the user's words: "we always want to do 2.x, but 1.24 is what's
# actually work related so that should always work." 2.x is the aspiration;
# the older line is the hard requirement. Everything below encodes that so it
# is enforced rather than remembered — the baseline is declared once in
# ``.niflow.env`` (``NIFLOW_MIN_NIFI_VERSION``, default 1.24) and read from
# there by validate, push and doctor alike.

# Resolved baselines, keyed by the environment that produced them. The
# baseline is read on *every* validate — including once per case in a
# thousand-case fuzz sweep — and re-reading .niflow.env each time would both
# cost a file stat and log "Loaded connection config from …" a thousand times.
# Keying on the inputs (rather than caching outright) keeps a test that
# monkeypatches the environment honest.
_BASELINE_CACHE: Dict[tuple, Optional[str]] = {}


def baseline_version(override: Optional[str] = None) -> Optional[str]:
    """The declared compatibility baseline, or ``None`` when switched off.

    ``override`` (a CLI ``--target-version``, say) wins over the configuration;
    the value ``"none"`` — from either — disables the check for someone who
    genuinely only cares about 2.x. Never raises: an unreadable config file
    falls back to the built-in default rather than breaking an offline
    ``validate``.
    """
    import os

    from niflow.config import _BASELINE_OFF, DEFAULT_MIN_NIFI_VERSION

    if override is not None:
        value = str(override).strip()
        return None if value.lower() in _BASELINE_OFF else value

    key = (os.getenv("NIFLOW_MIN_NIFI_VERSION"), os.getenv("NIFLOW_CONFIG"),
           os.getcwd())
    if key not in _BASELINE_CACHE:
        try:
            from niflow.config import NiFiConfig

            _BASELINE_CACHE[key] = NiFiConfig.from_env().compat_baseline
        except Exception:
            _BASELINE_CACHE[key] = DEFAULT_MIN_NIFI_VERSION
    return _BASELINE_CACHE[key]


def baseline_major(override: Optional[str] = None) -> Optional[int]:
    """:func:`baseline_version` reduced to a NiFi major, or ``None``."""
    version = baseline_version(override)
    return None if version is None else parse_major(version)


def baseline_covered(override: Optional[str] = None) -> bool:
    """Whether the generated map can actually judge the baseline.

    False means the check would be silently vacuous — no map, or one built
    against a different pair of lines — which callers should say out loud
    rather than reporting a clean bill of health.
    """
    major = baseline_major(override)
    return major is not None and _direction(major) is not None


def baseline_issues(flow, override: Optional[str] = None) -> List[dict]:
    """Cross-version issues for *flow* against the declared baseline.

    Empty when the baseline is switched off or the map cannot judge it, so a
    caller can always call this and let the result speak.
    """
    major = baseline_major(override)
    if major is None or _direction(major) is None:
        return []
    return flow_issues(flow, major)


def describe_baseline(override: Optional[str] = None) -> str:
    """One line stating the baseline and whether it can be checked."""
    version = baseline_version(override)
    if version is None:
        return ("compatibility baseline: none (NIFLOW_MIN_NIFI_VERSION=none) — "
                "flows are only checked against the server you push to")
    major = parse_major(version)
    if major is None:
        return (f"compatibility baseline: {version!r} is not a NiFi version — "
                f"set NIFLOW_MIN_NIFI_VERSION to something like 1.24, or to "
                f"'none' to switch the check off")
    if _direction(major) is None:
        return (f"compatibility baseline: NiFi {version}, but no generated map "
                f"covers it — the check cannot run. Regenerate with "
                f"`make version-map` against the pair you use")
    return f"compatibility baseline: NiFi {version} — {describe_target(major)}"
