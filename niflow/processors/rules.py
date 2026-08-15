"""Access to the harvested processor *rulebook* (relationships, ...).

NiFi's per-type rules — the relationship names, required properties, allowable
values — aren't available from any "describe type" endpoint; they only exist
once a processor is instantiated. ``make catalog`` harvests them (see
:func:`niflow.codegen._harvest_rules`) into the generated catalog, and this
module reads them back so the validator can check a flow with pure logic, no
push required.

Everything degrades gracefully: a catalog generated before harvesting (or
without write access) simply has no ``RELATIONSHIPS`` map, and lookups return
``None`` so callers fall back to their heuristic.
"""
from __future__ import annotations

from typing import Dict, List, Optional


def _catalog_table(name: str):
    try:
        from niflow.processors import catalog
    except Exception:  # pragma: no cover - catalog import should never fail
        return None
    return getattr(catalog, name, None) or None


def relationships_for(type_str: str) -> Optional[List[str]]:
    """The relationship names for a processor type, or ``None`` if not harvested.

    A return of ``None`` means "unknown — fall back to a heuristic"; an empty
    list means "known to have no relationships".
    """
    table = _catalog_table("RELATIONSHIPS")
    return table.get(type_str) if table else None


def descriptors_for(type_str: str) -> Optional[Dict[str, dict]]:
    """Harvested property descriptors for a type, or ``None`` if not harvested.

    Maps property name -> ``{required, default, allowable, service,
    dependencies, display}`` (only the keys that apply). ``None`` means
    "unknown — don't check properties for this type".
    """
    table = _catalog_table("DESCRIPTORS")
    if table is None:
        return None
    return table.get(type_str)


def property_names_for(type_str: str) -> Optional[List[str]]:
    """All canonical property names for a type, or ``None`` if not harvested."""
    table = _catalog_table("PROPERTY_NAMES")
    if table is None:
        return None
    names = table.get(type_str)
    return list(names) if names is not None else None


# NiFi 2.x renamed many canonical property keys to match their display names
# (its migrateProperties machinery translates old keys on import, but the old
# names are exposed nowhere in the REST API). The renames niflow has actually
# hit are curated here; an entry only applies when the old key is NOT a real
# property of the type and the new key IS — so a 1.x catalog (where the old
# key is still canonical) is never corrupted.
LEGACY_PROPERTY_ALIASES = {
    "record-reader": "Record Reader",
    "record-writer": "Record Writer",
    "generate-ff-custom-text": "Custom Text",
    "character-set": "Character Set",
    "mime-type": "Mime Type",
    # ReplaceText (the guard keeps this off types that have their own
    # canonical "Regular Expression" property)
    "Regular Expression": "Search Value",
}


def canonical_properties(type_str: str, props: Dict[str, object]) -> Dict[str, object]:
    """Rewrite display-name and legacy property keys to canonical names.

    Users naturally write the name the NiFi UI shows ("Custom Text") — or a key
    from the NiFi version they migrated off — while the server keys the
    property map by the canonical name. Left as-is, a push can create a
    *dynamic* property with the intended one unset, and every diff shows
    phantom drift. Rewrites happen only when unambiguous:

    * a display name used by exactly one property maps to it (``CopyS3Object``
      has two properties both displayed as "Bucket" — those are left alone);
    * a curated legacy key maps only when the type really has the new key and
      really lacks the old one;
    * a key never overwrites one already set canonically;
    * unknown keys (true dynamic properties) and un-harvested types pass
      through untouched.
    """
    if not props:
        return props
    descriptors = descriptors_for(type_str) or {}
    names = property_names_for(type_str)
    aliases: Dict[str, str] = {}

    display_counts: Dict[str, int] = {}
    for name, entry in descriptors.items():
        display = entry.get("display")
        if display:
            display_counts[display] = display_counts.get(display, 0) + 1
    for name, entry in descriptors.items():
        display = entry.get("display")
        if display and display_counts[display] == 1:
            aliases[display] = name

    if names is not None:
        known = set(names)
        for old, new in LEGACY_PROPERTY_ALIASES.items():
            if old not in known and new in known:
                aliases.setdefault(old, new)

    if not aliases.keys() & props.keys():
        return props
    out: Dict[str, object] = {}
    for key, value in props.items():
        canonical = aliases.get(key, key)
        if canonical != key and canonical in props:
            canonical = key  # canonical key set explicitly elsewhere — keep both as-is
        out[canonical] = value
    return out
