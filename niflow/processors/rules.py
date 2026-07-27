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
    dependencies}`` (only the keys that apply). ``None`` means "unknown — don't
    check properties for this type".
    """
    table = _catalog_table("DESCRIPTORS")
    if table is None:
        return None
    return table.get(type_str)
