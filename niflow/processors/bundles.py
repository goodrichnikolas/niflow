"""NAR bundle coordinates for processor / controller-service types.

NiFi resolves a component by the *pair* ``(type, bundle)`` — the fully-qualified
type string alone is not enough. Emit the wrong NAR coordinate and NiFi rejects
an otherwise-valid type with *"... is not a valid processor type"*, even though
the type really exists on the instance.

Most processors live in ``nifi-standard-nar``, but several ship in their own NAR
(``UpdateAttribute`` -> ``nifi-update-attribute-nar``, ...), and *which* NAR a
type lives in can change between NiFi versions (e.g. Jolt moved out of
standard-nar in 2.x). So the authoritative source is the live instance:

* ``make catalog`` records each type's real bundle in the generated catalogs
  (the ``BUNDLES`` map), captured from the NiFi it ran against.
* At push time :meth:`NiFiClient.push_flow` overrides every bundle from the
  *target* instance's installed NARs — the last word on artifact *and* version.

This module resolves the best *offline* default for a type, used when a flow is
serialised without a live connection (``niflow convert``, ``niflow diff``,
``to_json``). Order: the generated catalog first, then a small curated set of
version-stable exceptions, then the plain standard-nar default. Keeping the
offline guess close to reality keeps ``niflow diff`` quiet.
"""
from __future__ import annotations

from typing import Dict, Optional

_GROUP = "org.apache.nifi"

# Offline placeholder version. The real NAR version is instance-specific, so
# push() overrides it from the target; NiFi's compatible-bundle resolution also
# tolerates a version mismatch when the group+artifact identify a single NAR.
DEFAULT_VERSION = "2.7.2"

STANDARD_PROCESSOR_BUNDLE: Dict[str, str] = {
    "group": _GROUP,
    "artifact": "nifi-standard-nar",
    "version": DEFAULT_VERSION,
}
STANDARD_SERVICE_BUNDLE: Dict[str, str] = {
    "group": _GROUP,
    "artifact": "nifi-standard-services-api-nar",
    "version": DEFAULT_VERSION,
}

# Curated ``type -> artifact`` for standard-distribution processors that do NOT
# live in nifi-standard-nar, limited to entries stable across NiFi 1.x and 2.x.
# The generated catalog ``BUNDLES`` (when present) takes precedence and is how
# everything else gets covered — regenerate with ``make catalog``.
_CURATED_ARTIFACTS: Dict[str, str] = {
    "org.apache.nifi.processors.attributes.UpdateAttribute": "nifi-update-attribute-nar",
}


def _standard(service: bool) -> Dict[str, str]:
    return STANDARD_SERVICE_BUNDLE if service else STANDARD_PROCESSOR_BUNDLE


def _catalog_bundles(service: bool) -> Dict[str, dict]:
    """The generated ``BUNDLES`` map, or ``{}`` if the catalog predates it."""
    try:
        if service:
            from niflow.services import catalog
        else:
            from niflow.processors import catalog
    except Exception:  # pragma: no cover - catalog import should never fail
        return {}
    return getattr(catalog, "BUNDLES", None) or {}


def default_bundle(type_str: str, *, service: bool = False) -> Dict[str, str]:
    """Best offline NAR coordinates for *type_str* — always a full dict.

    Used both to *emit* a bundle for a component built without one and to decide
    whether an incoming bundle is just the default (so it collapses back to
    ``None`` and round-trips cleanly).
    """
    std = _standard(service)
    recorded = _catalog_bundles(service).get(type_str)
    if recorded:
        return {
            "group": recorded.get("group") or std["group"],
            "artifact": recorded.get("artifact") or std["artifact"],
            "version": recorded.get("version") or DEFAULT_VERSION,
        }
    artifact: Optional[str] = _CURATED_ARTIFACTS.get(type_str)
    if artifact:
        return {"group": _GROUP, "artifact": artifact, "version": DEFAULT_VERSION}
    return dict(std)
