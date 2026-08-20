"""Build the cross-version property difference map from two harvested rulebooks.

The problem this exists for: you author flows against NiFi 2.x at home and run
them on 1.24/1.28 at work. Between those lines Apache renamed most property
*keys* to their display names, added properties that simply don't exist on 1.x,
and dropped others. A flow that sets a 2.x-only key against a 1.x server does
not fail loudly — NiFi stores it as an inert *dynamic* property and the real
one keeps its default, so the processor quietly does the wrong thing.

:func:`build_map` diffs two rulebook dumps (``python -m niflow.codegen
--dump-rulebook``, one per server) and classifies every property of every type
that exists on both lines:

``renamed``
    same property, different key — matched by display name, then by
    description, both normalised and only when the pairing is 1:1 in *both*
    directions, plus the hand-confirmed pairs in
    :data:`niflow.processors.rules.CURATED_TYPE_RENAMES` for the renames that
    changed key *and* display name *and* description and so defeat every
    automatic signal. This is the bulk of the difference and it is fully
    translatable, so niflow rewrites these keys on push.
``only_new`` / ``only_old``
    a key with no counterpart on the other line. Either genuinely added/removed
    or a rename too creative to detect — the consequence is identical (setting
    it does nothing on the other line), so both are reported as unsupported.
``allowable_changed`` / ``required_changed`` / ``default_changed``
    the key survives but its contract moved: an enum value that no longer
    exists, a property that became mandatory, a default that shifted.

What this CANNOT see is behavioural drift: a property present on both lines
with the same name, type and allowable values but different *semantics* in the
engine. Descriptor harvesting is blind to that; see docs/version-compat.md.
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

# Types most likely to bite a 1.x/2.x crossing: heavily used, heavily renamed,
# or both. Surfaced first in the generated report so the user sees the ones
# that matter before the long tail.
WORST_OFFENDER_HINTS = (
    "ConvertRecord", "ReplaceText", "InvokeHTTP", "ExecuteSQL",
    "ExecuteSQLRecord", "QueryRecord", "PutDatabaseRecord", "LookupRecord",
    "SplitRecord", "PartitionRecord", "ValidateRecord", "UpdateRecord",
    "MergeRecord", "GenerateFlowFile", "LogAttribute", "PutFile", "GetFile",
    "FetchFile", "ListFile", "PutS3Object", "FetchS3Object", "ListS3",
    "PutSFTP", "GetSFTP", "FetchSFTP", "ListSFTP", "PutKafkaRecord",
    "ConsumeKafkaRecord", "PutElasticsearchRecord", "PutDatabaseRecord",
    "JsonTreeReader", "JsonRecordSetWriter", "CSVReader", "CSVRecordSetWriter",
    "AvroReader", "AvroRecordSetWriter", "DBCPConnectionPool",
    "StandardRestrictedSSLContextService", "StandardSSLContextService",
)


# Pairings that look like renames but are NOT safe to translate, so they stay
# in the only-one-line buckets and are printed in the report for the user to
# confirm (or dismiss) against their own estate. Each is either a 1.x property
# that 2.x *split* in two — translating would have to pick a half — or a pair
# whose meaning moved with the name. Deliberately data, not behaviour: nothing
# reads this table except the report renderer.
POSSIBLE_RENAMES = (
    {"kind": "processors", "type": "org.apache.nifi.processors.standard.ListenSyslog",
     "new": "TCP Port", "old": "Port",
     "why": "1.x had one 'Port' + a Protocol property; 2.x split it into 'TCP Port' "
            "and 'UDP Port' (identical descriptions), so one old key maps to two new "
            "ones and only the configured protocol says which"},
    {"kind": "processors", "type": "org.apache.nifi.processors.standard.ListenSyslog",
     "new": "UDP Port", "old": "Port", "why": "the other half of the same split"},
    {"kind": "processors", "type": "org.apache.nifi.processors.standard.ListenSyslog",
     "new": "Worker Threads", "old": "Max Number of TCP Connections",
     "why": "same default (2) and same ordinal, but 'threads that decode messages' "
            "and 'concurrent TCP connections accepted' are not the same quantity — "
            "carrying a tuned value across would be a guess"},
    {"kind": "services",
     "type": "org.apache.nifi.processors.aws.s3.encryption.StandardS3EncryptionService",
     "new": "KMS Key ID", "old": "key-id-or-key-material",
     "why": "1.x 'Key ID or Key Material' served both roles; 2.x split it into "
            "'KMS Key ID' and 'Key Material'. Which half a value belongs to depends "
            "on the encryption strategy, so niflow will not choose for you"},
    {"kind": "services",
     "type": "org.apache.nifi.processors.aws.s3.encryption.StandardS3EncryptionService",
     "new": "Key Material", "old": "key-id-or-key-material",
     "why": "the other half of the same split"},
    {"kind": "processors", "type": "org.apache.nifi.processors.standard.IdentifyMimeType",
     "new": "Custom MIME Configuration", "old": "config-body",
     "why": "2.x folded 1.x's 'Config Body' and 'Config File' into one property that "
            "takes a URL, a path, or the config text, gated by the new 'Config "
            "Strategy'. A body translates cleanly; a file path does not, and the "
            "strategy has to be set either way"},
)


def _norm(text: Optional[str]) -> str:
    """Fold a display name or description to a comparison key.

    Case, punctuation and whitespace all drifted between the lines ("Attributes
    to Log by Regular Expression" -> "Attributes to Log Regular Expression"
    kept the words but not the spacing), so only alphanumerics survive.
    """
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def _pair_one_to_one(
    new_keys, old_keys, new_raw: dict, old_raw: dict, field: str
) -> Dict[str, str]:
    """Pair leftover keys whose *field* normalises identically, 1:1 only.

    A many-to-one match is ambiguous (``CopyS3Object`` really does have two
    properties displayed "Bucket"), and guessing wrong is worse than not
    guessing: niflow would rewrite a key onto the wrong property and push a
    value that silently lands somewhere else. So a candidate is only accepted
    when exactly one key on each side carries that normalised value.
    """
    new_by: Dict[str, List[str]] = {}
    old_by: Dict[str, List[str]] = {}
    for key in new_keys:
        new_by.setdefault(_norm((new_raw.get(key) or {}).get(field)), []).append(key)
    for key in old_keys:
        old_by.setdefault(_norm((old_raw.get(key) or {}).get(field)), []).append(key)
    pairs: Dict[str, str] = {}
    for value, news in new_by.items():
        if not value or len(news) != 1:
            continue
        olds = old_by.get(value)
        if olds and len(olds) == 1:
            pairs[news[0]] = olds[0]
    return pairs


def detect_renames(
    new_props: dict, old_props: dict, curated: Optional[Dict[str, str]] = None,
    curated_pairs: Optional[Dict[str, str]] = None,
) -> Tuple[Dict[str, str], List[str], List[str]]:
    """Return ``(renamed_new_to_old, only_new, only_old)`` for one type.

    ``renamed`` maps the *new* (2.x, catalog/authoring) key to the *old* (1.x)
    key, which is the direction the push path translates in. ``curated`` is
    consulted last for the handful of renames where the display name itself
    changed, so no automatic signal survives.

    ``curated_pairs`` is this *type's* hand-confirmed ``{new: old}`` renames
    (:data:`niflow.processors.rules.CURATED_TYPE_RENAMES`) and is applied
    first, before any automatic pass: a pairing a human checked against both
    harvests outranks one the matcher inferred, and taking those keys out of
    the running early stops them being mis-paired with something else.
    """
    new_names = set(new_props)
    old_names = set(old_props)
    only_new = new_names - old_names
    only_old = old_names - new_names
    renamed: Dict[str, str] = {}

    for new_key, old_key in (curated_pairs or {}).items():
        if new_key in only_new and old_key in only_old:
            renamed[new_key] = old_key
            only_new.discard(new_key)
            only_old.discard(old_key)

    for field in ("display", "description"):
        pairs = _pair_one_to_one(only_new, only_old, new_props, old_props, field)
        renamed.update(pairs)
        only_new -= set(pairs)
        only_old -= set(pairs.values())

    for old_key, new_key in (curated or {}).items():
        if new_key in only_new and old_key in only_old:
            renamed[new_key] = old_key
            only_new.discard(new_key)
            only_old.discard(old_key)

    return renamed, sorted(only_new), sorted(only_old)


def _contract_changes(
    new_props: dict, old_props: dict, shared: List[str], renamed: Dict[str, str]
) -> Tuple[dict, dict, dict]:
    """Allowable-value, required-ness and default drift on properties that survived.

    Keyed by the *new* name throughout (including for renamed properties, whose
    old-side entry is looked up through the rename), because every caller works
    in the catalog's 2.x namespace.
    """
    allowable: dict = {}
    required: dict = {}
    default: dict = {}
    pairs = [(name, name) for name in shared] + list(renamed.items())
    for new_key, old_key in pairs:
        new_entry = new_props.get(new_key) or {}
        old_entry = old_props.get(old_key) or {}

        # A controller-service reference's "allowable values" are the ids of the
        # service instances that happened to exist on the harvest server — pure
        # instance noise, different on every run and on every machine. Diffing
        # them would both break determinism and invent "not an allowed value"
        # warnings for perfectly good service references.
        if new_entry.get("service") or old_entry.get("service"):
            new_allow = old_allow = set()
        else:
            new_allow = set(new_entry.get("allowable") or ())
            old_allow = set(old_entry.get("allowable") or ())
        if new_allow != old_allow and (new_allow or old_allow):
            allowable[new_key] = {
                "only_new": sorted(new_allow - old_allow),
                "only_old": sorted(old_allow - new_allow),
            }
        if bool(new_entry.get("required")) != bool(old_entry.get("required")):
            required[new_key] = {
                "new": bool(new_entry.get("required")),
                "old": bool(old_entry.get("required")),
            }
        if new_entry.get("default") != old_entry.get("default"):
            default[new_key] = {
                "new": new_entry.get("default"),
                "old": old_entry.get("default"),
            }
    return allowable, required, default


def build_map(new_book: dict, old_book: dict) -> dict:
    """Diff two rulebook dumps into the map the generated module carries.

    ``new_book`` is the newer line (2.x, the authoring namespace), ``old_book``
    the older one (1.x, what work runs). Returns a plain dict so the emitter and
    the report renderer can both consume it, and so it round-trips through JSON
    for tests.
    """
    from niflow.processors.rules import (
        CURATED_TYPE_RENAMES,
        LEGACY_PROPERTY_ALIASES,
    )

    result: dict = {
        "new_version": new_book.get("nifi_version", "?"),
        "old_version": old_book.get("nifi_version", "?"),
        "kinds": {},
    }
    for kind in ("processors", "services"):
        new_types = new_book.get(kind) or {}
        old_types = old_book.get(kind) or {}
        entries: dict = {}
        for type_str in sorted(set(new_types) & set(old_types)):
            new_props = new_types[type_str].get("raw") or {}
            old_props = old_types[type_str].get("raw") or {}
            renamed, only_new, only_old = detect_renames(
                new_props, old_props, LEGACY_PROPERTY_ALIASES,
                CURATED_TYPE_RENAMES.get(type_str),
            )
            shared = sorted(set(new_props) & set(old_props))
            allowable, required, default = _contract_changes(
                new_props, old_props, shared, renamed
            )
            entry = {}
            if renamed:
                entry["renamed"] = dict(sorted(renamed.items()))
            if only_new:
                entry["only_new"] = only_new
            if only_old:
                entry["only_old"] = only_old
            if allowable:
                entry["allowable_changed"] = dict(sorted(allowable.items()))
            if required:
                entry["required_changed"] = dict(sorted(required.items()))
            if default:
                entry["default_changed"] = dict(sorted(default.items()))
            if entry:
                entries[type_str] = entry
        result["kinds"][kind] = {
            "types": entries,
            "only_new": sorted(set(new_types) - set(old_types)),
            "only_old": sorted(set(old_types) - set(new_types)),
            "both": sorted(set(new_types) & set(old_types)),
        }
    return result


# --- summary + reporting ----------------------------------------------------

def summarise(version_map: dict) -> dict:
    """Counts per kind: types compared, types that differ, properties per bucket."""
    out: dict = {}
    for kind, block in version_map["kinds"].items():
        types = block["types"]
        counts = {
            "types_both": len(block["both"]),
            "types_only_new": len(block["only_new"]),
            "types_only_old": len(block["only_old"]),
            "types_differing": len(types),
        }
        for bucket in ("renamed", "only_new", "only_old", "allowable_changed",
                       "required_changed", "default_changed"):
            counts[bucket] = sum(len(e.get(bucket) or ()) for e in types.values())
        out[kind] = counts
    return out


def _short(type_str: str) -> str:
    return type_str.rsplit(".", 1)[-1]


def rank_offenders(version_map: dict, kind: str, limit: int = 25) -> List[tuple]:
    """Types ordered by how badly a cross-version push would hurt.

    Score is unsupported properties (either direction) weighted heaviest —
    those carry a value that cannot land — plus contract changes, plus a bonus
    for types on :data:`WORST_OFFENDER_HINTS`, because a rename on a processor
    nobody uses is not the same problem as one on ConvertRecord.
    """
    ranked = []
    for type_str, entry in version_map["kinds"][kind]["types"].items():
        unsupported = len(entry.get("only_new") or ()) + len(entry.get("only_old") or ())
        contract = (len(entry.get("allowable_changed") or ())
                    + len(entry.get("required_changed") or ()))
        renamed = len(entry.get("renamed") or ())
        score = unsupported * 10 + contract * 5 + renamed
        if _short(type_str) in WORST_OFFENDER_HINTS:
            score += 100
        ranked.append((score, type_str, entry))
    ranked.sort(key=lambda row: (-row[0], row[1]))
    return ranked[:limit]


_MODULE_HEADER = '''"""AUTO-GENERATED by ``python -m niflow.versiondiff`` — do not edit by hand.

Cross-version property difference map: **NiFi {new_version}** (the authoring
namespace the main catalog is harvested from) vs **NiFi {old_version}** (the
older line). Generated {generated} by diffing a full property-descriptor
harvest of each server:

    python -m niflow.codegen --dump-rulebook new.json                     # 2.x
    NIFLOW_NIFI_HOST=…:8444 python -m niflow.codegen --dump-rulebook old.json
    python -m niflow.versiondiff new.json old.json

or just ``make version-map`` with both containers up. :mod:`niflow.compat`
reads these tables; docs/version-compat.md is the human-readable rendering.

Each ``*_DIFF`` entry is keyed by fully-qualified type and may carry:

* ``renamed``          — ``{{new_key: old_key}}``, the same property under two names
* ``only_new``         — keys that exist only on {new_version}
* ``only_old``         — keys that exist only on {old_version}
* ``allowable_changed``— ``{{key: {{only_new, only_old}}}}`` enum drift
* ``required_changed`` — ``{{key: {{new, old}}}}``
* ``default_changed``  — ``{{key: {{new, old}}}}``

A type absent from ``*_DIFF`` but present in ``*_TYPES_BOTH`` has an identical
property surface on both lines. A type in neither was never harvested — treat
it as unknown, not as compatible.
"""
from __future__ import annotations

'''


def emit_module(version_map: dict, generated: str) -> str:
    """Render the generated ``niflow/version_map.py`` source."""
    out = [_MODULE_HEADER.format(
        new_version=version_map["new_version"],
        old_version=version_map["old_version"],
        generated=generated,
    )]
    out.append(
        "VERSION_MAP_META = {\n"
        f"    'new_version': {version_map['new_version']!r},\n"
        f"    'old_version': {version_map['old_version']!r},\n"
        f"    'generated': {generated!r},\n"
        "}\n\n"
    )
    out.append(f"SUMMARY = {summarise(version_map)!r}\n\n")
    for kind, prefix in (("processors", "PROCESSOR"), ("services", "SERVICE")):
        block = version_map["kinds"][kind]
        # One type per line keeps the committed diff reviewable when a NiFi
        # upgrade shifts a handful of properties.
        rows = "".join(
            f"    {type_str!r}: {entry!r},\n"
            for type_str, entry in sorted(block["types"].items())
        )
        out.append(f"{prefix}_DIFF = {{\n{rows}}}\n\n")
        for name, values in (("TYPES_BOTH", block["both"]),
                             ("TYPES_ONLY_NEW", block["only_new"]),
                             ("TYPES_ONLY_OLD", block["only_old"])):
            rows = "".join(f"    {value!r},\n" for value in values)
            out.append(f"{prefix}_{name} = frozenset({{\n{rows}}})\n\n")
    return "".join(out)


# --- markdown report --------------------------------------------------------

def _fmt_list(values, limit: int = 12) -> str:
    values = list(values)
    shown = ", ".join(f"`{v}`" for v in values[:limit])
    if len(values) > limit:
        shown += f", … (+{len(values) - limit} more)"
    return shown or "—"


def _render_type(type_str: str, entry: dict, new_v: str, old_v: str) -> List[str]:
    """The Markdown block describing one type's cross-version differences."""
    lines = [f"### {_short(type_str)}", "", f"`{type_str}`", ""]
    if entry.get("only_new"):
        lines.append(f"* **Only on {new_v}** (dropped when pushing to {old_v}): "
                     + _fmt_list(entry["only_new"]))
    if entry.get("only_old"):
        lines.append(f"* **Only on {old_v}** (unreachable from a {new_v} flow): "
                     + _fmt_list(entry["only_old"]))
    if entry.get("renamed"):
        pairs = list(entry["renamed"].items())[:10]
        lines.append("* **Renamed** (translated automatically): "
                     + ", ".join(f"`{n}` ← `{o}`" for n, o in pairs)
                     + (f", … (+{len(entry['renamed']) - 10} more)"
                        if len(entry["renamed"]) > 10 else ""))
    for key, change in (entry.get("allowable_changed") or {}).items():
        lines.append(f"* **Allowable values changed** for `{key}`: "
                     f"only on {new_v} {_fmt_list(change['only_new'], 8)}; "
                     f"only on {old_v} {_fmt_list(change['only_old'], 8)}")
    for key, change in (entry.get("required_changed") or {}).items():
        lines.append(f"* **Required-ness changed** for `{key}`: "
                     f"{new_v} required={change['new']}, {old_v} required={change['old']}")
    for key, change in (entry.get("default_changed") or {}).items():
        lines.append(f"* **Default changed** for `{key}`: "
                     f"{new_v} `{change['new']}` vs {old_v} `{change['old']}`")
    lines.append("")
    return lines


def _render_rename_recovery(new_v: str, old_v: str) -> List[str]:
    """The two hand-curated rename sections: translated, and "verify these".

    The automatic matcher refuses to pair a property whose key *and* display
    name *and* description all moved, so genuine renames sit in the
    only-one-line buckets looking like properties that cannot cross. Mining
    those buckets by hand splits them in two: the confirmed ones (now
    translated for you, and no longer counted as unsupported anywhere) and the
    plausible ones, which are printed rather than acted on because translating
    the wrong pair writes a value onto the wrong property.
    """
    from niflow.processors.rules import CURATED_TYPE_RENAMES

    lines: List[str] = []
    add = lines.append
    curated_count = sum(len(v) for v in CURATED_TYPE_RENAMES.values())
    add("## Renames recovered by hand")
    add("")
    add(f"Matched renames are found automatically on display name, then "
        f"description, and only 1:1 in both directions. A rename that changed "
        f"key *and* display name *and* description defeats all of that and "
        f"lands in the two 'only on one line' buckets, where it reads as a "
        f"property that cannot cross when in fact it can. These "
        f"{curated_count} pairs were mined out of those buckets and confirmed "
        f"by hand (same allowable set, same default, same required-ness and "
        f"sensitivity, same dependencies, same ordinal, description still "
        f"saying the same thing). niflow **translates them for you**, "
        f"processors and controller services alike — they are counted as "
        f"renames in the totals above, not as unsupported.")
    add("")
    add(f"| Type | {new_v} key | {old_v} key |")
    add("|---|---|---|")
    for type_str, pairs in sorted(CURATED_TYPE_RENAMES.items(),
                                  key=lambda row: _short(row[0])):
        for new_key, old_key in sorted(pairs.items()):
            add(f"| `{_short(type_str)}` | `{new_key}` | `{old_key}` |")
    add("")
    add("Curated in `niflow/processors/rules.py` (`CURATED_TYPE_RENAMES`); add "
        "to it and re-run `make version-map` to fold new pairs into the map.")
    add("")
    add("## Possible renames — verify before trusting")
    add("")
    add("These pairs are plausible but not certain, so niflow does **not** "
        "translate them: it still reports the key as one that cannot land, "
        "which is the safe answer. Every one is either a 1.x property that 2.x "
        "split in two (translating would have to pick a half) or a pair whose "
        "meaning moved with its name. If one of them matters to you, confirm it "
        "on your own servers and move it into `CURATED_TYPE_RENAMES`.")
    add("")
    for row in POSSIBLE_RENAMES:
        add(f"* **{_short(row['type'])}** — `{row['new']}` ({new_v}) ≟ "
            f"`{row['old']}` ({old_v}): {row['why']}.")
    add("")
    return lines


def render_report(version_map: dict, generated: str) -> str:
    """The human-readable docs/version-compat.md the user reads at work."""
    new_v = version_map["new_version"]
    old_v = version_map["old_version"]
    counts = summarise(version_map)
    lines: List[str] = []
    add = lines.append

    add(f"# NiFi {new_v} vs {old_v} — property difference map")
    add("")
    add(f"*Generated {generated} by `make version-map`, from a live "
        f"property-descriptor harvest of both servers. Do not edit by hand — "
        f"regenerate instead.*")
    add("")
    add("You author flows against the 2.x catalog; work runs 1.24/1.28. Between")
    add("those lines Apache renamed most property **keys** to their display names,")
    add("added properties that do not exist on 1.x, and dropped others. A 2.x-only")
    add("key pushed at a 1.x server does **not** error — NiFi stores it as an inert")
    add("*dynamic* property while the real property keeps its default, so the")
    add("processor quietly does the wrong thing. That silence is what this map ends.")
    add("")
    add("## What niflow does with it")
    add("")
    add(f"* `niflow validate FILE` — offline, no server needed. Checks the flow")
    add(f"  against the declared **compatibility baseline** (`NIFLOW_MIN_NIFI_VERSION`")
    add(f"  in `.niflow.env`, default {old_v[:4]}) with no flag, and **exits non-zero**")
    add("  if the flow sets a property that cannot land there. It fails rather than")
    add("  warns on purpose: on the server that failure is silent, and a warning in")
    add("  a wall of output is how this class of bug reaches production.")
    add(f"  `--target-version {new_v}` checks some other line instead;")
    add("  `--no-compat-check` (or a baseline of `none`) turns it off.")
    add("* `niflow plan` / `niflow push` — the same check runs automatically against")
    add("  the live server's own version and logs every affected component **before**")
    add("  the first mutation. It warns, it does not block: NiFi accepts the flow, and")
    add("  that acceptance is precisely the problem. Pushing to a *different* line")
    add("  from the baseline (a 2.x server, say) is legitimate and is never blocked —")
    add("  but a flow that would not survive the baseline is called out there too.")
    add("* `niflow doctor` — states the baseline, reports catalog-vs-server skew, and")
    add("  names the flows under `flows/` that would not survive either.")
    add("* Push-time key translation — renamed keys are rewritten to the target's")
    add("  namespace automatically; unsupported keys are dropped **with a warning**.")
    add("")
    add("## Totals")
    add("")
    add("| | Processors | Controller services |")
    add("|---|---|---|")
    rows = [
        ("Types on both lines", "types_both"),
        (f"Types only on {new_v}", "types_only_new"),
        (f"Types only on {old_v}", "types_only_old"),
        ("Types whose properties differ", "types_differing"),
        ("Properties renamed between lines", "renamed"),
        (f"Properties only on {new_v}", "only_new"),
        (f"Properties only on {old_v}", "only_old"),
        ("Properties with changed allowable values", "allowable_changed"),
        ("Properties with changed required-ness", "required_changed"),
        ("Properties with changed default", "default_changed"),
    ]
    for label, key in rows:
        add(f"| {label} | {counts['processors'][key]} | {counts['services'][key]} |")
    add("")
    add(f"Renames are **translatable** — niflow rewrites them on push. The "
        f"{counts['processors']['only_new'] + counts['services']['only_new']} "
        f"properties that exist only on {new_v} and the "
        f"{counts['processors']['only_old'] + counts['services']['only_old']} "
        f"that exist only on {old_v} are **not**: they carry a value that cannot "
        f"land on the other line.")
    add("")

    for kind, title in (("processors", "Processors"), ("services", "Controller services")):
        block = version_map["kinds"][kind]
        add(f"## {title}: types missing on the other line")
        add("")
        add("Using one of these in a flow bound for the other line fails at "
            "push — the type simply is not installed.")
        add("")
        add(f"**Only on {new_v}** ({len(block['only_new'])}): "
            + _fmt_list(_short(t) for t in block["only_new"]))
        add("")
        add(f"**Only on {old_v}** ({len(block['only_old'])}): "
            + _fmt_list(_short(t) for t in block["only_old"]))
        add("")
        add(f"## {title}: worst offenders")
        add("")
        add("Ranked by unsupported properties (weighted heaviest — a value that "
            "cannot land), then contract changes, then renames, with a bonus for "
            "types in common use.")
        add("")
        shown = set()
        for _, type_str, entry in rank_offenders(version_map, kind, 20):
            shown.add(type_str)
            lines.extend(_render_type(type_str, entry, new_v, old_v))

        # The ranking is by damage, which buries types that are merely
        # *renamed* — and ConvertRecord being "merely renamed" is exactly the
        # lookup someone needs at 5pm. So every everyday type gets its entry
        # too, whether or not it scored.
        everyday = [
            (type_str, entry)
            for type_str, entry in sorted(block["types"].items())
            if _short(type_str) in WORST_OFFENDER_HINTS and type_str not in shown
        ]
        if everyday:
            add(f"## {title}: other everyday types")
            add("")
            add("These score low — mostly renames, which niflow translates for you —")
            add("but they are the ones you actually use, so here they are in full.")
            add("")
            for type_str, entry in everyday:
                lines.extend(_render_type(type_str, entry, new_v, old_v))

        add(f"## {title}: complete index")
        add("")
        add(f"Every type whose properties differ. `renamed` is handled for you; "
            f"`only {new_v}` and `only {old_v}` are the counts that can bite. "
            f"Full detail for every one of them is in `niflow/version_map.py`.")
        add("")
        add(f"| Type | renamed | only {new_v} | only {old_v} | allowable | required |")
        add("|---|---|---|---|---|---|")
        for type_str, entry in sorted(block["types"].items(),
                                      key=lambda row: _short(row[0])):
            add(f"| `{_short(type_str)}` "
                f"| {len(entry.get('renamed') or ())} "
                f"| {len(entry.get('only_new') or ())} "
                f"| {len(entry.get('only_old') or ())} "
                f"| {len(entry.get('allowable_changed') or ())} "
                f"| {len(entry.get('required_changed') or ())} |")
        add("")

    lines.extend(_render_rename_recovery(new_v, old_v))

    add("## What this map cannot tell you")
    add("")
    add("The harvest reads NiFi's own property *descriptors*. That makes it exact")
    add("about names, allowable values, required-ness and defaults — and blind to")
    add("everything else:")
    add("")
    add("* **Behavioural drift.** A property present on both lines under the same")
    add("  name, with the same allowable values, can still *mean* something")
    add("  different in the engine (parsing, rounding, retry semantics, what an")
    add("  empty value implies). Nothing in the descriptor exposes that, so this")
    add("  map reports such a property as identical. It is the largest known gap.")
    add("  One confirmed instance sits in the curated renames above:")
    add("  `DBCPConnectionPool`'s `Maximum Connection Lifetime` is the same property")
    add("  as 1.x's `dbcp-max-conn-lifetime` (same default), but the 1.x description")
    add("  says *milliseconds* where 2.x describes a duration — the key translates,")
    add("  the value may need re-reading.")
    add("* **Relationship and attribute changes.** Only properties are diffed here.")
    add("* **Undetected renames.** Renames are matched on display name, then")
    add("  description, and only when the pairing is 1:1 in both directions —")
    add("  deliberately conservative, because a wrong pairing would silently write")
    add("  a value to the wrong property. A rename that changed *both* key and")
    add("  display name and description defeats every automatic signal and shows up")
    add("  as one `only_new` plus one `only_old` entry. Those buckets have since")
    add("  been mined by hand — see *Renames recovered by hand* above for the pairs")
    add("  now translated, and *Possible renames* for the ones left for you to")
    add("  confirm — but the mining was done against these two servers only, so a")
    add("  regenerated map may surface fresh ones.")
    add("* **Types that would not instantiate.** Restricted or dependency-hungry")
    add("  types are skipped by the harvest and appear in neither table.")
    add("* **Your NARs, not ours.** This was harvested from stock Apache NiFi")
    add(f"  {new_v} and {old_v} containers. A work server with extra NARs (or a")
    add("  1.28 rather than 1.24) differs; re-run `make version-map` pointed at")
    add("  the real pair to get a map that matches your estate.")
    add("")
    return "\n".join(lines) + "\n"


def main() -> None:
    import json
    import sys
    from datetime import datetime, timezone
    from pathlib import Path

    args = [a for a in sys.argv[1:] if not a.startswith("-")]
    if len(args) != 2:
        raise SystemExit(
            "usage: python -m niflow.versiondiff <new-rulebook.json> <old-rulebook.json>\n"
            "  rulebooks come from: python -m niflow.codegen --dump-rulebook PATH"
        )
    new_book = json.loads(Path(args[0]).read_text())
    old_book = json.loads(Path(args[1]).read_text())
    if new_book.get("nifi_version", "9")[0] < old_book.get("nifi_version", "0")[0]:
        raise SystemExit(
            f"argument order is <new> <old>, but got NiFi {new_book['nifi_version']} "
            f"then {old_book['nifi_version']}"
        )
    version_map = build_map(new_book, old_book)
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    root = Path(__file__).resolve().parent
    module_path = root / "version_map.py"
    report_path = root.parent / "docs" / "version-compat.md"
    module_path.write_text(emit_module(version_map, generated))
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_report(version_map, generated))

    counts = summarise(version_map)
    print(f"Wrote {module_path.relative_to(root.parent)} and "
          f"{report_path.relative_to(root.parent)}")
    for kind, c in counts.items():
        print(f"  {kind}: {c['types_differing']}/{c['types_both']} types differ; "
              f"{c['renamed']} renamed, {c['only_new']} only-new, "
              f"{c['only_old']} only-old properties; "
              f"{c['types_only_new']}/{c['types_only_old']} types one-sided")


if __name__ == "__main__":
    main()
