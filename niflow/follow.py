"""Live FlowFile stepper — a debugger for flows (``niflow follow``).

The live counterpart of ``niflow trace``: trace replays a finished journey
from provenance; follow *creates* the journey interactively. The group is
deliberately stopped first — nothing may race the stepper — then each step
run-onces the processor consuming the followed FlowFile's queue and renders
the provenance events that run produced, in the same hop/diff view trace
uses (:func:`format_hop` is shared by both commands).

::

    niflow follow "My Flow (copy)"                   # pick a start point
    niflow follow "My Flow (copy)" --list            # just show the start points
    niflow follow "My Flow (copy)" --source Gen      # mint a FlowFile first
    niflow follow "My Flow (copy)" --mute failure    # never follow that branch
    niflow follow "My Flow (copy)" --auto --restore  # run to the end, then restart

Three things make it a debugger rather than a log reader:

* **Start points.** :func:`entry_points` lists the plausible places a journey
  can begin — non-empty queues, source processors with no inbound connection,
  input ports — so starting is a menu pick, not a UUID hunt.
* **History.** Every hop is kept per branch in a :class:`FollowSession` that is
  written to disk after each step, so hop 3 is re-viewable without re-running
  anything and a page refresh (or a crashed process) does not lose the journey.
* **Diffs that jump out.** Each hop carries a cross-hop attribute diff
  (added/changed/removed, old → new) plus a content-change flag, computed
  against the previous hop of the *same branch*.

Forks get real branch management: children are tracked with the relationship
and queue they went down, and any branch can be **muted** — by child UUID,
relationship, destination, or connection id, before or after the fork.
Muting is a view decision only; the muted branch keeps running in NiFi
(nothing in the mute path issues a mutating REST call).

The group is left stopped on exit unless ``--restore`` is given: quiescing
was the point, and restarting silently would surprise.

Trace-verified provenance facts this leans on (live on 1.24.0 and 2.7.2):
event ids are monotonic across the instance, ``previousValue`` semantics
drive the diffs, and indexing lands in well under a second after run-once —
though 1.24 has been seen to lag, which is why every wait has a timeout and
a "retry?" affordance rather than a silent empty result.
"""
from __future__ import annotations

import copy
import json
import logging
import os
import re
import time
from datetime import datetime
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from niflow.testing import INJECTOR_NAME, inject_flowfile, remove_injector

logger = logging.getLogger("niflow")

# How long one step waits for run-once's provenance to be indexed. Live
# measurements say <0.2s on both lines; 1.24's index has been seen to lag
# under load, so the wait is generous and a stall is retryable, not fatal.
_PROV_TIMEOUT_S = 5.0
_PROV_INTERVAL_S = 0.2

# One run-once serves ONE FlowFile from ONE of a processor's inbound queues —
# not necessarily the file being followed. Live on 1.24: a processor with two
# inbound queues happily consumed the *other* one and the step looked stalled.
# A step therefore re-runs the destination until OUR file moves, up to this
# many times (the group is quiesced, so the only files moving are the ones
# already queued ahead of it).
_RUN_ATTEMPTS = 8

# …and a queue at work holds thousands, so the real budget for one step is
# "one run per FlowFile ahead of ours" (NiFi will not list past 100, and will
# not raise the cap). These bound that: never more than _RUN_CAP run-onces,
# never longer than _STEP_TIMEOUT_S, and the step then reports honestly how
# far it got instead of pretending the file vanished.
_RUN_CAP = 500
_STEP_TIMEOUT_S = 60.0

#: Filename given to an injected fixture unless the caller names one.
FIXTURE_FILENAME = "niflow-fixture"

_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$")

# Attribute values can be megabytes (a whole record set in an attribute is a
# real work pattern); the diff view clips them so one hop cannot flood a
# terminal. Full values stay one keypress away (`a` / the Attributes button).
_VALUE_CLIP = 160


class FollowError(RuntimeError):
    """A stepping problem the user can act on (printed, never a traceback)."""


# --------------------------------------------------------------- rendering


def _short(value: Any) -> str:
    text = "" if value is None else str(value)
    return text if len(text) <= _VALUE_CLIP else text[:_VALUE_CLIP] + "…"


def format_hop(index: int, hop: dict, full: bool = False) -> str:
    """One trace/follow hop as the multi-line block both commands print.

    ``index`` is the 1-based hop number; ``hop`` is the dict
    :meth:`~niflow.client.NiFiClient.trace_flowfile` builds per event. When
    the hop has been through :func:`annotate_hops` it carries a cross-hop
    ``diff`` (``~`` changed, ``+`` added, ``-`` removed) and a
    ``content_change`` note; otherwise it falls back to NiFi's own per-event
    ``changes`` list.
    """
    rel = f" -> {hop['relationship']}" if hop["relationship"] else ""
    head = f"{index:>3}. {hop['component'] or '(flow)'}  [{hop['event_type']}{rel}]"
    # A synthetic hop has no event behind it, so it has no time and no size of
    # its own; printing "  0 B" there would read as an empty FlowFile.
    if not hop.get("synthetic"):
        head += f"  {hop['time']}  {hop['size']} B"
    lines = [head]
    if hop.get("lineage"):
        lines.append(f"       ⤳ {hop['lineage']}")
        for child in hop["children"]:
            lines.append(f"       continues as {child}  (niflow trace {child})")
        return "\n".join(lines)
    diff = hop.get("diff")
    if diff is None:
        for change in hop["changes"]:
            before = "(new)" if change["before"] is None else _short(change["before"])
            lines.append(f"       {change['name']}: {before} -> "
                         f"{_short(change['after'])}")
    else:
        for entry in _ordered_diff(diff):
            lines.append(f"       {_diff_line(entry)}")
    content = hop.get("content_change")
    if content:
        # A same-size rewrite is still a rewrite (NiFi's contentEqual flags it);
        # saying "93 B -> 93 B" would read like nothing happened.
        if content["before"] is None or content["before"] == content["after"]:
            lines.append(f"       ~ content rewritten ({content['after']} B)")
        else:
            lines.append(f"       ~ content: {content['before']} B -> "
                         f"{content['after']} B")
    if full:
        for name, value in sorted(hop["attributes"].items()):
            lines.append(f"       = {name}: {value}")
    for child in hop["children"]:
        lines.append(f"       spawned {child}  (niflow trace {child})")
    return "\n".join(lines)


_DIFF_ORDER = {"changed": 0, "added": 1, "removed": 2}


def _ordered_diff(diff: Sequence[dict]) -> List[dict]:
    """Changed first (the headline), then added, then removed; name-sorted."""
    return sorted(diff, key=lambda e: (_DIFF_ORDER.get(e["status"], 9), e["name"]))


def _diff_line(entry: dict) -> str:
    if entry["status"] == "added":
        return f"+ {entry['name']}: {_short(entry['after'])}"
    if entry["status"] == "removed":
        return f"- {entry['name']}: {_short(entry['before'])}  (removed)"
    return (f"~ {entry['name']}: {_short(entry['before'])} -> "
            f"{_short(entry['after'])}")


def diff_attributes(previous: Optional[dict], hop: dict) -> List[dict]:
    """Classify a hop's attributes against the previous hop's.

    Entries are ``{"name", "before", "after", "status"}`` with status
    ``added`` / ``changed`` / ``removed`` (unchanged keys are omitted — they
    are the ``--full`` view's job).

    ``previous`` is ``None`` for the first hop of a branch, where there is
    nothing to diff against; that case falls back to NiFi's own per-event
    ``changes`` (``previousValue`` is absent for a key born at that event,
    and equal to ``value`` for untouched ones). Removals are only visible
    from the cross-hop diff: NiFi records the attributes a FlowFile *has*,
    never the ones it lost.
    """
    if previous is None:
        return [
            {"name": c["name"], "before": c["before"], "after": c["after"],
             "status": "added" if c["before"] is None else "changed"}
            for c in hop.get("changes") or []
        ]
    attrs = hop.get("attributes") or {}
    out: List[dict] = []
    for name in sorted(set(attrs) | set(previous)):
        before, after = previous.get(name), attrs.get(name)
        if name not in previous:
            out.append({"name": name, "before": None, "after": after,
                        "status": "added"})
        elif name not in attrs:
            out.append({"name": name, "before": before, "after": None,
                        "status": "removed"})
        elif before != after:
            out.append({"name": name, "before": before, "after": after,
                        "status": "changed"})
    return out


def content_change(previous_size: Optional[int], hop: dict) -> Optional[dict]:
    """Whether this hop rewrote the payload, as ``{"before", "after"}``.

    Two signals: NiFi's own ``contentEqual`` (False = the event wrote a new
    content claim) and a size delta against the previous hop. Either one
    counts — a same-size rewrite is still a rewrite, and ``contentEqual`` is
    absent on some event types.
    """
    size = hop.get("size") or 0
    changed = hop.get("content_equal") is False
    if previous_size is not None and previous_size != size:
        changed = True
    if not changed:
        return None
    return {"before": previous_size, "after": size}


def lineage_note(hop: dict) -> str:
    """Why a hop that is *not* this FlowFile's shows up in its journey.

    NiFi's ``FlowFileUUID`` search is a lineage query, not a filter (verified
    live on 1.24.0), so a split child's journey also carries the parent's
    FORK and — the case that matters at work — the JOIN of the merged file it
    was consumed into. Those events describe a *different* FlowFile; diffing
    them against this one produces nonsense ("size 4 B -> 1030 B, 40
    attributes changed"), so they are labelled instead.
    """
    if hop.get("own", True):
        return ""
    origin = (hop.get("flowfile_uuid") or "")[:8] or "another FlowFile"
    kind = hop.get("event_type", "")
    if kind in ("FORK", "CLONE"):
        return (f"this FlowFile was born here — {hop.get('component') or 'a processor'} "
                f"split {origin}… into {len(hop.get('children') or [])} children")
    if kind in ("JOIN", "MERGE"):
        return (f"this FlowFile was merged into {origin}… here, together with "
                f"{max(len(hop.get('parents') or []) - 1, 0)} other(s)")
    return f"lineage event belonging to {origin}…, not to this FlowFile"


def annotate_hops(
    hops: Sequence[dict],
    previous: Optional[dict] = None,
    previous_size: Optional[int] = None,
) -> Tuple[Optional[dict], Optional[int]]:
    """Attach ``diff`` / ``content_change`` / ``lineage`` to an ordered run.

    Mutates the hops in place and returns the ``(attributes, size)`` baseline
    after the last one, so a stepper can carry it into the next run-once (and
    a branch can inherit its parent's at fork time).

    A hop belonging to a *relative* (``own`` is False — see
    :func:`lineage_note`) gets a note instead of a diff and does **not**
    become the baseline: the next real hop must still be diffed against the
    last thing that actually happened to this FlowFile.
    """
    for hop in hops:
        note = lineage_note(hop)
        if note:
            hop["lineage"] = note
        # A synthetic hop (a port crossing, a transfer NiFi did not record)
        # carries no attributes of its own. Diffing against it, or letting it
        # become the baseline, would report every attribute as freshly added
        # at the next real hop.
        if note or hop.get("synthetic"):
            hop["diff"] = []
            hop["content_change"] = None
            continue
        hop["diff"] = diff_attributes(previous, hop)
        hop["content_change"] = content_change(previous_size, hop)
        previous = dict(hop.get("attributes") or {})
        previous_size = hop.get("size") or 0
    return previous, previous_size


# ------------------------------------------------------------------ watches

#: Watch specs that are not attributes: a hop has a size and a shape too, and
#: "did this processor stop rewriting the content" is a debugging question.
_WATCH_PSEUDO: Dict[str, Callable[[dict], Any]] = {
    "@size": lambda hop: hop.get("size"),
    "@component": lambda hop: hop.get("component"),
    "@event": lambda hop: hop.get("event_type"),
    "@rel": lambda hop: hop.get("relationship"),
}

_WATCH_MARKS = {"changed": "~", "added": "+", "removed": "-"}

#: How wide one watched column is printed. Values are clipped, not wrapped —
#: the table is for spotting *when* something changed, and `a` shows the full
#: attribute set of a hop.
_WATCH_CELL = 22


def _clip(value: Any, limit: int) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\n", "⏎").replace("\t", " ")
    return text if len(text) <= limit else text[: limit - 1] + "…"


def expand_watches(hops: Sequence[dict], watches: Sequence[str]) -> List[str]:
    """Concrete columns for a watch list: a glob becomes the names it matched.

    ``w http.*`` is one thing to type and however many columns the flow turns
    out to set. An exact name that no hop carries is kept as a column anyway —
    "this attribute is never set" is often exactly what was being watched.
    """
    columns: List[str] = []
    for spec in watches:
        if spec in _WATCH_PSEUDO or not any(ch in spec for ch in "*?["):
            if spec not in columns:
                columns.append(spec)
            continue
        matched = sorted({name for hop in hops
                          for name in (hop.get("attributes") or {})
                          if fnmatch(name, spec)})
        for name in matched or [spec]:
            if name not in columns:
                columns.append(name)
    return columns


def watch_value(hop: dict, column: str) -> Optional[str]:
    """One watched value at one hop, or ``None`` when it is not set there."""
    if column in _WATCH_PSEUDO:
        value = _WATCH_PSEUDO[column](hop)
        return None if value in (None, "") else str(value)
    return (hop.get("attributes") or {}).get(column)


def watch_rows(hops: Sequence[dict],
               watches: Sequence[str]) -> Tuple[List[str], List[dict]]:
    """The hop x attribute table: one row per hop, one cell per watch.

    Each cell carries its ``value`` and a ``status`` (``set`` at the first
    hop, then ``added`` / ``changed`` / ``removed`` / ``same``, or ``absent``
    when it was never there) — the same vocabulary the per-hop diff uses, so
    the table and the hops agree about what changed where.

    Hops that belong to a relative (a lineage event) or are synthetic carry no
    attributes of their own; their cells are blank and do not become the
    baseline, or every attribute would read as removed and then re-added.
    """
    columns = expand_watches(hops, watches)
    previous: Dict[str, Optional[str]] = {}
    rows: List[dict] = []
    for index, hop in enumerate(hops, 1):
        borrowed = bool(hop.get("lineage")) or bool(hop.get("synthetic"))
        cells: Dict[str, dict] = {}
        for column in columns:
            if borrowed:
                cells[column] = {"value": None, "status": "n/a"}
                continue
            value = watch_value(hop, column)
            before = previous.get(column)
            if not previous:
                status = "set" if value is not None else "absent"
            elif value is None:
                status = "removed" if before is not None else "absent"
            elif before is None:
                status = "added"
            else:
                status = "changed" if before != value else "same"
            cells[column] = {"value": value, "status": status}
        if not borrowed:
            for column in columns:
                previous[column] = cells[column]["value"]
        rows.append({
            "hop": index, "component": hop.get("component") or "(flow)",
            "event": hop.get("event_type", ""),
            "relationship": hop.get("relationship", ""),
            "cells": cells,
        })
    return columns, rows


def format_watch_table(columns: Sequence[str], rows: Sequence[dict],
                       width: int = _WATCH_CELL) -> str:
    """Render :func:`watch_rows` as the table the `w` key prints."""
    if not columns:
        return ("Watching nothing yet — `w NAME` watches an attribute "
                "(globs allowed: `w http.*`; `@size` watches the payload).")
    if not rows:
        return "No hops on this branch yet — step first."
    head = f"{'hop':>3}  {'component':<20}" + "".join(
        f"  {_clip(c, width):<{width}}" for c in columns)
    lines = [head, "-" * len(head)]
    for row in rows:
        line = f"{row['hop']:>3}  {_clip(row['component'], 20):<20}"
        for column in columns:
            cell = row["cells"][column]
            text = "·" if cell["value"] is None else _clip(cell["value"], width - 2)
            line += f"  {_WATCH_MARKS.get(cell['status'], ' ')}{text:<{width - 1}}"
        lines.append(line.rstrip())
    return "\n".join(lines)


# --------------------------------------------------------- replay-after-fix


#: Attributes that identify a *particular* FlowFile rather than describe it.
#: They can never match across two runs and can never be what a fix changed,
#: so a replay comparison that reported them would be pure noise. (The
#: per-hop diff still shows them: there they mean "this hop rewrote it".)
_RUN_IDENTITY_ATTRIBUTES = frozenset({"uuid", "entryDate", "lineageStartDate"})


def compare_runs(before: Sequence[dict],
                 after: Sequence[dict]) -> List[dict]:
    """Hop-by-hop divergence between two runs of the same fixture.

    The question replay answers is "did my fix change what happens", so the
    comparison is positional: hop 3 against hop 3. Anything an attribute diff
    would hide — a different processor, a different relationship, a hop only
    one run has — becomes a note of its own.
    """
    rows: List[dict] = []
    for index in range(max(len(before), len(after))):
        b = before[index] if index < len(before) else None
        a = after[index] if index < len(after) else None
        notes: List[str] = []
        if b is None:
            status = "only_after"
        elif a is None:
            status = "only_before"
        else:
            for field, label in (("component", "component"),
                                 ("event_type", "event"),
                                 ("relationship", "relationship")):
                # Backslash-free f-string expressions: 3.9 is a supported line.
                was = b.get(field) or "—"
                now = a.get(field) or "—"
                if was != now:
                    notes.append(f"{label}: {was} -> {now}")
            if (b.get("size") or 0) != (a.get("size") or 0):
                notes.append(f"size: {b.get('size') or 0} B -> {a.get('size') or 0} B")
            notes += [_diff_line(entry) for entry in _ordered_diff(
                diff_attributes(dict(b.get("attributes") or {}), a))
                if entry["name"] not in _RUN_IDENTITY_ATTRIBUTES]
            status = "changed" if notes else "same"
        rows.append({"hop": index + 1, "status": status, "notes": notes,
                     "before": b, "after": a})
    return rows


def format_run_comparison(before_n: int, after_n: int,
                          rows: Sequence[dict]) -> str:
    """Render :func:`compare_runs` as the block the `cmp` key prints."""
    before_hops = sum(1 for row in rows if row["before"])
    after_hops = sum(1 for row in rows if row["after"])
    differing = [row for row in rows if row["status"] != "same"]
    lines = [f"Run {before_n} vs run {after_n}: {before_hops} hop(s) -> "
             f"{after_hops}; {len(differing)} differ."]
    if not differing:
        lines.append("      Identical journey — nothing this FlowFile can "
                     "see changed.")
        return "\n".join(lines)
    for row in differing:
        hop = row["after"] or row["before"]
        label = f"{hop.get('component') or '(flow)'} [{hop.get('event_type', '')}]"
        if row["status"] == "only_after":
            lines.append(f"{row['hop']:>3}. + only in run {after_n}: {label}")
        elif row["status"] == "only_before":
            lines.append(f"{row['hop']:>3}. - only in run {before_n}: {label}")
        else:
            lines.append(f"{row['hop']:>3}. {label}")
            lines += [f"       {note}" for note in row["notes"]]
    return "\n".join(lines)


# ------------------------------------------------------------------- mutes

_MUTE_KINDS = ("uuid", "rel", "dest", "queue")
_MUTE_ALIASES = {
    "relationship": "rel", "rels": "rel", "r": "rel",
    "destination": "dest", "processor": "dest", "proc": "dest", "d": "dest",
    "connection": "queue", "conn": "queue", "q": "queue",
    "id": "uuid", "child": "uuid", "u": "uuid",
}
_MUTE_LABELS = {"uuid": "child UUID", "rel": "relationship",
                "dest": "destination", "queue": "connection id"}


def parse_mute_spec(spec: str) -> Tuple[str, str]:
    """``"rel:failure"`` → ``("rel", "failure")``; guess when unprefixed.

    A bare value that looks like a UUID mutes that child; anything else is
    treated as a relationship name, which is the common pre-emptive case
    (``--mute failure``).
    """
    text = (spec or "").strip()
    if not text:
        raise FollowError("empty mute spec — try 'failure', 'rel:failure', "
                          "'dest:PutFile', 'queue:<conn-id>' or a child UUID")
    kind, _, value = text.partition(":")
    kind, value = kind.strip().lower(), value.strip()
    if not value:
        bare = text
        return ("uuid" if _UUID_RE.match(bare) else "rel"), bare
    kind = _MUTE_ALIASES.get(kind, kind)
    if kind not in _MUTE_KINDS:
        raise FollowError(
            f"unknown mute kind {kind!r} — use one of "
            + ", ".join(f"{k} ({_MUTE_LABELS[k]})" for k in _MUTE_KINDS))
    return kind, value


class BranchMutes:
    """Which fork branches the stepper ignores — a **view decision only**.

    Nothing here talks to NiFi: muting never stops, disables, empties or
    otherwise touches a component. The muted branch keeps running exactly as
    it would have; the stepper simply stops following and rendering it, and
    ``unmute`` brings it back (branch records are kept, never dropped).

    Rules are matched against a branch record's child UUID, the relationship
    it left the fork on, its destination (name or id) and the connection it
    is queued in. Relationship and destination matching is case-insensitive:
    ``--mute FAILURE`` is what a tired analyst types.
    """

    def __init__(self, rules: Optional[Dict[str, Iterable[str]]] = None) -> None:
        self.rules: Dict[str, List[str]] = {k: [] for k in _MUTE_KINDS}
        for kind, values in (rules or {}).items():
            if kind in self.rules:
                self.rules[kind] = list(values)

    def add(self, spec: str) -> Tuple[str, str]:
        kind, value = parse_mute_spec(spec)
        if value not in self.rules[kind]:
            self.rules[kind].append(value)
        return kind, value

    def remove(self, spec: str) -> Tuple[str, str]:
        """Drop a rule. An unprefixed spec is looked for in every kind.

        ``m dest:Sink`` then ``u Sink`` has to work: the user is reading a
        branch list, not remembering which flavour of mute they typed.
        """
        kind, value = parse_mute_spec(spec)
        kinds = [kind] if ":" in spec else list(_MUTE_KINDS)
        for candidate in kinds:
            matches = [v for v in self.rules[candidate]
                       if v.lower() == value.lower()]
            if matches:
                for v in matches:
                    self.rules[candidate].remove(v)
                return candidate, value
        raise FollowError(
            f"nothing muted matches {value!r} — active mutes: "
            + (self.describe() or "none"))

    def clear(self) -> None:
        for kind in self.rules:
            self.rules[kind] = []

    def match(self, branch: dict) -> Optional[str]:
        """The rule muting this branch (``"rel:failure"``), or ``None``."""
        for value in self.rules["uuid"]:
            if branch.get("uuid") == value:
                return f"uuid:{value}"
        # One connection can carry several relationships; muting any of them
        # mutes the branch.
        rels = {r.strip().lower()
                for r in (branch.get("relationship") or "").split(",")} - {""}
        for value in self.rules["rel"]:
            if value.lower() in rels:
                return f"rel:{value}"
        targets = {str(branch.get("destination") or "").lower(),
                   str(branch.get("destination_id") or "").lower()} - {""}
        for value in self.rules["dest"]:
            if value.lower() in targets:
                return f"dest:{value}"
        for value in self.rules["queue"]:
            if branch.get("queue_id") == value:
                return f"queue:{value}"
        return None

    def describe(self) -> str:
        parts = [f"{kind}:{value}" for kind in _MUTE_KINDS
                 for value in self.rules[kind]]
        return "  ".join(parts)

    def to_dict(self) -> Dict[str, List[str]]:
        return {k: list(v) for k, v in self.rules.items() if v}


# ----------------------------------------------------------------- session


def default_session_dir() -> Path:
    """Where sessions are written (git-ignored ``.niflow-follow/``)."""
    return Path(os.environ.get("NIFLOW_FOLLOW_DIR", ".niflow-follow"))


class FollowSession:
    """Everything a stepping session knows, and the file it survives in.

    The follower is behaviour; this is state — the branch tree (child UUID →
    parent, relationship, queue, mute status), the ordered hop history per
    branch, the provenance cursor, the mute rules, and the pre-quiesce
    RUNNING set. It is written after every step, so a refreshed GUI page or a
    restarted process re-attaches to the same journey instead of losing it.
    It also holds the *fixture* (what was injected, and the injector minting
    it) and the runs already finished, which is what makes replay-after-fix a
    comparison rather than a fresh start.
    """

    #: 2 added watches/fixture/injector/runs. Older files still load: every
    #: one of those fields defaults to empty, so a v1 session resumes as a
    #: journey that simply has no fixture to replay.
    VERSION = 2

    def __init__(self, group: str, pg_id: str, path: Optional[Path] = None,
                 session_id: Optional[str] = None) -> None:
        self.group = group
        self.pg_id = pg_id
        self.path = Path(path) if path else None
        self.id = session_id or datetime.now().strftime("%Y%m%d-%H%M%S")
        self.started = datetime.now().isoformat(timespec="seconds")
        self.current: Optional[str] = None
        self.last_event_id: int = -1
        self.prior_running: List[dict] = []
        self.mutes = BranchMutes()
        self.branches: Dict[str, dict] = {}
        self.order: List[str] = []
        self.watches: List[str] = []
        self.fixture: Optional[dict] = None      # what to inject on replay
        self.injector: Optional[dict] = None     # the temp components doing it
        self.runs: List[dict] = []               # finished runs, for `cmp`

    # --- branches -------------------------------------------------------

    def branch(self, uuid: str, **fields: Any) -> dict:
        """Get (creating if needed) the record for one followed UUID."""
        record = self.branches.get(uuid)
        if record is None:
            record = {
                "uuid": uuid, "parent": None, "relationship": "", "origin": "",
                "event_id": None, "queue_id": "", "queue": "",
                "destination": "", "destination_id": "", "state": "live",
                "muted_by": None, "hops": [], "baseline": None,
                "baseline_size": None,
                "created": datetime.now().isoformat(timespec="seconds"),
            }
            self.branches[uuid] = record
            self.order.append(uuid)
        record.update({k: v for k, v in fields.items() if v is not None})
        return record

    # --- watches --------------------------------------------------------

    def add_watch(self, spec: str) -> bool:
        """Start watching an attribute (or glob); False if already watched."""
        spec = spec.strip()
        if not spec or spec in self.watches:
            return False
        self.watches.append(spec)
        return True

    def remove_watch(self, spec: str) -> bool:
        """Stop watching; False if it was not being watched."""
        spec = spec.strip()
        if spec not in self.watches:
            return False
        self.watches.remove(spec)
        return True

    # --- runs -----------------------------------------------------------

    def flat_hops(self) -> List[dict]:
        """Every hop of the run in progress, in branch order."""
        return [hop for _, hop in self.all_hops()]

    def run_hops(self, n: int) -> List[dict]:
        """Every hop of finished run ``n`` (1-based), in branch order."""
        run = self.runs[n - 1]
        return [hop for uuid in run["order"] for hop in run["branches"][uuid]["hops"]]

    def archive_run(self) -> dict:
        """Freeze the journey so far as a finished run and start a clean one.

        Replay compares what happens *this* time against what happened last
        time, so the hops cannot simply be dropped. Mutes and watches survive
        deliberately: they are how you are looking at the flow, not what the
        flow did.
        """
        run = {
            "n": len(self.runs) + 1,
            "at": datetime.now().isoformat(timespec="seconds"),
            "fixture": copy.deepcopy(self.fixture),
            "order": list(self.order),
            "branches": copy.deepcopy(self.branches),
        }
        self.runs.append(run)
        self.branches, self.order = {}, []
        self.current, self.last_event_id = None, -1
        return run

    def record_hops(self, uuid: str, hops: Sequence[dict]) -> None:
        """Append hops to a branch's history, diffed against its baseline."""
        record = self.branch(uuid)
        baseline, size = annotate_hops(
            hops, record.get("baseline"), record.get("baseline_size"))
        record["hops"].extend(hops)
        record["baseline"], record["baseline_size"] = baseline, size

    def history(self, uuid: Optional[str] = None) -> List[dict]:
        """One branch's hops (default: the current branch), oldest first."""
        target = uuid or self.current
        record = self.branches.get(target or "")
        return list(record["hops"]) if record else []

    def all_hops(self) -> List[Tuple[str, dict]]:
        """``(uuid, hop)`` for every branch in the order branches appeared."""
        return [(u, h) for u in self.order for h in self.branches[u]["hops"]]

    def live_branches(self) -> List[dict]:
        return [self.branches[u] for u in self.order
                if self.branches[u]["state"] == "live"]

    def reapply_mutes(self) -> List[dict]:
        """Re-classify every branch against the current rules.

        Returns the records whose state changed — muting is retroactive
        (mute a branch you already forked into) and reversible.
        """
        changed = []
        for uuid in self.order:
            record = self.branches[uuid]
            if record["state"] == "done":
                continue
            rule = self.mutes.match(record)
            state = "muted" if rule else "live"
            if state != record["state"] or rule != record["muted_by"]:
                record["state"], record["muted_by"] = state, rule
                changed.append(record)
        return changed

    # --- persistence ----------------------------------------------------

    def to_dict(self) -> dict:
        return {
            "version": self.VERSION, "id": self.id, "group": self.group,
            "pg_id": self.pg_id, "started": self.started,
            "updated": datetime.now().isoformat(timespec="seconds"),
            "current": self.current, "last_event_id": self.last_event_id,
            "prior_running": self.prior_running,
            "mutes": self.mutes.to_dict(),
            "order": list(self.order), "branches": self.branches,
            "watches": list(self.watches), "fixture": self.fixture,
            "injector": self.injector, "runs": self.runs,
        }

    @classmethod
    def from_dict(cls, data: dict, path: Optional[Path] = None) -> "FollowSession":
        session = cls(data.get("group", ""), data.get("pg_id", ""), path=path,
                      session_id=data.get("id"))
        session.started = data.get("started", session.started)
        session.current = data.get("current")
        session.last_event_id = int(data.get("last_event_id", -1))
        session.prior_running = list(data.get("prior_running") or [])
        session.mutes = BranchMutes(data.get("mutes") or {})
        session.branches = dict(data.get("branches") or {})
        session.order = [u for u in (data.get("order") or [])
                         if u in session.branches]
        session.watches = list(data.get("watches") or [])
        session.fixture = data.get("fixture") or None
        session.injector = data.get("injector") or None
        session.runs = list(data.get("runs") or [])
        return session

    def save(self) -> Optional[Path]:
        """Write the session file (best effort — a full disk must not end a run)."""
        if self.path is None:
            return None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.to_dict(), indent=1), encoding="utf-8")
            tmp.replace(self.path)
        except OSError as exc:  # a session file is a convenience, not the job
            logger.warning("Could not save follow session %s: %s", self.path, exc)
            return None
        return self.path

    @classmethod
    def load(cls, path: Path) -> "FollowSession":
        path = Path(path)
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise FollowError(f"cannot read follow session {path}: {exc}") from exc
        return cls.from_dict(data, path=path)

    @classmethod
    def open(cls, group: str, pg_id: str,
             directory: Optional[Path] = None) -> "FollowSession":
        """A fresh session backed by a file under ``directory``."""
        base = Path(directory) if directory else default_session_dir()
        session = cls(group, pg_id)
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", group or "root")[:40]
        session.path = base / f"{safe}-{session.id}.json"
        return session

    @classmethod
    def latest(cls, group: Optional[str] = None,
               directory: Optional[Path] = None) -> Optional["FollowSession"]:
        """The most recently written session (optionally for one group)."""
        base = Path(directory) if directory else default_session_dir()
        if not base.is_dir():
            return None
        newest, newest_time = None, -1.0
        for path in base.glob("*.json"):
            try:
                session = cls.load(path)
            except FollowError:
                continue
            if group and session.group != group and session.pg_id != group:
                continue
            mtime = path.stat().st_mtime
            if mtime > newest_time:
                newest, newest_time = session, mtime
        return newest


# ----------------------------------------------------------- start points


def entry_points(client: Any, group: str) -> List[dict]:
    """The plausible places a journey can start in ``group``.

    Three kinds, in the order they are worth trying:

    * ``queue`` — a connection that already holds FlowFiles (start here and
      nothing has to be minted);
    * ``source`` — a processor with no inbound connection, which run-once can
      mint a FlowFile from;
    * ``input_port`` — a group entry point; there is nothing to run, so the
      stepper picks up whatever is queued downstream of it.

    Every entry carries ``kind``/``id``/``label``/``detail`` plus the
    ``queue_ids`` to look in once a file exists, which is all
    :meth:`FlowFollower.start_from` needs.
    """
    pg_id = client.resolve_group(group)
    queues = client.list_queues(pg_id)
    procs = client.find_processors(group=pg_id)

    # An endpoint is "fed" if any connection lands on it. Ids are the truth;
    # names are the fallback for servers/status snapshots that omit ids.
    fed_ids = {q.get("destination_id") for q in queues if q.get("destination_id")}
    fed_names = {q.get("destination") for q in queues if q.get("destination")}

    def outbound(comp_id: str, name: str) -> List[str]:
        return [q["id"] for q in queues
                if (q.get("source_id") == comp_id
                    or (not q.get("source_id") and q.get("source") == name))]

    out: List[dict] = []
    for q in sorted(queues, key=lambda q: -(q.get("queued") or 0)):
        if not q.get("queued"):
            continue
        out.append({
            "kind": "queue", "id": q["id"],
            "label": f"{q.get('source', '?')} -> {q.get('destination', '?')}",
            "detail": str(q.get("queued_label") or q.get("queued") or ""),
            "path": q.get("path", ""), "queue_ids": [q["id"]],
            "group_id": q.get("group_id", ""),
        })
    for proc in procs:
        if proc["id"] in fed_ids or (not fed_ids and proc["name"] in fed_names):
            continue
        out.append({
            "kind": "source", "id": proc["id"],
            "label": f"{proc['path']}/{proc['name']}".lstrip("/"),
            "detail": proc.get("type", "").rsplit(".", 1)[-1],
            "path": proc.get("path", ""),
            "queue_ids": outbound(proc["id"], proc["name"]),
            "group_id": proc.get("group_id", ""),
        })
    for port in _input_ports(client, pg_id):
        out.append({
            "kind": "input_port", "id": port["id"],
            "label": f"{port['path']}/{port['name']}".lstrip("/"),
            "detail": "waits for an upstream file",
            "path": port.get("path", ""),
            "queue_ids": outbound(port["id"], port["name"]),
            "group_id": port.get("group_id", ""),
        })
    return out


def _input_ports(client: Any, pg_id: str) -> List[dict]:
    """Input ports under a group — tolerated as empty on odd servers."""
    lister = getattr(client, "list_ports", None)
    if lister is None:
        return []
    try:
        return [p for p in lister(pg_id) if p.get("kind") == "input_port"]
    except Exception as exc:  # a missing port listing must not kill the menu
        logger.debug("Could not list input ports: %s", exc)
        return []


class FlowFollower:
    """Steps one FlowFile through a quiesced group via run-once.

    The loop each step runs: find the queue holding the followed uuid →
    run-once that queue's destination processor → collect the provenance
    events the run produced (``flowfile_events_since`` above the last event
    id already rendered) → diff them against the branch's previous hop. All
    NiFi access goes through the injected ``client`` (a
    :class:`~niflow.client.NiFiClient`), so this class is plain orchestration
    — and unit-testable with a stub.

    State (branches, history, mutes, cursor) lives in :class:`FollowSession`,
    which is saved after every mutation when it has a path.
    """

    def __init__(
        self,
        client: Any,
        group: str,
        *,
        max_hops: int = 50,
        poll_timeout: float = _PROV_TIMEOUT_S,
        poll_interval: float = _PROV_INTERVAL_S,
        run_attempts: int = _RUN_ATTEMPTS,
        max_runs: int = _RUN_CAP,
        step_timeout: float = _STEP_TIMEOUT_S,
        session: Optional[FollowSession] = None,
    ) -> None:
        self.client = client
        self.group = group
        self.pg_id = session.pg_id if session else client.resolve_group(group)
        self.max_hops = max_hops
        self.poll_timeout = poll_timeout
        self.poll_interval = poll_interval
        self.run_attempts = max(1, run_attempts)
        self.max_runs = max(1, max_runs)
        self.step_timeout = step_timeout
        self.session = session or FollowSession(group, self.pg_id)
        self._rel_cache: Dict[str, str] = {}  # conn id -> relationship label

    # --- session-backed state (kept as attributes for callers' sanity) ---

    @property
    def uuid(self) -> Optional[str]:
        return self.session.current

    @uuid.setter
    def uuid(self, value: Optional[str]) -> None:
        self.session.current = value

    @property
    def last_event_id(self) -> int:
        return self.session.last_event_id

    @last_event_id.setter
    def last_event_id(self, value: int) -> None:
        self.session.last_event_id = int(value)

    @property
    def prior_running(self) -> List[dict]:
        return self.session.prior_running

    @prior_running.setter
    def prior_running(self, value: List[dict]) -> None:
        self.session.prior_running = list(value)

    @property
    def mutes(self) -> BranchMutes:
        return self.session.mutes

    @property
    def pending_children(self) -> List[str]:
        """Live branches other than the one being stepped (muted excluded)."""
        return [b["uuid"] for b in self.session.live_branches()
                if b["uuid"] != self.uuid]

    def _save(self) -> None:
        self.session.save()

    # ----------------------------------------------------------- quiesce

    def quiesce(self, remember: bool = True) -> int:
        """Stop everything in the group, remembering what was RUNNING.

        Stop-group (not per-processor stops) so ports and nested groups
        quiesce too — a running port would move the file on its own and
        break the stepping. Returns how many processors were running.

        ``remember=False`` keeps the RUNNING set a previous session recorded:
        re-quiescing on ``--resume`` must not conclude that "nothing was
        running", which would make ``--restore`` a no-op.
        """
        procs = self.client.find_processors(group=self.pg_id)
        running = [p for p in procs if p.get("state") == "RUNNING"]
        # Ports count. stop-group stops them too, and a session that only
        # remembered processors used to hand the group back with every port
        # stopped — a flow that silently stops moving data after a debugging
        # session is worse than one that never stepped.
        running += [dict(p, is_port=True) for p in self._ports()
                    if p.get("state") == "RUNNING"]
        if remember or not self.prior_running:
            self.prior_running = running
        try:
            self.client.stop_group(self.pg_id)
        except Exception as exc:
            raise FollowError(
                f"could not stop {self.group!r} ({exc}) — the stepper needs the "
                "group quiesced; check you have write access to it"
            ) from exc
        self._save()
        return len(running)

    def _ports(self) -> List[dict]:
        """Input and output ports under the group; empty on odd servers."""
        lister = getattr(self.client, "list_ports", None)
        if lister is None:
            return []
        try:
            return list(lister(self.pg_id))
        except Exception as exc:  # never worth failing a quiesce over
            logger.debug("Could not list ports: %s", exc)
            return []

    def restore(self) -> int:
        """Restart exactly what was RUNNING before quiesce — ports included."""
        restored = 0
        for comp in self.prior_running:
            try:
                if comp.get("is_port") or comp.get("kind") in (
                        "input_port", "output_port"):
                    self.client.set_port_state(
                        comp.get("kind", "input_port"), comp["id"], "RUNNING")
                else:
                    self.client.start_processor(comp["id"])
                restored += 1
            except Exception as exc:  # invalid/disabled components refuse
                logger.warning("Could not restart %s: %s", comp.get("name"), exc)
        return restored

    # ----------------------------------------------------- picking a file

    def kick_source(self, source: str) -> str:
        """Run-once a source processor (by name, ``path/name``, or id)."""
        matches = [
            p for p in self.client.find_processors(group=self.pg_id)
            if source in (p["id"], p["name"],
                          f"{p['path']}/{p['name']}".lstrip("/"))
        ]
        if not matches:
            raise FollowError(
                f"no processor named {source!r} in {self.group!r} — "
                "`niflow follow <group> --list` shows the start points")
        if len(matches) > 1:
            paths = ", ".join(
                f"{m['path']}/{m['name']}".lstrip("/") for m in matches)
            raise FollowError(
                f"{source!r} is ambiguous ({paths}); use a path or id")
        self._run_once(matches[0]["id"], matches[0]["name"])
        return matches[0]["name"]

    # ------------------------------------------------- fixture injection

    def inject(self, target: str, content: str = "",
               attributes: Optional[Dict[str, str]] = None,
               wait: Optional[float] = None) -> dict:
        """Mint a FlowFile of your own choosing and follow it.

        Every other start point waits for the flow to produce something: a
        file already queued, a source worth running, a port someone else
        feeds. This one is the debugger's own input — the content and the
        attributes you want, at the component you care about — minted by the
        same temporary GenerateFlowFile the test harness uses
        (:func:`niflow.testing.inject_flowfile`), which is created stopped and
        triggered exactly once so nothing else on the quiesced canvas moves.

        The injector is recorded on the session: replaced on replay, removed
        when the session ends and the fixture has left its queue.

        ``target`` is a processor or a **nested** input port, by name,
        ``path/name`` or id.
        """
        resolved = self._injection_target(target)
        label = f"{resolved['path']}/{resolved['name']}".lstrip("/")
        # GenerateFlowFile names its file after a fresh UUID, which would make
        # every replay differ at every hop for a reason that is not the flow's.
        # Pinning it keeps the comparison honest — and a rename BY the flow is
        # still visible, which is why filename is not simply ignored later.
        attributes = {"filename": FIXTURE_FILENAME, **(attributes or {})}
        self.cleanup_injector()  # never leave two injectors on one canvas
        proc_id, conn_id = inject_flowfile(
            self.client, resolved, content=content, attributes=attributes,
            start_target=False)
        self.session.injector = {"processor": proc_id, "connection": conn_id,
                                 "target": resolved["id"], "label": label}
        self.session.fixture = {"target": target, "kind": resolved["kind"],
                                "label": label, "content": content,
                                "attributes": dict(attributes or {})}
        self._save()
        wait = self.poll_timeout * 4 if wait is None else wait
        try:
            picked = self.pick_flowfile(queue_ids=[conn_id], wait=wait)
        except FollowError as exc:
            errors = self._validation_errors(proc_id)
            self.cleanup_injector()
            detail = f" ({'; '.join(errors)})" if errors else ""
            raise FollowError(
                f"the injector minted nothing into {label!r}{detail} — "
                "nothing was left on the canvas, so it is safe to retry") from exc
        picked["injected"] = label
        picked["node"] = (picked.get("flowfile") or {}).get("node_address", "")
        picked["siblings"] = self._injector_siblings(conn_id, picked["uuid"])
        return picked

    def _injector_siblings(self, conn_id: str, uuid: str) -> int:
        """How many *other* FlowFiles that one run-once minted.

        On a cluster the answer is not zero: **run-once fires on every node**
        (verified on a live two-node 1.24.0 — one trigger, one FlowFile per
        node), so an injector mints a twin the stepper is not following. It is
        not a problem — the twins sit in the injector's queue and go when the
        injector does — but a stepper that silently followed one of two files
        would be lying by omission.
        """
        try:
            return max(len(self._list_flowfiles(conn_id)) - 1, 0)
        except Exception:  # a counting nicety must never fail an injection
            return 0

    def _injection_target(self, spec: str) -> dict:
        """Resolve an injection target to ``{kind,id,name,path,group_id,...}``."""
        candidates = [
            {"kind": "processor", "id": proc["id"], "name": proc["name"],
             "path": proc.get("path", ""), "group_id": proc.get("group_id", "")}
            for proc in self.client.find_processors(group=self.pg_id)
        ] + [
            {"kind": "input_port", "id": port["id"], "name": port["name"],
             "path": port.get("path", ""), "group_id": port.get("group_id", "")}
            for port in _input_ports(self.client, self.pg_id)
        ]
        matches = [c for c in candidates
                   if spec in (c["id"], c["name"],
                               f"{c['path']}/{c['name']}".lstrip("/"))]
        if not matches:
            raise FollowError(
                f"no processor or input port named {spec!r} in {self.group!r} "
                "— `niflow follow <group> --list` shows what is there")
        if len(matches) > 1:
            paths = ", ".join(f"{m['path']}/{m['name']}".lstrip("/")
                              for m in matches)
            raise FollowError(f"{spec!r} is ambiguous ({paths}); use a path or id")
        target = matches[0]
        if target["kind"] == "input_port":
            # A port is fed from outside its own group, so the injector has to
            # live in the parent. For the followed group's OWN port that parent
            # is outside the journey: the connection would not even show up in
            # its queues, so the file would be un-followable.
            if target["group_id"] == self.pg_id:
                raise FollowError(
                    f"{spec!r} is {self.group!r}'s own input port — it is fed "
                    "from outside the followed group, so the injector would "
                    "land outside it too. Inject at the processor it feeds.")
            parent = self._parent_group(target["group_id"])
            if not parent:
                raise FollowError(
                    f"could not find the group that feeds input port {spec!r} "
                    "— inject at the processor it feeds instead")
            target["parent_group_id"] = parent
        return target

    def _parent_group(self, pg_id: str) -> str:
        try:
            entity = self.client._get_json(f"/process-groups/{pg_id}")
        except Exception as exc:  # a missing parent is a clear message, not a traceback
            logger.debug("Could not read the parent of %s: %s", pg_id, exc)
            return ""
        return (entity.get("component") or {}).get("parentGroupId") or ""

    def injector_holds_file(self) -> bool:
        """Is the fixture still sitting in the injector's queue, unstepped?"""
        record = self.session.injector
        if not record:
            return False
        try:
            queues = self._queues()
        except FollowError:  # at teardown, an unreadable group means leave it be
            return True
        return any(q.get("id") == record.get("connection") and q.get("queued")
                   for q in queues)

    def cleanup_injector(self) -> bool:
        """Remove this session's temporary injector; False if there was none.

        Best effort by design: a leftover injector is a tidiness problem, and
        failing to delete it must not be the thing that ends a session.
        """
        record = self.session.injector
        if not record:
            return False
        try:
            remove_injector(self.client, record.get("processor"),
                            record.get("connection"))
        except Exception as exc:
            logger.warning("Could not remove the injector (%s) — it is on the "
                           "canvas as %r", exc, INJECTOR_NAME)
            return False
        self.session.injector = None
        self._save()
        return True

    def replay(self, wait: Optional[float] = None) -> dict:
        """Re-inject the recorded fixture and start the journey over.

        The loop this closes: step through, see the bug, fix the flow, push,
        replay — the same bytes and attributes go in at the same place, and
        :func:`compare_runs` says what the fix changed. The finished run is
        archived first so there is something to compare against.
        """
        fixture = self.session.fixture
        if not fixture:
            raise FollowError(
                "nothing to replay — this journey did not start from a "
                "fixture. Start one with `--inject-at NAME` (with --content "
                "and --attr) and replay re-runs exactly that.")
        self.cleanup_injector()
        run = self.session.archive_run()
        self._save()
        picked = self.inject(fixture["target"], content=fixture.get("content", ""),
                             attributes=fixture.get("attributes") or {},
                             wait=wait)
        picked["run"] = run["n"] + 1
        return picked

    # ------------------------------------------------------------ watches

    def watch(self, spec: str) -> bool:
        """Watch an attribute (or glob, or ``@size``) across every hop."""
        added = self.session.add_watch(spec)
        if added:
            self._save()
        return added

    def unwatch(self, spec: str) -> bool:
        removed = self.session.remove_watch(spec)
        if removed:
            self._save()
        return removed

    def watch_table(self, uuid: Optional[str] = None) -> Tuple[List[str], List[dict]]:
        """``(columns, rows)`` for one branch's hop x attribute table."""
        return watch_rows(self.session.history(uuid), self.session.watches)

    def start_from(self, entry: dict, wait: Optional[float] = None) -> dict:
        """Begin a journey at one :func:`entry_points` entry.

        Queues are read straight away; a source is run once first and the
        file is then looked for in *its own* outbound queues (not "the first
        non-empty queue anywhere", which would grab an unrelated leftover).
        """
        kind = entry.get("kind")
        # A minted file lands a moment after run-once returns; give it the
        # same patience the provenance poll gets, doubled.
        wait = self.poll_timeout * 2 if wait is None else wait
        queue_ids = list(entry.get("queue_ids") or [])
        if kind == "queue":
            return self.pick_flowfile(queue_id=entry["id"])
        if kind == "source":
            self._run_once(entry["id"], entry.get("label", ""))
            return self.pick_flowfile(queue_ids=queue_ids or None, wait=wait)
        if kind == "input_port":
            if not queue_ids:
                raise FollowError(
                    f"input port {entry.get('label')!r} feeds nothing — connect "
                    "it, or start from a queue/source instead")
            return self.pick_flowfile(queue_ids=queue_ids, wait=wait)
        raise FollowError(f"unknown start point kind {kind!r}")

    def pick_flowfile(
        self,
        queue_id: Optional[str] = None,
        uuid: Optional[str] = None,
        wait: float = 0.0,
        queue_ids: Optional[Sequence[str]] = None,
    ) -> dict:
        """Choose the FlowFile to follow and baseline its provenance.

        Default: the front of the first non-empty queue. ``queue_id`` (or
        ``queue_ids``) pins where to look, ``uuid`` pins the file. ``wait``
        keeps re-scanning for up to that many seconds (used after a source
        run-once: the file lands a moment later). Events the file already has
        are *not* replayed — the baseline is remembered so stepping renders
        only what's new.
        """
        wanted = list(queue_ids) if queue_ids else ([queue_id] if queue_id else None)
        deadline = time.monotonic() + wait
        while True:
            found = self._find(wanted, uuid)
            if found is not None or time.monotonic() >= deadline:
                break
            time.sleep(self.poll_interval)
        if found is None:
            raise FollowError(self._nothing_found_hint(wanted, uuid, wait))
        self.uuid = found["uuid"]
        prior = self._events_since(-1)
        self.last_event_id = max(
            (int(h["event_id"]) for h in prior), default=-1)
        found["prior_events"] = len(prior)
        queue = found["queue"]
        # The starting file is branch zero: same record shape as a fork child,
        # so history/mute/switch treat it like any other branch.
        self.session.branch(
            found["uuid"], queue_id=queue.get("id"),
            queue=f"{queue.get('source', '?')} -> {queue.get('destination', '?')}",
            destination=queue.get("destination"),
            destination_id=queue.get("destination_id"),
            baseline=(prior[-1].get("attributes") if prior else None),
            baseline_size=(prior[-1].get("size") if prior else None),
        )
        self._save()
        return found

    def _nothing_found_hint(self, queue_ids: Optional[Sequence[str]],
                            uuid: Optional[str], wait: float) -> str:
        where = "the group's queues"
        if queue_ids:
            where = ("that queue" if len(queue_ids) == 1
                     else "those queues")
        if uuid:
            return (f"FlowFile {uuid} is not in {where} — it was already "
                    "processed, dropped or expired; `niflow trace "
                    f"{uuid}` replays whatever provenance still has")
        waited = f" after waiting {wait:g}s" if wait else ""
        return (f"no queued FlowFile in {where}{waited} — run a source once "
                "(--source NAME, or pick one from --list) or queue a file, "
                "then retry")

    def _find(self, queue_ids: Optional[Sequence[str]],
              uuid: Optional[str]) -> Optional[dict]:
        """Locate ``uuid`` (or the front file) among the candidate queues.

        **NiFi lists at most 100 FlowFiles per queue** and will not raise the
        cap, so on a work-sized queue the followed file is very often not in
        the listing at all. When a listing comes back short of the queue's
        own count, a targeted lookup by id (:meth:`locate_flowfile`) settles
        it — that endpoint resolves a FlowFile at any depth.
        """
        deep: List[dict] = []
        for q in self._queues():
            if queue_ids and q["id"] not in queue_ids:
                continue
            if not q.get("queued"):
                continue
            files = self._list_flowfiles(q["id"])
            truncated = len(files) < int(q.get("queued") or 0)
            if uuid:
                for f in files:
                    if f["uuid"] == uuid:
                        return {"uuid": uuid, "queue": q, "flowfile": f}
                if truncated:
                    deep.append(q)  # only worth a lookup if it could hide it
                continue
            if not files:
                continue
            front = min(files, key=lambda s: s.get("position", 0))
            return {"uuid": front["uuid"], "queue": q, "flowfile": front}
        for q in deep:
            summary = self._deep_lookup(q["id"], uuid or "")
            if summary is not None:
                return {"uuid": uuid, "queue": q, "flowfile": summary,
                        "beyond_listing": True}
        return None

    def _deep_lookup(self, conn_id: str, uuid: str) -> Optional[dict]:
        """Ask one queue about one uuid; ``None`` when it isn't there."""
        lookup = getattr(self.client, "locate_flowfile", None)
        if lookup is None or not uuid:
            return None
        try:
            return lookup(conn_id, uuid)
        except Exception as exc:  # a drained/deleted queue must not end a step
            logger.debug("Deep lookup of %s in %s failed: %s", uuid, conn_id, exc)
            return None

    def _queues(self) -> List[dict]:
        try:
            return self.client.list_queues(self.pg_id)
        except Exception as exc:
            raise FollowError(
                f"could not list the queues of {self.group!r} ({exc}) — "
                "is the group still there, and readable?") from exc

    def _list_flowfiles(self, conn_id: str) -> List[dict]:
        """List one queue, tolerating a queue that drains under us.

        A listing request can 404/409 when the connection is emptied (or
        deleted) between the status snapshot and the listing — that is a
        normal race in a live instance, not an error worth ending a session.
        """
        try:
            return self.client.list_flowfiles(conn_id)
        except Exception as exc:
            logger.debug("Queue %s could not be listed: %s", conn_id, exc)
            return []

    def _locate_many(self, uuids: Sequence[str]) -> Dict[str, dict]:
        """Map uuids to the queues holding them in ONE pass over the queues.

        A 50-way split can easily overflow one queue's 100-file listing, so
        queues that answered short are re-asked per missing uuid.
        """
        wanted = set(uuids)
        found: Dict[str, dict] = {}
        if not wanted:
            return found
        deep: List[dict] = []
        for q in self._queues():
            if not wanted:
                break
            if not q.get("queued"):
                continue
            files = self._list_flowfiles(q["id"])
            for f in files:
                if f["uuid"] in wanted:
                    found[f["uuid"]] = q
                    wanted.discard(f["uuid"])
            if len(files) < int(q.get("queued") or 0):
                deep.append(q)
        for q in deep:
            for uuid in list(wanted):
                if self._deep_lookup(q["id"], uuid) is not None:
                    found[uuid] = q
                    wanted.discard(uuid)
        return found

    def _files_ahead(self, queue: dict) -> Optional[int]:
        """How many FlowFiles sit in front of ours in its queue (or ``None``).

        ``None`` means our file is no longer in that queue at all. NiFi's
        ``position`` is **1-based**, so the file at the head reports position
        1 and has nothing ahead of it. ``-1`` is "somewhere past the 100
        FlowFiles NiFi will list" — present, depth unknown.

        Not merely diagnostic: the step loop runs the destination once per
        file ahead, because run-once serves exactly one FlowFile.
        """
        conn_id = queue.get("id", "")
        for summary in self._list_flowfiles(conn_id):
            if summary["uuid"] == self.uuid:
                return max(int(summary.get("position", 0) or 0) - 1, 0)
        if self._deep_lookup(conn_id, self.uuid or "") is not None:
            return -1
        return None

    def locate(self) -> Optional[dict]:
        """The queue currently holding the followed FlowFile, or ``None``."""
        found = self._find(None, self.uuid)
        return found["queue"] if found else None

    # -------------------------------------------------------------- stepping

    def step(self) -> dict:
        """Advance the followed FlowFile one processor; describe what happened.

        Returns ``{"status", "hops", "children", ...}`` where status is:

        * ``advanced`` — the file moved and new hops were indexed (``dropped``
          is True when one of them ended the file's life);
        * ``moved`` — the file moved to another queue but the processor
          recorded **no** provenance event. Very common: a plain
          ``session.transfer`` (RouteOnAttribute, SplitJson's ``failure``, …)
          writes nothing to the provenance repository, and reporting that as
          a stall sends people hunting an indexing problem that isn't there.
          Verified live on 1.24.0;
        * ``crossed`` — the queue fed a port or funnel, which run-once cannot
          drive, so the stepper briefly ran the port to carry the file over
          the group boundary (ports emit no provenance either);
        * ``terminal`` — the queue's destination is something the stepper
          cannot drive at all (a remote port); ``end`` carries the ref;
        * ``stalled`` — the file did not move and no provenance appeared
          (``retryable``: re-poll with :meth:`repoll`);
        * ``blocked`` — the destination cannot run (invalid, disabled, or not
          permitted); ``message`` says which;
        * ``gone`` — the file is in no queue at all (dropped, sent, or
          consumed by the previous hop).
        """
        if not self.uuid:
            raise FollowError("nothing is being followed yet — pick a start "
                              "point first")
        # One listing does both jobs: find the queue AND read our position in
        # it (how many files run-once has to chew through first).
        found = self._find(None, self.uuid)
        queue = found["queue"] if found else None
        if queue is None:
            return self._gone()
        try:
            end = self.client.connection_end(queue["id"], "destination")
        except Exception as exc:
            return {"status": "blocked", "queue": queue, "hops": [],
                    "children": [], "branches": [], "muted": [],
                    "message": f"could not read the queue's destination: {exc}"}
        kind = (end.get("type") or "").upper()
        if kind in ("INPUT_PORT", "OUTPUT_PORT", "FUNNEL"):
            return self._cross(queue, end, kind)
        if kind != "PROCESSOR":
            return {"status": "terminal", "queue": queue, "end": end,
                    "hops": [], "children": [], "branches": [], "muted": [],
                    "message": (
                        f"the queue ends in a {kind.replace('_', ' ').lower() or 'component'} "
                        "the stepper cannot drive — follow the FlowFile on the "
                        "far side, or start a new journey there")}
        summary = found["flowfile"] or {}
        if summary.get("penalized"):
            secs = int(summary.get("penalty_expires_in") or 0) / 1000.0
            return {"status": "blocked", "queue": queue, "processor": end,
                    "hops": [], "children": [], "branches": [], "muted": [],
                    "runs": 0, "penalized": True,
                    "message": (
                        f"this FlowFile is penalised for another {secs:.0f}s — "
                        "NiFi will not hand a penalised file to a processor, so "
                        "run-once cannot move it. Wait it out and step again")}
        refusal = self._refuses_to_run(end)
        if refusal:
            return {"status": "blocked", "queue": queue, "processor": end,
                    "hops": [], "children": [], "branches": [], "muted": [],
                    "runs": 0, "message": refusal}
        ahead = self._ahead_of(summary)
        runs, hops, moved = self._run_until_it_moves(queue, end, ahead)
        ahead_now = self._files_ahead(queue)
        if isinstance(moved, dict) and moved.get("blocked"):
            return dict(moved["blocked"], runs=runs)
        outcome = self._collect(queue=queue, processor=end, hops=hops)
        outcome["runs"] = runs
        outcome["ahead"] = ahead_now
        if hops:
            return outcome
        if ahead_now is None:
            # The file left this queue and nothing was recorded about it. That
            # is a real hop, not a stall — say where it went.
            return self._moved_without_event(queue, end, runs)
        return self._explain_stall(outcome, queue, end, runs, ahead_now)

    # ---------------------------------------------------- step: sub-outcomes

    def _gone(self) -> dict:
        """The file is in no queue — but its ending may still be unrendered.

        A FlowFile consumed by a merge, or auto-terminated by the last hop,
        leaves provenance behind (JOIN, then DROP) that nothing has collected
        yet. Ask once before declaring it gone, so the journey ends with what
        actually happened rather than with a shrug.
        """
        outcome = self._collect(queue=None, processor=None,
                                timeout=self.poll_interval * 3)
        self._finish_branch(self.uuid)
        self._save()
        if outcome["hops"]:
            outcome["status"] = "gone"
            outcome["retryable"] = False
            # NiFi records EXPIRE like any other terminal event, so the
            # journey's own last hop can say why the file vanished — which is
            # a much better answer than "it left every queue" when a short
            # FlowFile Expiration deleted it while you were stepping.
            expired = any(hop.get("event_type") == "EXPIRE"
                          for hop in outcome["hops"])
            outcome["message"] = (
                "the FlowFile EXPIRED out of its queue — it was deleted for "
                "sitting there too long, not consumed" + self._expiry_note()
                if expired else
                "the FlowFile has left every queue — the events above are how "
                "it ended")
            return outcome
        outcome["status"] = "gone"
        outcome["retryable"] = False
        outcome["message"] = ("the FlowFile is in no queue — it was dropped, "
                              "sent onward, or consumed by the last hop"
                              + self._expiry_note())
        return outcome

    def _expiry_note(self) -> str:
        """Did the queue we were watching expire FlowFiles out from under us?

        A queue with a non-zero ``FlowFile Expiration`` deletes files that sit
        in it too long, and NiFi records an EXPIRE event that arrives like any
        other DROP. Quiescing a group to step through it is exactly the
        situation where a file sits still long enough for that to fire, so
        "it vanished" deserves the likeliest explanation attached.
        """
        record = self.session.branches.get(self.uuid or "") or {}
        conn_id = record.get("queue_id")
        if not conn_id:
            return ""
        try:
            component = self.client._get_json(f"/connections/{conn_id}")["component"]
        except Exception as exc:
            logger.debug("Could not read connection %s: %s", conn_id, exc)
            return ""
        expiration = (component.get("flowFileExpiration") or "").strip()
        if not expiration or expiration.startswith("0 "):
            return ""
        return (f". Note the queue it was in expires FlowFiles after "
                f"{expiration} — stepping is slow enough that a short "
                "expiration will delete the file mid-journey")

    def _refuses_to_run(self, end: dict) -> str:
        """Why run-once must NOT be sent to this processor, or ``""``.

        Checked **before** the first run, not after it fails, because NiFi
        does not fail: RUN_ONCE on an invalid processor returns 200 and does
        nothing on 1.24.0, and on 2.7.2 it additionally leaves the processor
        wedged in ``RUN_ONCE`` — where a start 409s and a config change is
        refused ("cannot modify … while the Processor is running"), locking
        the very property that has to be fixed. A debugger must not brick the
        thing it is debugging.
        """
        reader = getattr(self.client, "processor_validation", None)
        if reader is None:
            return ""
        try:
            info = reader(end["id"])
        except Exception as exc:  # never block a step on a diagnostic call
            logger.debug("Could not read %s's validation: %s", end.get("name"), exc)
            return ""
        label = end.get("name") or end.get("id", "")
        if info.get("errors"):
            return (f"{label!r} is invalid, so run-once would do nothing: "
                    + "; ".join(info["errors"][:3])
                    + " — fix it in NiFi, then step again. (Not attempted: NiFi "
                    "accepts RUN_ONCE on an invalid processor and 2.x then "
                    "wedges it in RUN_ONCE, where its config can no longer be "
                    "changed.)")
        if (info.get("state") or "").upper() == "DISABLED":
            return (f"{label!r} is DISABLED — enable it in NiFi before stepping "
                    "through it")
        if (info.get("status") or "").upper() == "VALIDATING":
            if (info.get("state") or "").upper() == "RUN_ONCE":
                return (
                    f"{label!r} is stuck in RUN_ONCE and will never run again. "
                    "This is NiFi's own trap: a RUN_ONCE sent to an invalid "
                    "processor wedges it there. On 1.24.0 nothing clears it — "
                    "stop-group, terminate-threads and run-status STOPPED all "
                    "return 200 and change nothing; on 2.7.2 a group-level stop "
                    "clears it. The processor has to be recreated. (The stepper "
                    "checks validity before every run-once so it cannot cause "
                    "this.)")
            return f"{label!r} is still validating — step again in a moment"
        return ""

    @staticmethod
    def _ahead_of(summary: dict) -> int:
        """Files in front of ours; ``position`` is 1-based. ``-1`` = unknown.

        Unknown means the file was found by a targeted lookup because the
        queue holds more than the 100 NiFi will list, so it is at least 100
        deep — treat that as "many", not as "at the front".
        """
        position = summary.get("position")
        if position is None:
            return -1
        return max(int(position or 0) - 1, 0)

    def _run_until_it_moves(self, queue: dict, end: dict,
                            ahead: int) -> Tuple[int, List[dict], Any]:
        """Run-once the destination until *our* FlowFile leaves the queue.

        run-once serves exactly ONE FlowFile from ONE inbound queue, so a file
        sitting behind others needs one run per file in front of it — a fixed
        attempt count silently gives up on any real queue. The budget is
        therefore ``files ahead + run_attempts``, capped, and the loop skips
        the provenance wait while files are still known to be ahead (checking
        the cheap queue listing instead).
        """
        budget = (self.max_runs if ahead < 0 else
                  min(max(self.run_attempts, ahead + self.run_attempts),
                      self.max_runs))
        deadline = time.monotonic() + self.step_timeout
        runs, hops = 0, []
        if ahead < 0:
            ahead = 1  # unknown depth: use the "chew through the queue" path
        while runs < budget:
            try:
                self._run_once(end["id"], end.get("name", ""))
            except FollowError as exc:
                return runs, [], {"blocked": {
                    "status": "blocked", "queue": queue, "processor": end,
                    "hops": [], "children": [], "branches": [], "muted": [],
                    "message": str(exc)}}
            runs += 1
            if ahead > 0:
                # Still behind other files: don't pay for a provenance poll,
                # just watch our position fall.
                ahead = self._files_ahead(queue)
                if ahead is not None and ahead > 0:
                    if time.monotonic() < deadline:
                        continue
                    break
            else:
                last = runs >= budget
                hops = self._await_hops(None if last else self.poll_interval * 3)
                if hops:
                    break
                ahead = self._files_ahead(queue)
            if ahead is None:
                # Our file left the queue without an indexed event yet: stop
                # running the processor (that would only eat other files) and
                # give the provenance index its full window.
                hops = self._await_hops()
                break
            if time.monotonic() >= deadline:
                break
        return runs, hops, None

    def _cross(self, queue: dict, end: dict, kind: str) -> dict:
        """Carry the FlowFile over a port or funnel — the group boundary.

        This is the case a real work tree hits on hop one or two: nesting is
        four or five levels deep and every level is entered through an input
        port and left through an output port. run-once does not exist for
        ports, and quiesce stopped them, so the journey used to dead-end at
        the first boundary with "terminal".

        A port is therefore started, watched until *our* FlowFile leaves the
        queue, and stopped again — measured at ~0.3s on 1.24.0. A funnel has
        no run state at all and moves files by itself, so it is only watched.
        Neither emits a provenance event (verified live on 1.24.0), so the hop
        is synthesised.

        **The honest caveat**: a running port drains the whole queue, not just
        the followed file. The group is quiesced, so nothing downstream
        consumes them — they land in the next queue and stay there — but a
        port hop is not as surgical as run-once.
        """
        name = end.get("name") or kind.replace("_", " ").lower()
        started = False
        if kind in ("INPUT_PORT", "OUTPUT_PORT"):
            setter = getattr(self.client, "set_port_state", None)
            if setter is None:
                return {"status": "terminal", "queue": queue, "end": end,
                        "hops": [], "children": [], "branches": [], "muted": [],
                        "message": f"cannot drive port {name!r} with this client"}
            try:
                setter(kind.lower(), end["id"], "RUNNING")
                started = True
            except Exception as exc:
                return {"status": "blocked", "queue": queue, "end": end,
                        "hops": [], "children": [], "branches": [], "muted": [],
                        "message": (
                            f"could not start port {name!r} to carry the FlowFile "
                            f"across the group boundary ({exc}) — the port may be "
                            "disabled, invalid, or need 'modify the component'")}
        try:
            landed = self._wait_until_it_leaves(queue)
        finally:
            if started:
                try:
                    setter(kind.lower(), end["id"], "STOPPED")
                except Exception as exc:  # leaving a port running is the real harm
                    logger.warning("Could not stop port %s again: %s", name, exc)
        if landed is None and self.locate() is not None:
            return {"status": "stalled", "queue": queue, "end": end,
                    "hops": [], "children": [], "branches": [], "muted": [],
                    "retryable": True,
                    "message": (
                        f"{name!r} did not move the FlowFile — the far side may be "
                        "back-pressured, or the port disabled. Step again to retry")}
        hop = {
            "flowfile_uuid": self.uuid, "own": True, "event_id": self.last_event_id,
            "event_type": "CROSS", "time": "", "component": name,
            "component_id": end.get("id", ""), "group_id": end.get("groupId", ""),
            "component_type": kind, "relationship": "", "size": 0,
            "attributes": {}, "changes": [], "children": [], "parents": [],
            "input_available": False, "output_available": False,
            "content_equal": None, "synthetic": True,
            "lineage": (
                f"crossed {kind.replace('_', ' ').lower()} {name!r} into "
                + (f"{landed.get('path') or 'the parent group'} "
                   f"({landed.get('source', '?')} -> {landed.get('destination', '?')})"
                   if landed else "the next group")
                + " — ports and funnels record no provenance event"),
        }
        self.session.record_hops(self.uuid, [hop])
        if landed is None:
            self._finish_branch(self.uuid)
        self._save()
        return {"status": "crossed", "uuid": self.uuid, "queue": queue,
                "end": end, "processor": end, "hops": [hop], "children": [],
                "branches": [], "muted": [], "dropped": False,
                "retryable": False, "landed": landed,
                "message": hop["lineage"]}

    def _wait_until_it_leaves(self, queue: dict) -> Optional[dict]:
        """Poll until the followed file is out of ``queue``; where it landed."""
        deadline = time.monotonic() + self.poll_timeout
        while True:
            if self._files_ahead(queue) is None:
                return self.locate()
            if time.monotonic() >= deadline:
                return None
            time.sleep(self.poll_interval)

    def _moved_without_event(self, queue: dict, end: dict, runs: int) -> dict:
        """The file left the queue with nothing recorded — say where it is now."""
        landed = self.locate()
        hop = {
            "flowfile_uuid": self.uuid, "own": True, "event_id": self.last_event_id,
            "event_type": "MOVED", "time": "", "component": end.get("name", ""),
            "component_id": end.get("id", ""), "group_id": end.get("groupId", ""),
            "component_type": "", "relationship":
                self._queue_relationship(landed.get("id")) if landed else "",
            "size": 0, "attributes": {}, "changes": [], "children": [],
            "parents": [], "input_available": False, "output_available": False,
            "content_equal": None, "synthetic": True,
        }
        where = (f"{landed.get('source', '?')} -> {landed.get('destination', '?')}"
                 if landed else "another queue")
        hop["lineage"] = (
            f"{end.get('name', 'the processor')!r} transferred this FlowFile to "
            f"{where} without recording a provenance event")
        self.session.record_hops(self.uuid, [hop])
        self._save()
        return {
            "status": "moved", "uuid": self.uuid, "queue": queue,
            "processor": end, "hops": [hop], "children": [], "branches": [],
            "muted": [], "dropped": False, "retryable": False, "runs": runs,
            "ahead": None, "landed": landed,
            "message": (
                f"{end.get('name', '?')!r} moved the FlowFile to {where} but "
                "recorded no provenance event — plain transfers (RouteOnAttribute, "
                "SplitJson's failure, most 'just route it' processors) write "
                "nothing to the provenance repository. Step again to keep going."),
        }

    def _explain_stall(self, outcome: dict, queue: dict, end: dict,
                       runs: int, ahead: Optional[int]) -> dict:
        """Nothing moved. Name the actual reason before blaming the index.

        On 1.24.0 a run-once against an **invalid** processor returns 200 and
        does nothing, so "it refused to run" never fires and the honest
        diagnosis has to be pulled from the validation status.
        """
        errors = self._validation_errors(end["id"])
        if errors:
            outcome["status"] = "blocked"
            outcome["retryable"] = False
            outcome["runs"] = runs
            outcome["message"] = (
                f"{end.get('name', '?')!r} cannot run: " + "; ".join(errors[:3])
                + " — NiFi accepts run-once on an invalid processor and silently "
                "does nothing (1.24). Fix it, then step again.")
            return outcome
        # Back pressure: the destination ran and had nowhere to put the
        # result. Running it again cannot help, and the fix is downstream —
        # so saying "ran it 8x, nothing moved" would send the reader hunting
        # in the wrong place entirely.
        blocked = self._back_pressure_note(end, queue)
        if blocked:
            outcome["status"] = "blocked"
            outcome["retryable"] = False
            outcome["runs"] = runs
            outcome["message"] = blocked
            return outcome
        # A merging destination has not failed to run — it ran and correctly
        # emitted nothing, because its bin is not full. Saying "ran it 8x and
        # nothing moved" there sends the reader hunting an indexing problem.
        binning = self._binning_note(end, queue)
        if binning:
            outcome["runs"] = runs
            outcome["retryable"] = False
            outcome["message"] = binning
            return outcome
        deep = ahead == -1
        behind = ("" if not ahead else (
            " — it is still behind other FlowFiles in the queue"
            if deep else f" — {ahead} FlowFile(s) are still queued ahead of it"))
        outcome["message"] = (
            f"ran {end.get('name', '?')!r} {runs}x and this FlowFile has not "
            f"moved{behind}. NiFi serves one file per run from one inbound queue"
            + (", and a queue this deep needs one run per file" if ahead else "")
            + "; Enter runs it again, `r` re-polls provenance.")
        return outcome

    def repoll(self, timeout: Optional[float] = None) -> dict:
        """Re-check provenance without running anything — the stall retry.

        NiFi 1.24 indexes provenance asynchronously and has been seen to lag
        seconds behind a run-once on a loaded box. Rather than re-running the
        processor (which would move a second FlowFile), the stepper asks the
        index again.
        """
        if not self.uuid:
            raise FollowError("nothing is being followed yet")
        return self._collect(queue=None, processor=None, timeout=timeout)

    def _collect(self, queue: Optional[dict], processor: Optional[dict],
                 timeout: Optional[float] = None,
                 hops: Optional[List[dict]] = None) -> dict:
        hops = self._await_hops(timeout) if hops is None else hops
        for hop in hops:
            # The branch this hop is rendered under; hop["flowfile_uuid"] keeps
            # the truth for a lineage event that belongs to a relative.
            hop["uuid"] = self.uuid
        self.session.record_hops(self.uuid, hops)
        if hops:
            self.last_event_id = max(int(h["event_id"]) for h in hops)
        new_branches, muted = self._register_children(hops)
        # Only *our* DROP ends the branch: a lineage query also returns the
        # merged file's events, and its eventual DROP is not ours.
        dropped = any(h["event_type"] == "DROP" and h.get("own", True)
                      for h in hops)
        if dropped:
            self._finish_branch(self.uuid)
        self._save()
        return {
            "status": "advanced" if hops else "stalled",
            "uuid": self.uuid,
            "queue": queue,
            "processor": processor,
            "hops": hops,
            "children": [c for hop in hops for c in hop["children"]],
            "branches": new_branches,
            "muted": muted,
            "dropped": dropped,
            "retryable": not hops,
            "message": "" if hops else (
                self._binning_note(processor, queue) or
                "no provenance event yet — NiFi indexes provenance "
                "asynchronously (1.24 can lag under load); retry the poll"),
        }

    # "This processor bins FlowFiles before it emits anything", in both key
    # namespaces — a 1.x server keys MergeRecord's thresholds in kebab-case
    # while MergeContent uses the same display names on both lines.
    _BIN_ENTRY_KEYS = ("Minimum Number of Entries",)
    _BIN_RECORD_KEYS = ("Minimum Number of Records", "min-records")
    _BIN_AGE_KEYS = ("Max Bin Age", "max-bin-age")

    def _back_pressure_note(self, end: dict, queue: dict) -> str:
        """Is the destination's own outbound queue full? Then nothing can move.

        NiFi stops a component transferring into a connection that has reached
        its back-pressure threshold, and the component simply produces nothing
        — indistinguishable, from the stepper's side, from "it did not run".
        The threshold that matters is *downstream* of the destination, which
        is why this looks at what the destination feeds rather than at the
        queue being stepped.

        Endpoints are matched by id when the server gives one and by name
        within the same group when it does not: **NiFi's recursive status
        snapshot carries no endpoint ids on either line** (measured on 1.24.0
        and 2.7.2), which is the same fallback ``entry_points`` makes.
        """
        end_id, end_name = end.get("id", ""), end.get("name", "")
        if not (end_id or end_name):
            return ""
        try:
            queues = self._queues()
        except FollowError:
            return ""

        def feeds_it(q: dict) -> bool:
            if q.get("source_id"):
                return q["source_id"] == end_id
            return (bool(end_name) and q.get("source") == end_name
                    and q.get("path", "") == queue.get("path", ""))

        full = [q for q in queues
                if feeds_it(q) and int(q.get("back_pressure_pct") or 0) >= 100]
        if not full:
            return ""
        names = ", ".join(f"{q.get('source', '?')} -> {q.get('destination', '?')}"
                          for q in full[:3])
        return (
            f"{end.get('name', '?')!r} is blocked by BACK PRESSURE: its "
            f"outbound queue is full ({names}). NiFi will not let it transfer "
            "anything until that queue drains, so stepping again cannot move "
            "this FlowFile — drain the queue downstream (or raise its "
            "threshold) first."
        )

    def _binning_note(self, processor: Optional[dict],
                      queue: Optional[dict]) -> str:
        """Why a merging destination did nothing: it is waiting for its bin.

        Following one child into a 50-entry ``MergeContent`` bin, the step
        run-onces the merger, nothing happens — correctly, 49 files are still
        missing — and "stalled" sends the reader hunting a provenance
        indexing problem that isn't there. Naming the threshold is the whole
        fix.

        Deliberately property-driven rather than type-driven: any processor
        that declares a minimum-entries/records threshold bins, including the
        ones work wrote themselves, and no curated list has to be maintained.
        """
        proc_id = (processor or {}).get("id")
        if not proc_id:
            return ""
        try:
            config = self.client.processor_config(proc_id)
        except Exception as exc:  # never let an explanation break a step
            logger.debug("Could not read %s for a binning check: %s", proc_id, exc)
            return ""
        properties = config.get("properties") or {}

        def number(keys) -> Optional[int]:
            for key in keys:
                raw = properties.get(key)
                try:
                    if raw is not None and str(raw).strip():
                        return int(str(raw).strip())
                except ValueError:
                    return None  # an EL/parameter reference — unknowable here
            return None

        def text(keys) -> str:
            for key in keys:
                raw = properties.get(key)
                if raw:
                    return str(raw)
            return ""

        name = (processor or {}).get("name") or config.get("name") or "the destination"
        queued = int((queue or {}).get("queued") or 0)
        age = text(self._BIN_AGE_KEYS)
        age_note = f", or {age} passes" if age else ""
        minimum = number(self._BIN_ENTRY_KEYS)
        if minimum and minimum > queued:
            return (
                f"{name!r} is binning, not stuck: its queue holds {queued} "
                f"FlowFile(s) and it needs {minimum} before it emits anything "
                f"({minimum - queued} more{age_note}). Send more files in, or "
                f"follow another branch — stepping again will not move this one."
            )
        records = number(self._BIN_RECORD_KEYS)
        if records:
            return (
                f"{name!r} is binning by *records*, not FlowFiles: it needs "
                f"{records} record(s) before it emits{age_note}, and the queue "
                f"holds {queued} FlowFile(s). Stepping again will not move it "
                f"until enough records have arrived."
            )
        return ""

    def _await_hops(self, timeout: Optional[float] = None) -> List[dict]:
        deadline = time.monotonic() + (
            self.poll_timeout if timeout is None else timeout)
        while True:
            hops = self._events_since(self.last_event_id)
            if hops or time.monotonic() >= deadline:
                return hops
            time.sleep(self.poll_interval)

    def _events_since(self, after_event_id: int) -> List[dict]:
        try:
            return self.client.flowfile_events_since(self.uuid, after_event_id)
        except Exception as exc:
            status = getattr(exc, "status", None) or getattr(exc, "status_code", None)
            if status == 403:
                raise FollowError(
                    "not allowed to query provenance — the account needs the "
                    "'query provenance' global policy (and 'view provenance' on "
                    f"the components). Without it the stepper cannot see where "
                    f"{self.uuid} went; queue listings still work, so `niflow "
                    "queues` is the fallback") from exc
            if status == 408:
                raise FollowError(
                    f"the provenance query for {self.uuid} timed out — the "
                    "provenance repository is busy or rebuilding its index; "
                    "wait and re-poll (`r`) rather than running the processor "
                    "again") from exc
            raise FollowError(
                f"provenance query for {self.uuid} failed ({exc}) — the "
                "provenance repository may be busy or the user may lack the "
                "'query provenance' policy") from exc

    def _run_once(self, proc_id: str, name: str = "") -> None:
        """Run-once a processor, turning NiFi's refusal into advice."""
        try:
            self.client.run_processor_once(proc_id)
        except Exception as exc:
            raise FollowError(self._run_once_hint(proc_id, name, exc)) from exc

    def _run_once_hint(self, proc_id: str, name: str, exc: Exception) -> str:
        label = name or proc_id
        status = getattr(exc, "status_code", None) or getattr(exc, "status", None)
        if status == 403:
            return (f"not allowed to run {label!r} — the account needs "
                    "'modify the component' on it")
        errors = self._validation_errors(proc_id)
        if errors:
            return (f"{label!r} cannot run: " + "; ".join(errors[:3])
                    + " — fix it in NiFi, then step again")
        return (f"{label!r} refused to run once ({exc}) — it may be disabled, "
                "have no upstream connection, or already be scheduled")

    def _validation_errors(self, proc_id: str) -> List[str]:
        try:
            for entry in self.client.validation_errors(self.pg_id):
                if entry.get("id") == proc_id:
                    return [str(e) for e in entry.get("errors") or []]
        except Exception as exc:  # diagnosis is best-effort
            logger.debug("Could not read validation errors: %s", exc)
        return []

    # ---------------------------------------------------------- branching

    def _register_children(self, hops: Sequence[dict]) -> Tuple[List[dict], List[dict]]:
        """Record the fork children a run produced; return (live, muted).

        The relationship and the spawning component come from the FORK/CLONE
        event; the queue each child landed in is resolved in one pass so a
        24-way fanout costs one queue scan, not 24.

        Lineage events belonging to a *relative* are filtered here. A split
        child's own journey carries its parent's FORK event, whose 50
        ``childUuids`` are its **siblings**, not its children — adopting them
        would fabricate 49 branches. The one relative's event that does count
        is the one that consumed us: a JOIN naming this FlowFile among its
        parents, whose single child is the merged file the journey continues
        in. Both verified live on NiFi 1.24.0.
        """
        spawned: List[Tuple[str, dict]] = []
        for hop in hops:
            if not hop.get("own", True) and self.uuid not in (hop.get("parents") or []):
                continue
            for child in hop.get("children") or []:
                if child not in self.session.branches:
                    spawned.append((child, hop))
        if not spawned:
            return [], []
        located = self._locate_many([c for c, _ in spawned])
        live, muted = [], []
        for child, hop in spawned:
            queue = located.get(child) or {}
            record = self.session.branch(
                child,
                parent=self.uuid,
                relationship=(hop.get("relationship")
                              or self._queue_relationship(queue.get("id"))),
                origin=hop.get("component") or "",
                # JOIN vs FORK/CLONE: a merge is not a fork, and calling it
                # one in the announcement is how "1 branch(es)" reads as a
                # bug rather than as "your file became the merged file".
                spawned_by=hop.get("event_type") or "",
                event_id=hop.get("event_id"),
                queue_id=queue.get("id"),
                queue=(f"{queue.get('source', '?')} -> {queue.get('destination', '?')}"
                       if queue else ""),
                destination=queue.get("destination"),
                destination_id=queue.get("destination_id"),
                # A fork child inherits the parent's attributes, so diffing it
                # against the forking hop shows what the split itself added.
                baseline=dict(hop.get("attributes") or {}),
                baseline_size=hop.get("size"),
            )
            rule = self.mutes.match(record)
            record["state"], record["muted_by"] = ("muted" if rule else "live"), rule
            (muted if rule else live).append(record)
        return live, muted

    def _queue_relationship(self, conn_id: Optional[str]) -> str:
        """The relationship a branch left on, read from the queue it sits in.

        CLONE/FORK events have no ``relationship`` of their own, so the
        branch name comes from the connection's ``selectedRelationships``
        (cached per connection — a 24-way fanout must not cost 24 lookups
        twice).
        """
        if not conn_id:
            return ""
        if conn_id not in self._rel_cache:
            reader = getattr(self.client, "connection_relationships", None)
            try:
                rels = reader(conn_id) if reader else []
            except Exception as exc:  # a label is never worth failing a step
                logger.debug("Could not read relationships of %s: %s",
                             conn_id, exc)
                rels = []
            self._rel_cache[conn_id] = ", ".join(rels)
        return self._rel_cache[conn_id]

    def _finish_branch(self, uuid: Optional[str]) -> None:
        record = self.session.branches.get(uuid or "")
        if record:
            record["state"] = "done"

    def branches(self) -> List[dict]:
        """Every branch this session has seen, in the order it appeared."""
        return [dict(self.session.branches[u], current=(u == self.uuid))
                for u in self.session.order]

    def branch_groups(self) -> List[dict]:
        """Branches folded by (relationship, destination) — the wide-fork view.

        A 50-way split registers 50 branches correctly and then prints 50
        rows, which is technically fine and practically unusable: they are the
        *same* branch fifty times over, and the one thing worth knowing is
        "50 went to Merge on 'split', none are muted". Each group keeps the
        branch numbers `s`/`m` take, so nothing is lost by collapsing.
        """
        groups: Dict[Tuple[str, str], dict] = {}
        for index, branch in enumerate(self.branches(), 1):
            key = (branch.get("relationship") or "", branch.get("destination") or "")
            group = groups.get(key)
            if group is None:
                group = groups[key] = {
                    "relationship": key[0], "destination": key[1],
                    "queue": branch.get("queue") or "", "total": 0,
                    "live": 0, "muted": 0, "done": 0, "current": False,
                    "indexes": [], "sample": [],
                }
            group["total"] += 1
            state = branch.get("state", "")
            if state in ("live", "muted", "done"):
                group[state] += 1
            group["current"] = group["current"] or bool(branch.get("current"))
            group["indexes"].append(index)
            if len(group["sample"]) < 3:
                group["sample"].append({"index": index, "uuid": branch["uuid"]})
        return list(groups.values())

    def mute(self, spec: str) -> dict:
        """Stop following/rendering a branch. **Never touches NiFi.**

        ``spec`` is ``uuid:<id>``, ``rel:<relationship>``, ``dest:<processor>``,
        ``queue:<connection id>``, or a bare value (UUID-shaped → uuid, else
        relationship). Rules apply to branches already forked *and* to ones
        that appear later, which is the common case: ``--mute failure`` before
        the run means the failure branch is never followed at all.

        The muted branch keeps running in NiFi exactly as before — this is a
        view decision, and no mutating REST call is made here or by anything
        it calls.
        """
        kind, value = self.mutes.add(spec)
        changed = self.session.reapply_mutes()
        self._save()
        return {"rule": f"{kind}:{value}",
                "muted": [b["uuid"] for b in changed if b["state"] == "muted"]}

    def unmute(self, spec: str) -> dict:
        """Undo :meth:`mute` — branch records are kept, so it just resumes."""
        kind, value = self.mutes.remove(spec)
        changed = self.session.reapply_mutes()
        self._save()
        return {"rule": f"{kind}:{value}",
                "unmuted": [b["uuid"] for b in changed if b["state"] == "live"]}

    def switch_to(self, uuid: str) -> None:
        """Continue the journey on another branch (a fork/clone child).

        ``last_event_id`` carries over: event ids are monotonic across the
        instance, so the incremental query stays correct across the switch.
        """
        record = self.session.branches.get(uuid)
        if record is None:
            raise FollowError(f"{uuid} is not a branch of this session")
        if record["state"] == "muted":
            record["state"], record["muted_by"] = "live", None  # explicit wins
        self.uuid = uuid
        self._save()

    def next_live(self) -> Optional[str]:
        """The next branch worth stepping (muted and finished ones skipped)."""
        for uuid in self.session.order:
            record = self.session.branches[uuid]
            if record["state"] == "live" and uuid != self.uuid:
                return uuid
        return None

    def auto(self, on_event: Optional[Callable[[dict], None]] = None) -> dict:
        """Step until the journey ends; returns ``{"reason", "steps", ...}``.

        Reasons: ``dropped``/``gone`` (journey complete), ``terminal`` (a
        port/funnel blocks run-once), ``stalled`` (run-once produced no
        events), ``blocked`` (a processor refused to run), ``max-hops``
        (safety cap). When the followed uuid's journey ends but it spawned
        branches, following jumps to the next un-muted one (``on_event`` sees
        a ``{"status": "switched"}`` marker).
        """
        emit = on_event or (lambda outcome: None)
        steps = 0
        while True:
            if steps >= self.max_hops:
                return {"reason": "max-hops", "steps": steps}
            outcome = self.step()
            emit(outcome)
            status = outcome["status"]
            if status == "gone":
                if self._switch_to_next(emit):
                    continue
                return {"reason": "gone", "steps": steps}
            if status == "terminal":
                return {"reason": "terminal", "steps": steps,
                        "end": outcome.get("end")}
            if status == "blocked":
                return {"reason": "blocked", "steps": steps,
                        "message": outcome.get("message", "")}
            if status == "stalled":
                return {"reason": "stalled", "steps": steps,
                        "processor": outcome.get("processor")}
            steps += 1
            if outcome.get("dropped"):
                if self._switch_to_next(emit):
                    continue
                return {"reason": "dropped", "steps": steps}

    def _switch_to_next(self, emit: Callable[[dict], None]) -> bool:
        nxt = self.next_live()
        if nxt is None:
            return False
        self.switch_to(nxt)
        emit({"status": "switched", "uuid": nxt, "hops": [], "children": []})
        return True


# ------------------------------------------------------------------ CLI driver

_REASON_LINES: Dict[str, str] = {
    "dropped": "FlowFile dropped — journey complete.",
    "gone": "FlowFile has left every queue (dropped, sent, or consumed) — "
            "journey complete.",
    "terminal": "Queue feeds a port/funnel — run-once cannot cross it.",
    "stalled": "Run-once produced no new provenance for the followed "
               "FlowFile — stopping (retry interactively with `r`).",
    "blocked": "A processor refused to run — stopping.",
    "max-hops": "Hop cap reached (--max-hops) — stopping.",
}

_KEYS = ("Enter=step  r=retry poll  b=branches [all]  s=switch N  m=mute SPEC  "
         "u=unmute SPEC  a=attrs  c=content  h=history [N]  q=quit\n"
         "        w=watch table [NAME|-NAME|clear]  rr=replay the fixture  "
         "cmp=compare runs [N]")


def format_entry(index: int, entry: dict) -> str:
    """One start point as the picker prints it."""
    where = f"  ({entry['path']})" if entry.get("path") else ""
    return (f"{index:>3}. {entry['kind']:<11}{entry['label']}{where}"
            f"   {entry.get('detail', '')}".rstrip())


def format_branch(index: int, branch: dict) -> str:
    """One branch row: number, uuid, where it came from, state, hop count."""
    mark = "*" if branch.get("current") else " "
    origin = branch.get("origin") or "start"
    if branch.get("origin") and branch.get("relationship"):
        origin = f"{origin} -> {branch['relationship']}"
    state = branch.get("state", "")
    if state == "muted":
        state = f"muted ({branch.get('muted_by')})"
    return (f" {mark}{index:>3}. {branch['uuid']}  {origin:<28} "
            f"{branch.get('queue') or '-':<34} {state:<22} "
            f"{len(branch.get('hops') or [])} hop(s)")


def format_branch_group(group: dict) -> str:
    """One folded row: how many went this way, and how to act on all of them."""
    mark = "*" if group.get("current") else " "
    indexes = group.get("indexes") or []
    span = (f"{indexes[0]}" if len(indexes) == 1
            else f"{indexes[0]}-{indexes[-1]}" if indexes == list(
                range(indexes[0], indexes[-1] + 1))
            else f"{indexes[0]}…{indexes[-1]}")
    where = " -> ".join(x for x in (group.get("relationship"),
                                    group.get("destination")) if x) or "start"
    states = ", ".join(
        f"{group[state]} {state}" for state in ("live", "muted", "done")
        if group.get(state))
    spec = (f"dest:{group['destination']}" if group.get("destination")
            else f"rel:{group['relationship']}" if group.get("relationship")
            else "")
    tail = f"   mute all: m {spec}" if spec and group["total"] > 1 else ""
    sample = ", ".join(f"#{s['index']} {s['uuid'][:8]}…"
                       for s in group.get("sample") or [])
    return (f" {mark}{span:>9}. {group['total']:>3} branch(es)  {where:<34} "
            f"{states:<26}{tail}\n              {sample}")


def follow_command(
    client: Any,
    group: str,
    *,
    uuid: Optional[str] = None,
    queue: Optional[str] = None,
    source: Optional[str] = None,
    start: Optional[str] = None,
    inject_at: Optional[str] = None,
    content: str = "",
    attributes: Optional[Dict[str, str]] = None,
    watch: Sequence[str] = (),
    replay: bool = False,
    list_only: bool = False,
    auto: bool = False,
    max_hops: int = 50,
    restore: bool = False,
    full: bool = False,
    mute: Sequence[str] = (),
    resume: bool = False,
    session_dir: Optional[Path] = None,
    poll_timeout: float = _PROV_TIMEOUT_S,
    input_fn: Callable[[str], str] = input,
    print_fn: Callable[[str], None] = print,
) -> int:
    """The ``niflow follow`` command body (I/O injectable for tests)."""
    if list_only:  # a read-only look: never quiesce for this
        entries = entry_points(client, group)
        if not entries:
            print_fn(f"No start points in {group!r} — no queued FlowFiles, no "
                     "source processors, no input ports.")
            return 1
        print_fn(f"Start points in {group!r}:")
        for i, entry in enumerate(entries, 1):
            print_fn(format_entry(i, entry))
        return 0

    if replay and not resume:
        print_fn("--replay needs a saved session: it re-injects the fixture a "
                 "previous run recorded. Add --resume.")
        return 1

    session = None
    if resume:
        session = FollowSession.latest(group, session_dir)
        if session is None:
            print_fn(f"No saved session for {group!r} — starting a new one.")
    fresh = session is None
    if fresh:
        session = FollowSession.open(group, client.resolve_group(group),
                                     session_dir)
    follower = FlowFollower(client, group, max_hops=max_hops,
                            poll_timeout=poll_timeout, session=session)
    stopped = follower.quiesce(remember=fresh)
    print_fn(f"Quiesced {group!r}: stopped {stopped} running processor(s).")
    for spec in mute:
        rule = follower.mute(spec)["rule"]
        print_fn(f"Muted {rule} — that branch keeps running in NiFi, the "
                 "stepper just ignores it.")
    for spec in watch or ():
        follower.watch(spec)
    if follower.session.watches:
        print_fn(f"Watching {', '.join(follower.session.watches)} — `w` prints "
                 "the hop table.")

    hop_no = 0

    def reset_hop_numbers() -> None:
        """A replay is run 2 of the same journey, so hop 1 is hop 1 again."""
        nonlocal hop_no
        hop_no = 0

    def render(outcome: dict) -> None:
        nonlocal hop_no
        for hop in outcome.get("hops", []):
            hop_no += 1
            print_fn(format_hop(hop_no, hop, full=full))
        status = outcome.get("status")
        if outcome.get("branches") or outcome.get("muted"):
            _announce_fork(outcome, print_fn)
        if status == "switched":
            print_fn(f"Following spawned child {outcome['uuid']}.")
        elif status == "stalled":
            print_fn(outcome.get("message", ""))
        elif status == "blocked":
            print_fn(outcome.get("message", "blocked"))
        elif status == "terminal":
            end = outcome.get("end") or {}
            kind = (end.get("type") or "unknown").replace("_", " ").lower()
            print_fn(f"Queue feeds a {kind} ({end.get('name', '')!r}) — "
                     "run-once cannot cross it.")

    render.reset = reset_hop_numbers  # type: ignore[attr-defined]

    try:
        if resume and not fresh and replay:
            print_fn(f"Resumed session {session.id}; replaying its fixture.")
            _replay(follower, render, print_fn)
        elif resume and not fresh and follower.uuid:
            print_fn(f"Resumed session {session.id} — following "
                     f"{follower.uuid} ({len(session.history())} hop(s) "
                     "already taken).")
        else:
            if replay:
                print_fn("No saved session to replay — starting a fresh "
                         "journey instead.")
            picked = _start(follower, client, group, uuid=uuid, queue=queue,
                            source=source, start=start, inject_at=inject_at,
                            content=content, attributes=attributes, auto=auto,
                            input_fn=input_fn, print_fn=print_fn)
            if picked is None:
                return 1
            q = picked["queue"]
            print_fn(
                f"Following {picked['uuid']} in {q['source']} -> "
                f"{q['destination']}  ({picked['prior_events']} prior "
                f"event(s); `niflow trace {picked['uuid']}` replays them)."
            )
        if auto:
            result = follower.auto(on_event=render)
            print_fn(_REASON_LINES.get(result["reason"], result["reason"]))
        else:
            print_fn(_KEYS)
            _interactive(follower, client, render, input_fn, print_fn)
    finally:
        _retire_injector(follower, print_fn)
        path = follower.session.save()
        if path:
            print_fn(f"Session saved to {path} (`--resume` picks it up).")
        if restore:
            # "component(s)": quiesce stops ports too, so restore puts them
            # back as well — a session must not leave a port stopped.
            print_fn(f"Restored {follower.restore()} previously-running "
                     "component(s).")
        else:
            print_fn(f"Group left stopped — `niflow start {group!r}` to "
                     "resume, or re-run with --restore.")
    return 0


def _announce_fork(outcome: dict, print_fn: Callable[[str], None]) -> None:
    """One line per fork: how many branches, how many the mutes hid."""
    live, muted = outcome.get("branches") or [], outcome.get("muted") or []
    first = (live + muted)[0]
    origin = first.get("origin") or "?"
    merge = (first.get("spawned_by") or "") in ("JOIN", "MERGE")
    if merge:
        print_fn(f"Merge at {origin!r}: this FlowFile was consumed into "
                 f"{len(live) + len(muted)} merged file(s) — the journey "
                 "continues there.")
        return
    bits = [f"{len(live)} following"] if live else []
    if muted:
        rules = ", ".join(sorted({b.get("muted_by") or "" for b in muted}))
        bits.append(f"{len(muted)} muted ({rules})")
    print_fn(f"Fork at {origin!r}: {len(live) + len(muted)} branch(es) — "
             f"{', '.join(bits)}.  `b` lists them, `m N` mutes one.")


def _start(
    follower: FlowFollower,
    client: Any,
    group: str,
    *,
    uuid: Optional[str],
    queue: Optional[str],
    source: Optional[str],
    start: Optional[str],
    inject_at: Optional[str],
    content: str,
    attributes: Optional[Dict[str, str]],
    auto: bool,
    input_fn: Callable[[str], str],
    print_fn: Callable[[str], None],
) -> Optional[dict]:
    """Resolve where the journey begins, prompting only when it has to."""
    if inject_at:
        picked = follower.inject(inject_at, content=content,
                                 attributes=attributes)
        where = f" on {picked['node']}" if picked.get("node") else ""
        print_fn(f"Injected a fixture FlowFile at {picked['injected']!r}"
                 f"{where} ({len(content)} byte(s) of content, "
                 f"{len(attributes or {})} attribute(s)).")
        if picked.get("siblings"):
            print_fn(f"Cluster: run-once fires on EVERY node, so it minted "
                     f"{picked['siblings'] + 1} FlowFile(s) — one per node. "
                     "Following the one above; the rest stay queued and go "
                     "when the injector does.")
        return picked
    if uuid or queue:
        return follower.pick_flowfile(queue_id=queue, uuid=uuid)
    if source:
        name = follower.kick_source(source)
        print_fn(f"Ran source {name!r} once.")
        return follower.pick_flowfile(wait=follower.poll_timeout * 2)

    entries = entry_points(client, group)
    if not entries:
        raise FollowError(
            f"nothing to start from in {group!r} — no queued FlowFiles, no "
            "source processors, no input ports. Queue a file (or push a flow "
            "with a source) and retry.")
    entry = _choose_entry(entries, start, auto, input_fn, print_fn)
    if entry is None:
        return None
    print_fn(f"Starting from {entry['kind']} {entry['label']!r}.")
    return follower.start_from(entry)


def _choose_entry(
    entries: List[dict],
    start: Optional[str],
    auto: bool,
    input_fn: Callable[[str], str],
    print_fn: Callable[[str], None],
) -> Optional[dict]:
    if start:
        chosen = _match_entry(entries, start)
        if chosen is None:
            raise FollowError(
                f"no start point matching {start!r} — `--list` shows them")
        return chosen
    if len(entries) == 1 or auto:
        if len(entries) > 1:
            print_fn(f"{len(entries)} start points; --auto takes the first "
                     "(use --start to choose).")
        return entries[0]
    print_fn("Start points:")
    for i, entry in enumerate(entries, 1):
        print_fn(format_entry(i, entry))
    try:
        answer = input_fn(f"Start where? [1-{len(entries)}, Enter=1, q=quit] ")
    except (EOFError, KeyboardInterrupt):
        answer = "q"
    answer = answer.strip()
    if answer.lower() in ("q", "quit"):
        print_fn("Nothing started.")
        return None
    if not answer:
        return entries[0]
    if answer.isdigit() and 1 <= int(answer) <= len(entries):
        return entries[int(answer) - 1]
    chosen = _match_entry(entries, answer)
    if chosen is None:
        raise FollowError(f"{answer!r} is not one of the start points")
    return chosen


def _match_entry(entries: List[dict], spec: str) -> Optional[dict]:
    """Match a start point by number, id, label, or ``kind:id``."""
    spec = spec.strip()
    if spec.isdigit() and 1 <= int(spec) <= len(entries):
        return entries[int(spec) - 1]
    kind, _, value = spec.partition(":")
    for entry in entries:
        if value and entry["kind"] == kind and value in (entry["id"], entry["label"]):
            return entry
        if not value and spec in (entry["id"], entry["label"]):
            return entry
    return None


def _interactive(
    follower: FlowFollower,
    client: Any,
    render: Callable[[dict], None],
    input_fn: Callable[[str], str],
    print_fn: Callable[[str], None],
) -> None:
    """Prompt-step-render loop — the F5 key of the flow debugger."""
    last_hop: Optional[dict] = None
    while True:
        try:
            answer = input_fn("[step] ")
        except (EOFError, KeyboardInterrupt):
            answer = "q"
        key, _, arg = answer.strip().partition(" ")
        key, arg = key.lower(), arg.strip()
        if key in ("q", "quit"):
            return
        if key in ("?", "help"):
            print_fn(_KEYS)
            continue
        if key == "a":
            last_hop = _show_attrs(follower, last_hop, print_fn)
            continue
        if key == "c":
            _show_content(follower, client, last_hop, print_fn)
            continue
        if key == "b":
            _show_branches(follower, print_fn, arg)
            continue
        if key == "h":
            _show_history(follower, arg, print_fn)
            continue
        if key == "w":
            _watch(follower, arg, print_fn)
            continue
        if key in ("rr", "replay"):
            _replay(follower, render, print_fn)
            last_hop = None
            continue
        if key in ("cmp", "compare"):
            _compare(follower, arg, print_fn)
            continue
        if key in ("m", "u", "s") and not arg:
            print_fn(f"{key} needs an argument — {_KEYS}")
            continue
        if key == "m":
            _mute(follower, arg, print_fn)
            continue
        if key == "u":
            _unmute(follower, arg, print_fn)
            continue
        if key == "s":
            _switch(follower, arg, print_fn)
            continue
        if key not in ("", "r"):
            print_fn(f"Unknown key {key!r} — {_KEYS}")
            continue

        outcome = follower.repoll() if key == "r" else follower.step()
        render(outcome)
        if outcome.get("hops"):
            last_hop = outcome["hops"][-1]
        status = outcome["status"]
        if status == "terminal":
            _next_branch_or_stop(follower, print_fn, _REASON_LINES["terminal"])
            continue
        if status == "gone" or outcome.get("dropped"):
            reason = _REASON_LINES["dropped" if outcome.get("dropped") else "gone"]
            # The loop stays open when the journey ends: a debugger you can
            # still scroll back in (`h`, `a`, `c`) beats one that exits.
            if not _next_branch_or_stop(follower, print_fn, reason):
                print_fn("Nothing left to follow — `h` reviews the hops, "
                         "`b` the branches, `q` quits.")


def _next_branch_or_stop(follower: FlowFollower,
                         print_fn: Callable[[str], None], reason: str) -> bool:
    """Move to the next live branch after one ends; True if there was one."""
    nxt = follower.next_live()
    if nxt is None:
        print_fn(reason)
        return False
    follower.switch_to(nxt)
    record = follower.session.branches[nxt]
    origin = " -> ".join(x for x in (record.get("origin"),
                                     record.get("relationship")) if x) or "start"
    print_fn(f"{reason} Now following {nxt} (from {origin}); "
             "`b` lists the rest.")
    return True


def _show_attrs(follower: FlowFollower, last_hop: Optional[dict],
                print_fn: Callable[[str], None]) -> Optional[dict]:
    hop = last_hop or (follower.session.history() or [None])[-1]
    if hop is None:
        print_fn("No hop yet — step first.")
        return last_hop
    for name, value in sorted(hop["attributes"].items()):
        print_fn(f"       = {name}: {value}")
    return hop


def _show_content(follower: FlowFollower, client: Any,
                  last_hop: Optional[dict],
                  print_fn: Callable[[str], None]) -> None:
    hop = last_hop or (follower.session.history() or [None])[-1]
    if hop is None:
        print_fn("No hop yet — step first.")
    elif hop.get("output_available"):
        print_fn(client.event_content(hop["event_id"]) or "(empty)")
    else:
        print_fn("(content no longer available for the last event)")


# Above this many branches the per-branch table stops being readable and the
# grouped view takes over (`b all` still prints every row).
_BRANCH_TABLE_LIMIT = 12


def _show_branches(follower: FlowFollower,
                   print_fn: Callable[[str], None], arg: str = "") -> None:
    branches = follower.branches()
    if not branches:
        print_fn("No branches yet.")
        return
    full = arg.lower() in ("all", "full", "*")
    if full or len(branches) <= _BRANCH_TABLE_LIMIT:
        print_fn("Branches (* = current):")
        for i, branch in enumerate(branches, 1):
            print_fn(format_branch(i, branch))
    else:
        groups = follower.branch_groups()
        print_fn(f"Branches: {len(branches)} in {len(groups)} group(s) "
                 f"(* = current; `b all` lists every one):")
        for group in groups:
            print_fn(format_branch_group(group))
    active = follower.mutes.describe()
    print_fn(f"Mutes: {active or 'none'} — muted branches keep running in "
             "NiFi, they are just not followed.")


def _show_history(follower: FlowFollower, arg: str,
                  print_fn: Callable[[str], None]) -> None:
    hops = follower.session.history()
    if not hops:
        print_fn("No hops on this branch yet.")
        return
    if arg.isdigit():
        index = int(arg)
        if not 1 <= index <= len(hops):
            print_fn(f"Hop {index} is outside 1-{len(hops)}.")
            return
        print_fn(format_hop(index, hops[index - 1], full=True))
        return
    for i, hop in enumerate(hops, 1):
        print_fn(format_hop(i, hop))


def _watch(follower: FlowFollower, arg: str,
           print_fn: Callable[[str], None]) -> None:
    """`w` — add/remove a watch, then print the hop x attribute table."""
    if arg.lower() in ("clear", "none", "-"):
        follower.session.watches = []
        follower._save()
        print_fn("Watching nothing.")
        return
    if arg.startswith("-") and len(arg) > 1:
        spec = arg[1:].strip()
        print_fn(f"No longer watching {spec!r}." if follower.unwatch(spec)
                 else f"{spec!r} was not being watched.")
    elif arg:
        follower.watch(arg)
        print_fn(f"Watching {arg!r}.")
    columns, rows = follower.watch_table()
    print_fn(format_watch_table(columns, rows))


def _replay(follower: FlowFollower, render: Callable[[dict], None],
            print_fn: Callable[[str], None]) -> None:
    """`rr` — re-inject the recorded fixture and start the journey again."""
    try:
        picked = follower.replay()
    except FollowError as exc:
        print_fn(str(exc))
        return
    # The renderer numbers hops across the whole journey; a replay is a new
    # journey, so hop 1 has to be hop 1 again.
    getattr(render, "reset", lambda: None)()
    queue = picked["queue"]
    print_fn(f"Run {picked['run']}: re-injected the fixture at "
             f"{picked['injected']!r} — now following {picked['uuid']} in "
             f"{queue['source']} -> {queue['destination']}. Step it, then "
             "`cmp` says what the fix changed.")


def _compare(follower: FlowFollower, arg: str,
             print_fn: Callable[[str], None]) -> None:
    """`cmp` — this run against a finished one (the previous one by default)."""
    runs = follower.session.runs
    if not runs:
        print_fn("Only one run so far — `rr` replays the fixture, and `cmp` "
                 "then compares the two.")
        return
    which = int(arg) if arg.isdigit() else len(runs)
    if not 1 <= which <= len(runs):
        print_fn(f"No run {which} — there are {len(runs)}.")
        return
    rows = compare_runs(follower.session.run_hops(which),
                        follower.session.flat_hops())
    print_fn(format_run_comparison(which, len(runs) + 1, rows))


def _retire_injector(follower: FlowFollower,
                     print_fn: Callable[[str], None]) -> None:
    """Take the temporary injector away — unless doing so kills the fixture.

    Removing the injector means draining its connection, and if the fixture
    FlowFile has not been stepped out of that queue yet, the drain *is* the
    file. Leaving it there is the option that keeps `--resume` honest.
    """
    if not follower.session.injector:
        return
    if follower.injector_holds_file():
        print_fn(f"Left the {INJECTOR_NAME!r} processor in place: the fixture "
                 "FlowFile is still in its queue, and removing it would drop "
                 "the file. `--resume` picks the journey up.")
    elif follower.cleanup_injector():
        print_fn(f"Removed the temporary {INJECTOR_NAME!r} processor.")


def _branch_spec(follower: FlowFollower, arg: str) -> str:
    """Turn a branch *number* from ``b`` into a uuid mute spec."""
    if arg.isdigit():
        branches = follower.branches()
        index = int(arg)
        if not 1 <= index <= len(branches):
            raise FollowError(f"branch {index} is outside 1-{len(branches)}")
        return f"uuid:{branches[index - 1]['uuid']}"
    return arg


def _mute(follower: FlowFollower, arg: str,
          print_fn: Callable[[str], None]) -> None:
    try:
        result = follower.mute(_branch_spec(follower, arg))
    except FollowError as exc:
        print_fn(str(exc))
        return
    count = len(result["muted"])
    print_fn(f"Muted {result['rule']} ({count} branch(es) hidden) — they keep "
             "running in NiFi.")
    if follower.uuid in result["muted"]:
        nxt = follower.next_live()
        if nxt:
            follower.switch_to(nxt)
            print_fn(f"That was the current branch; now following {nxt}.")
        else:
            print_fn("That was the current branch and nothing else is live — "
                     "`u` to unmute, or `q`.")


def _unmute(follower: FlowFollower, arg: str,
            print_fn: Callable[[str], None]) -> None:
    try:
        result = follower.unmute(_branch_spec(follower, arg))
    except FollowError as exc:
        print_fn(str(exc))
        return
    print_fn(f"Unmuted {result['rule']} ({len(result['unmuted'])} branch(es) "
             "back in the journey).")


def _switch(follower: FlowFollower, arg: str,
            print_fn: Callable[[str], None]) -> None:
    branches = follower.branches()
    target = None
    if arg.isdigit() and 1 <= int(arg) <= len(branches):
        target = branches[int(arg) - 1]["uuid"]
    else:
        matches = [b["uuid"] for b in branches if b["uuid"].startswith(arg)]
        if len(matches) == 1:
            target = matches[0]
        elif len(matches) > 1:
            print_fn(f"{arg!r} matches {len(matches)} branches — use the "
                     "number from `b`.")
            return
    if target is None:
        print_fn(f"No branch {arg!r} — `b` lists them.")
        return
    try:
        follower.switch_to(target)
    except FollowError as exc:
        print_fn(str(exc))
        return
    print_fn(f"Following {target}.")
