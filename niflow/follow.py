"""Live FlowFile stepper: quiesce a group, then walk one file through it
one processor at a time (``niflow follow``).

The live counterpart of ``niflow trace``: trace replays a finished journey
from provenance; follow *creates* the journey interactively. The group is
deliberately stopped first — nothing may race the stepper — then each step
run-onces the processor consuming the followed FlowFile's queue and renders
the provenance events that run produced, in the same hop/diff view trace
uses (:func:`format_hop` is shared by both commands).

::

    niflow follow "My Flow (copy)"                   # front of first non-empty queue
    niflow follow "My Flow (copy)" --source Gen      # mint a FlowFile first
    niflow follow "My Flow (copy)" --auto --restore  # run to the end, then restart

The group is left stopped on exit unless ``--restore`` is given: quiescing
was the point, and restarting silently would surprise. When the followed
file forks, the child uuids are surfaced and the user picks which branch to
keep following (``--auto`` switches to the first child when the parent's
journey ends, with a note).

Trace-verified provenance facts this leans on (live on 1.24.0 and 2.7.2):
event ids are monotonic across the instance, ``previousValue`` semantics
drive the diffs, and indexing lands in well under a second after run-once.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("niflow")

# How long one step waits for run-once's provenance to be indexed. Live
# measurements say <0.2s on both lines; the loop is cheap insurance.
_PROV_TIMEOUT_S = 5.0
_PROV_INTERVAL_S = 0.2


def format_hop(index: int, hop: dict, full: bool = False) -> str:
    """One trace/follow hop as the multi-line block both commands print.

    ``index`` is the 1-based hop number; ``hop`` is the dict
    :meth:`~niflow.client.NiFiClient.trace_flowfile` builds per event.
    """
    rel = f" -> {hop['relationship']}" if hop["relationship"] else ""
    lines = [f"{index:>3}. {hop['component'] or '(flow)'}  "
             f"[{hop['event_type']}{rel}]  {hop['time']}  {hop['size']} B"]
    for change in hop["changes"]:
        before = "(new)" if change["before"] is None else change["before"]
        lines.append(f"       {change['name']}: {before} -> {change['after']}")
    if full:
        for name, value in sorted(hop["attributes"].items()):
            lines.append(f"       = {name}: {value}")
    for child in hop["children"]:
        lines.append(f"       spawned {child}  (niflow trace {child})")
    return "\n".join(lines)


class FlowFollower:
    """Steps one FlowFile through a quiesced group via run-once.

    The loop each step runs: find the queue holding the followed uuid →
    run-once that queue's destination processor → collect the provenance
    events the run produced (``flowfile_events_since`` above the last event
    id already rendered). All NiFi access goes through the injected
    ``client`` (a :class:`~niflow.client.NiFiClient`), so this class is plain
    orchestration — and unit-testable with a stub.
    """

    def __init__(
        self,
        client: Any,
        group: str,
        *,
        max_hops: int = 50,
        poll_timeout: float = _PROV_TIMEOUT_S,
        poll_interval: float = _PROV_INTERVAL_S,
    ) -> None:
        self.client = client
        self.group = group
        self.pg_id = client.resolve_group(group)
        self.max_hops = max_hops
        self.poll_timeout = poll_timeout
        self.poll_interval = poll_interval
        self.prior_running: List[dict] = []  # find_processors dicts, pre-quiesce
        self.uuid: Optional[str] = None
        self.last_event_id: int = -1
        self.pending_children: List[str] = []  # spawned but not followed

    # ----------------------------------------------------------- quiesce

    def quiesce(self) -> int:
        """Stop everything in the group, remembering what was RUNNING.

        Stop-group (not per-processor stops) so ports and nested groups
        quiesce too — a running port would move the file on its own and
        break the stepping. Returns how many processors were running.
        """
        procs = self.client.find_processors(group=self.pg_id)
        self.prior_running = [p for p in procs if p.get("state") == "RUNNING"]
        self.client.stop_group(self.pg_id)
        return len(self.prior_running)

    def restore(self) -> int:
        """Restart exactly the processors that were RUNNING before quiesce."""
        restored = 0
        for proc in self.prior_running:
            try:
                self.client.start_processor(proc["id"])
                restored += 1
            except Exception as exc:  # invalid/disabled processors refuse
                logger.warning("Could not restart %s: %s", proc.get("name"), exc)
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
            raise ValueError(f"no processor named {source!r} in the group")
        if len(matches) > 1:
            paths = ", ".join(
                f"{m['path']}/{m['name']}".lstrip("/") for m in matches)
            raise ValueError(f"{source!r} is ambiguous ({paths}); use a path or id")
        self.client.run_processor_once(matches[0]["id"])
        return matches[0]["name"]

    def pick_flowfile(
        self,
        queue_id: Optional[str] = None,
        uuid: Optional[str] = None,
        wait: float = 0.0,
    ) -> dict:
        """Choose the FlowFile to follow and baseline its provenance.

        Default: the front of the first non-empty queue. ``queue_id`` pins
        the queue, ``uuid`` pins the file. ``wait`` keeps re-scanning for up
        to that many seconds (used after ``kick_source``: run-once lands the
        file a moment later). Events the file already has are *not* replayed
        — the baseline is remembered so stepping renders only what's new.
        """
        deadline = time.monotonic() + wait
        while True:
            found = self._find(queue_id, uuid)
            if found is not None or time.monotonic() >= deadline:
                break
            time.sleep(self.poll_interval)
        if found is None:
            want = f"FlowFile {uuid}" if uuid else "a queued FlowFile"
            raise ValueError(
                f"no {want} found in the group's queues — run a source once "
                "(--source NAME) or queue a file, then retry"
            )
        self.uuid = found["uuid"]
        prior = self.client.flowfile_events_since(self.uuid, -1)
        self.last_event_id = max(
            (int(h["event_id"]) for h in prior), default=-1)
        found["prior_events"] = len(prior)
        return found

    def _find(self, queue_id: Optional[str], uuid: Optional[str]) -> Optional[dict]:
        for q in self.client.list_queues(self.pg_id):
            if queue_id and q["id"] != queue_id:
                continue
            if not q.get("queued"):
                continue
            files = self.client.list_flowfiles(q["id"])
            if not files:
                continue
            if uuid:
                for f in files:
                    if f["uuid"] == uuid:
                        return {"uuid": uuid, "queue": q, "flowfile": f}
                continue
            front = min(files, key=lambda s: s.get("position", 0))
            return {"uuid": front["uuid"], "queue": q, "flowfile": front}
        return None

    def locate(self) -> Optional[dict]:
        """The queue currently holding the followed FlowFile, or ``None``."""
        found = self._find(None, self.uuid)
        return found["queue"] if found else None

    # -------------------------------------------------------------- stepping

    def step(self) -> dict:
        """Advance the followed FlowFile one processor; describe what happened.

        Returns ``{"status", "hops", "children", ...}`` where status is:

        * ``advanced`` — run-once fired and new hops were indexed (``dropped``
          is True when one of them ended the file's life);
        * ``terminal`` — the queue feeds a port/funnel; run-once can't cross
          it (``end`` carries the ref);
        * ``stalled`` — run-once fired but no new provenance appeared within
          the poll window;
        * ``gone`` — the file is in no queue at all (dropped, sent, or
          consumed by the previous hop).
        """
        queue = self.locate()
        if queue is None:
            return {"status": "gone", "hops": [],
                    "children": list(self.pending_children)}
        end = self.client.connection_end(queue["id"], "destination")
        if (end.get("type") or "") != "PROCESSOR":
            return {"status": "terminal", "queue": queue, "end": end,
                    "hops": [], "children": []}
        self.client.run_processor_once(end["id"])
        hops = self._await_hops()
        children = [c for hop in hops for c in hop["children"]]
        self.pending_children.extend(
            c for c in children if c not in self.pending_children)
        if hops:
            self.last_event_id = max(int(h["event_id"]) for h in hops)
        return {
            "status": "advanced" if hops else "stalled",
            "queue": queue,
            "processor": end,
            "hops": hops,
            "children": children,
            "dropped": any(h["event_type"] == "DROP" for h in hops),
        }

    def _await_hops(self) -> List[dict]:
        deadline = time.monotonic() + self.poll_timeout
        while True:
            hops = self.client.flowfile_events_since(
                self.uuid, self.last_event_id)
            if hops or time.monotonic() >= deadline:
                return hops
            time.sleep(self.poll_interval)

    def switch_to(self, uuid: str) -> None:
        """Continue the journey on another uuid (a fork/clone child).

        ``last_event_id`` carries over: event ids are monotonic across the
        instance, so the incremental query stays correct across the switch.
        """
        self.uuid = uuid
        if uuid in self.pending_children:
            self.pending_children.remove(uuid)

    def auto(self, on_event: Optional[Callable[[dict], None]] = None) -> dict:
        """Step until the journey ends; returns ``{"reason", "steps", ...}``.

        Reasons: ``dropped``/``gone`` (journey complete), ``terminal`` (a
        port/funnel blocks run-once), ``stalled`` (run-once produced no
        events), ``max-hops`` (safety cap). When the followed uuid's journey
        ends but it spawned children, following jumps to the first child
        (``on_event`` sees a ``{"status": "switched"}`` marker).
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
                if self._switch_to_first_child(emit):
                    continue
                return {"reason": "gone", "steps": steps}
            if status == "terminal":
                return {"reason": "terminal", "steps": steps,
                        "end": outcome.get("end")}
            if status == "stalled":
                return {"reason": "stalled", "steps": steps,
                        "processor": outcome.get("processor")}
            steps += 1
            if outcome.get("dropped"):
                if self._switch_to_first_child(emit):
                    continue
                return {"reason": "dropped", "steps": steps}

    def _switch_to_first_child(self, emit: Callable[[dict], None]) -> bool:
        if not self.pending_children:
            return False
        child = self.pending_children[0]
        self.switch_to(child)
        emit({"status": "switched", "uuid": child, "hops": [], "children": []})
        return True


# ------------------------------------------------------------------ CLI driver

_REASON_LINES: Dict[str, str] = {
    "dropped": "FlowFile dropped — journey complete.",
    "gone": "FlowFile has left every queue (dropped, sent, or consumed) — "
            "journey complete.",
    "terminal": "Queue feeds a port/funnel — run-once cannot cross it.",
    "stalled": "Run-once produced no new provenance for the followed "
               "FlowFile — stopping.",
    "max-hops": "Hop cap reached (--max-hops) — stopping.",
}


def follow_command(
    client: Any,
    group: str,
    *,
    uuid: Optional[str] = None,
    queue: Optional[str] = None,
    source: Optional[str] = None,
    auto: bool = False,
    max_hops: int = 50,
    restore: bool = False,
    full: bool = False,
    input_fn: Callable[[str], str] = input,
    print_fn: Callable[[str], None] = print,
) -> int:
    """The ``niflow follow`` command body (I/O injectable for tests)."""
    follower = FlowFollower(client, group, max_hops=max_hops)
    stopped = follower.quiesce()
    print_fn(f"Quiesced {group!r}: stopped {stopped} running processor(s).")

    hop_no = 0

    def render(outcome: dict) -> None:
        nonlocal hop_no
        for hop in outcome.get("hops", []):
            hop_no += 1
            print_fn(format_hop(hop_no, hop, full=full))
        status = outcome.get("status")
        if status == "switched":
            print_fn(f"Following spawned child {outcome['uuid']}.")
        elif status == "stalled":
            proc = outcome.get("processor") or {}
            print_fn(f"Ran {proc.get('name', '?')!r} once but saw no new "
                     "provenance for the followed FlowFile.")
        elif status == "terminal":
            end = outcome.get("end") or {}
            kind = (end.get("type") or "unknown").replace("_", " ").lower()
            print_fn(f"Queue feeds a {kind} ({end.get('name', '')!r}) — "
                     "run-once cannot cross it.")

    try:
        wait = 0.0
        if source:
            name = follower.kick_source(source)
            print_fn(f"Ran source {name!r} once.")
            wait = 10.0
        picked = follower.pick_flowfile(queue_id=queue, uuid=uuid, wait=wait)
        q = picked["queue"]
        print_fn(
            f"Following {picked['uuid']} in {q['source']} -> {q['destination']}"
            f"  ({picked['prior_events']} prior event(s); "
            f"`niflow trace {picked['uuid']}` replays them)."
        )
        if auto:
            result = follower.auto(on_event=render)
            print_fn(_REASON_LINES.get(result["reason"], result["reason"]))
        else:
            _interactive(follower, client, render, input_fn, print_fn)
    finally:
        if restore:
            print_fn(f"Restored {follower.restore()} previously-running "
                     "processor(s).")
        else:
            print_fn(f"Group left stopped — `niflow start {group!r}` to "
                     "resume, or re-run with --restore.")
    return 0


def _interactive(
    follower: FlowFollower,
    client: Any,
    render: Callable[[dict], None],
    input_fn: Callable[[str], str],
    print_fn: Callable[[str], None],
) -> None:
    """Prompt-step-render loop: Enter=step, a=attrs, c=content, q=quit."""
    last_hop: Optional[dict] = None
    while True:
        try:
            answer = input_fn("[Enter=step  a=attrs  c=content  q=quit] ")
        except (EOFError, KeyboardInterrupt):
            answer = "q"
        answer = answer.strip().lower()
        if answer == "q":
            return
        if answer == "a":
            if last_hop is None:
                print_fn("No hop yet — step first.")
            else:
                for name, value in sorted(last_hop["attributes"].items()):
                    print_fn(f"       = {name}: {value}")
            continue
        if answer == "c":
            if last_hop is None:
                print_fn("No hop yet — step first.")
            elif last_hop.get("output_available"):
                print_fn(client.event_content(last_hop["event_id"]) or "(empty)")
            else:
                print_fn("(content no longer available for the last event)")
            continue

        outcome = follower.step()
        render(outcome)
        if outcome.get("hops"):
            last_hop = outcome["hops"][-1]
        status = outcome["status"]
        if status == "terminal":
            return
        if status == "gone" or outcome.get("dropped"):
            if follower.pending_children:
                _pick_child(follower, input_fn, print_fn, forced=True)
                continue
            print_fn(_REASON_LINES["dropped" if outcome.get("dropped")
                                   else "gone"])
            return
        if outcome.get("children"):
            _pick_child(follower, input_fn, print_fn, forced=False)


def _pick_child(
    follower: FlowFollower,
    input_fn: Callable[[str], str],
    print_fn: Callable[[str], None],
    forced: bool,
) -> None:
    """Let the user choose which fork child to follow next.

    ``forced``: the current uuid's journey is over, so Enter takes the first
    child; otherwise Enter keeps following the current uuid.
    """
    choices = list(follower.pending_children)
    default = f"Enter={choices[0][:8]}…" if forced else "Enter=keep current"
    labels = "  ".join(f"{i}={c}" for i, c in enumerate(choices, 1))
    try:
        answer = input_fn(f"Fork — follow which? {labels}  [{default}] ")
    except (EOFError, KeyboardInterrupt):
        answer = ""
    answer = answer.strip()
    if answer.isdigit() and 1 <= int(answer) <= len(choices):
        follower.switch_to(choices[int(answer) - 1])
        print_fn(f"Following {follower.uuid}.")
    elif forced:
        follower.switch_to(choices[0])
        print_fn(f"Following spawned child {follower.uuid}.")
