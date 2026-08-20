"""Unit tests for the live FlowFile stepper (niflow follow).

A NiFiClient-shaped stub scripts a tiny flow — Gen -(c1)-> Mid -(c2)-> Sink —
and run-once side effects move the file and append provenance hops, so the
follower's quiesce/step/fork/auto/restore logic runs without a live NiFi.
"""
import argparse
import pytest

from niflow.follow import (
    BranchMutes,
    FlowFollower,
    FollowError,
    FollowSession,
    annotate_hops,
    entry_points,
    follow_command,
    format_hop,
)


def hop(event_id, event_type="ATTRIBUTES_MODIFIED", component="Mid",
        children=(), changes=(), relationship="", size=3, drop=()):
    """A provenance hop the way flowfile_events_since builds one.

    ``changes`` is applied to the attribute map (real events carry the
    post-event attributes AND the previousValue diff; the cross-hop diff the
    stepper renders reads the map), and ``drop`` names attributes this event
    removed — only visible from the cross-hop diff.
    """
    attributes = {"a": "1", "filename": "f"}
    for change in changes:
        attributes[change["name"]] = change["after"]
    for name in drop:
        attributes.pop(name, None)
    return {
        "event_id": event_id, "event_type": event_type, "time": "12:00:00",
        "component": component, "component_id": "", "component_type": "",
        "relationship": relationship, "size": size,
        "attributes": attributes,
        "changes": list(changes), "input_available": True,
        "output_available": True, "content_equal": True,
        "parents": [], "children": list(children),
    }


class StubClient:
    """Scripted stand-in for NiFiClient — only what FlowFollower touches."""

    def __init__(self):
        self.procs = [
            {"id": "gen", "name": "Gen", "state": "RUNNING", "path": "",
             "type": "GenerateFlowFile", "group_id": "pg-1"},
            {"id": "mid", "name": "Mid", "state": "STOPPED", "path": "",
             "type": "UpdateAttribute", "group_id": "pg-1"},
            {"id": "sink", "name": "Sink", "state": "RUNNING", "path": "",
             "type": "PutFile", "group_id": "pg-1"},
        ]
        self.queues = [
            {"id": "c1", "source": "Gen", "destination": "Mid", "path": "",
             "source_id": "gen", "destination_id": "mid"},
            {"id": "c2", "source": "Mid", "destination": "Sink", "path": "",
             "source_id": "mid", "destination_id": "sink"},
        ]
        self.ports = []        # list_ports payload
        self.rels = {"c1": ["success"], "c2": ["success"]}  # per-connection
        self.invalid = {}      # processor id -> validation errors
        self.queue_files = {"c1": [], "c2": []}  # conn id -> [summaries]
        self.dests = {"c1": {"id": "mid", "name": "Mid", "type": "PROCESSOR"},
                      "c2": {"id": "sink", "name": "Sink", "type": "PROCESSOR"}}
        self.events = {}       # uuid -> full hop history
        self.on_run_once = {}  # processor id -> side-effect callable
        self.stopped_groups = []
        self.started = []
        self.ran_once = []
        self.content = {}      # event id -> payload
        self.port_states = []  # (kind, port id, state) PUTs, in order
        self.on_port_start = {}   # port id -> side-effect callable
        self.proc_states = {}     # processor id -> run state
        self.proc_types = {}      # processor id -> FQCN
        self.proc_properties = {}  # processor id -> materialised property map
        self.cap = 100            # how many FlowFiles a listing will show

    # --- the client surface the follower uses ---

    def resolve_group(self, group):
        return "pg-1"

    def find_processors(self, type_contains="", group="root"):
        return [dict(p) for p in self.procs]

    def stop_group(self, group):
        self.stopped_groups.append(group)

    def start_processor(self, proc_id):
        self.started.append(proc_id)

    def run_processor_once(self, proc_id):
        self.ran_once.append(proc_id)
        effect = self.on_run_once.get(proc_id)
        if effect:
            effect()

    def list_queues(self, group="root"):
        return [dict(q, queued=len(self.queue_files[q["id"]]))
                for q in self.queues]

    def list_flowfiles(self, conn_id, max_results=100):
        # NiFi caps a listing at 100 whatever you ask for; `cap` lets a test
        # make a queue deeper than the listing without queueing 101 files.
        return [dict(f) for f in self.queue_files[conn_id]][:self.cap]

    def list_ports(self, group="root"):
        return [dict(p) for p in self.ports]

    def set_port_state(self, kind, port_id, state):
        self.port_states.append((kind, port_id, state))
        effect = self.on_port_start.get(port_id)
        if effect and state == "RUNNING":
            effect()

    def processor_config(self, proc_id):
        return {"type": self.proc_types.get(proc_id, "org.x.P"),
                "name": proc_id,
                "properties": dict(self.proc_properties.get(proc_id) or {})}

    def processor_validation(self, proc_id):
        return {"state": self.proc_states.get(proc_id, "STOPPED"),
                "status": "INVALID" if self.invalid.get(proc_id) else "VALID",
                "errors": list(self.invalid.get(proc_id) or [])}

    def locate_flowfile(self, conn_id, uuid):
        """The targeted lookup that sees past the 100-file listing cap."""
        for summary in self.queue_files.get(conn_id, []):
            if summary["uuid"] == uuid:
                return dict(summary, position=None)
        return None

    def connection_relationships(self, conn_id):
        return list(self.rels.get(conn_id, []))

    def validation_errors(self, group="root"):
        return [{"id": pid, "name": pid, "errors": list(errs)}
                for pid, errs in self.invalid.items()]

    def connection_end(self, conn_id, which):
        assert which == "destination"
        return dict(self.dests[conn_id])

    def flowfile_events_since(self, uuid, after_event_id=-1, max_events=100):
        return [h for h in self.events.get(uuid, [])
                if int(h["event_id"]) > after_event_id]

    def event_content(self, event_id, direction="output"):
        return self.content.get(event_id, "")

    # --- scripting helpers ---

    def put(self, conn_id, uuid, position=0):
        self.queue_files[conn_id].append(
            {"uuid": uuid, "filename": "f", "size": 3, "position": position})

    def take(self, conn_id, uuid):
        self.queue_files[conn_id] = [
            f for f in self.queue_files[conn_id] if f["uuid"] != uuid]


@pytest.fixture()
def stub():
    client = StubClient()
    client.put("c1", "ff-1")
    client.events["ff-1"] = [hop(1, "CREATE", "Gen")]
    return client


def follower(client, **kw):
    kw.setdefault("poll_timeout", 0)   # never sleep in unit tests
    kw.setdefault("poll_interval", 0)
    return FlowFollower(client, "pg-1", **kw)


def script_linear(stub):
    """Mid moves ff-1 to c2 with an attr change; Sink drops it."""
    def run_mid():
        stub.take("c1", "ff-1")
        stub.put("c2", "ff-1")
        stub.events["ff-1"].append(hop(
            2, changes=[{"name": "a", "before": "1", "after": "2"}]))

    def run_sink():
        stub.take("c2", "ff-1")
        stub.events["ff-1"].append(hop(3, "DROP", "Sink"))

    stub.on_run_once = {"mid": run_mid, "sink": run_sink}


# ------------------------------------------------------------------ quiesce


def test_quiesce_records_prior_state_and_stops_group(stub):
    f = follower(stub)
    assert f.quiesce() == 2
    assert stub.stopped_groups == ["pg-1"]
    assert [p["id"] for p in f.prior_running] == ["gen", "sink"]


def test_restore_restarts_only_previously_running(stub):
    f = follower(stub)
    f.quiesce()
    assert f.restore() == 2
    assert stub.started == ["gen", "sink"]  # Mid was stopped; it stays stopped


def test_restore_tolerates_a_processor_that_refuses(stub):
    f = follower(stub)
    f.quiesce()
    boom = Exception("invalid")
    stub.start_processor = lambda pid: (_ for _ in ()).throw(boom)
    assert f.restore() == 0  # warned, not raised


# ------------------------------------------------------------- picking


def test_pick_flowfile_takes_front_of_first_nonempty_queue(stub):
    f = follower(stub)
    picked = f.pick_flowfile()
    assert picked["uuid"] == "ff-1"
    assert picked["queue"]["id"] == "c1"
    assert picked["prior_events"] == 1   # the CREATE is baselined, not replayed
    assert f.last_event_id == 1


def test_pick_flowfile_by_uuid_searches_every_queue(stub):
    stub.put("c2", "ff-9")
    stub.events["ff-9"] = [hop(5, "CREATE", "Mid")]
    f = follower(stub)
    picked = f.pick_flowfile(uuid="ff-9")
    assert picked["queue"]["id"] == "c2"
    assert f.last_event_id == 5


def test_pick_flowfile_errors_helpfully_when_queues_are_empty(stub):
    stub.take("c1", "ff-1")
    with pytest.raises(FollowError, match="--source"):
        follower(stub).pick_flowfile()


def test_pick_flowfile_by_uuid_says_the_file_is_gone(stub):
    with pytest.raises(FollowError, match="already .*processed|niflow trace"):
        follower(stub).pick_flowfile(uuid="ff-vanished")


def test_kick_source_runs_once_by_name(stub):
    f = follower(stub)
    assert f.kick_source("Gen") == "Gen"
    assert stub.ran_once == ["gen"]
    with pytest.raises(FollowError, match="no processor"):
        f.kick_source("Nope")


# ------------------------------------------------------------- stepping


def test_step_runs_once_the_destination_and_picks_up_new_events(stub):
    script_linear(stub)
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["status"] == "advanced"
    assert stub.ran_once == ["mid"]
    assert [h["event_id"] for h in outcome["hops"]] == [2]  # not the baseline
    assert outcome["dropped"] is False
    assert f.last_event_id == 2


def test_step_reports_drop_then_gone(stub):
    script_linear(stub)
    f = follower(stub)
    f.pick_flowfile()
    f.step()
    outcome = f.step()
    assert outcome["dropped"] is True
    assert [h["event_type"] for h in outcome["hops"]] == ["DROP"]
    assert f.step()["status"] == "gone"


def test_step_is_terminal_at_a_destination_it_cannot_drive(stub):
    """Local ports are crossed now; a *remote* port is still the end of the road."""
    stub.take("c1", "ff-1")
    stub.put("c2", "ff-1")
    stub.dests["c2"] = {"id": "rp", "name": "remote", "type": "REMOTE_INPUT_PORT"}
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["status"] == "terminal"
    assert outcome["end"]["type"] == "REMOTE_INPUT_PORT"
    assert "remote input port" in outcome["message"]
    assert stub.ran_once == []  # never tried to run-once a port


def test_step_stalls_when_no_provenance_appears(stub):
    stub.on_run_once["mid"] = lambda: None  # runs, but nothing indexed
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["status"] == "stalled"
    assert outcome["processor"]["id"] == "mid"


# ------------------------------------------------------------- forks


def script_fork(stub):
    """Mid forks ff-1 into kid-1/kid-2 (parent dropped); Sink drops kid-1."""
    def run_mid():
        stub.take("c1", "ff-1")
        stub.put("c2", "kid-1")
        stub.events["ff-1"] += [hop(2, "FORK", children=["kid-1", "kid-2"]),
                                hop(3, "DROP")]
        stub.events["kid-1"] = []

    def run_sink():
        stub.take("c2", "kid-1")
        stub.events["kid-1"].append(hop(9, "DROP", "Sink"))

    stub.on_run_once = {"mid": run_mid, "sink": run_sink}


def test_fork_surfaces_children_and_switch_keeps_the_event_cursor(stub):
    script_fork(stub)
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["children"] == ["kid-1", "kid-2"]
    assert f.pending_children == ["kid-1", "kid-2"]
    f.switch_to("kid-1")
    assert f.uuid == "kid-1"
    assert f.pending_children == ["kid-2"]
    # Event ids are instance-monotonic, so the cursor survives the switch.
    assert f.last_event_id == 3


# ------------------------------------------------------------- auto


def test_auto_terminates_on_drop(stub):
    script_linear(stub)
    f = follower(stub)
    f.pick_flowfile()
    seen = []
    result = f.auto(on_event=seen.append)
    assert result == {"reason": "dropped", "steps": 2}
    assert [o["status"] for o in seen] == ["advanced", "advanced"]


def test_auto_switches_to_first_child_when_the_parent_journey_ends(stub):
    script_fork(stub)
    f = follower(stub)
    f.pick_flowfile()
    seen = []
    result = f.auto(on_event=seen.append)
    switched = [o["uuid"] for o in seen if o["status"] == "switched"]
    assert switched[0] == "kid-1"          # parent dropped -> first child
    assert stub.ran_once == ["mid", "sink"]  # kid-1 was actually stepped
    # kid-1's own DROP was rendered before auto moved on.
    dropped_hops = [h for o in seen for h in o["hops"]
                    if h["event_type"] == "DROP" and h["component"] == "Sink"]
    assert len(dropped_hops) == 1
    # kid-2 never reached a queue, so auto ends with a "gone" after trying it.
    assert switched == ["kid-1", "kid-2"]
    assert result["reason"] == "gone"


def test_auto_terminates_on_terminal_destination(stub):
    stub.dests["c2"] = {"id": "rp", "name": "remote", "type": "REMOTE_INPUT_PORT"}

    def run_mid():
        stub.take("c1", "ff-1")
        stub.put("c2", "ff-1")
        stub.events["ff-1"].append(hop(2))

    stub.on_run_once["mid"] = run_mid
    f = follower(stub)
    f.pick_flowfile()
    result = f.auto()
    assert result["reason"] == "terminal"
    assert result["end"]["type"] == "REMOTE_INPUT_PORT"


def test_auto_respects_the_max_hops_cap(stub):
    # Ping-pong: Mid and Sink keep tossing the file back and forth forever.
    counter = {"n": 1}

    def bounce(src, dst):
        def run():
            stub.take(src, "ff-1")
            stub.put(dst, "ff-1")
            counter["n"] += 1
            stub.events["ff-1"].append(hop(counter["n"]))
        return run

    stub.on_run_once = {"mid": bounce("c1", "c2"), "sink": bounce("c2", "c1")}
    f = follower(stub, max_hops=3)
    f.pick_flowfile()
    assert f.auto() == {"reason": "max-hops", "steps": 3}
    assert stub.ran_once == ["mid", "sink", "mid"]


# ------------------------------------------------------- CLI driver


def run_command(stub, inputs, **kw):
    """Drive follow_command with scripted keystrokes; return printed lines."""
    lines = []
    feed = iter(inputs)

    def input_fn(prompt: str) -> str:
        try:
            return next(feed)
        except StopIteration:
            return "q"

    kw.setdefault("print_fn", lines.append)
    kw.setdefault("start", "1")   # start point 1 = the queue holding the file
    kw.setdefault("poll_timeout", 0)  # never sleep in unit tests
    rc = follow_command(stub, "pg-1", input_fn=input_fn, **kw)
    assert rc == 0
    return lines


def test_follow_command_interactive_steps_to_completion(stub):
    script_linear(stub)
    lines = run_command(stub, ["", ""])
    text = "\n".join(lines)
    assert "Quiesced 'pg-1': stopped 2 running processor(s)." in lines
    assert "Following ff-1 in Gen -> Mid" in text
    assert "1 prior event(s)" in text
    assert "~ a: 1 -> 2" in text          # the cross-hop diff, via format_hop
    assert "[DROP]" in text
    assert "FlowFile dropped — journey complete." in lines
    assert "Group left stopped" in text   # no --restore -> hint, no restarts
    assert stub.started == []


def test_follow_command_quit_leaves_the_flow_untouched(stub):
    lines = run_command(stub, ["q"])
    assert stub.ran_once == []
    assert any("Group left stopped" in line for line in lines)


def test_follow_command_restore_restarts_prior_running(stub):
    script_linear(stub)
    lines = run_command(stub, [], auto=True, restore=True)
    assert stub.started == ["gen", "sink"]
    assert "Restored 2 previously-running component(s)." in lines


def test_follow_command_auto_renders_hops_and_reason(stub):
    script_linear(stub)
    lines = run_command(stub, [], auto=True)
    text = "\n".join(lines)
    assert "  1. Mid  [ATTRIBUTES_MODIFIED]" in text
    assert "  2. Sink  [DROP]" in text
    assert "FlowFile dropped — journey complete." in lines


def test_follow_command_source_mints_the_flowfile(stub):
    stub.take("c1", "ff-1")  # queues start empty; Gen must produce the file
    script_linear(stub)

    def run_gen():
        stub.put("c1", "ff-1")

    stub.on_run_once["gen"] = run_gen
    lines = run_command(stub, [], auto=True, source="Gen")
    assert stub.ran_once[0] == "gen"
    assert "Ran source 'Gen' once." in lines
    assert "FlowFile dropped — journey complete." in lines


def test_follow_command_fork_switches_to_the_next_branch_itself(stub):
    script_fork(stub)
    # One step forks and drops the parent: the stepper moves on by itself
    # (fewer decisions), announcing where it went.
    lines = run_command(stub, ["", "q"])
    text = "\n".join(lines)
    assert "spawned kid-1" in text and "spawned kid-2" in text
    assert "Fork at 'Mid': 2 branch(es) — 2 following" in text
    assert "Now following kid-1" in text


def test_follow_command_switch_key_moves_between_branches(stub):
    script_fork(stub)
    lines = run_command(stub, ["", "s 3", "b", "q"])   # branch 3 = kid-2
    assert "Following kid-2." in lines
    assert "Branches (* = current):" in lines


def test_follow_command_mute_key_hides_a_branch_without_touching_nifi(stub):
    script_fork(stub)
    before = list(stub.ran_once)
    lines = run_command(stub, ["", "m 3", "b", "q"])
    text = "\n".join(lines)
    assert "Muted uuid:kid-2" in text and "keep running in NiFi" in text
    # Nothing but the fork's own run-once happened: mute is view-only.
    assert stub.ran_once == before + ["mid"]
    assert stub.stopped_groups == ["pg-1"] and stub.started == []


def test_follow_command_restore_hint_survives_pick_failure(stub):
    stub.take("c1", "ff-1")  # nothing queued; the only start point mints nothing
    lines = []
    with pytest.raises(FollowError):
        follow_command(stub, "pg-1", print_fn=lines.append,
                       poll_timeout=0, input_fn=lambda prompt: "q")
    assert any("Group left stopped" in line for line in lines)


def test_follow_command_attrs_and_content_keys(stub):
    script_linear(stub)
    stub.content[2] = "payload"
    # a/c before any hop, step once, then show attrs and content, then quit.
    lines = run_command(stub, ["a", "c", "", "a", "c", "q"])
    assert lines.count("No hop yet — step first.") == 2
    assert "       = a: 2" in lines       # post-hop attributes
    assert "payload" in lines             # event output content


# ------------------------------------------------------------- rendering


def test_format_hop_matches_the_trace_layout():
    text = format_hop(3, hop(
        7, "ROUTE", "Router", relationship="unmatched",
        children=["kid-1"],
        changes=[{"name": "a", "before": "1", "after": "2"},
                 {"name": "fresh", "before": None, "after": "x"}]))
    assert text.splitlines() == [
        "  3. Router  [ROUTE -> unmatched]  12:00:00  3 B",
        "       a: 1 -> 2",
        "       fresh: (new) -> x",
        "       spawned kid-1  (niflow trace kid-1)",
    ]


def test_format_hop_full_lists_every_attribute():
    text = format_hop(1, hop(7), full=True)
    assert "       = a: 1" in text
    assert "       = filename: f" in text


# ------------------------------------------------------- start points


def test_entry_points_lists_queues_then_sources_then_ports(stub):
    stub.ports = [{"kind": "input_port", "id": "in1", "name": "in",
                   "path": "", "state": "STOPPED", "group_id": "pg-1"}]
    entries = entry_points(stub, "pg-1")
    assert [e["kind"] for e in entries] == ["queue", "source", "input_port"]
    queue, source, port = entries
    assert queue["id"] == "c1" and queue["label"] == "Gen -> Mid"
    # Gen has no inbound connection; Mid and Sink do, so they are not starts.
    assert source["label"] == "Gen" and source["queue_ids"] == ["c1"]
    assert port["queue_ids"] == []  # nothing wired downstream of it


def test_entry_points_skips_empty_queues(stub):
    stub.take("c1", "ff-1")
    assert [e["kind"] for e in entry_points(stub, "pg-1")] == ["source"]


def test_start_from_a_source_looks_only_in_its_own_queues(stub):
    stub.take("c1", "ff-1")
    stub.put("c2", "stray")          # an unrelated leftover elsewhere
    stub.events["stray"] = [hop(1)]
    stub.on_run_once["gen"] = lambda: stub.put("c1", "ff-2")
    stub.events["ff-2"] = [hop(4, "CREATE", "Gen")]
    f = follower(stub)
    entry = [e for e in entry_points(stub, "pg-1") if e["kind"] == "source"][0]
    assert f.start_from(entry, wait=0)["uuid"] == "ff-2"


def test_follow_command_list_only_never_quiesces(stub):
    lines = []
    rc = follow_command(stub, "pg-1", list_only=True, print_fn=lines.append)
    assert rc == 0
    assert stub.stopped_groups == []
    assert any("Gen -> Mid" in line for line in lines)


def test_follow_command_picks_a_start_point_interactively(stub):
    script_linear(stub)
    lines = run_command(stub, ["2", "", ""], start=None)  # 2 = the Gen source
    stub.on_run_once["gen"] = lambda: None
    assert "Starting from source 'Gen'." in lines


# --------------------------------------------------------- hop diffing


def test_annotate_hops_classifies_added_changed_and_removed():
    hops = [
        hop(1, "CREATE", "Gen", changes=[{"name": "a", "before": None,
                                          "after": "1"}]),
        hop(2, changes=[{"name": "b", "before": None, "after": "x"}]),
        hop(3, drop=("a",), size=9),
    ]
    annotate_hops(hops)
    # First hop has nothing to diff against: NiFi's own per-event changes.
    assert hops[0]["diff"] == [{"name": "a", "before": None, "after": "1",
                                "status": "added"}]
    assert hops[1]["diff"] == [{"name": "b", "before": None, "after": "x",
                                "status": "added"}]
    # A removed attribute is ONLY visible from the cross-hop diff.
    assert {"name": "a", "before": "1", "after": None,
            "status": "removed"} in hops[2]["diff"]
    assert hops[2]["content_change"] == {"before": 3, "after": 9}


def test_annotate_hops_flags_a_same_size_content_rewrite():
    hops = [hop(1), hop(2)]
    hops[1]["content_equal"] = False
    annotate_hops(hops)
    assert hops[0]["content_change"] is None
    assert hops[1]["content_change"] == {"before": 3, "after": 3}
    # ...and it reads as a rewrite, not as "3 B -> 3 B" (live: ReplaceText).
    assert "~ content rewritten (3 B)" in format_hop(2, hops[1])


def test_format_hop_marks_the_cross_hop_diff():
    hops = [hop(1), hop(2, changes=[{"name": "a", "before": "1", "after": "2"},
                                    {"name": "n", "before": None, "after": "y"}],
                       drop=("filename",), size=8)]
    annotate_hops(hops)
    lines = format_hop(2, hops[1]).splitlines()
    assert lines[1:] == [
        "       ~ a: 1 -> 2",
        "       + n: y",
        "       - filename: f  (removed)",
        "       ~ content: 3 B -> 8 B",
    ]


def test_step_diffs_against_the_previous_hop_of_the_branch(stub):
    script_linear(stub)
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["hops"][0]["diff"] == [
        {"name": "a", "before": "1", "after": "2", "status": "changed"}]


# ------------------------------------------------- branches and mutes


def script_fork_rel(stub):
    """Mid clones ff-1 down 'split' (kid-1 -> Sink) and drops the parent."""
    def run_mid():
        stub.take("c1", "ff-1")
        stub.put("c2", "kid-1")
        stub.events["ff-1"] += [
            hop(2, "CLONE", children=["kid-1", "kid-2"], relationship="split"),
            hop(3, "DROP")]
        stub.events["kid-1"] = []

    stub.on_run_once["mid"] = run_mid


def test_mute_before_the_fork_means_the_branch_is_never_followed(stub):
    script_fork_rel(stub)
    f = follower(stub)
    f.mute("split")                     # bare spec -> relationship
    f.pick_flowfile()
    outcome = f.step()
    assert [b["uuid"] for b in outcome["muted"]] == ["kid-1", "kid-2"]
    assert outcome["branches"] == []
    assert f.pending_children == []     # nothing left to follow
    assert f.next_live() is None


def test_mute_by_destination_hits_only_the_branch_that_went_there(stub):
    script_fork_rel(stub)
    f = follower(stub)
    f.mute("dest:Sink")                 # kid-1 is queued into Sink; kid-2 isn't
    f.pick_flowfile()
    outcome = f.step()
    assert [b["uuid"] for b in outcome["muted"]] == ["kid-1"]
    assert [b["uuid"] for b in outcome["branches"]] == ["kid-2"]


def test_mute_is_retroactive_and_reversible_and_never_calls_nifi(stub):
    script_fork_rel(stub)
    f = follower(stub)
    f.pick_flowfile()
    f.step()
    calls = (list(stub.ran_once), list(stub.started), list(stub.stopped_groups))
    assert f.pending_children == ["kid-1", "kid-2"]

    assert f.mute("uuid:kid-2") == {"rule": "uuid:kid-2", "muted": ["kid-2"]}
    assert f.pending_children == ["kid-1"]
    assert f.unmute("kid-2") == {"rule": "uuid:kid-2", "unmuted": ["kid-2"]}
    assert f.pending_children == ["kid-1", "kid-2"]
    # Muting is a view decision: not one REST mutation went out.
    assert (stub.ran_once, stub.started, stub.stopped_groups) == calls


def test_unmute_of_an_unknown_rule_says_what_is_muted(stub):
    f = follower(stub)
    f.mute("failure")
    with pytest.raises(FollowError, match="rel:failure"):
        f.unmute("success")


def test_unmute_finds_the_rule_whatever_kind_it_was(stub):
    f = follower(stub)
    f.mute("dest:Sink")
    assert f.unmute("Sink")["rule"] == "dest:Sink"      # unprefixed, any kind
    assert f.mutes.describe() == ""


def test_branch_relationship_falls_back_to_the_connection(stub):
    """CLONE events carry no relationship — the queue's does (live 1.24 fact)."""
    script_fork(stub)                     # fork hop has relationship ""
    stub.rels["c2"] = ["failure"]
    f = follower(stub)
    f.mute("failure")
    f.pick_flowfile()
    outcome = f.step()
    assert [b["uuid"] for b in outcome["muted"]] == ["kid-1"]
    assert f.session.branches["kid-1"]["relationship"] == "failure"


def test_branch_records_carry_where_the_child_came_from(stub):
    script_fork_rel(stub)
    f = follower(stub)
    f.pick_flowfile()
    f.step()
    kid = [b for b in f.branches() if b["uuid"] == "kid-1"][0]
    assert kid["parent"] == "ff-1"
    assert kid["relationship"] == "split" and kid["origin"] == "Mid"
    assert kid["queue"] == "Mid -> Sink" and kid["queue_id"] == "c2"
    # The child inherits the forking hop's attributes as its diff baseline.
    assert f.session.branches["kid-1"]["baseline"]["a"] == "1"


def test_auto_skips_muted_branches(stub):
    script_fork(stub)
    f = follower(stub)
    f.mute("uuid:kid-1")
    f.pick_flowfile()
    seen = []
    f.auto(on_event=seen.append)
    assert "kid-1" not in [o.get("uuid") for o in seen if o["status"] == "switched"]
    assert stub.ran_once == ["mid"]     # Sink (kid-1's destination) never ran


def test_switching_to_a_muted_branch_unmutes_it_explicitly(stub):
    script_fork_rel(stub)
    f = follower(stub)
    f.mute("split")
    f.pick_flowfile()
    f.step()
    f.switch_to("kid-1")
    assert f.uuid == "kid-1"
    assert f.session.branches["kid-1"]["state"] == "live"


def test_parse_mute_spec_guesses_uuid_versus_relationship():
    mutes = BranchMutes()
    assert mutes.add("failure") == ("rel", "failure")
    assert mutes.add("2f1c7a4e-1111-2222-3333-444455556666")[0] == "uuid"
    assert mutes.add("queue:c9") == ("queue", "c9")
    with pytest.raises(FollowError, match="unknown mute kind"):
        mutes.add("nope:x")


# ------------------------------------------------ robustness / retries


def test_step_reports_a_processor_that_cannot_run(stub):
    def boom(proc_id):
        raise RuntimeError("Processor is invalid")

    stub.run_processor_once = boom
    stub.invalid["mid"] = ["'Custom Text' is invalid because it is required"]
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["status"] == "blocked"
    assert "'Custom Text' is invalid" in outcome["message"]
    assert "fix it in NiFi" in outcome["message"]


def test_step_reports_a_permission_refusal_as_a_policy_hint(stub):
    class Denied(RuntimeError):
        status_code = 403

    stub.run_processor_once = lambda pid: (_ for _ in ()).throw(Denied("no"))
    f = follower(stub)
    f.pick_flowfile()
    assert "modify the component" in f.step()["message"]


def test_stalled_step_is_retryable_and_repoll_picks_the_events_up(stub):
    stub.on_run_once["mid"] = lambda: None      # run-once, nothing indexed yet
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["status"] == "stalled" and outcome["retryable"] is True
    assert "`r` re-polls" in outcome["message"]
    assert stub.ran_once == ["mid"] * f.run_attempts   # tried, file never moved
    # Provenance catches up (1.24 lags); repoll must NOT run the processor again.
    stub.events["ff-1"].append(hop(2))
    again = f.repoll()
    assert again["status"] == "advanced"
    assert stub.ran_once == ["mid"] * f.run_attempts


def test_step_reruns_the_destination_until_our_file_is_the_one_served(stub):
    """Run-once serves the queue front — live 1.24 fact that broke stepping."""
    stub.queue_files["c1"] = []             # two files ahead of ff-1
    stub.put("c1", "other-1", position=0)
    stub.put("c1", "other-2", position=1)
    stub.put("c1", "ff-1", position=2)

    def run_mid():
        front = stub.queue_files["c1"].pop(0)    # NiFi serves the front
        if front["uuid"] == "ff-1":
            stub.put("c2", "ff-1")
            stub.events["ff-1"].append(hop(2))

    stub.on_run_once["mid"] = run_mid
    f = follower(stub)
    f.pick_flowfile(uuid="ff-1")
    outcome = f.step()
    assert outcome["status"] == "advanced"
    # `ahead` now reports what is STILL in front of the file (nothing, it left)
    assert outcome["runs"] == 3 and outcome["ahead"] is None
    assert stub.ran_once == ["mid", "mid", "mid"]


def test_a_deep_queue_gets_one_run_per_file_ahead_not_a_fixed_eight(stub):
    """A work-sized queue: 20 files ahead must not exhaust an 8-run budget."""
    stub.queue_files["c1"] = []
    for i in range(20):
        stub.put("c1", f"other-{i}", position=i + 1)
    stub.put("c1", "ff-1", position=21)

    def run_mid():
        front = stub.queue_files["c1"].pop(0)
        for i, summary in enumerate(stub.queue_files["c1"]):
            summary["position"] = i + 1          # NiFi renumbers the queue
        if front["uuid"] == "ff-1":
            stub.put("c2", "ff-1")
            stub.events["ff-1"].append(hop(2))

    stub.on_run_once["mid"] = run_mid
    f = follower(stub)
    f.pick_flowfile(uuid="ff-1")
    outcome = f.step()
    assert outcome["status"] == "advanced"
    assert outcome["runs"] == 21          # 20 ahead + ours, not 8
    assert len(stub.ran_once) == 21


def test_a_file_that_moved_without_a_provenance_event_is_not_a_stall(stub):
    """1.24 fact: a plain transfer writes nothing to provenance."""
    stub.on_run_once["mid"] = lambda: (stub.take("c1", "ff-1"),
                                       stub.put("c2", "ff-1"))
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["status"] == "moved"
    assert outcome["retryable"] is False
    assert "no provenance event" in outcome["message"]
    assert outcome["hops"] and outcome["hops"][0]["synthetic"] is True
    assert stub.ran_once == ["mid"]   # not eight times


def test_a_file_that_vanished_without_an_event_is_still_reported_as_moved(stub):
    """Moved out of every queue: honest about the transfer, not a stall."""
    stub.on_run_once["mid"] = lambda: stub.take("c1", "ff-1")
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["status"] == "moved" and outcome["landed"] is None
    assert stub.ran_once == ["mid"]


def test_a_queue_that_cannot_be_listed_is_skipped_not_fatal(stub):
    real = stub.list_flowfiles

    def flaky(conn_id, max_results=100):
        if conn_id == "c1":
            raise RuntimeError("409 queue is being emptied")
        return real(conn_id, max_results)

    stub.list_flowfiles = flaky
    stub.put("c2", "ff-9")
    stub.events["ff-9"] = [hop(5, "CREATE", "Mid")]
    assert follower(stub).pick_flowfile()["uuid"] == "ff-9"


def test_a_failing_provenance_query_explains_the_policy(stub):
    def boom(uuid, after_event_id=-1, max_events=100):
        raise RuntimeError("500 provenance repository is busy")

    stub.flowfile_events_since = boom
    stub.queue_files["c1"] = [{"uuid": "ff-1", "filename": "f", "size": 3,
                               "position": 0}]
    with pytest.raises(FollowError, match="query provenance"):
        follower(stub).pick_flowfile()


def test_quiesce_failure_is_actionable(stub):
    stub.stop_group = lambda g: (_ for _ in ()).throw(RuntimeError("403"))
    with pytest.raises(FollowError, match="write access"):
        follower(stub).quiesce()


# ------------------------------------------------- history & sessions


def test_history_is_kept_per_branch_and_replayable(stub):
    script_fork(stub)
    f = follower(stub)
    f.pick_flowfile()
    f.step()                       # parent forks and drops
    f.switch_to("kid-1")
    f.step()                       # kid-1 reaches Sink and drops
    assert [h["event_id"] for h in f.session.history("ff-1")] == [2, 3]
    assert [h["event_id"] for h in f.session.history("kid-1")] == [9]
    assert [h["event_id"] for h in f.session.history()] == [9]  # current branch
    assert [u for u, _ in f.session.all_hops()] == ["ff-1", "ff-1", "kid-1"]


def test_session_round_trips_through_its_file(stub, tmp_path):
    script_linear(stub)
    session = FollowSession.open("pg-1", "pg-1", tmp_path)
    f = follower(stub, session=session)
    f.quiesce()
    f.pick_flowfile()
    f.mute("failure")
    f.step()

    loaded = FollowSession.load(session.path)
    assert loaded.current == "ff-1"
    assert loaded.last_event_id == 2
    assert [h["event_id"] for h in loaded.history()] == [2]
    assert loaded.mutes.rules["rel"] == ["failure"]
    assert [p["id"] for p in loaded.prior_running] == ["gen", "sink"]
    assert FollowSession.latest("pg-1", tmp_path).id == session.id


def test_follow_command_resumes_a_saved_session(stub, tmp_path):
    script_linear(stub)
    run_command(stub, ["", "q"], session_dir=tmp_path)
    lines = run_command(stub, ["", "q"], session_dir=tmp_path, resume=True,
                        start=None)
    text = "\n".join(lines)
    assert "Resumed session" in text and "1 hop(s) already taken" in text
    # The resumed session keeps stepping the same file to its DROP.
    assert "[DROP]" in text


def test_resume_keeps_the_prior_running_set_for_restore(stub, tmp_path):
    script_linear(stub)
    run_command(stub, ["q"], session_dir=tmp_path)
    # Everything is stopped now; a resume must still know what to restart.
    for proc in stub.procs:
        proc["state"] = "STOPPED"
    lines = run_command(stub, ["q"], session_dir=tmp_path, resume=True,
                        start=None, restore=True)
    assert "Restored 2 previously-running component(s)." in lines


def test_history_key_reprints_a_past_hop_without_rerunning(stub):
    script_linear(stub)
    lines = run_command(stub, ["", "", "h", "h 1", "q"])
    # `h` re-renders from the session; nothing extra ran.
    assert stub.ran_once == ["mid", "sink"]
    assert sum(line.startswith("  1. Mid") for line in lines) >= 2


# ------------------------------------------------ crossing a group boundary


def _nest(stub):
    """Give the stub a child group entered through an input port.

    Gen -(c1)-> [in] -(c3)-> Mid: the shape every work flow has, and the one
    run-once cannot drive.
    """
    stub.dests["c1"] = {"id": "in-1", "name": "in", "type": "INPUT_PORT",
                        "groupId": "pg-2"}
    stub.queues.append({"id": "c3", "source": "in", "destination": "Mid",
                        "path": "Child", "source_id": "in-1",
                        "destination_id": "mid"})
    stub.queue_files["c3"] = []
    stub.rels["c3"] = ["success"]
    stub.dests["c3"] = {"id": "mid", "name": "Mid", "type": "PROCESSOR"}
    stub.ports = [{"id": "in-1", "name": "in", "kind": "input_port",
                   "state": "STOPPED", "path": "Child", "group_id": "pg-2"}]

    def cross():
        stub.take("c1", "ff-1")
        stub.put("c3", "ff-1")

    stub.on_port_start["in-1"] = cross


def test_step_carries_the_flowfile_across_an_input_port(stub):
    _nest(stub)
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["status"] == "crossed"
    assert outcome["landed"]["id"] == "c3"
    # started, then stopped again — a debugger hands the flow back as it found it
    assert stub.port_states == [("input_port", "in-1", "RUNNING"),
                                ("input_port", "in-1", "STOPPED")]
    assert stub.ran_once == []          # run-once is not how a port moves data
    hop_ = outcome["hops"][0]
    assert hop_["event_type"] == "CROSS" and hop_["synthetic"] is True
    assert "crossed input port" in hop_["lineage"]


def test_a_port_that_will_not_start_is_reported_not_crossed(stub):
    _nest(stub)

    def refuse(kind, port_id, state):
        raise RuntimeError("403 not authorized")

    stub.set_port_state = refuse
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["status"] == "blocked"
    assert "group boundary" in outcome["message"]


def test_the_port_is_stopped_again_even_when_the_file_does_not_move(stub):
    _nest(stub)
    stub.on_port_start.pop("in-1")      # back-pressured: nothing crosses
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["status"] == "stalled" and outcome["retryable"] is True
    assert stub.port_states[-1] == ("input_port", "in-1", "STOPPED")


def test_quiesce_and_restore_put_running_ports_back(stub):
    _nest(stub)
    stub.ports[0]["state"] = "RUNNING"
    f = follower(stub)
    f.quiesce()
    assert f.restore() == 3            # two processors AND the port
    assert ("input_port", "in-1", "RUNNING") in stub.port_states


# ------------------------------------------------------ invalid destinations


def test_run_once_is_never_sent_to_an_invalid_processor(stub):
    """NiFi 200s on RUN_ONCE against an invalid processor and wedges it."""
    stub.invalid["mid"] = ["'x' is invalid because reasons"]
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["status"] == "blocked" and outcome["runs"] == 0
    assert stub.ran_once == []
    assert "is invalid" in outcome["message"] and "reasons" in outcome["message"]


def test_a_processor_already_wedged_in_run_once_is_named(stub):
    stub.proc_states["mid"] = "RUN_ONCE"
    stub.processor_validation = lambda pid: {
        "state": "RUN_ONCE", "status": "VALIDATING", "errors": []}
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["status"] == "blocked"
    assert "stuck in RUN_ONCE" in outcome["message"]
    assert stub.ran_once == []


def test_a_disabled_destination_is_blocked_with_advice(stub):
    stub.proc_states["mid"] = "DISABLED"
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["status"] == "blocked" and "DISABLED" in outcome["message"]


# --------------------------------------------------- past the listing cap


def test_a_flowfile_deeper_than_the_listing_is_still_located(stub):
    """NiFi lists 100 per queue and will not raise the cap."""
    stub.cap = 1                         # the listing only ever shows one file
    stub.put("c1", "ff-deep", position=2)
    f = follower(stub)
    found = f.pick_flowfile(uuid="ff-deep")
    assert found["uuid"] == "ff-deep" and found["beyond_listing"] is True
    assert found["flowfile"]["position"] is None


def test_a_buried_flowfile_is_not_reported_as_dropped(stub):
    stub.cap = 1
    stub.put("c1", "ff-deep", position=2)
    stub.on_run_once["mid"] = lambda: (stub.take("c1", "ff-deep"),
                                       stub.put("c2", "ff-deep"))
    f = follower(stub)
    f.pick_flowfile(uuid="ff-deep")
    outcome = f.step()
    assert outcome["status"] != "gone"


# ------------------------------------------------------- lineage attribution


def _lineage_hop(**over):
    base = hop(9, "JOIN", "Merger")
    base.update({"flowfile_uuid": "merged-1", "own": False,
                 "parents": ["ff-1", "ff-2"], "children": ["merged-1"],
                 "size": 999})
    base.update(over)
    return base


def test_a_relatives_event_is_labelled_not_diffed():
    """A FlowFileUUID query is a lineage query — verified on 1.24 and 2.7.2."""
    hops = [hop(1, "CREATE", "Gen", size=3), _lineage_hop(),
            hop(10, "DROP", "Merger", size=3)]
    annotate_hops(hops)
    assert hops[1]["diff"] == [] and hops[1]["content_change"] is None
    assert "merged into merged-1" in hops[1]["lineage"]
    # …and it must not become the baseline for the hop after it.
    assert hops[2].get("lineage") in (None, "")
    assert hops[2]["content_change"] is None      # 3 B -> 3 B, not 999 B -> 3 B


def test_a_fork_belonging_to_the_parent_says_so():
    forked = _lineage_hop(event_type="FORK", component="Splitter",
                          flowfile_uuid="parent-1",
                          children=["ff-1", "sib-1", "sib-2"], parents=[])
    annotate_hops([forked])
    assert "was born here" in forked["lineage"]
    assert format_hop(1, forked).count("continues as") == 3


def test_a_merge_registers_the_merged_file_as_the_next_branch(stub):
    """The one relative's event that counts: the JOIN that consumed us."""
    def merge():
        stub.take("c1", "ff-1")
        stub.put("c2", "merged-1")
        stub.events["ff-1"].append(_lineage_hop())

    stub.on_run_once["mid"] = merge
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    branches = {b["uuid"]: b for b in outcome["branches"]}
    assert "merged-1" in branches
    assert branches["merged-1"]["destination"] == "Sink"


def test_a_relatives_fork_children_are_not_adopted_as_ours(stub):
    """A sibling set is not a child set — 49 phantom branches otherwise."""
    def forked():
        stub.take("c1", "ff-1")
        stub.events["ff-1"].append(_lineage_hop(
            event_type="FORK", flowfile_uuid="parent-1", parents=[],
            children=["ff-1", "sib-1", "sib-2"]))

    stub.on_run_once["mid"] = forked
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["branches"] == [] and outcome["muted"] == []


def test_only_our_own_drop_ends_the_branch(stub):
    """A merged file's DROP is not ours, and must not close our branch."""
    def consumed():
        stub.take("c1", "ff-1")
        stub.put("c2", "ff-1")
        stub.events["ff-1"].append(_lineage_hop(
            event_type="DROP", flowfile_uuid="merged-1", parents=[], children=[]))

    stub.on_run_once["mid"] = consumed
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["dropped"] is False


def test_cli_trace_says_when_the_journey_was_capped(monkeypatch, capsys):
    """T7g: hop #1 of a capped trace is not the origin — the CLI has to say so."""
    from niflow import cli

    hop = {
        "flowfile_uuid": "u1", "own": True, "event_id": 9, "event_type": "SEND",
        "time": "12:00:00.000", "component": "Put", "component_id": "p1",
        "group_id": "g1", "relationship": "success", "size": 10,
        "attributes": {"a": "1"}, "changes": [], "children": [], "parents": [],
        "input_available": False, "output_available": False, "content_equal": None,
    }

    class C:
        def trace_flowfile(self, uuid, max_events=1000):
            assert max_events == 5
            return {"uuid": uuid, "hops": [hop], "truncated": True}

    monkeypatch.setattr(cli, "_client", lambda: C())
    args = argparse.Namespace(uuid="u1", full=False, max_events=5)
    assert cli.cmd_trace(args) == 0
    out = capsys.readouterr().out
    assert "newest 1 hops of a longer journey" in out


def test_cli_trace_is_quiet_when_the_journey_is_complete(monkeypatch, capsys):
    from niflow import cli

    class C:
        def trace_flowfile(self, uuid, max_events=1000):
            return {"uuid": uuid, "hops": [], "truncated": False}

    monkeypatch.setattr(cli, "_client", lambda: C())
    assert cli.cmd_trace(argparse.Namespace(uuid="u1", full=False, max_events=1000)) == 1
    out = capsys.readouterr().out
    assert "No provenance events" in out and "aged out" in out


def test_a_merging_destination_says_it_is_binning_not_stuck(stub):
    """T7c: 'stalled' at a MergeContent sends people hunting the wrong thing.

    The step is correct — 49 files really are missing — but the reader cannot
    tell that from "no provenance event yet", which reads as an indexing lag.
    """
    stub.dests["c1"] = {"id": "merge-1", "name": "Merge", "type": "PROCESSOR"}
    stub.proc_properties["merge-1"] = {
        "Minimum Number of Entries": "50", "Max Bin Age": "10 sec"}
    f = follower(stub)
    f.pick_flowfile()
    stub.events["ff-1"] = []          # run-once produced nothing: the bin isn't full
    outcome = f.step()

    assert outcome["status"] == "stalled"
    assert "binning, not stuck" in outcome["message"]
    assert "needs 50" in outcome["message"] and "49 more" in outcome["message"]
    assert "10 sec" in outcome["message"]


def test_a_record_merger_says_records_not_flowfiles(stub):
    stub.dests["c1"] = {"id": "merge-1", "name": "MergeRecord", "type": "PROCESSOR"}
    stub.proc_properties["merge-1"] = {"min-records": "1000"}   # 1.24 keys it kebab-case
    f = follower(stub)
    f.pick_flowfile()
    stub.events["ff-1"] = []
    assert "by *records*" in f.step()["message"]


def test_a_plain_processor_still_gets_the_run_once_explanation(stub):
    """Nothing changes for a destination that does not declare a bin."""
    f = follower(stub)
    f.pick_flowfile()
    stub.events["ff-1"] = []
    message = f.step()["message"]
    assert "has not moved" in message and "binning" not in message


def _wide_fork(stub, count=50):
    """One CLONE hop that spawns `count` children on the same relationship."""
    children = [f"ff-c{i}" for i in range(count)]
    stub.rels["c2"] = ["split"]
    stub.dests["c2"] = {"id": "merge-1", "name": "Merge", "type": "PROCESSOR"}

    def run_mid():
        stub.take("c1", "ff-1")
        for child in children:
            stub.put("c2", child)
        stub.events["ff-1"].append(hop(2, "CLONE", "Split", children=children))

    stub.on_run_once = {"mid": run_mid}
    return children


def test_a_wide_fork_folds_into_one_group_row(stub, capsys):
    """T7b: 50 rows for what is one branch fifty times over is unusable."""
    from niflow.follow import _show_branches

    children = _wide_fork(stub)
    f = follower(stub)
    f.pick_flowfile()
    f.step()
    assert len(f.branches()) >= len(children)

    groups = f.branch_groups()
    fork = [g for g in groups if g["relationship"] == "split"]
    assert len(fork) == 1
    assert fork[0]["total"] == len(children)
    assert fork[0]["live"] == len(children)
    assert len(fork[0]["sample"]) == 3          # enough to act on, not 50 rows

    lines = []
    _show_branches(f, lines.append)
    printed = "\n".join(lines)
    assert "group(s)" in printed and "`b all` lists every one" in printed
    assert "mute all: m dest:" in printed
    assert printed.count("ff-c") <= 6           # samples only

    lines = []
    _show_branches(f, lines.append, "all")
    assert "\n".join(lines).count("ff-c") >= len(children)


def test_a_narrow_fork_still_prints_every_branch(stub):
    from niflow.follow import _show_branches

    _wide_fork(stub, count=3)
    f = follower(stub)
    f.pick_flowfile()
    f.step()
    lines = []
    _show_branches(f, lines.append)
    assert "group(s)" not in "\n".join(lines)
    assert "\n".join(lines).count("ff-c") == 3
