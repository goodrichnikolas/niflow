"""Unit tests for the live FlowFile stepper (niflow follow).

A NiFiClient-shaped stub scripts a tiny flow — Gen -(c1)-> Mid -(c2)-> Sink —
and run-once side effects move the file and append provenance hops, so the
follower's quiesce/step/fork/auto/restore logic runs without a live NiFi.
"""
import pytest

from niflow.follow import FlowFollower, follow_command, format_hop


def hop(event_id, event_type="ATTRIBUTES_MODIFIED", component="Mid",
        children=(), changes=(), relationship=""):
    return {
        "event_id": event_id, "event_type": event_type, "time": "12:00:00",
        "component": component, "component_id": "", "component_type": "",
        "relationship": relationship, "size": 3,
        "attributes": {"a": "1", "filename": "f"},
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
            {"id": "c1", "source": "Gen", "destination": "Mid", "path": ""},
            {"id": "c2", "source": "Mid", "destination": "Sink", "path": ""},
        ]
        self.queue_files = {"c1": [], "c2": []}  # conn id -> [summaries]
        self.dests = {"c1": {"id": "mid", "name": "Mid", "type": "PROCESSOR"},
                      "c2": {"id": "sink", "name": "Sink", "type": "PROCESSOR"}}
        self.events = {}       # uuid -> full hop history
        self.on_run_once = {}  # processor id -> side-effect callable
        self.stopped_groups = []
        self.started = []
        self.ran_once = []
        self.content = {}      # event id -> payload

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
        return list(self.queue_files[conn_id])

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
    kw.setdefault("poll_timeout", 0)  # never sleep in unit tests
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
    with pytest.raises(ValueError, match="--source"):
        follower(stub).pick_flowfile()


def test_kick_source_runs_once_by_name(stub):
    f = follower(stub)
    assert f.kick_source("Gen") == "Gen"
    assert stub.ran_once == ["gen"]
    with pytest.raises(ValueError, match="no processor"):
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


def test_step_is_terminal_at_a_non_processor_destination(stub):
    stub.take("c1", "ff-1")
    stub.put("c2", "ff-1")
    stub.dests["c2"] = {"id": "op", "name": "out", "type": "OUTPUT_PORT"}
    f = follower(stub)
    f.pick_flowfile()
    outcome = f.step()
    assert outcome["status"] == "terminal"
    assert outcome["end"]["type"] == "OUTPUT_PORT"
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
    stub.dests["c2"] = {"id": "op", "name": "out", "type": "OUTPUT_PORT"}

    def run_mid():
        stub.take("c1", "ff-1")
        stub.put("c2", "ff-1")
        stub.events["ff-1"].append(hop(2))

    stub.on_run_once["mid"] = run_mid
    f = follower(stub)
    f.pick_flowfile()
    result = f.auto()
    assert result["reason"] == "terminal"
    assert result["end"]["type"] == "OUTPUT_PORT"


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
    assert "a: 1 -> 2" in text            # the hop diff, via format_hop
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
    assert "Restored 2 previously-running processor(s)." in lines


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


def test_follow_command_fork_prompt_picks_a_child(stub):
    script_fork(stub)
    # Step (fork + parent drop), then choose child 2 at the fork prompt,
    # then quit.
    lines = run_command(stub, ["", "2", "q"])
    text = "\n".join(lines)
    assert "spawned kid-1" in text and "spawned kid-2" in text
    assert "Following kid-2." in lines


def test_follow_command_restore_hint_survives_pick_failure(stub):
    stub.take("c1", "ff-1")  # nothing queued, no --source -> pick fails
    lines = []
    with pytest.raises(ValueError):
        follow_command(stub, "pg-1", print_fn=lines.append,
                       input_fn=lambda prompt: "q")
    assert any("Group left stopped" in line for line in lines)


def test_follow_command_attrs_and_content_keys(stub):
    script_linear(stub)
    stub.content[2] = "payload"
    # a/c before any hop, step once, then show attrs and content, then quit.
    lines = run_command(stub, ["a", "c", "", "a", "c", "q"])
    assert lines.count("No hop yet — step first.") == 2
    assert "       = a: 1" in lines       # post-hop attributes
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
