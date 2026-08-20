# Trace and follow

Two answers to "what happened to my file?".

* **`niflow trace <uuid>`** — post-hoc. Replay a FlowFile's journey from NiFi's
  provenance repository, hop by hop, with the attribute diff at each one.
* **`niflow follow <group>`** — live. Quiesce a group and step **one** FlowFile
  through it, one processor at a time, like hitting F5 in a debugger.

Both render a hop the same way, and both have a tab in the web GUI.

## trace

```bash
niflow trace 0d4e1f2a-...           # a uuid from a queue listing, bulletin or log
niflow trace <uuid> --full          # every attribute at every hop, not just changes
niflow trace <uuid> --max-events 50 # cap the journey (newest N)
```

```
  1. CreateFlowFile  [CREATE]  12:00:01.412  0 B
       + filename: 0d4e1f2a
  2. AddA  [ATTRIBUTES_MODIFIED -> success]  12:00:01.480  0 B
       ~ a: (new) -> 1
  3. Merge  [JOIN]  12:00:02.004  1030 B
       ⤳ this FlowFile was merged into 91ab34c2… here, together with 49 other(s)
```

Things worth knowing:

* **A `FlowFileUUID` query is a lineage query, not a filter.** Asking for a
  split child's uuid also returns the parent's `FORK` and the merged file's
  `JOIN`. Those describe a *different* FlowFile, so they are labelled (`⤳`)
  rather than diffed — diffing them produces nonsense like "40 attributes
  changed".
* **A capped journey shows the NEWEST N hops**, so hop #1 is then *not* where
  the file began. It says so when that happens.
* **No hops** means the uuid is wrong or the events have aged out of the
  provenance repository — it says that too, rather than printing nothing.
* Content in/out is fetchable per hop where NiFi still has it (the GUI has
  buttons; `--full` shows attributes).

## follow

```bash
niflow follow "Prod Flow (copy)" --list          # plausible start points, read-only
niflow follow "Prod Flow (copy)"                 # start stepping
niflow follow "Prod Flow (copy)" --source "CreateFlowFile"   # mint a file first
niflow follow "Prod Flow (copy)" --auto --max-hops 30        # run to a terminal state
niflow follow "Prod Flow (copy)" --resume        # re-attach to the saved session
niflow follow "Prod Flow (copy)" --restore       # restart what was running, afterwards
```

The stepper **quiesces the group** (that is the point: nothing else moves while
you step) and then uses NiFi's run-once, one hop at a time. Sessions live in
`.niflow-follow/` and survive a restart.

### Branches

When the file forks, you get a branch tree instead of a prompt storm:

```bash
niflow follow "Prod Flow" --mute failure          # before you start
niflow follow "Prod Flow" --mute dest:PutFile --mute 'queue:<conn-id>'
```

In the loop: `m`/`u` mute and unmute, `s` steps, `b` lists branches. Mute specs
are `rel:<name>`, `dest:<component>`, `queue:<id>`, `uuid:<child>`, or a bare
value (UUID-shaped means a uuid, anything else a relationship).

**Muting is a view decision.** It never issues a mutating REST call — NiFi keeps
running that branch, you just stop following it. Branch records are kept, so
unmuting brings the history back. When a branch ends, the stepper moves to the
next un-muted one by itself.

### Two live facts that shaped it

* **`CLONE`/`FORK` events carry no relationship** (checked on 1.24 and 2.7.2),
  so a branch's name comes from its queue's selected relationships.
* **Run-once serves one FlowFile from one of a processor's inbound queues** —
  not necessarily the one you are following. A step therefore re-runs the
  destination until *your* file moves, and reports "ran X 3x, 2 FlowFile(s)
  were ahead of it". Without that, stepping silently did nothing on any
  processor with a busy second inbound queue.
* Ports and funnels record **no provenance event**, so crossing one produces a
  synthetic hop that says where the file went. Synthetic hops carry no time or
  size (printing `0 B` would read as an empty FlowFile) and are drawn dashed in
  the GUI.
