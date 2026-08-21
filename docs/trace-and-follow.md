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
* **NiFi's `maxResults` does not mean "the newest N".** The cap is applied per
  index shard, so a capped query answers with an arbitrary subset — measured on
  1.24 against a component with 800 events, asking for 10 returned events from
  the *previous day*. niflow therefore raises the cap until the answer comes
  back complete, and for a component too busy for that (more events than the
  5,000 ceiling, however far the cap is raised) it asks for a **time window**
  instead: NiFi answers a window that fits under the cap completely, so the
  walk steps backwards in windows — narrowing while a slice is still capped,
  widening as it moves into sparser history — until it has the newest N. It is
  also much cheaper: 0.06s and three queries against ~200k events on 1.24,
  where escalating the count alone took 0.64s and still could not settle it.
* **No hops** means the uuid is wrong or the events have aged out of the
  provenance repository — it says that too, rather than printing nothing.
* Content in/out is fetchable per hop where NiFi still has it (the GUI has
  buttons; `--full` shows attributes).

## follow

```bash
niflow follow "Prod Flow (copy)" --list          # plausible start points, read-only
niflow follow "Prod Flow (copy)"                 # start stepping
niflow follow "Prod Flow (copy)" --source "CreateFlowFile"   # mint a file first
niflow follow "Prod Flow (copy)" --inject-at Stamp --content 'id\n1' --attr case=urgent
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

### Inject your own FlowFile

Every other start point waits for the flow to produce something. `--inject-at`
is the debugger's own input: the bytes and the attributes **you** choose, at the
component you care about.

```bash
niflow follow "Prod Flow" --inject-at Stamp \
    --content 'id,priority
1,urgent' --attr case=urgent --attr source=fixture
niflow follow "Prod Flow" --inject-at "Stage/Enrich" --content-file sample.csv
```

It works the way `niflow test` injects: a temporary `GenerateFlowFile` named
`niflow-inject` is wired to the target, triggered **once**, and the file it
mints is the one you follow. On a quiesced canvas that is the only thing that
moves. Dynamic properties become the FlowFile's attributes — and the
custom-text property is named per NiFi line, because a REST create is *not*
config-migrated the way a snapshot import is (the wrong key becomes an inert
dynamic property and the file arrives empty).

The target can be a processor or a **nested** input port. The group's own input
port is refused with a reason: it is fed from outside the followed group, so the
injector would land outside it too — and the file would never appear in any
queue the stepper watches.

The injector is removed when the session ends — **unless the fixture is still
sitting in its queue**, because removing it means draining that queue, and the
drain would be the file. Then it stays, and `--resume` picks the journey up.

### Watch expressions

`--watch NAME` (or `w NAME` in the loop) follows one attribute across every hop
and prints the hop × attribute table:

```
hop  component             filename              case
-----------------------------------------------------------------
  1  niflow-inject          fixture.txt          urgent
  2  Stamp                  fixture.txt         ~triaged
  3  Route                  fixture.txt         ~triaged
  4  Enrich                ~enriched.txt         -·
```

`~` changed here, `+` first set here, `-` removed here, `·` not set. A glob
(`w http.*`) expands to whatever names the flow actually set; an exact name that
nothing sets stays as a column, because "never set" is usually the answer you
were after. `@size`, `@component`, `@event` and `@rel` watch the hop itself.

Hops belonging to a *relative* — a merge's `JOIN`, a fork's parent event — are
blank in the table and do not become the baseline, for the same reason they are
not diffed: they describe a different FlowFile.

### Replay after a fix

The loop this closes: step through, see the bug, fix the flow, push, replay.

```
[step] rr            # re-inject the same fixture, from the top
[step] ␍ ␍ ␍         # step it again
[step] cmp           # what changed between the runs
```

```
Run 1 vs run 2: 4 hop(s) -> 5; 2 differ.
  3. Route [ATTRIBUTES_MODIFIED]
       relationship: unmatched -> matched
       ~ priority: (new) -> urgent
  5. + only in run 2: PutFile [SEND]
```

`rr` archives the finished run, drops the old injector, and injects the same
bytes and attributes at the same place — so `cmp` is a like-for-like
comparison, positional hop by hop. Mutes and watches carry over deliberately:
they are how you are looking at the flow, not what the flow did.

From a fresh shell, `--resume --replay` does the same thing to the last saved
session, which is the shape that fits the fix-push-retest loop.

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
