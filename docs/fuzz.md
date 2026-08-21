# Fuzz — hunting niflow's own bugs in bulk

The problem this solves is a work-day problem: you find a niflow bug at work,
you cannot fix it there, and you carry it home. One painful discovery per day
does not converge. `niflow fuzz` generates thousands of micro-flows and finds
them in batches instead.

```bash
make fuzz                       # tier 1, offline: ~3,400 cases in ~10s
make fuzz TIER=2                # + NiFi's own validation in a sandbox
make fuzz TIER=3                # + live push/pull/plan convergence
make fuzz-v1 TIER=3             # the same against the 1.24 container
niflow fuzz --replay shape-219134a6fe    # re-run one case and print everything
```

Exit code 1 means it found something — it drops into CI next to the tests.

## The three tiers

1. **Offline.** For every harvested type: build a flow, emit JSON and XML,
   re-parse, plan it against itself, translate it to the other NiFi line. No
   server. This catches emitter/parser/differ bugs — and the invariant
   `to_json(from_json(to_json(f))) == to_json(f)`.
2. **+ NiFi's validation.** Push each case to a sandbox and compare **NiFi's**
   validation errors with `niflow validate`'s. Disagreements in either
   direction are findings: a false positive trains people to ignore the
   validator, a false negative is the bug that reaches the server.
3. **+ live convergence.** Push, pull it back, plan — the plan must be empty —
   then `push --update` again, which must be a no-op. This is where
   "drift forever" bugs surface, and most of the interesting ones did.

## What it generates

Case kinds: `solo` (one processor), `props` (property variants — allowable
values, defaults, empty strings), `pair` (two wired processors), `service`
(a processor plus the controller service it references), `svc` (a controller
service **on its own**, with the same property variants), `params` (a parameter
context, including a sensitive parameter, referenced from a property), `shape`
(self-loops, fan-outs, nested groups, deliberately broken wiring).

Three checks run on every case beyond the round trips:

* **Secret containment.** A sensitive parameter's *value* must appear in
  nothing niflow writes — snapshot, template, or generated Python — and the
  parameter itself must still survive, or "no leak" could be satisfied by
  quietly dropping it. A secret in a flow file is a secret in git.
* **Apply fault injection.** Each case's plan is applied against an in-memory
  NiFi (`niflow/fuzz/fakeserver.py`) that can fail on the Nth mutating call,
  for the add, update and remove shapes. Two invariants: a clean apply must not
  raise, and a failing call must surface as an `ApplyError` whose progress adds
  up — a bare `KeyError` mid-push is unreadable, and wrong `applied`/`remaining`
  sends someone rolling back the wrong thing. Until this existed, `apply.py`
  was only exercised against a real server, where faults cannot be induced.
* **Not-comparable changes are excluded.** A sensitive property reads back as
  nothing however it was set, so a plan that consists only of secrets is not a
  convergence failure; it is counted as an observation instead.

Generation is **seeded and content-addressed**: `--seed` reproduces a sweep,
and every case has a stable id you can `--replay`. `--types` takes a regex,
`--kinds` a comma list, `--count 0` runs everything.

## Reading the output

Findings are grouped by **root-cause signature**, so 172 failures read as one
bug rather than 172. Each group names the first case, the replay command, and a
standalone repro file:

```
  [live_push_update:remove:controller_service|update:processor|properties[…]]  ×10
    push --update right after a clean push still applies 2 change(s)
    first case: shape-219134a6fe  (shape=self_loop source=PutDynamoDB target=PutRedisHashRecord)
    replay:     niflow fuzz --replay shape-219134a6fe
    repro:      .niflow-fuzz/repro/shape-219134a6fe/flow.py
```

A repro is a normal flow module — `niflow validate`, `plan` and `push` all work
on it directly.

Results stream to `.niflow-fuzz/results.jsonl` (git-ignored); `--resume`
continues an interrupted sweep. NiFi rejecting a generated flow is **not** a
finding — the sweep says so separately, along with types that do not exist on
1.x and the properties dropped when crossing lines.

## Where it got to

Round one found 8 offline signatures and a "cries wolf" cluster of
drift-forever bugs. As of 2026-08-20 the offline sweep is **3,419 cases / 0
findings**, and tier 3 is **120 / 0 on both 2.7.2 and 1.24.0**. Everything it
found along the way is written up in `todo.md`, with the fix and the live
evidence next to each entry.

What the sweep still cannot see (worth knowing before you trust a green run):
NiFi's *runtime* behaviour — a flow that pushes, validates and round-trips
cleanly can still do the wrong thing with data. That is what
[`niflow test`](testing.md) and [`follow`](trace-and-follow.md) are for.

One live-tier gotcha it now handles, having been bitten: **parameter contexts
are global** and outlive the group that used them, so the sweep names its
context like its sandbox groups and deletes it at the end. A stale one left
thirteen later cases unable to export their group at all on 1.24.
