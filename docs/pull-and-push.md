# Pull and push

The loop niflow exists for: take a group off the canvas, edit it as Python,
put it back.

```bash
niflow list                                   # the tree, with ids
niflow copy "Prod Flow"                       # detached working copy — edit THIS
niflow pull "Prod Flow (copy)" -o flows/prod.py
$EDITOR flows/prod.py
niflow push flows/prod.py --update            # apply just the delta
```

## Pull

```bash
niflow pull "Prod Flow" -o flows/prod.py      # a name, an a/b path, or an id
niflow pull "Prod Flow" --format json         # the raw VersionedFlowSnapshot
niflow pull --all -o flows/                   # every top-level group, one file each
niflow pull --all --parent "Team A" -o flows/ # mirror one subtree instead of root
```

A pulled module is a normal Python file exposing a top-level `flow`:

```python
from niflow import Flow, Processor

flow = Flow('AbcToJson')
gen = Processor(
    name='CreateFlowFile',
    type='org.apache.nifi.processors.standard.GenerateFlowFile',
    properties={'File Size': '0 B', 'Batch Size': '1'},
    scheduling_period='60 sec',
)
sink = Processor(
    name='Sink',
    type='org.apache.nifi.processors.standard.PutFile',
    properties={'Directory': '/out'},
    auto_terminate=['success', 'failure'],
)
flow.add(gen, sink)
flow.add_connection(gen >> sink)
```

`a >> b` connects on `success`; for anything else use `a.to(b, ["failure"])`,
which also takes queue settings (`back_pressure_object_threshold=...`).
Nested groups use a context manager:

```python
with flow.process_group("Stage 2") as stage:
    stage.add_processor(Processor(name="Work", type=...))
```

**What a pull carries:** processors, controller services, ports, funnels,
labels, connections (with queue settings and prioritizers), parameter-context
bindings, nested groups, and positions. Run state is overlaid from the live
server — NiFi's `/download` sanitises it (every service reads DISABLED, no
processor reads RUNNING), so niflow asks the controller-services and processor
endpoints for the truth.

**What it cannot carry:** sensitive parameter *values*. NiFi never exports
them. They come from `.niflow-secrets.env` at push time — see the Secrets
section of the top-level README.

`flows/` is git-ignored by default (a pulled flow holds real hostnames, SQL and
queue names); the repo's own example flows are the listed exceptions.

## Push

Two strategies, and the difference matters:

```bash
niflow push flows/prod.py                # replace the group wholesale
niflow push flows/prod.py --update       # apply only the diff, in place
niflow push flows/prod.py --start        # ...and start it afterwards
niflow push --all flows/ --update        # reconcile a whole directory
```

* **`--update`** computes a plan (see [plan-and-apply.md](plan-and-apply.md))
  and issues targeted REST calls. Queues, run state and component ids survive.
  This is the one to use against anything you care about.
* **Plain push** rebuilds the group. If the group is under NiFi Registry
  version control, niflow does an **in-place rebuild** — the group id and its
  registry linkage are kept, so the push shows up as *local changes* you can
  review and commit (`niflow commit`), instead of orphaning a new group. If it
  is not versioned, it is delete-and-recreate.

Either way a backup is written first (see
[backup-and-rollback.md](backup-and-rollback.md)).

### Before it touches NiFi

`push` refuses, before any mutation, when:

* two components of the same kind in one group share a name — niflow's identity
  is name-based, so pushing would silently merge or clobber them;
* a component is *wired* but never added (a controller service used as a
  property value, a connection endpoint not in the flow).

Both used to surface as an unreadable error in the middle of an emit — i.e.
after a delete-and-recreate push had already torn the old group down.

It also *warns* (never blocks) when properties or types cannot survive the
crossing to the server's NiFi line, and when the flow would not work on your
declared compatibility baseline (`NIFLOW_MIN_NIFI_VERSION`, default 1.24). Run
[`niflow validate`](validate.md) to see all of it offline, with a non-zero exit.

## Environments and secrets

```bash
niflow push flows/prod.py --env prod      # .niflow-params.prod.env overlays parameter values
niflow push flows/prod.py --secrets .niflow-secrets.prod.env
```

`.niflow-params.<env>.env` is meant to be committed; `.niflow-secrets*.env`
never is (it is git-ignored).

## Keeping code and canvas honest

```bash
niflow diff flows/prod.py     # raw JSON diff, local vs live
niflow drift                  # one line per module in flows/, exit 1 if any diverged
```

`drift` is the cron/CI shape: it answers "has anyone changed this on the canvas
behind our backs?" without touching anything.
