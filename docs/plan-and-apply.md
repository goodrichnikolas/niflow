# Plan and apply

`niflow plan` answers "what would `push --update` do?", semantically, without
touching anything:

```bash
niflow plan flows/prod.py
```

```
~ processor Ingest: Fetch Orders
    properties[Batch Size]: '10' -> '50'
    scheduling_period: '1 min' -> '30 sec'
+ connection .: Split -> Audit
- processor .: Old Debug Logger
Plan: 1 to add, 1 to change, 1 to remove.
```

`push --update` applies exactly that plan with targeted REST calls: queues,
component ids and run state survive. A processor is stopped only if it has to
be, and restarted afterwards if it was running.

## What identity means here

Components are matched **by name within their group** — that is why `push`
refuses a group with two processors of the same name. Connections are matched
by their endpoints; funnels (which have no name) by their connection topology;
labels by their text.

## Silence is not a request

The differ compares *effective* values, not raw dicts. A model that says
nothing about a property is taken to mean "whatever the server's default is" —
otherwise every plan would propose unsetting every default forever. Concretely:

* **Descriptor defaults** count as the value on both sides.
* **The target line's own defaults** are used, not the 2.x catalog's — a
  property that exists only on 1.x (`QueryRecord`'s `cache-schema`) sitting at
  its 1.x default is not drift when planning against a 1.24 server.
* **An empty string is unset** when the descriptor has no default: NiFi
  materialises "no value" as `""` for some properties.
* **What the import writes** counts too. Importing a flow that never mentions
  `Allow Scientific Notation` gives a `JsonRecordSetWriter` `'true'` on 2.7.2,
  though its descriptor says `'false'` — see
  [catalog-and-versions.md](catalog-and-versions.md).
* **Components the server creates for itself** are left alone: NiFi 2.x's
  import creates an `AWSCredentialsProviderControllerService` for the AWS
  processors and wires it in. Deleting it would break the processor, so an
  unnamed service of such a type is not diffed. Name it in your flow and it
  becomes an ordinary, fully diffed service.
* **A property the target line does not have** is not reported either: the
  push omits it and warns, `validate` fails on it, and the live side can never
  hold it.
* **Run state** is only diffed when the model states it. A live RUNNING
  processor is not drift against a model that never mentioned run state —
  turning a plan into an outage is not a niflow's job.

Everything else *is* drift. In particular, a property you delete from a pulled
flow really does plan as an unset, and `push --update` really does send it
(NiFi merges the properties map on `PUT`, so a removal has to be sent as an
explicit `null` — it is).

## Applying safely

```bash
niflow plan flows/prod.py            # read-only
niflow push flows/prod.py --update   # apply
niflow push --all flows/ --update    # a whole directory
```

* A backup is written before any mutation
  ([backup-and-rollback.md](backup-and-rollback.md)).
* The applier pre-flights the plan (dangling endpoints and the like) before it
  changes anything.
* If an apply fails part-way it raises with what it had already done, so you
  know where you are — and `niflow rollback` can put it back.
* Controller services are disabled and their referencing processors stopped
  around a service update, then restored.

## Cross-version behaviour

The plan is computed against the **server's own NiFi line**: the client asks
the server its version rather than guessing. Offline callers (tests, the fuzz
harness) fall back to inferring it from 1.x-only property keys in the live
snapshot. See [catalog-and-versions.md](catalog-and-versions.md) for what
"the same property on two lines" means.
