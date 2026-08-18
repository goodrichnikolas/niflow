# NiFi University

A running set of plain-language notes on how NiFi actually works under the hood —
written as we hit each concept while building niflow.

---

## What is a NAR?

**NAR = NiFi ARchive.** It's NiFi's plugin/package format — a `.nar` file is just
a ZIP (same idea as a Java `.jar` or a `.war`) that bundles a set of processors,
controller services, and *all the Java libraries they depend on* into one
self-contained unit that NiFi can load.

Think of it as **one installable "pack" of components.** When NiFi starts, it
scans its `lib/` (and extensions) directory, loads every `.nar` it finds, and
that's how it knows which processor types exist. Install a new `.nar`, restart,
and new processors show up in the palette.

### Why NARs exist (the one-sentence version)
Different processors need different — and often *conflicting* — versions of the
same Java library. A NAR gives each pack its own isolated classloader, so the
Kafka processors can use one version of a library and the AWS processors another
without colliding. It's dependency isolation, packaged.

### A NAR is identified by three coordinates
Every NAR has a **bundle coordinate** — three strings:

| Coordinate | Example                       | What it is                     |
|------------|-------------------------------|--------------------------------|
| `group`    | `org.apache.nifi`             | who publishes it (like a Maven group) |
| `artifact` | `nifi-standard-nar`           | the pack's name (the NAR file) |
| `version`  | `1.24.0` / `2.7.2`            | which release                  |

So `org.apache.nifi : nifi-standard-nar : 2.7.2` names exactly one NAR.

### The key insight (the thing that bit us)
A processor is **not** identified by its type string alone. NiFi resolves it by
the **pair `(type, bundle)`**:

```
type:   org.apache.nifi.processors.attributes.UpdateAttribute
bundle: org.apache.nifi : nifi-update-attribute-nar : 2.7.2
```

Both halves have to agree. The type string says *which class*; the bundle says
*which NAR to find that class in*. If you hand NiFi a real type string but point
it at the **wrong NAR**, NiFi looks in that NAR, doesn't find the class, and
reports:

> *"... is of type ... UpdateAttribute, but this is not a valid processor type."*

That error is misleading — the type is perfectly valid; it's just **not in the
NAR you named.**

### Which NAR is a processor in?
Most of the common processors live together in **`nifi-standard-nar`**
(`GetFile`, `PutFile`, `AttributesToJSON`, `GenerateFlowFile`, ...). But plenty
ship in their **own** NAR:

- `UpdateAttribute` → `nifi-update-attribute-nar`
- Kafka processors → `nifi-kafka-nar`
- AWS processors → `nifi-aws-nar`
- ...and which NAR a type lives in **can change between NiFi versions** (Jolt
  processors moved out of `nifi-standard-nar` in 2.x, for example).

Because of that last point, the *only* fully reliable source of truth is the
**running instance itself** — it knows exactly which NARs are installed and what
type lives in each. NiFi exposes this at `/flow/processor-types`.

### How niflow handles it
1. **Offline (writing JSON/XML, `niflow diff`):** niflow guesses the right NAR
   per type from a generated map (`make catalog` records it from your NiFi) plus
   a small curated list of stable exceptions like `UpdateAttribute`.
2. **At push time:** niflow asks the *target* NiFi for its installed NARs and
   rewrites every component's bundle — artifact **and** version — to match. That
   's what lets the same flow import cleanly on NiFi 1.24/1.28 **and** 2.x.

### NiFi is forgiving about *version* (but not artifact)
When importing a flow, if the exact bundle `version` isn't installed but there's
exactly one NAR with the same `group` + `artifact`, NiFi uses it ("compatible
bundle" resolution). So a version mismatch usually self-heals — but a wrong
**artifact** is fatal, because the class simply isn't in there.

### TL;DR
- A **NAR** is a self-contained pack of NiFi components + their dependencies.
- It's named by **group : artifact : version**.
- NiFi finds a processor by **(type + bundle)**, not type alone.
- Point at the wrong NAR → "not a valid processor type," even for a real type.
- The running instance is the source of truth for which NAR holds what.

---

## What is a Controller Service?

A **controller service** is a *shared, reusable resource* that processors borrow
instead of each configuring their own. A processor does work on one FlowFile at a
time; a controller service just **sits there and provides something** — a database
connection pool, a way to read/write a record format, an SSL context, a cache
client — that many processors can point at.

The mental model: **processors are verbs, controller services are nouns they
share.** Ten processors all writing to the same Postgres shouldn't each open their
own connection pool — they share one `DBCPConnectionPool` controller service.

### Why they exist
- **Sharing:** one connection pool / one schema definition, used by many processors.
- **Lifecycle:** services are **enabled/disabled** independently of processors.
  A service must be *enabled* before a processor that references it can run. (In
  niflow's push, `enable_services` runs before `start_group` for exactly this
  reason.)
- **Config in one place:** change the DB URL once, every processor follows.

### How a processor references one
In a processor's properties, certain fields don't take a literal value — they take
**"a controller service of type X."** You pick an existing service instance from a
dropdown. In the flow JSON this shows up as `identifiesControllerService` (the
service *type* the property expects) plus the chosen service's id. niflow models
this by letting a property *value be a `ControllerService` object* rather than a
string — that's why the emitter has special handling for service-ref properties.

### Controller services also have bundles
Same `(type, bundle)` rule as processors (see the NAR section). A service like
`JsonTreeReader` lives in a NAR too, so niflow resolves its bundle the same way.
Note the split: **API NARs** (`nifi-standard-services-api-nar`) define the
*interface* a processor codes against; the **implementation** lives in another
NAR. That's how you can swap implementations behind the same property.

### The big one: Record Readers & Writers
The most common controller services you'll meet are **Record Readers** and
**Record Writers** (`JsonTreeReader`, `CsvReader`, `AvroRecordSetWriter`, ...).
They unlock NiFi's **record-oriented** processors (`ConvertRecord`, `QueryRecord`,
`PartitionRecord`, ...), which process a whole batch of structured records in one
FlowFile instead of splitting into one-FlowFile-per-row. This is **dramatically**
faster and the recommended way to handle CSV/JSON/Avro/Parquet. A Reader parses
the incoming bytes into records; a Writer serializes them back out — often in a
*different* format, so "CSV → JSON" can be a single processor.

---

## Other things every NiFi user should know

### FlowFile — the unit that moves
A **FlowFile** is one piece of data flowing through the system. It's two parts:
- **Content** — the actual bytes (a file, a message, a record batch).
- **Attributes** — key/value metadata *about* the content (`filename`, `uuid`,
  `mime.type`, plus anything you add). Attributes are cheap to read/route on;
  content is the heavy payload. Routing/decisions usually happen on attributes.

Your `AbcToJson` flow is literally: make a FlowFile → add attributes `a/b/c` →
turn those attributes into JSON *content* → write it out.

### Processors and Relationships
A **processor** does one job and routes each FlowFile out via a **relationship** —
almost always `success` and `failure` (some add `matched`/`unmatched`,
`original`, etc.). You **must** handle every relationship: either connect it to
the next processor or **auto-terminate** it (tell NiFi "drop FlowFiles that exit
here"). An unhandled relationship is a validation error and the processor won't
start. That's the yellow ⚠ triangle.

### Connections, Queues, and Back Pressure
A **connection** is the link between two processors — and it's also a **queue**.
FlowFiles wait in the queue until the downstream processor picks them up. Key
settings:
- **Back pressure** — pause the upstream processor when the queue hits N
  FlowFiles or M bytes. This is NiFi's flow control; it stops a fast producer
  from overwhelming a slow consumer.
- **FlowFile expiration** — auto-drop FlowFiles older than a duration.
- **Prioritizers** — decide which queued FlowFile goes next (FIFO, oldest, etc.).

### Process Groups
A **process group** is a folder/sub-flow — a box you put processors in to organize
and nest flows. Groups can be **nested arbitrarily deep** (which is exactly the
pain niflow's GUI helper targets). Data crosses a group boundary through **input
ports** and **output ports**. A group can also be **versioned** against NiFi
Registry as a unit.

### Parameters vs. Variables (and Sensitive values)
- **Parameter Contexts** are the modern way to externalize config (URLs,
  credentials, paths). A context holds **parameters** referenced as `#{param.name}`.
  Parameters can be marked **sensitive** — NiFi stores them encrypted and **never
  exports their values** (niflow never serializes them either; they live only in
  your git-ignored `.niflow-secrets.env`).
- **Variables** are the older, deprecated mechanism (`${var}`) — prefer parameters.
- Don't confuse `#{...}` (parameters, resolved at config time) with `${...}`
  (Expression Language, evaluated per-FlowFile at runtime).

### Expression Language (EL)
`${...}` is NiFi's per-FlowFile mini-language for computing values from attributes:
`${filename:toUpper()}`, `${now()}`, `${uuid}`. Many processor properties accept
it, so a property can be dynamic per FlowFile rather than a fixed string.

### Scheduling
How often a processor runs:
- **Timer driven** (default) — every N seconds (`0 sec` = as fast as possible).
- **CRON driven** — at clock times (`0 0 * * * ?`).
- **Event driven / source processors** — `GenerateFlowFile`, `GetFile`, etc. are
  *sources*; they create FlowFiles on their schedule with no input. Everything
  downstream runs when a FlowFile arrives in its queue.
- **Concurrent tasks** — how many threads run the processor at once.

(niflow's "Run File" button stops a source `GenerateFlowFile` and triggers it for
exactly **one** scheduling pass via `RUN_ONCE` — one FlowFile per click.)

### Execution Node — and why "Primary Node Only" is sources-only
In a **cluster**, every node runs its own copy of the whole flow. That's usually
what you want — more nodes, more throughput — but it's a disaster for some
*sources*: three nodes all running `ListFile` against the same shared directory
would ingest every file **three times**. The fix is the processor's **Execution
Node** setting:

- **All nodes** (default) — the processor runs everywhere.
- **Primary node** — it runs only on the one node the cluster has elected
  *primary*, so a listing/polling source produces each item exactly once.

**The rule that bit us:** NiFi refuses "Primary Node Only" on any processor with
an **incoming connection** — the validation error reads *"'Execution Node' is
invalid because Processors with incoming connections cannot be scheduled for
Primary Node Only."* The processor sits invalid (⚠) and won't start.

Why the rule exists: a connection's queue is **local to each node** — FlowFiles
live in the node's own repositories and don't teleport between nodes. If a
mid-flow processor ran primary-only, FlowFiles queued on the *other* nodes would
sit in front of a processor that never runs there — stranded forever. Only a
processor with no inputs (a source) can safely run on one node, because there's
nothing queued for it anywhere else. (The cluster-friendly way to funnel work
onto fewer nodes mid-flow is the **connection's** load-balance strategy, e.g.
"Single node" — the queue itself moves the data, which primary-only scheduling
can't do.)

Two corollaries worth knowing:
- The primary node can **change** (election on node loss). Primary-only sources
  simply resume on the new primary — with their **component state** (e.g.
  `ListFile`'s "already seen" list) shared via ZooKeeper so the handoff doesn't
  re-ingest everything.
- On a **standalone** NiFi the setting is accepted and effectively meaningless —
  but the incoming-connection rule is enforced *everywhere*, standalone included.
  That's how the torture flow's `Cron 'audit'` (funnel → PRIMARY-scheduled
  `LogAttribute`) got stuck invalid on a single-node dev box.

`niflow validate` now catches this combination statically (execution node
`PRIMARY` + any incoming connection) before a push ever reaches the server.

### Data Provenance
NiFi records **every event** in a FlowFile's life — created, cloned, modified,
sent, dropped — with full lineage. You can pick any FlowFile and replay its entire
history. This is one of NiFi's superpowers for debugging "where did this data go?"

### Bulletins
Transient **notifications** a component posts (the little messages that flash on a
processor, and the Bulletin Board). They're how you see runtime errors/warnings —
distinct from **validation errors** (the ⚠ triangle), which are *config* problems
that stop a processor from even starting. niflow's GUI surfaces both in separate
panels.

### State
Some processors remember things between runs (e.g. `ListFile` tracks which files
it's already seen, `GetFile`/tail processors track position). NiFi persists this
**component state** locally or in ZooKeeper (clustered), so a restart doesn't
re-process everything.

### Versioning & "Templates are dead"
- **NiFi Registry** + **flow definitions** (the versioned JSON niflow pulls/pushes)
  are the modern way to version and move flows between environments.
- **Templates** (the old XML export) are **deprecated** — avoid them for new work.
  niflow speaks the flow-definition JSON for round-tripping; the XML path exists
  only for legacy import/convert.

### Quick glossary
| Term | One-liner |
|------|-----------|
| FlowFile | one piece of data = content + attributes |
| Processor | a component that does one job on FlowFiles |
| Relationship | a labeled exit from a processor (`success`/`failure`) |
| Connection | a queue linking two components |
| Back pressure | pause upstream when a queue fills |
| Controller Service | shared resource (DB pool, record reader, SSL context) |
| Process Group | a nestable folder/sub-flow |
| Port | how data crosses a process-group boundary |
| Parameter Context | externalized config, `#{param}`, supports sensitive values |
| Expression Language | `${...}` per-FlowFile value computation |
| Provenance | full per-FlowFile lineage/history |
| Bulletin | transient runtime notification |
| NAR | a packaged bundle of components (see top of doc) |
