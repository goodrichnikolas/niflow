# NiFlow

Work on Apache **NiFi** flows as Python code. Pull a live process group off the
canvas into a `.py` module, edit it (by hand or with an AI agent), and push it
back — against **NiFi 1.x (1.24/1.28) and 2.x** alike. Think Dagster/CDK-style
definitions for NiFi: flows become readable, diff-able, git-friendly code
instead of click-marathons in the UI.

```bash
niflow list                                  # see the canvas tree + ids
niflow copy "Prod Flow"                      # safe, detached working copy
niflow pull "Prod Flow (copy)" -o flows/prod_flow.py
# ...edit flows/prod_flow.py: tweak a SQL query, add a processor, rewire...
niflow plan flows/prod_flow.py               # semantic "what will change"
niflow push flows/prod_flow.py --update      # apply just that delta in place
niflow diff flows/prod_flow.py               # raw JSON diff vs the live canvas
niflow test flows/prod_flow.py               # inject FlowFiles, assert what comes out
niflow push flows/prod_flow.py --start       # or: full replace and start

niflow pull --all -o flows/                  # mirror EVERY top-level group
niflow drift                                 # exit 1 if code and canvas diverged
niflow push --all flows/ --update            # reconcile the whole directory

niflow validate flows/prod_flow.py --live    # NiFi's own validation, via a sandbox
niflow push flows/prod_flow.py --env prod    # per-environment parameter values
niflow diagram flows/prod_flow.py -o doc.md  # Mermaid flowchart for PR review
niflow commit "Prod Flow" -m "tuned batch"   # save a versioned group to the Registry
```

…or define a flow from scratch:

```python
from niflow import Flow
from niflow.processors.standard import GetFile, PutFile

flow = Flow("MyETLFlow", parent_pg="root")

get = GetFile(name="Ingest", properties={"Input Directory": "/in", "File Filter": ".*"})
put = PutFile(name="Output", properties={"Directory": "/out"})

flow.add_processor(get, put)
flow.add_connection(get >> put)

flow.push(start=True)
```

## The pull → edit → push loop

`pull` downloads the group as a NiFi *flow definition* (`GET
/process-groups/{id}/download`, available since NiFi 1.11) and generates a
runnable Python module. Pulls that hit something the model can't represent
(remote process groups, services defined above the pulled group) say so loudly
instead of dropping components silently.

`plan` diffs your `.py` against the live group into a terraform-style change
plan — adds, removes, and field-level updates by component. `push --update`
applies exactly that plan with targeted REST calls: only changed processors
are stopped/updated/restarted, removed connections are drained first, and
everything untouched keeps its id, state, and **queued FlowFiles**. The loop
converges: a pull followed by a plan reports zero changes.

Identity is name-based, so renaming a component is really remove+add — the
plan detects that shape (same type, same group) and **warns before you lose
state or queued data**. And every mutating push first snapshots the live group
to `.niflow-backups/`; `niflow rollback <group>` rebuilds it from the newest
backup (`--list` to browse, `niflow backup` for a manual save point), so a bad
apply is one command away from undone.

Plain `push` remains the full **delete-and-recreate**: the old group is
stopped, drained, and replaced, keeping its canvas position. Parameter contexts
survive replacement (they live outside the group), so sensitive values stored
in NiFi are not lost.

`copy` automates the "duplicate it so I can't break prod" step: it clones a
group **with registry version-control coordinates stripped**, so the copy is
fully detached — edit and push at will, delete when done.

`diff` compares your local `.py` against the live group using the
deterministic JSON emission (UUID5 identifiers seeded on component paths), so
structurally identical flows produce identical bytes.

## Test flows with real data (`niflow test`)

Declare cases next to the flow and `niflow test` answers "what does this flow
actually DO to a file" without a single canvas click:

```python
from niflow.testing import TestCase

tests = [
    TestCase(
        name="urgent CSV is routed, converted, and audited as JSON",
        inject_at="Stamp",                       # any processor or input port; "Group/Name" paths work
        content="id,priority\n1,urgent\n",
        attributes={"origin": "unit-test"},      # extra FlowFile attributes
        expect_at="Audit",                       # results are read from its input queue
        expect_attributes={"mime.type": "application/json"},
        expect_content_contains='"priority"',
    ),
]
```

The harness pushes a **sandbox copy** (the real group is untouched), starts
everything *except* data sources (the test injects its own FlowFile via a
temporary run-once generator) and the `expect_at` collector (so results queue
up instead of being consumed), then checks every arriving FlowFile's
attributes and content. `--keep` leaves the sandbox on the canvas for
autopsy. Works identically on 1.x and 2.x; see `examples/kitchen_sink.py`
for runnable cases.

## Mirror the whole instance (`--all`, `drift`)

`niflow pull --all -o flows/` writes every child group of `--parent`
(default: the root canvas) to `flows/<slug>.py` — the repo becomes the
instance. `niflow plan --all flows/` and `niflow push --all flows/ --update`
iterate the directory; `niflow drift` prints one `ok`/`DRIFT` line per flow
and exits non-zero on any divergence, made for cron or CI.

## What's supported

- **Processors** — curated factories (`GetFile`, `PutFile`, `InvokeHTTP`, …), the
  auto-generated catalog, or any type via `Processor(name=..., type="<fqcn>")`.
  Full fidelity round-trip: scheduling, comments, penalty/yield, bulletin level,
  execution node, run duration, retry settings, enabled/disabled state, custom
  NAR bundles.
- **Connections** — `a >> b` or `a.to(b, relationships=["failure"], back_pressure_object_threshold=0)`.
  Queue settings (back pressure, expiration, prioritizers, load balancing) round-trip.
- **Process groups** — nestable (`with flow.process_group("X") as x:`), variables included.
- **Auto-layout** — components without an explicit `position` are placed along
  the connection graph; `Flow("X", layout="horizontal")` chains left-to-right
  (the default), `layout="vertical"` top-to-bottom. Parallel branches fan out
  on the cross axis. Explicit positions always win.
- **Ports, funnels, labels** — `InputPort`/`OutputPort`, `Funnel()`, `Label("note")`.
- **Controller services** — pass an instance as a processor property value; the
  UUID is wired up automatically.
- **Parameter contexts** — first-class. Bound per group (`flow.parameter_context = ctx`),
  matched by name on push, shared instances preserved. Properties reference them
  as `#{param}` strings.
- **Secrets** — NiFi never exports sensitive values, so they live in a
  git-ignored `.niflow-secrets.env` and are applied at push time (see below).
- **Convert** — offline round-trip between Python, flow-definition JSON, and
  NiFi 1.x XML templates. See [Convert flows](#convert-flows).

## NiFi version support

The CLI and `Flow.push()/Flow.pull()` use a small direct REST client
(`niflow/client.py`) that speaks the endpoints common to **1.x and 2.x**;
username/password login covers single-user *and* LDAP. The legacy
`Flow.deploy()` (component-by-component via nipyapi) remains for 2.x only.

Pushing to a group already under **NiFi Registry version control** rebuilds it
*in place* — same group id, registry link intact, surfaced as *local changes*
you commit in the Registry — instead of delete-and-recreate. The in-place
vehicle differs by line (NiFi 1.x templates, NiFi 2.x copy/paste, since
templates were removed in 2.x) but the behaviour is identical. Local registries
for testing come up with `docker compose up` (2.7.2, `:18080`) and the `v1`
profile (1.24.0, `:18081`).

Local test instances (Docker is only ever used to host these disposable
NiFis — niflow's runtime is pure Python):

```bash
make nifi-up      && make nifi-wait       # NiFi 2.7.2  -> https://localhost:8443/nifi
make nifi1-up     && make nifi1-wait      # NiFi 1.24.0 -> https://localhost:8444/nifi
make nifi-mtls-up && make nifi-mtls-wait  # NiFi 1.24.0 secured by CLIENT CERTS -> :8445
```

The first two log in as **admin** / **adminpassword123**; the mTLS one
authenticates with the generated `certs/mtls/client.pem`.

Every claim above is tested against real servers, two ways. CI boots
dockerized NiFi 2.7.2 **and** 1.24.0 and runs the integration suite against
both on every push. And the unit suite parses **golden fixtures captured from
live servers** (`tests/fixtures/real/`, refreshed with `make fixtures`) — real
server output, not hand-built dicts, which is where version quirks actually
show up (e.g. 2.x silently migrates ConvertRecord's property keys from 1.x
`record-reader` to `Record Reader`).

Connection settings resolve **defaults < `.niflow.env` config file < real
environment**. Copy `.niflow.env.example` to `.niflow.env` (git-ignored; also
searched at `$NIFLOW_CONFIG` and `~/.niflow.env`), fill it out once, and every
command — CLI, both GUIs, library — connects the same way:

| Key | Meaning |
| --- | --- |
| `NIFLOW_NIFI_HOST` | REST base, must end in `/nifi-api` |
| `NIFLOW_NIFI_USER` / `NIFLOW_NIFI_PASSWORD` | token login (single-user or LDAP); empty password = anonymous |
| `NIFLOW_NIFI_CLIENT_CERT` / `NIFLOW_NIFI_CLIENT_KEY` | PEM client certificate -> mTLS (token login skipped) |
| `NIFLOW_NIFI_CA_BUNDLE` | CA/server PEM to trust (beats `VERIFY_SSL`) |
| `NIFLOW_NIFI_VERIFY_SSL` | `false` for self-signed dev certs |

**`niflow doctor`** diagnoses an unknown server step by step — reachability,
TLS trust, which auth mode the server wants, whether your credentials work —
and each failure names the `.niflow.env` key to fix. Full playbook for a work
instance (Podman, unknown auth): [docs/work-nifi-setup.md](docs/work-nifi-setup.md).

## Secrets / sensitive parameters

Per-environment values use the same file format: `niflow push --env prod`
overrides non-sensitive parameter values from `.niflow-params.prod.env`
(committed — it's config, not secrets) and reads sensitive ones from
`.niflow-secrets.prod.env` (git-ignored). One flow module, N environments.

A pulled flow renders sensitive parameters without values:

```python
etl_params = ParameterContext(
    "etl-params",
    parameters=[
        Parameter("db.url", value="jdbc:postgresql://db/x"),
        Parameter("db.password", sensitive=True),   # value lives in NiFi + secrets file
    ],
)
```

At push time NiFlow applies values from `.niflow-secrets.env` (or
`--secrets path`, or a dict passed to `push_flow`):

```
db.password=hunter2
etl-params::api.key=scoped-to-one-context
```

Add `.niflow-secrets.env` to `.gitignore`. If a sensitive parameter has no
secret entry, whatever value NiFi already holds is kept — contexts are reused
by name across pushes, so values stick.

## Quickstart

```bash
make install        # pip install -e ".[dev]"
make nifi1-up && make nifi1-wait     # or nifi-up for 2.x

make list                            # niflow list
make pull GROUP="My Flow" OUT=flows/my_flow.py
make push FILE=flows/my_flow.py
make test                            # unit tests (no NiFi needed)
make test-integration-v1             # integration tests against 1.24
```

## GUIs (both optional)

- **`niflow-web`** (`make webgui`) — browser-based helper on
  `http://127.0.0.1:7777`, zero extra dependencies: processor list with
  run-once/start/stop, queue browser with FlowFile attribute+content
  inspection, bulletins/error panels, and plan-preview + incremental push for
  `flows/*.py`. Under WSL it opens the *Windows* default browser.
- **`niflow-gui`** (`make gui`) — the PyQt6 desktop helper (`pip install -e
  ".[gui]"`), same capabilities as a native window.

## Convert flows

Offline conversion between Python, flow-definition JSON, and NiFi 1.x XML
templates — pure stdlib, no NiFi needed. Identifiers are deterministic (UUID5
of the component's path) so round-trips are byte-stable and diff-friendly.

```bash
python -m niflow.convert flow.json flow.py        # JSON  -> Python
python -m niflow.convert flow.xml  flow.json      # XML   -> JSON  (1.x template -> definition)
python -m niflow.convert flow.py   flow.xml       # Python -> XML
make convert IN=flow.json OUT=flow.py             # Makefile wrapper
```

…or as a library: `Flow.from_json(...)` / `flow.to_json()` / `flow.to_python()`
/ `Flow.from_xml(...)` / `flow.to_xml()`. When the input is `.py`, the file is
imported and its top-level `flow` variable is read (configurable via `--var`).

## Processor catalog

Beyond the curated factories, NiFlow ships an auto-generated catalog of every
processor/controller-service type from the NiFi it was generated against (292
processors / 123 services for the 2.x bundle set):

```python
from niflow.processors import catalog as proc_catalog
proc_catalog.AttributesToJSON()                   # thin shell over the FQCN
proc_catalog.RESTRICTED, proc_catalog.DEPRECATED  # FQCN sets
```

Regenerate against *your* NiFi (picks up custom NARs): `make catalog`.

## MCP integration

For AI-driven inspection/debugging straight against a running NiFi, this repo
is wired to the [NiFiMCP](https://github.com/ms82119/NiFiMCP) server via
`.mcp.json` (sibling checkout). It complements NiFlow: NiFlow defines, pulls,
and pushes flows as code; the MCP server inspects and pokes the live canvas.

## Project layout

```
niflow/
  core.py          # models: Flow, ProcessGroup, Processor, Connection, Port,
                   #         ControllerService, ParameterContext, Funnel, Label
  client.py        # direct REST client: pull/push/copy/delete (NiFi 1.x + 2.x)
  cli.py           # `niflow` CLI (also `python -m niflow`)
  config.py        # NiFiConfig (+ legacy nipyapi connect())
  deployment.py    # legacy deploy(): nipyapi orchestration, 2.x only
  convert.py       # `python -m niflow.convert` offline converter CLI
  formats/         # offline converters (Python <-> JSON <-> XML)
  processors/      # processor factories (curated + generated catalog)
  services/        # controller-service factories
```

## Requirements

Python ≥ 3.9, `pydantic >= 2`, `requests` (CLI/client), `nipyapi >= 1.0` (only
for the legacy 2.x deploy path and codegen). Docker for local NiFi.

## Out of scope (for now)

Remote Process Groups (pull warns when it drops one), NiFi Registry *sync*
(copy detaches from it instead), and mTLS/token-file auth. NiFi 1.x variable
registry edits are reported by `plan` but not applied incrementally — use
parameter contexts.
