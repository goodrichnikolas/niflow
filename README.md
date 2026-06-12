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
niflow diff flows/prod_flow.py               # what changed vs the live canvas?
niflow push flows/prod_flow.py --start       # replace the live group and start it
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
runnable Python module. `push` re-serialises the model to a flow definition and
creates the group in one POST — **delete-and-recreate**: the old group is
stopped, drained, and replaced, keeping its canvas position. Parameter contexts
survive replacement (they live outside the group), so sensitive values stored
in NiFi are not lost.

`copy` automates the "duplicate it so I can't break prod" step: it clones a
group **with registry version-control coordinates stripped**, so the copy is
fully detached — edit and push at will, delete when done.

`diff` compares your local `.py` against the live group using the
deterministic JSON emission (UUID5 identifiers seeded on component paths), so
structurally identical flows produce identical bytes.

## What's supported

- **Processors** — curated factories (`GetFile`, `PutFile`, `InvokeHTTP`, …), the
  auto-generated catalog, or any type via `Processor(name=..., type="<fqcn>")`.
  Full fidelity round-trip: scheduling, comments, penalty/yield, bulletin level,
  execution node, run duration, retry settings, enabled/disabled state, custom
  NAR bundles.
- **Connections** — `a >> b` or `a.to(b, relationships=["failure"], back_pressure_object_threshold=0)`.
  Queue settings (back pressure, expiration, prioritizers, load balancing) round-trip.
- **Process groups** — nestable (`with flow.process_group("X") as x:`), variables included.
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

Local test instances:

```bash
make nifi-up   && make nifi-wait    # NiFi 2.7.2  -> https://localhost:8443/nifi
make nifi1-up  && make nifi1-wait   # NiFi 1.24.0 -> https://localhost:8444/nifi
```

Both log in as **admin** / **adminpassword123**. Point the CLI at either (or at
a real instance) with env vars:

| Env var | Default |
| --- | --- |
| `NIFLOW_NIFI_HOST` | `https://localhost:8443/nifi-api` |
| `NIFLOW_NIFI_USER` | `admin` |
| `NIFLOW_NIFI_PASSWORD` | `adminpassword123` |
| `NIFLOW_NIFI_VERIFY_SSL` | `false` |

> The host URL must include the `/nifi-api` base path.

## Secrets / sensitive parameters

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

Remote Process Groups, NiFi Registry *sync* (copy detaches from it instead),
mTLS/token-file auth, and incremental in-place updates (push is
delete-and-recreate by design — simple and predictable; contexts and their
sensitive values survive).
