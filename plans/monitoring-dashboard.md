# NiFlow live monitoring dashboard

## Context

NiFi's UI forces constant double-clicking — into the Bulletin Board, into queues, into
processors — just to answer "what's flowing and what's breaking right now?" That's slow
and literally painful (RSI). We want an **always-open, auto-refreshing dashboard** that
gives an at-a-glance snapshot of the running flow: errors surfaced automatically, queue
depths and throughput visible at a glance, and **hover-to-inspect** a queue's top FlowFile
(its attributes and payload) without clicking. Refresh every ~5s is acceptable.

This is purely **read-only** monitoring; it complements (does not touch) NiFlow's deploy
path and the NiFiMCP server.

### Decisions (confirmed with user)
- **Web dashboard** (browser tab) — FastAPI backend + a single static HTML/JS page, no JS build step.
- **Self-contained on nipyapi** — reuse niflow's `connect()`; talk to NiFi directly. No dependency on the MCP server running.
- **Status-tables layout** — an ERRORS panel on top, then a CONNECTIONS table and a PROCESSORS table, color-coded; hover a connection row to inspect its top FlowFile.

### Why this is cheap to build
nipyapi 1.5.0 (already pinned) gives us everything read-only:
- **One call** for the whole-flow snapshot: `nipyapi.nifi.FlowApi().get_process_group_status(root_id, recursive=True)` returns a nested tree with every connection's `flow_files_queued`/`queued_size`/`flow_files_in`/`flow_files_out`/`percent_use_count` and every processor's `run_status`/in-out counts/`active_thread_count`.
- **One call** for errors: `nipyapi.bulletins.get_bulletin_board(limit=...)` → flat list of `BulletinDTO` (`level`, `message`, `source_name`, `source_id`, `group_id`, `timestamp`).
- **Hover peek** (on demand, non-destructive): `nipyapi.canvas.peek_flowfiles(conn_id, limit=1)` (attributes) + `nipyapi.canvas.get_flowfile_content(conn_id, uuid, decode="auto")` (payload). Both accept a connection-ID string (verified: `canvas.py:1684`, `1844`, `1952`).

So the 5s poll is **2 API calls**; per-FlowFile inspection happens only while the cursor is over a row.

## Reused code
- `niflow/config.py:48` — `connect(config)` (HTTPS single-user token login, env-var config via `NiFiConfig.from_env`). Call once at server startup.
- `niflow/utils.py:10` — `get_logger()` for the shared `niflow` logger.
- `nipyapi.canvas.get_root_pg_id()` — resolve the root process-group id at startup.

## New code

```
niflow/
  dashboard/
    __init__.py        # exports create_app, run
    monitor.py         # data layer: nipyapi -> plain dicts (no web concerns)
    server.py          # FastAPI app + JSON endpoints + static mount
    __main__.py        # `python -m niflow.dashboard` -> run()
    static/
      index.html       # the whole UI: HTML + CSS + vanilla JS (no build step)
```

Keep `niflow/__init__.py` free of any FastAPI import so the core library stays import-light;
the dashboard is an optional extra.

### `monitor.py` (the data layer — unit-testable, no FastAPI)
- `snapshot(root_id) -> dict`:
  - `status = FlowApi().get_process_group_status(root_id, recursive=True)`.
  - Walk the recursive `process_group_status_snapshots` tree with a helper
    `_flatten(snapshot_dto)` that collects:
    - **connections**: `{id, name, source_name, destination_name, flow_files_queued,
      queued_size (human str via the DTO's `queued` field), flow_files_in, flow_files_out,
      percent_use_count, backpressure: percent_use_count>=100 or percent_use_bytes>=100}`.
    - **processors**: `{id, name, group_name, run_status, flow_files_in, flow_files_out,
      bytes_in, bytes_out, active_thread_count}`.
  - `bulletins = get_bulletin_board(limit=50)` → map to `{level, message, source_name,
    source_id, group_id, timestamp}`; sort ERROR→WARN→INFO, newest first; tag processors that
    have a matching `source_id` so the PROCESSORS table can flag them red.
  - Return `{ts, host, connections, processors, bulletins, controller:{queued_count, active_threads}}`.
  - Wrap in try/except: on auth failure (token expiry on a long-running tab) call `connect()`
    once and retry; on other failure return `{error: str}` so the UI shows a "disconnected" banner.
- `peek_connection(conn_id, max_content_bytes=4096) -> dict`:
  - `flowfiles = peek_flowfiles(conn_id, limit=1)`; if empty return `{empty: True}`.
  - For the front file: `{uuid, filename, size, queued_duration, attributes}` +
    `content = get_flowfile_content(conn_id, uuid, decode="auto")`, truncated to
    `max_content_bytes` with `content_truncated` flag; if `bytes` (binary) return a
    `"<binary, N bytes>"` placeholder instead of raw bytes.
  - try/except → `{error}` (the file may have left the queue between poll and hover).

### `server.py` (FastAPI)
- `create_app(config: NiFiConfig | None = None) -> FastAPI`:
  - On startup event: `cfg = connect(config)`; cache `root_id = get_root_pg_id()` and `cfg.host`.
  - `GET /` → `FileResponse(static/index.html)`.
  - `GET /api/snapshot` → `monitor.snapshot(root_id)`.
  - `GET /api/connections/{conn_id}/peek` → `monitor.peek_connection(conn_id)`.
  - `GET /api/health` → `{ok: True}`.
  - Mount `static/` for any assets.
- `run(host="127.0.0.1", port=8000)`: `uvicorn.run(create_app(NiFiConfig.from_env()), ...)`.
- `__main__.py` reads `NIFLOW_DASHBOARD_HOST`/`_PORT` (defaults `127.0.0.1:8000`) and calls `run`.

### `static/index.html` (vanilla JS, one file)
- **Header**: green/red connection dot, NiFi host, "updated Ns ago", refresh spinner.
- **ERRORS panel** (top, red): bulletin list (level chip + source + message + time), count
  badge; brief flash/highlight when a new error id appears since last poll. Keep a small
  client-side set of recently-seen bulletins so they linger in the panel a bit even though
  NiFi expires bulletins after ~5 min (optional nicety).
- **CONNECTIONS table**: `source → dest | queued (▲ + amber/red when >0 / backpressure) | in/out`.
  Hovering a row: after a ~300ms debounce, fetch `/api/connections/{id}/peek` and show an
  inline expander/side panel with the top FlowFile's uuid, attributes (key/value list), and a
  payload preview; while the cursor stays on the row, re-fetch every 5s so it "keeps updating";
  clear the interval on mouseleave.
- **PROCESSORS table**: `name | state (● RUN green / ⚠ STOP grey / ✖ INVALID or has-error red) | in/out | threads`.
- **Polling**: `fetchSnapshot()` on load + `setInterval(..., 5000)`. Re-render tables each poll
  but preserve the currently-hovered row's open peek panel. No external JS libs.

## Dependencies, Makefile, docs
- `pyproject.toml`: add an optional group, leaving the core deps untouched:
  ```toml
  [project.optional-dependencies]
  dashboard = ["fastapi>=0.110", "uvicorn[standard]>=0.29"]
  ```
  (nipyapi is already a core dep; the frontend is static, so no Node/JS deps.)
- `Makefile`: add
  ```make
  dashboard:  ## run the live monitoring dashboard (needs the [dashboard] extra)
  	python -m niflow.dashboard
  ```
  and document `pip install -e ".[dashboard]"` (or extend `make install`).
- `README.md`: add a short **"## Dashboard"** section (mirroring the MCP section) — what it is,
  `pip install -e ".[dashboard]"`, `make dashboard`, open http://localhost:8000, and the
  hover-to-inspect note.

## Verification (end-to-end)
1. `pip install -e ".[dashboard]"`.
2. `make nifi-up && make nifi-wait`.
3. Deploy a flow with live data: `python examples/attribute_pipeline.py`, then start it (set
   `start=True` or start it from the UI) so queues fill and FlowFiles move.
4. `make dashboard`; open http://localhost:8000.
   - Snapshot lists the AttributePipeline connections + processors and refreshes every 5s.
   - Start/stop a processor or watch a queue fill → counts change within ~5s.
   - Hover the `AddKey1 → EmptyJson` (or any non-empty) connection → see the top FlowFile's
     attributes (incl. `key1=value1`) and payload (`{}`); panel keeps refreshing while hovered.
   - Trigger an error (e.g. point `Sink`/PutFile at an unwritable dir, or leave a processor
     invalid) → the ERRORS panel populates automatically and the offending processor row turns red.
5. Add a small unit test for `monitor._flatten` using a synthetic status-snapshot object
   (plain attribute stubs) to lock the connection/processor field mapping; run `make test`.

## Risks / notes
- **Payloads are real data**: content is rendered in the browser. Mitigations: localhost-only
  bind by default, `max_content_bytes` cap, binary placeholder, and never log payload content.
- **Peek latency**: `peek_flowfiles` uses NiFi's async listing-request (submit→poll→delete),
  so it's ~0.5–2s — that's why it's on-demand (hover) and kept out of the 5s snapshot loop.
- **Token expiry** on a long-lived tab: `snapshot()` reconnects once on auth error.
- **Bulletins expire** (~5 min) server-side; the optional client-side "seen" set keeps them
  visible a little longer. Acceptable for "what's breaking now".
- Status-snapshot DTO field names (`flow_files_queued`, `queued`, `percent_use_count`, etc.)
  will be confirmed against the live `recursive=True` response during implementation; the
  flatten helper is the single place they're read.
