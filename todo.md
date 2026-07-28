# niflow — TODO / future work

## Push & version control
### 2.x in-place push (replace templates with copy/paste)
The in-place rebuild (preserve group id + registry linkage) currently relies on NiFi 1.x
templates (`/templates/upload` + `/template-instance`), removed in 2.x. On a 2.x target,
a push to a versioned group **refuses** (501) rather than delete-and-recreate. Plan: use
NiFi 2.0's copy/paste — `POST /process-groups/{id}/paste` with a `CopyResponseEntity`
built by lifting `flow.to_json()`'s `flowContents` children to the top level. Symmetric
with 1.x: `_empty_group_contents` (reuse) then paste instead of template-instance.
(See `client.py: _push_in_place` / `_instantiate_template`.)

Steps (do registry/test infra FIRST so we can probe the schema live before committing):
- [x] **Registry container + wiring.** `docker-compose.yml` now runs `registry` (2.7.2,
      `:18080`) + `registry1` (1.24.0, `:18081`, profile v1). Healthchecks probe
      `$(hostname)` (registry binds to its hostname, not localhost).
- [x] **Client helpers:** `create_registry_client` (1.x `uri` vs 2.x `type`+`properties.url`),
      `list_registry_buckets`, `start_version_control` (2.x needs `action="COMMIT"`),
      `version_control_state`. All verified live on 2.7.2.
- [x] **Live VC fixture** — `tests/test_push_version_control.py::versioned` creates a registry
      client + bucket and version-controls a pushed group.
- [x] **Verify paste schema live** (probed against 2.7.2, `scratch_vc_probe.py`):
      - Registry client (2.x body w/ `type` + `properties.url`): **works**.
      - `start_version_control`: needs `versionedFlow.action="COMMIT"` (added); state
        reads back `UP_TO_DATE`. **works**.
      - Paste is **`PUT /process-groups/{id}/paste`** (not POST; `/copy` is POST). Body
        `{"copyResponse": {...}, "revision": {version, clientId}}`, no position needed.
      - Processors/connections/ports/funnels/labels + nested groups paste fine;
        **connection remapping works**; group ends `LOCALLY_MODIFIED` (registry link kept).
      - ⚠️ **Group-level controller services do NOT transport.** Canonical
        `CopyResponseEntity` has no service-definition field — only
        `externalControllerServiceReferences` (pointers). Top-of-group services must be
        handled out-of-band. **← open design decision (see below).**
- [x] **Group-level controller-service handling** — chose (A): `_recreate_group_services`
      pre-creates services (two passes for inter-service refs), `_remap_service_refs`
      rewrites processor→service property values to the new ids, and paste gets them as
      `externalControllerServiceReferences`. Keeps the "local changes you commit" contract.
- [x] **`_paste_into_group`** implemented (PUT `/paste`, `{copyResponse, revision}`);
      501 branch dropped from `_push_in_place`.
- [x] **Fixed a latent emitter bug** (`json_format._emit_service_ref_descriptors`):
      `identifiesControllerService` must be boolean `true`, not the service type string —
      it blocked pushing *any* service-bearing flow on the JSON path (1.x + 2.x).
- [x] **Unit tests** (`test_push_in_place.py`): `test_push_pastes_in_place_on_nifi_2x` +
      `test_in_place_recreates_group_services_and_remaps_refs`.
- [x] **Integration test** `tests/test_push_version_control.py` — passes live on 2.7.2
      (same pg_id, `LOCALLY_MODIFIED`, services recreated + wired round-trip).
- [x] **Validated live on NiFi 1.24** (`make test-integration-v1`, registry1 up). The 1.x
      template in-place path had never actually run against real NiFi (mock-only); doing so
      surfaced and fixed several 1.x-only issues:
      - `start_version_control` needs `action="COMMIT"` on **1.x too** (not just 2.x) — gate removed.
      - `_empty_queues`: NiFi 1.x returns 500 from `empty-all-connections-requests` when the
        group has zero connections → added `_has_connections` guard (helps both lines).
      - `_instantiate_template`: the upload response is **XML** on 1.x, not JSON → parse
        `<template>/<id>` via ElementTree; also delete any pre-existing same-named template
        first (templates are instance-global and an interrupted push leaves one behind).
- [x] **1.x pull limitation FIXED** (verified live on 1.24): the `/download` property value
      turned out to be the service's **`instanceIdentifier`** (1.24 flags the display-name
      descriptor `identifiesControllerService: false` and also emits duplicate internal-name
      descriptors — `record-reader` — flagged `true` but with `value: null`). `from_json` now
      resolves service refs **by value** (ids are UUIDs, so a match is unambiguous) instead of
      trusting the descriptor flag, and registers services under both `identifier` and
      `instanceIdentifier`. Bonus: service→service refs (lookup → pool) resolve now too.
      Unit regressions pin the 1.x snapshot shape in `test_json_format.py`.
- [x] **`niflow commit`** — commits a versioned group's local changes back to the
      Registry (`-m` for the message); verified live on 1.24 + 2.7.2 (2026-07-27).
      Original sketch: After an in-place push, niflow leaves the
      group with *local changes* for the user to commit in the Registry. Consider a CLI
      command to commit a new version (with a message) so the whole pull→edit→push→commit
      loop can stay in niflow.

## Helper GUI
- [x] **Diff/plan preview before push** — the push dialog now shows the semantic change
      plan (Details pane) and applies it incrementally by default (2026-07-27).
- [x] **Auto-refresh toggle** on the Inspector window (2026-07-27).
- [x] Bulletins/Errors panel links are now clickable anchors routed through the
      WSL-aware `open_url` (2026-07-27).

## Validation
- [x] **Live dry-run validator** — `niflow validate --live` pushes a throwaway
      sandbox, waits for NiFi's async validation, reports the server's own errors,
      deletes the sandbox. Verified live on both lines (2026-07-27).

## Incremental push (2026-07-27) — SHIPPED, verified live on 1.24 + 2.7.2
- [x] `plan_flow` / `push_update` / `niflow plan` / `niflow push --update`: semantic
      diff (niflow/plan.py) + targeted apply (niflow/apply.py). Group id, untouched
      component ids, and queued FlowFiles all survive; pull→plan converges to zero.
- [x] GUI push shows the plan first (Details pane) and applies incrementally by
      default; Full rebuild is the explicit destructive option. Inspector gained an
      auto-refresh toggle.
- [x] Lossy pulls warn (remote process groups, external service refs) via
      Flow.pull_warnings — CLI prints to stderr, GUI flags the status line.
- [x] Two 2.x snapshot-emitter bugs found by live verification: ports MUST carry
      concurrentlySchedulableTaskCount + scheduledState, and connection endpoints
      MUST carry groupId — 2.7.2's synchronizer NPEs otherwise (1.x tolerated both;
      first-ever snapshot push of a ported flow to 2.x flushed them out).
- [x] Variables (1.x registry) now APPLY incrementally (async update-request dance,
      null value = delete); live-verified on 1.24. 2.x still warns (no registry there).
- [ ] Funnel-heavy flows: connection identity for funnels is ordinal-based; inserting
      a funnel mid-list churns adjacent connections. Fine for now.

## Work-connection hardening + web GUI (2026-07-27) — SHIPPED
- [x] `.niflow.env` config file (defaults < file < env), mTLS client-cert auth
      (cert = identity, token login skipped), CA-bundle trust, `niflow doctor`
      diagnostician. Verified live against a cert-only NiFi 1.24 (compose profile
      `mtls`): doctor all green with strict CA verification + push/pull/plan round
      trip over cert auth (tests/test_mtls_integration.py).
- [x] docs/work-nifi-setup.md — determine an unknown server's auth, p12→PEM,
      trust, policies gotcha (initial admin lacks root PG policies).
- [x] Browser GUI `niflow-web` (stdlib-only) alongside the PyQt helper.
- [ ] OIDC/SSO login is unsupported by design — use a service account or cert.

## Trust & real-world testing (2026-07-27) — SHIPPED
Implements the top of the "how would you make this better" critique.
- [x] **Golden fixtures from real servers**: `make fixtures` pushes the kitchen-sink
      flow (examples/kitchen_sink.py — 3 nesting levels, services + cross-group refs,
      ports/funnels/labels, param context w/ sensitive param, tuned queues) to a live
      NiFi and commits the server's own snapshot (tests/fixtures/real/). Unit suite
      parses REAL 1.24.0 + 2.7.2 output (tests/test_real_snapshots.py). Immediately
      caught a live quirk: 2.x migrates ConvertRecord property keys
      (`record-reader` → `Record Reader`) — pull-based workflows are immune, hand-written
      flows using 1.x keys will show plan drift on a 2.x server.
- [x] **CI with real NiFi** (.github/workflows/ci.yml): unit matrix (3.9/3.13) + live
      integration against dockerized 2.7.2 AND 1.24.0 on every push. Catalog sweep and
      mTLS stay local-only (runtime / OpenSSL 3.2 constraints).
- [x] **Rename detection**: name-based identity means rename = remove+add; the plan
      now pairs same-type add/remove in a group and warns loudly (state/queue loss)
      before you find out the hard way.
- [x] **Backup + rollback**: every mutating push snapshots the live group to
      .niflow-backups/ first; `niflow rollback <group>` (and `niflow backup`) restore.
      Half-applied plan or bad edit is one command from undone.
- [x] **Live E2E proof** (tests/test_incremental_live.py): push → queue real
      FlowFiles → debugging edit → push --update → ids + queues survive, plan
      converges, rollback restores. Green on 1.24.0 and 2.7.2.

## Flow testing + whole-instance sync (2026-07-27) — SHIPPED
- [x] **`niflow test`** (niflow/testing.py): sandbox push, FlowFile injection via a
      temporary run-once GenerateFlowFile (dynamic props = attributes), selective
      start (sources & the collector stay stopped), queue collection + attribute/
      content assertions, `--keep` for autopsy. Cases live next to the flow
      (`tests = [TestCase(...)]`). Verified live on 1.24 + 2.7.2 — CSV in at a
      mid-flow processor AND a nested input port, JSON out at the audit tap.
      Found live: (a) start-group-then-stop-sources races the flow's own data —
      must start selectively; (b) 2.x renamed GenerateFlowFile's custom-text
      property AND direct REST creates are NOT config-migrated (snapshot imports
      are) — per-version property names required.
- [x] **Multi-instance sync** (niflow/sync.py): `pull --all -o flows/ [--parent G]`,
      `plan --all`, `push --all --update`, and `niflow drift` (ok/DRIFT per flow,
      exit 1) for cron/CI. Mirror→drift→reconcile loop verified live on both lines.
      Found live: to_python dropped the ROOT group's comment → permanent phantom
      drift on any commented group (fixed + regression test).

## Remaining critique items (in priority order)
- [x] client.py split into niflow/rest/ mixins — transport (197) / inspect (385) /
      flows (656) / ops (317) / common (81); client.py is a 56-line facade. Public
      import surface unchanged (`from niflow.client import NiFiClient`).
- [x] Catalogs carry CATALOG_META (source NiFi version + date); `niflow doctor`
      warns when the live server's version differs.
- [x] `push --env <name>`: non-sensitive values from .niflow-params.<name>.env
      override the model; .niflow-secrets.<name>.env becomes the default secrets file.
- [x] `niflow drift` shipped with the multi-group sync work.
- [x] walk_groups/list_queues fast path: ONE recursive /status call for the whole
      tree (per-group walk kept as fallback); verified live on both lines.
- [x] Decision: NiFiClient IS the shared ops layer — both GUIs are thin views over
      it (verified: neither holds flow logic of its own). Web GUI is primary
      (stdlib, remote-friendly); the Qt helper stays as the desktop fallback.
- [x] `niflow diagram` renders flows as Mermaid flowcharts (subgraphs per group,
      relationship-labelled edges, `--all` for a directory) — GitHub draws them inline.
