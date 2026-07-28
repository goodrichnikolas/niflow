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
- [ ] **Optional `niflow commit` command.** After an in-place push, niflow leaves the
      group with *local changes* for the user to commit in the Registry. Consider a CLI
      command to commit a new version (with a message) so the whole pull→edit→push→commit
      loop can stay in niflow.

## Helper GUI
- [x] **Diff/plan preview before push** — the push dialog now shows the semantic change
      plan (Details pane) and applies it incrementally by default (2026-07-27).
- [x] **Auto-refresh toggle** on the Inspector window (2026-07-27).
- [ ] Surface the WSL browser opener (`open_url`) in the Bulletins/Errors panels so those
      links open the Windows default browser too (currently plain selectable text).

## Validation
- [ ] **Live dry-run validator (phase 1).** Complement the static rulebook with an
      optional pre-push dry run against a live NiFi to catch value-level errors that
      can't be encoded as static rules.

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
- [ ] Variables (1.x registry) updates in the incremental path (async update-request
      dance) — plan reports them, apply skips with a warning.
- [ ] Funnel-heavy flows: connection identity for funnels is ordinal-based; inserting
      a funnel mid-list churns adjacent connections. Fine for now.
