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

## Torture-flow findings (2026-08-14) — flows/torture.py, round one
Adversarial fixture pushed to 2.7.2; the pull side survived everything (hostile
strings, retry config, DISABLED state, self-loops, cross-group wiring all
round-trip, and pull→diff→plan converges to zero). The push/identity side did not.

Root cause for most of it: UUID5 identity seeds (json_format._assign_identifiers)
collapse distinct components — connection seed is (name, src, dst) with no
relationships and no occurrence index; processors/groups seed on name alone.

- [x] **P0 — silent data loss on push.** *(fixed 2026-08-14)* Same-named siblings
      collapsed; 14 of 16 unnamed parallel Fanout→funnel edges dropped; cross-group
      connection endpoints hashed as "" (assignment order). Fix: two-pass identifier
      assignment (components across the whole tree, then connections); connection
      seeds now include sorted relationships + an occurrence index for exact
      duplicates; every named seed is claim-checked and same-kind name duplicates
      raise IdentityCollisionError instead of merging. Duplicates are also caught
      earlier: `find_identity_collisions` (core) runs in validate, plan_flow, and
      push_flow *before any live mutation* (previously to_json ran after teardown —
      an emit error would have destroyed the live group). Tests: tests/test_identity.py.
      Note: connection UUID5s changed once (relationships joined the seed) — the
      next VC push of an existing flow recreates its connections (queues were
      emptied by in-place rebuild anyway); incremental push is unaffected (no UUID5s).
- [x] **P0 — `push --update` crashed mid-apply, non-atomic.** *(fixed 2026-08-14)*
      Actual trigger was duplicate sibling "Stage" groups (path resolution picked the
      first), now rejected at plan time. Beyond that, PlanApplier now (a) preflights
      every planned connection endpoint against the desired model before the first
      REST mutation, (b) restarts whatever it stopped even when a change fails
      (finally), and (c) raises ApplyError reporting exactly N-of-M applied, which
      push_update enriches with the pre-push backup path + `niflow rollback` hint.
      Tests: tests/test_apply_unit.py (failure-containment section).
- [x] **P1 — plan/apply never converges on parallel edges** *(fixed 2026-08-18)*:
      `plan._diff_keyed` now pairs same-key buckets by field similarity
      (`_closest_index` — exact twin always pairs at cost zero) instead of
      popping in listed order. Live-verified on 1.24 + 2.7.2: torture-flow
      plan → push --update → plan converges to zero (baseline HEAD showed 33
      rotating ops on the same live group).
- [x] **P1 — property-name aliasing causes phantom drift.** *(fixed 2026-08-15)*
      `rules.canonical_properties(type, props)` rewrites display-name keys
      (harvested: descriptors now carry `display`, catalog gained a
      `PROPERTY_NAMES` table) and curated 1.x→2.x legacy keys
      (`LEGACY_PROPERTY_ALIASES`: record-reader/writer, generate-ff-custom-text,
      "Regular Expression"→"Search Value", …) to the server's canonical names.
      Guards: ambiguous display names (CopyS3Object's two "Bucket"s) untouched,
      legacy alias only when the type lacks the old key AND has the new one (so
      1.x catalogs are safe), never clobbers an explicitly-set canonical key,
      unharvested types pass through. Applied in the differ, the JSON emitter,
      the incremental applier, and validate. Tests:
      tests/test_property_canonicalization.py.
- [x] **P1 — NiFi-populated defaults show as drift.** *(fixed 2026-08-15)*
      `plan._diff_properties` now compares *effective* values: an unset side
      takes the harvested descriptor default, so live-materialised defaults no
      longer plan as `'…' -> None` (a genuinely non-default live value still
      unsets). Live proof: `niflow plan flows/torture.py` against the running
      group dropped from property drift on every processor to ZERO
      `properties[…]` lines — remaining ops are the parallel-edge P1 above.
- [x] **P2 — `niflow validate` false-positives on dynamic relationships**
      *(fixed 2026-08-18)*: `rules.DYNAMIC_RELATIONSHIP_TYPES` (RouteOnAttribute,
      RouteOnContent, RouteText, RouteHL7, QueryRecord; routing-strategy-gated)
      feeds validate — dynamic relationships are valid to wire/auto-terminate and
      the unterminated check covers them too. `validate flows/torture.py` is down
      to exactly the intentional PRIMARY-node error. Curated list, not harvested:
      ProcessorDTO doesn't expose `supportsDynamicRelationships`; harvesting it
      via the 2.x `/flow/processor-definition` endpoint in codegen would
      generalise this to custom NARs. (Bonus done 2026-08-15: validate now flags PRIMARY-node +
      incoming connection, which NiFi rejects — torture flow's Cron 'audit' is
      the live repro, kept invalid on purpose. Tests in test_validate.py.)
- [x] **P2 — funnel identity is ordinal** *(fixed 2026-08-18)*: funnels now match
      by connection topology, not list position — `plan.funnel_signatures` /
      `match_funnels` drive the differ, `apply._live_component_id` resolves
      through the same pairing, and `json_format._assign_identifiers` seeds
      funnel UUID5s on topology order (`_canonical_funnels`). Live-verified on
      1.24 + 2.7.2: clean push → plan = 0, `niflow diff` has zero
      connection/funnel churn against real server ordering. One-time caveat:
      multi-funnel flows whose declaration order ≠ topology order get new funnel
      UUID5s, so their next VC push recreates those funnel connections once.
- [x] **P2 — autoTerminatedRelationships order-sensitive in diff** *(fixed
      2026-08-18)*: emitted sorted (`retriedRelationships` too) and normalised
      at model construction (`core.Processor._sorted_relationship_sets`).

Repro state: flows/torture.py now uses unique names (duplicates are rejected by
design; that behaviour is locked in tests/test_identity.py). The half-applied
live NiflowTorture group from round one was deleted and re-pushed clean after
the P0 fixes.

## Live stepper (2026-08-18) — SHIPPED (CLI + webgui, see T1/T15)
- [x] **`niflow follow <group>`** (niflow/follow.py): quiesce (records prior
      RUNNING set), pick a FlowFile (front of first non-empty queue, `--uuid`,
      `--queue`, or `--source NAME` run-once mint), then step hop by hop —
      locate the queue holding the uuid, run-once the destination, poll the
      incremental provenance cursor (`flowfile_events_since`), render with the
      SAME hop renderer as trace (`format_hop`, now shared; trace output
      byte-identical). Interactive keys Enter/a/c/q; `--auto` (+`--max-hops`),
      `--restore` restarts only the previously-running set, `--full`. Forks
      surface child uuids with a picker (`--auto` follows the first child).
      Live-verified end-to-end on 1.24.0 AND 2.7.2 (mint → attribute diff →
      ROUTE relationship → DROP terminal).
- [x] **Webgui Follow tab** *(2026-08-19, with T1+T15)* — start-point picker,
      Step / Retry poll / Next branch, a branch table with per-branch mute,
      and the SAME hop card as the Trace tab (`hopCard()` is now shared, the
      way `format_hop` is on the CLI side). `/api/follow/*` keeps one session
      in module state and on disk, so a page refresh re-attaches.
- [ ] Fixture injection for follow (testing.py's injector as `--source`-like
      input), watch expressions (hop × attribute table), replay-after-fix.

## Cross-version property fidelity (found 2026-08-18) — FIXED same day
- [x] **Pushing with a 2.x-harvested catalog to a 1.x server silently mis-sets
      renamed properties.** *(fixed 2026-08-18)*: snapshot emission is now
      server-version-aware — `to_json(target_major=…)` translates canonical
      keys to the target's namespace via the existing `properties_for_target`
      compat join (processors AND controller services, `propertyDescriptors`
      included; unsupported keys are omitted with a warning). Every push path
      passes the live server's major version (full push, in-place push,
      incremental subtree instantiation); offline emission stays canonical, so
      files and diff normalisation are unchanged. The incremental applier
      already translated. Live-verified on 1.24: torture push → plan = 0 and
      the server's real `Regular Expression` key carries the pushed regex; the
      previously-deterministic `test_flow_testing_live` failures now pass in
      9s (root cause confirmed — ConvertRecord's `Record Reader` had been
      landing as an inert dynamic property, leaving the processor invalid).
      Original description follows. The emitter canonicalizes legacy keys to the
      catalog's names (e.g. ReplaceText `Regular Expression` → `Search Value`),
      but on 1.24 the real property is still `Regular Expression` — NiFi 1.x
      stores the canonicalized key as a *dynamic* property and the real one
      stays at its default, so the pushed value is NOT active. Symptom: plan
      shows `properties[Regular Expression]: '<default>' -> None` after a clean
      push (live snapshot carries both keys; canonicalization correctly refuses
      to clobber). Repro: `niflow push flows/torture.py` against 1.24 with the
      stock (2.x) catalog. Fix direction: key emission must be server-version
      aware — de-canonicalize via LEGACY_PROPERTY_ALIASES when the target is
      1.x (push/plan know the server version; offline to_json can stay
      canonical). Workaround today: use a 1.x-harvested catalog (`make
      catalog-v1`) against 1.x servers — doctor already warns on mismatch.
      Matters because work NiFi is 1.24/1.28 (the priority axis).
      Possibly related (verified pre-existing at 62b795a, NOT from the
      2026-08-18 fixes): `tests/test_flow_testing_live.py` fails
      deterministically against local 1.24 — sandbox teardown 409s with
      "Upstream component of Connection (ConvertRecord…) is running", and
      ConvertRecord's keys (`record-reader`→`Record Reader`) are exactly the
      renamed ones. CONFIRMED same root cause: green after the fix. The 71
      `test_catalog.py` failures on 1.24 remain expected — the 2.x catalog
      sweep against a 1.x server (CI rightly ignores test_catalog.py).

## Ticket sweep — 2026-08-19 (work-driven backlog)
Sixteen tickets raised after using niflow against work NiFi (1.24/1.28). Ordered
by ticket number, not priority; the priority axis is "things that hurt at work".

- [x] **T1 — Branching traces: mute the branches you don't care about.**
      Done 2026-08-19. The fork picker is gone; `FlowFollower` now keeps a
      branch tree (child uuid → parent, the relationship it left on, the queue
      and destination it landed in, hop history, state live/muted/done) inside
      the session. `mute`/`unmute` take `rel:failure`, `dest:PutFile`,
      `queue:<conn-id>`, `uuid:<child>` or a bare value (UUID-shaped → uuid,
      else relationship), work **before** the fork (`--mute failure` from the
      command line, "mute up front" in the GUI) and retroactively, and are
      reversible — branch records are kept, never dropped. `m`/`u`/`s`/`b` in
      the CLI loop, per-branch Mute/Unmute/Follow buttons in the GUI.
      **Muting never issues a mutating REST call** (docstring says so, and
      `test_mute_is_retroactive_and_reversible_and_never_calls_nifi` +
      `test_follow_mute_routes_change_the_view_not_nifi` pin it). When a
      branch ends the stepper moves to the next un-muted one by itself
      (fewer decisions), keeping per-branch history.
      Live fact found on 1.24 and 2.7.2: **CLONE/FORK events carry no
      relationship**, so a branch's name comes from its queue's
      `selectedRelationships` (`connection_relationships`, cached per
      connection).
- [x] **T2 — Purge from the Queues tab, per queue.** Done 2026-08-19: per-row
      "Purge" on every queue row (plus one in the FlowFile drill-down) via
      `POST /api/queues/{id}/purge` -> `drain_connection`, which now returns
      NiFi's `dropped` summary; "Purge this flow's queues" on the Queues bar
      (and the Processors strip) posts the *selected* flow to
      `/api/group/purge`, never root behind your back. Both confirm first and
      report what was dropped. Mirrored in the PyQt Inspector ("Purge queue"
      button, enabled on queue selection). Verified live on 2.7.2:
      `1 / 93 bytes` dropped from one connection.
- [ ] **T3 — docs/*.md per major component.** A short tutorial-style doc for each
      major piece (pull/push, plan/apply, trace/follow, explain, validate, gui,
      webgui, testing, backup/rollback, catalog).
- [x] **T4 — .gitignore flows/ by default.** Done 2026-08-19: `flows/*` with
      per-file negations for the shipped examples (`.gitkeep`, abc_to_json,
      torture, labyrinth) — it has to be `flows/*`, not `flows/`, because git
      never descends into an ignored directory. Same treatment for
      `docs/explanations/*` (LLM prose about whatever flow you pointed it at),
      and for the new local state dirs: `.niflow-follow/`, `.niflow-watch/`,
      `.niflow-fuzz/`, `.niflow-rulebooks/`.
- [x] **T5 — TLS-verify quirk found at work.** Root-caused 2026-08-19: it is
      `requests`' env-var merge. With `trust_env` on (the default) requests
      merges environment settings into *every* request, and
      `REQUESTS_CA_BUNDLE`/`CURL_CA_BUNDLE` replace `session.verify` whenever
      the call itself doesn't pass `verify=` — so a corporate image that
      exports one silently beat `NIFLOW_NIFI_CA_BUNDLE` and even flipped
      `NIFLOW_NIFI_VERIFY_SSL=false` back on. Passing `verify=` explicitly is
      what makes the session-level setting stick, which is exactly why the
      hand-patch worked. Fixed properly: `TransportMixin` stores one trust
      decision (`self._verify`, adopted from a caller-supplied session), pins
      it on `_request` and on the token call, and `niflow doctor` now reports
      when either CA-bundle variable is set. Proxy env vars are deliberately
      left alone — a corporate proxy is usually how you reach the server.
- [x] **T6 — Web GUI auto-refresh ON by default.** Done 2026-08-19: remembered
      in `localStorage["niflow.autorefresh"]`, on when nothing is stored. A
      tick is skipped while a mutation is in flight, while a confirm() is up,
      while an input/select has focus, while the FlowFile drill-down is open,
      on a hidden tab, and on Trace/Explain/Flows (provenance queries and
      flow fingerprinting are far too expensive for a 3s poll).
- [~] **T7 — Harden trace on 1.24.** Main pass done 2026-08-19 against the
      live 1.24 container — see "T7 — trace/follow hardened against NiFi 1.24"
      below for what was fixed and what was proved. Eight leftovers (T7a–T7h)
      are written up there and still open.
- [x] **T8 — Makefile tolerant of python/python3/uv/.venv.** Done 2026-08-19:
      `PY` is discovered as $VIRTUAL_ENV → ./.venv → `uv run python` (only when
      uv is installed *and* uv.lock exists) → python3 → python, `PIP` follows
      it (`uv pip` under uv, else that interpreter's own pip, so `make install`
      lands where `make test` looks), and `make PY=/path/to/python <target>`
      always wins. `make help` prints which interpreter was detected.
- [x] **T9 — Clickable processor links everywhere.** Done 2026-08-19: one JS
      helper (`compLink`) renders every component mention as a new-tab anchor
      built from the UI base `/api/about` now returns; used on Processors
      rows, Errors, Bulletins, Queues (source/destination + the connection
      itself), Trace hops and the FlowFile drill-down. Group ids threaded
      through where they were missing: `list_queues` now carries
      `group_id`/`source_id`/`destination_id`, trace hops carry `group_id`.
      NiFi's status snapshot has no endpoint ids on 1.24 *or* 2.7.2, so queue
      endpoints are matched to the processor listing (cached) and stay plain
      text when there's no match rather than linking somewhere they didn't
      promise.
- [x] **T10 — Fuzz harness: thousands of generated single-hop flows.** Bulk-generate
      processor→processor combinations and push/validate them (1.24 first, then
      2.x) to surface niflow bugs in batch instead of one painful discovery per
      work day. Failures that are NiFi's own rejection are fine — we're hunting
      niflow's. **Shipped 2026-08-19** as the `niflow/fuzz/` package (cases /
      checks / runner) + `niflow fuzz` +
      `make fuzz`; see "Fuzz-harness findings" below for what it caught on its
      first run. Three tiers: (1) offline — build/emit/re-parse/plan/translate
      every harvested type, 3.4k cases in ~7s, no NiFi; (2) `--tier 2` adds
      NiFi's own validation in a sandbox; (3) `--tier 3` adds live
      push→pull→plan convergence and a `push --update` that must be a no-op.
      Seeded and content-addressed (`--seed`, `--replay <case-id>`), results
      stream to `.niflow-fuzz/results.jsonl` (`--resume` continues an
      interrupted sweep), every finding writes a standalone runnable repro
      under `.niflow-fuzz/repro/<case-id>/`, and the report groups by
      root-cause signature so 172 failures read as one bug. Harness tests:
      tests/test_fuzz.py (offline).
- [x] **T11 — Claude Code as an LLM backend alongside Gemini.** Done
      2026-08-19: `claude-code` is a first-class provider in `niflow/llm.py`
      that shells out to the local `claude` binary in headless mode (prompt on
      stdin, no session persistence, so it doesn't litter ~/.claude with
      transcripts of work flows). It is picked up **automatically** when no API
      key is configured, or pinned with `NIFLOW_LLM_PROVIDER=claude-code`;
      `NIFLOW_LLM_CLAUDE_BIN` points at the binary when it isn't on PATH.
      Resolution order: explicit provider → explicit URL → key → local
      `claude`. This is the path that works at work, where there is no Gemini
      key.
- [x] **T12 — Explain: stop listing stopped processors under Gotchas.** Bloat.
      Done 2026-08-19: run state dropped from the digest (it is operations,
      not flow logic, and it invalidated docs on every start/stop) and the
      Gotchas prompt re-aimed at dead ends, auto-terminated failure,
      mismatched schedules, primary-node-only with inputs. Pre-existing docs
      keep their fingerprint via `legacy_fingerprint`.
- [x] **T13 — Map 1.24 vs 2.x unsupported properties.** *(done 2026-08-19)*
      Harvested the FULL property-descriptor set — processors **and controller
      services** — from live 2.7.2 and 1.24.0 and diffed them.
      - `niflow/codegen.py` gained `_harvest_service_rules` (services expose
        descriptors at `component.descriptors`, not under `config`) and
        `--dump-rulebook PATH`. Services were the big blind spot: nothing had
        ever harvested them, so `properties_for_target` silently skipped every
        one and their 2.x keys landed on 1.24 as inert dynamic properties.
        `services/catalog.py` now carries DESCRIPTORS/PROPERTY_NAMES and
        `compat_v1.py` the SERVICE_* twins; `rules.py` looks in both.
      - **`niflow/version_map.py`** (generated, ~180 KB) + **docs/version-compat.md**
        (readable report) built by `niflow/versiondiff.py` via `make version-map`.
        Deterministic — which took two fixes: sorting descriptor `dependencies`
        in codegen (NiFi returns them from a Set), and excluding
        controller-service-reference properties from the allowable-value diff
        (NiFi reports the *ids of the service instances on the harvest server*
        as their allowable values — 40 of the service "allowable changes" were
        pure UUID noise that would also have invented false warnings).
      - **The shape of it (2.7.2 vs 1.24.0, stock containers):** processors 252
        types on both lines, 40 only on 2.x, 102 only on 1.24; 217 types differ.
        **1285 renamed** properties, **168 only-2.x**, **258 only-1.24**, 56
        allowable-value changes, 26 required-ness changes. Services: 90 both,
        33/30 one-sided, 76 differ; **435 renamed**, 58 only-2.x, 25 only-1.24,
        22 allowable changes. So ~1720 of the ~2229 property differences are
        renames (translatable), and **509 are properties that simply cannot
        land on the other line**.
      - **Worst offenders:** the whole AWS S3 family (PutS3Object: 3 only-2.x,
        14 only-1.24 — 1.x `Access Key`/`Secret Key`/`Proxy Host` all gone,
        `Region` and `Storage Class` allowable sets rewritten wholesale),
        InvokeHTTP (30 renames), PutDatabaseRecord (18 renames + 7 only-2.x),
        ListSFTP/FetchSFTP (proxy properties → `Proxy Configuration Service`),
        the Elasticsearch processors, and on the service side
        DBCPConnectionPool, the record readers/writers (Json/CSV/Avro all
        renamed en masse + `Schema Reference Reader` is 2.x-only) and
        GCPCredentialsControllerService.
      - **Wired in:** `niflow validate FILE --target-version 1.24` (offline,
        no server — the one that saves 45 minutes at work); the same check runs
        automatically in `plan`/`push` against the live server's version,
        logging every affected component BEFORE any mutation; `niflow doctor`
        now reports map coverage and scans `flows/` ("N properties will NOT
        survive a push to NiFi 1.24.0", naming them). Live-verified: the
        emitter's own omit-with-a-warning does fire and is visible, and a real
        push to 1.24 now writes `schema-access-strategy` for a JsonTreeReader
        instead of the dead 2.x key.
      - Found in our own repo: `flows/abc_to_json.py` sets `Pretty Print`,
        which does not exist on 1.24 — silently inert there today.
      - Tests: `tests/test_version_compat.py` (37, no live NiFi).
      - **NOT determined — behavioural drift.** The harvest reads descriptors,
        so it is exact about names/allowable/required/defaults and *blind* to a
        property that exists on both lines under the same name but means
        something different in the engine. Renames are only matched 1:1 on
        display name then description (deliberately conservative), so a rename
        that changed key *and* display *and* description shows up as one
        only-2.x plus one only-1.24 entry — the warning is still right, the
        explanation is missing. Restricted/un-instantiable types are skipped
        entirely. And the map is from stock containers: work's 1.28 with extra
        NARs needs `make version-map` pointed at the real pair.
- [x] **T14 — Explain: high level only.** Explaining a deep group spidered into ~300
      nested documents. Default to the selected group (and one-line child
      summaries), with opt-in depth. Done 2026-08-19: `depth` replaces
      `recurse` (default 1), children below the cut get a digest-derived
      one-liner instead of an LLM call, `--depth N` / `--all` opt in, and both
      CLI and webgui show the document/LLM-call count before spending it.
- [x] **T15 — Trace/Journey as a debugger.** Done 2026-08-19.
      * **Start points**: `entry_points()` lists non-empty queues, source
        processors with no inbound connection and input ports;
        `niflow follow <group> --list` prints them (read-only — no quiesce),
        `--start N|id`, an interactive picker, and a click-to-start table in
        the GUI. A source start looks for the minted file in *its own*
        outbound queues, not "the first non-empty queue anywhere".
      * **Step / history**: one Step = one hop, kept per branch in a
        `FollowSession` written to `.niflow-follow/` after every action, so
        `h` / `h N` re-reads hop 3 without re-running anything and `--resume`
        (or a GUI page refresh / server restart) re-attaches to the journey.
        The prior-RUNNING set is saved too, so `--restore` still works after a
        resume.
      * **Diffs**: every hop carries a cross-hop `diff` (added/changed/removed,
        old → new — removals are invisible in NiFi's own per-event `changes`)
        and a `content_change` (size delta or `contentEqual: false`). The CLI
        marks them `~ + -`; the GUI colours them and **flashes** the hops a
        step just produced. `trace` annotates the same way, so both commands
        and both tabs render one hop identically.
      * **Robustness** (T7's territory, all live-verified on 1.24): every
        failure is a `FollowError` with advice, not a traceback — invalid or
        unrunnable processor (validation errors quoted), 403 (policy hint),
        FlowFile already gone, a queue that empties or 404s under the listing
        (skipped), provenance query refused (policy hint), stop-group refused.
        A stall is retryable: `r` / "Retry poll" re-asks provenance without
        running anything, because 1.24's provenance index lags.
      * **The big 1.24 finding**: run-once serves ONE FlowFile from ONE of a
        processor's inbound queues — not necessarily the followed one, so
        stepping silently did nothing on any processor with a busy second
        inbound queue (the torture flow's self-loop reproduces it every time).
        A step now re-runs the destination until *our* file moves (capped at
        `run_attempts`, stopping early once it leaves the queue) and reports
        "ran X 3x, 2 FlowFile(s) were ahead of it".
- [x] **T16 — "It wasn't us" alerting.** Shipped 2026-08-19 as `niflow watch`
      (niflow/watch.py, `.niflow-watch/` baselines, tests/test_watch.py).
      Original ask: Analysts burn 45 minutes discovering that an
      external endpoint started 404ing. Want a background watcher that notices
      "this was healthy, nothing changed, now it's failing" and pops it on screen
      (no email) with the external cause called out.
- [x] **T17 — Mine the leftover renames; make 1.24 the default baseline.**
      *(done 2026-08-19)* Two halves, both aimed at the same failure: a
      property that silently does nothing at work.
      - **509 -> 471.** T13's matcher deliberately refuses to pair a property
        whose key *and* display name *and* description all moved, so genuine
        renames were sitting in the "cannot land on the other line" buckets as
        false alarms (168+58 only-2.x, 258+25 only-1.24). Only 54 types have
        *both* buckets non-empty, so the candidate space is small; every pair
        in it was scored on corroborating evidence (normalised key/display
        equality, prefix/suffix, description similarity, identical allowable
        set, identical default, required/sensitive agreement, matching
        `dependencies`, ordinal position) with mutual-best + margin, then
        confirmed by hand. **19 pairs = 38 properties were real renames**;
        the other 471 are genuine one-sided properties, dominated by two 2.x
        refactors: AWS `Access Key`/`Secret Key`/`Credentials File` folded
        into the credentials-provider service, and every processor's
        `Proxy Host`/`Port`/user/password folded into
        `Proxy Configuration Service`.
      - Curated in `rules.CURATED_TYPE_RENAMES` (`{type: {2.x key: 1.x key}}`),
        which both `versiondiff.build_map` (so the committed map, its counts
        and the report agree) and `rules.property_renames_for` (so translation
        survives a stale/absent map) read — processors *and* controller
        services. Highlights: ExecuteSQL/ExecuteSQLRecord `SQL Query` <-
        `SQL select query`, FetchFile `Permission Denied Log Level` <-
        `Log level when permission denied`, Consume/PublishMQTT
        `Connection Timeout`/`Keep Alive` <- the `(seconds)` names,
        Get/Put/DeleteDynamoDB `Batch Items Per Request` <- `Batch items for
        each request (between 1 and 50)`, PutDatabaseRecord `Database Name`
        <- `put-db-record-catalog-name`, ListenTrapSNMP, PutSNS,
        PutKinesisStream, QueryAirtableTable, and on the service side
        DBCPConnectionPool `Maximum Connection Lifetime` and
        ADLSCredentialsControllerService `Account Key`.
      - **Not** auto-translated: 6 plausible pairs are printed as a
        "Possible renames — verify before trusting" list in
        docs/version-compat.md (ListenSyslog's `Port` -> `TCP Port`/`UDP Port`
        split, S3 `key-id-or-key-material` -> `KMS Key ID`/`Key Material`,
        IdentifyMimeType's config-body/file merge, ListenSyslog `Worker
        Threads`). Splits cannot be translated without guessing which half a
        value belongs to, and a wrong pairing writes the value onto the wrong
        property — worse than not translating.
      - **Live-verified on 1.24.0**, which is the only proof that matters
        here: pushed a flow setting the curated 2.x keys and read the
        descriptors back — `SQL select query`, `Log level when permission
        denied`, `Connection Timeout (seconds)`, `Keep Alive Interval
        (seconds)`, `dbcp-max-conn-lifetime` and JsonRecordSetWriter's
        `Pretty Print JSON` all landed on the **real** property
        (`descriptors[key].dynamic == false`), with no 2.x key left behind as
        an inert dynamic one.
      - **1.24 is now the default baseline.** New setting
        `NIFLOW_MIN_NIFI_VERSION` (`.niflow.env`, default `1.24`, `none` to
        disable) declared once and read by everything via
        `compat.baseline_version/baseline_major/baseline_issues/
        describe_baseline`. `niflow validate FILE` checks it with **no flag**
        and **exits non-zero** — deliberately an error, because the failure it
        prevents is silent on the server and a warning in a wall of output is
        how this class of bug reached work in the first place.
        `--target-version` still does an ad-hoc check against another line
        (and replaces the baseline rather than doubling it up);
        `--no-compat-check` opts out. `push` is **never blocked**: pushing
        2.x-only properties to a 2.x server is legitimate, so it warns loudly
        instead ("this push to NiFi 2.7.2 is fine, but the same flow would NOT
        work on the baseline line") and stays silent when the server *is* the
        baseline line, where the existing warning already says it. `doctor`
        states the baseline offline (before it even connects) and names any
        flow under `flows/` that violates it.
      - Cost of running it on every validate: the 168K generated map is
        already imported lazily inside the lookup functions — 7.7 ms cold,
        0.8 ms warm (cached .pyc), against ~100 ms for the whole `niflow
        validate` process. No change needed.
      - `flows/abc_to_json.py`'s `Pretty Print` was **not** a rename: 1.24's
        AttributesToJSON has no pretty-print property under any name (the
        similar `Pretty Print JSON` belongs to JsonRecordSetWriter and exists
        on both lines). Removed with a comment saying why — its value was
        `'false'`, which is also 2.x's default. `flows/` and `examples/` now
        have zero baseline violations; torture.py's remaining finding is the
        pre-existing PRIMARY-execution-node one, not a property.
      - Tests: 17 new in `tests/test_version_compat.py` (58 total, no live
        NiFi), including an autouse fixture that pins the baseline so the
        suite cannot pass or fail on whose `.niflow.env` is on disk, and a
        guard that every curated pair is present in the committed map.

## Fuzz-harness findings (2026-08-19) — `niflow fuzz`, round one
First sweep of the T10 harness: 3,419 offline cases over the whole harvested
catalog (7s, no NiFi), plus live tier-2/tier-3 samples against **NiFi 1.24.0**
and 2.7.2. 268 offline failures collapsed into 13 root-cause signatures; the
live tiers added 4 more. Everything below is *niflow's* fault — NiFi refusing a
nonsensical generated combination is classified separately and is not listed.

What the sweep **cleared**: the JSON path is solid — 3,419 flows all round-trip
`to_json → from_json → to_json` byte-identically and re-plan to zero, including
emoji/quote/newline/backslash names and property values, EL and parameter
escapes, deep nesting, funnel chains, parallel edges, self-loops, and
cross-group port wiring. `to_python` reproduces every one of them except the
empty-group case below. The differ is not blind to a single definition field:
24 targeted mutations per case (every processor/connection/service/group field)
all produce a non-empty plan in both directions.

- [x] **P1 — the NiFi 1.x in-place (version-controlled) push loses most of the
      flow, and crashes outright on funnels.** FIXED 2026-08-19, proved live on
      1.24 (see "1.x in-place push fidelity" below). `_push_in_place` on a 1.x target
      uploads `flow.to_xml()` as a template (rest/flows.py:386), and `to_xml` is
      a stub next to the JSON emitter:
      * **crash** — `KeyError` in `xml_format._emit_endpoint:609` for any
        connection whose endpoint is a funnel: `_assign_identifiers` never
        assigns funnel (or label) identifiers. 16/200 shape cases.
        Repro: `niflow fuzz --replay shape-b96f2a04d7`, or any flow with
        `a >> funnel`.
      * **silent loss** — funnels and labels are never emitted; connections
        hard-code `backPressure*`/`flowFileExpiration` and omit prioritizers,
        load balancing and partitioning attribute; processors hard-code
        `bulletinLevel`/`comments`/`executionNode`/`penaltyDuration`/
        `runDurationMillis`/`yieldDuration` and `state=STOPPED`, so DISABLED /
        RUNNING, retry count, retried relationships and backoff all vanish;
        group `variables` and the parameter-context binding are not emitted.
        Six distinct signatures in the sweep, all reproducible offline via
        `to_xml → from_xml → diff_flows`.
      Server line: **1.x only** (2.x uses copy/paste and is unaffected), which
      makes it a work bug: registry-versioned groups on 1.24/1.28 are exactly
      the in-place path. Fix direction: either emit the missing state in
      `xml_format` (funnel/label ids first — that one is a two-line fix) or,
      better, stop using templates and drive the 1.x in-place rebuild from the
      component REST API the incremental applier already uses.

- [x] **P1 — `to_python` emits a syntactically invalid module for a process
      group with no components of its own.** `with flow.process_group('G') as g:`
      is written with an empty body → `IndentationError: expected an indented
      block`. Any pulled flow containing an empty (or child-groups-only)
      process group produces a `.py` file that will not even import — `niflow
      pull` is the front door of the whole workflow, and placeholder/empty
      groups are common in real canvases.
      Repro: `niflow fuzz --replay shape-3b0b2f4890`; or
      `Flow("F").process_group("Empty"); to_python(flow)`.
      Root cause: `python_format._emit_group_body` writes the `with` header
      unconditionally and emits nothing when the group has no
      services/ports/processors/connections/funnels/labels. Needs a `pass`.
      Server line: both (pure emitter).
      **Done 2026-08-19:** `_emit_group_body` now records the stream position
      before emitting a child's body and writes `pass` when the child wrote
      nothing — so nested empties (a group whose only content is another empty
      group) stay correct too, and the pull → emit → import → re-emit round
      trip is byte-stable for that shape. Tests in `tests/test_json_format.py`.

- [x] **P1 — a controller service never converges: `enabled: False -> True`
      forever.** Push any service-bearing flow without `--start`, then plan:
      NiFi leaves the service DISABLED, the model says `enabled=True` (the
      default), the differ sees drift, `push --update` "fixes" it, and the next
      plan drifts again. Every service-bearing flow reports DRIFT immediately
      after a clean push — `niflow drift` in cron/CI cries wolf on all of them.
      Repro: `niflow fuzz --tier 3 --replay service-1a6c3e11eb` (5/25 tier-3
      cases on both lines). Suspected root cause: `enabled` is a *deploy
      intent*, not observable state, but `plan._SERVICE_FIELDS` diffs it against
      the live `scheduledState`. Either push must enable services it created
      with `enabled=True`, or `enabled` must leave the diffed field set (with
      an explicit `niflow start` owning it).
      Server line: **both** (1.24 and 2.7.2).
      **Done 2026-08-19 — the second option, refined.** `enabled` is now diffed
      only when the model *states* it (`"enabled" in desired.model_fields_set`,
      `plan._diff_service_fields`): exactly the rule `_diff_properties` already
      uses, where a side that says nothing takes the other's value instead of
      proposing a change. The bare default no longer plans anything; an
      explicit `enabled=True`/`False` (every pulled flow carries one) still
      diffs and `push --update` still enables/disables the live service. Push
      was deliberately NOT made to enable services — activation is what
      `--start` is for, and enabling at push time would fail loudly on any
      service that is not yet fully configured.
      Verified live: push + plan = **zero ops** on 2.7.2 and 1.24.0 for a flow
      with a service, int/bool properties and a primary-node-only processor;
      `niflow fuzz --tier 3 --replay service-1a6c3e11eb` passes.
      **Found while fixing it — the live read is blind (new P2, see below).**

- [ ] **P1 — on NiFi 1.24, a controller-service *reference* silently does not
      wire up for the 42 catalog types with no 1.x compatibility data.**
      `properties_for_target` returns identity when `compat_v1` has no entry for
      a type ("unknown — don't translate"), so the property is emitted under its
      2.x key; 1.24 stores it as an inert dynamic property and the real
      reference stays unset. Live proof: `DeleteSFTP` + `Proxy Configuration
      Service` pushed to 1.24 pulls back with the property *absent*
      (`properties[Proxy Configuration Service]: None -> service 'Svc'` in the
      plan); the same case on 2.7.2 converges. This is the 2026-08-18
      cross-version bug again, in the hole the fix could not cover.
      Repro: `NIFLOW_NIFI_HOST=…:8444 niflow fuzz --tier 3 --replay service-12687f42cf`.
      The offline tier now lists the affected types (`types with NO 1.x
      compatibility data`: ConsumeKafka, PublishKafka, CopyS3Object, the Box
      family, …). Fix direction: harvest compat data for every type the 1.x
      server reports (not just the intersection), and treat "no data for this
      type" as a *warning* at push time rather than silent identity.
      Server line: **1.x only**. Feeds T13.

- [ ] **P2 — properties that exist only on 1.x drift forever against a 1.24
      server.** A live 1.24 processor materialises its own 1.x-only properties
      (`QueryRecord.cache-schema`, `ListFTP.Proxy Type`, …); the desired model
      (hand-written, or authored against the 2.x catalog) does not have them and
      the 2.x catalog has no descriptor, so `plan._diff_properties` reads the
      effective default as `None` and proposes unsetting them — on every plan,
      forever, and `push --update` really does send the unset.
      Repro: `NIFLOW_NIFI_HOST=…:8444 niflow fuzz --tier 3 --replay shape-4f6f5d6272`
      (`properties[cache-schema]: 'true' -> None`).
      Root cause: the differ has no notion of the target server's namespace —
      the emit side got that in the 2026-08-18 fix, the diff side did not. A
      live property that does not exist in the model's namespace is not ours to
      manage and should be ignored, not unset.
      Server line: **1.x only** (pull-based flows are immune — they carry the
      1.x keys; hand-written and 2.x-authored flows are not).

- [x] **P2 — `@PrimaryNodeOnly` processors drift forever: `execution_node:
      'PRIMARY' -> 'ALL'`.** NiFi forces `executionNode=PRIMARY` on types
      annotated primary-node-only (ListFTP, ListFile, ListS3, GetSFTP, …); the
      model default is `ALL`, so a clean push immediately plans a change back to
      ALL that the server will not accept.
      Repro: `niflow fuzz --tier 3 --replay shape-65fb045b08`.
      Fix direction: harvest the annotation into the catalog (the create
      response reports the applied `executionNode`) and use it as the model's
      effective default, the way `_diff_properties` already does for property
      defaults. Server line: **both**.
      **Done 2026-08-19 — harvested, not curated.** `ProcessorDTO` *does* expose
      it: `executionNodeRestricted` comes back on the create response on **both**
      lines (checked live: it matches the 2.x-only
      `/flow/processor-definition` `primaryNodeOnly` field exactly), so
      `codegen._harvest_rules` records it and the catalog gained
      `PRIMARY_NODE_ONLY` (26 types on 2.7.2). `rules.primary_node_only()`
      reads it and `plan._normalise_field` treats PRIMARY as the effective
      value for those types on both sides — so even an explicit `ALL` (a value
      the server refuses) stops planning a change that can never apply. Note
      ListFile is *not* one of them on either line; the real set is
      ListFTP/ListSFTP/ListS3/QueryDatabaseTable/the SaaS pollers.
      The 1.24 harvest agrees on every shared type but adds three that only
      exist there — `ListHDFS`, `GetJMSTopic`, `ListAzureBlobStorage` (v1) — so
      a flow using those against 1.x still drifts until `make catalog-v1`
      carries a `PRIMARY_NODE_ONLY` twin into `compat_v1`.

- [x] **P2 — Python `int`/`bool` property values cause permanent phantom
      drift.** `properties={"Batch Size": 10}` (or `True`) emits JSON `10` /
      `true`; NiFi's property map is `Map<String,String>`, so it comes back
      `"10"` and the differ reports `properties[Batch Size]: '10' -> 10` on
      every plan. 172 offline cases (one per harvested type with a numeric or
      boolean default). Writing an int is the natural thing to do in a Python
      DSL, and nothing warns.
      Repro: `niflow fuzz --replay props-dafe8392f4`.
      Fix direction: coerce non-string property values to their NiFi string
      form in `json_format._emit_properties` (and normalise in
      `plan._normalise_prop` so old files stop drifting).
      Server line: **both**.
      **Done 2026-08-19 — normalised at model construction**, the way
      `Processor._sorted_relationship_sets` already normalises relationship
      order: `core.nifi_property_values` coerces on `Processor` and
      `ControllerService`, so `{"Batch Size": 10}` *is* `"10"` from the moment
      the model exists and every consumer (emit, plan, to_python, validate)
      agrees. Booleans use NiFi's lower-case spelling — except for the seven
      properties whose allowable set really is `["True", "False"]`, where the
      descriptor's own spelling wins. `json_format._emit_properties` and
      `plan._normalise_prop` coerce too, for dicts edited in place after
      construction. This also killed a second signature nobody had attributed
      to it: `xml_roundtrip:update:processor|properties[…]` (172 cases).

- [ ] **P2 — bare `KeyError` (with a memory address) when a component is wired
      but never registered.** Two shapes, both easy hand-editing mistakes:
      a `ControllerService` passed as a property value but never
      `add_controller_service`'d (`json_format._emit_properties:846`), and a
      connection whose endpoint was never `add_processor`'d
      (`json_format._emit_endpoint:930`). `validate` does not catch either, so
      the first sign is an unreadable traceback in the middle of a push.
      Repro: `niflow fuzz --replay shape-25b2ccb4fc` / `--replay shape-ea0fcdf8b2`.
      Fix direction: a pre-emit reachability check next to
      `find_identity_collisions`, naming the component and the group.
      Server line: both (pure emitter).

- [ ] **P3 — a live property whose value is the empty string drifts against an
      unset model field** (`properties[FlowFile Description]: '' -> None` on
      DetectDuplicate). `_diff_properties` compares `""` against the descriptor
      default `None`. Same family as the P2 above; probably the same fix
      (treat `""` and "unset with no default" as equal). Server line: both.

- [ ] **P3 — `to_json` is not byte-stable for an explicitly `None` property
      value.** `properties={"x": None}` survives emission but `from_json`'s
      `_clean_properties` drops it, so `to_json(from_json(to_json(f)))` differs
      from `to_json(f)` — the one documented invariant of the JSON format.
      Either drop `None` on the way out too, or keep it on the way in.
      Server line: both (pure format).

- [ ] **P1 — niflow has no 1.x *relationship* data, so a push to 1.24 can leave
      relationships unhandled and `validate` never notices.** The compat harvest
      (`make catalog-v1`) records DESCRIPTORS and PROPERTY_NAMES but not
      RELATIONSHIPS, and the rulebook only knows the 2.x set. Live proof on
      1.24.0: `UpdateAttribute` with `Store State` set has a relationship
      `set state fail` that does not exist in the 2.x catalog —
      `validate_flow` returns `[]` while the server says *"Relationship 'set
      state fail' is not connected to any component and is not
      auto-terminated"*, i.e. the processor cannot start after a clean push.
      Repro:
      ```
      NIFLOW_NIFI_HOST=https://localhost:8444/nifi-api niflow fuzz --tier 2 \
          --replay props-1cb013c86a
      ```
      or push a lone `UpdateAttribute` with `{"Store State": "Store state
      locally"}` to 1.24 and read the Errors panel. Fix direction: harvest
      relationships into `compat_v1` and have `validate` (and the auto-terminate
      helper) take a target line, the way the emitter now does for properties.
      Server line: **1.x only**. Sibling of the 2026-08-18 property fix; same
      shape, different table.

- [ ] **P3 — `validate` misses two things NiFi rejects and niflow could see
      statically.** Both surfaced ~100 times in the tier-2 sweep:
      * a property value referencing `#{param}` while **no parameter context is
        bound** anywhere up the tree — NiFi: *"references one or more Parameters
        but no Parameter Context is currently set on the Process Group"*. The
        validator deliberately skips EL/parameter *values*, but whether a
        context is bound is statically knowable.
      * a connection whose two endpoints live in **different child groups**
        (NiFi requires ports for that). niflow emits it happily; the push fails
        with *"Connection has a source with identifier … but no component could
        be found in the Process Group"*, which reads like a niflow bug and
        wastes a push. Repro: `niflow fuzz --replay shape-267c6461ba`.
      Server line: both.

- [ ] **Follow-ups for the harness itself** (not bugs, just coverage gaps):
      a controller-service *catalog* sweep (services get exercised only as
      referenced types today), a case kind for parameter contexts with real
      secrets, and `apply.py` failure injection (the incremental applier is only
      covered through the live tier).
## Fuzz round one — "cries wolf" cluster closed (2026-08-19)
The four drift-forever items above (`enabled`, `@PrimaryNodeOnly`,
`int`/`bool` property values, empty-group `to_python`) are fixed; see each
entry for where and why. Offline sweep, same seed and case set, before → after:
**220 failing cases / 8 signatures → 40 / 5** (`make fuzz`, 3,419 cases). Tier 3
against the live pair: 2.7.2 **10 cases / 14 signatures → 4 / 4**; 1.24.0
**21 / 10 → 21 / 2** (the 1.24 count is unchanged because those cases were
*also* failing on the 1.x property-namespace bug below, which is untouched).
Everything still failing belongs to another entry: the 1.x XML/template
emitter, the unregistered-component `KeyError`, and the property-namespace
family.

The harness itself learned two things while this landed (`fuzz/checks.py`):
its plan-sensitivity check now states `ControllerService.enabled` on the
baseline before mutating it (niflow diffs stated intent, so an unstated field
is not a silently-dropped edit), and it skips the `execution_node` mutation on
primary-node-only types (NiFi refuses the edit, so there is nothing for the
plan to see). Without those two, the fix would have traded 344 findings for
517 of the opposite kind.

- [ ] **P2 — run state is invisible to `plan` and `pull`: NiFi's
      flow-definition download sanitises it.** Found while fixing the `enabled`
      drift. `/process-groups/{id}/download` reports **every controller service
      as `scheduledState: DISABLED`** even when it is live-ENABLED, and every
      processor as `ENABLED` even when it is RUNNING (verified on 2.7.2:
      `/flow/process-groups/{id}/controller-services` says ENABLED for the same
      service at the same moment). Consequences:
      * `niflow pull` writes `enabled=False` for services that are enabled on
        the canvas — a lossy pull, silently;
      * a *stated* `enabled=True` re-plans forever (the apply really does
        enable the service, the next read just can't see it), and a stated
        `enabled=False` can never disable an enabled service;
      * the same blindness applies to `scheduled_state='RUNNING'`, so anyone
        who states RUNNING gets permanent drift.
      Fix direction: overlay the real states onto the live model in
      `pull_flow`/`plan_flow` — one recursive read of
      `/flow/process-groups/{id}/controller-services` (and the processor list,
      which `walk_processors` already does) — then `enabled` becomes a fully
      honest two-way assertion. Server line: **both**.

- [ ] **P3 — a live property whose value NiFi materialises differently from its
      own descriptor default drifts forever.** Sibling of the `''` vs unset P3.
      Live on 2.7.2: a pushed `JsonRecordSetWriter` comes back with
      `Allow Scientific Notation = 'true'` while the service's own descriptor
      (harvest *and* live REST) says the default is `'false'`, so
      `_diff_properties` reads the model's effective value as `'false'` and
      plans `'true' -> None` on every run. niflow never emitted the property —
      NiFi wrote it on import. Repro: push any flow with a
      `JsonRecordSetWriter` to 2.7.2 and plan. Fix direction: probably the same
      one as the 1.x-only-property P2 — a live property the model does not
      state, and whose value niflow cannot have caused, is not ours to unset.
      Server line: 2.x (the 1.24 twin of it is `schema-protocol-version`).

## 1.x in-place push fidelity (2026-08-19) — FIXED, verified live on 1.24 + 2.7.2
The fuzz P1 above: a registry-versioned group on 1.24/1.28 is rebuilt by
uploading `flow.to_xml()` as a template, and `to_xml` had drifted far behind the
JSON emitter. Full inventory of what was being lost, measured field-by-field
against `json_format` and against a template downloaded from a live 1.24
(`GET /templates/{id}/download` of a snapshot-pushed torture flow):

* **crash** — funnels/labels got no identifier, so any connection touching a
  funnel raised `KeyError` in `_emit_endpoint`.
* **never emitted** — funnels, labels, group `variables`, service `comments`.
* **hard-coded** — connections: `backPressure*`, `flowFileExpiration`,
  `labelIndex`, and no prioritizers / load balancing / partitioning attribute at
  all; processors: `bulletinLevel`, `comments`, `executionNode`,
  `penaltyDuration`, `yieldDuration`, `runDurationMillis`, `state=STOPPED`
  (DISABLED lost) and the whole retry block (`retryCount`,
  `retriedRelationships`, `backoffMechanism`, `maxBackoffPeriod`).
* **wrong** — a connection endpoint's `<groupId>` was the *connection's* group,
  not the endpoint's owner (cross-group wiring to a child port); no `FUNNEL`
  endpoint type; parallel edges with the same name+endpoints+relationships
  hashed to one id (NiFi merges/drops); **no auto-layout**, so every component
  without an explicit position landed at (0,0) — the JSON path lays them out.
* **from_xml side** — a service referencing another service never resolved
  (only processors were swept), so service chains lost their wiring.

Fixed in `formats/xml_format.py` (emitter + parser now at JSON parity, checked
by `to_xml → from_xml → diff_flows == []` on `flows/torture.py`).

**What genuinely cannot cross a template — `xml_format.template_limitations()`.**
Verified live on 1.24, not guessed:
1. a group's **parameter-context binding** — the template schema has no element
   for it; NiFi's own export of a bound group drops it too;
2. the **target group's own** `variables`/`comments` — a `<snippet>` has no DTO
   for the group its contents land *in* (nested groups keep theirs, verified);
3. a connection's **load-balance compression** — the template carries it, but
   1.24's snippet instantiation applies the strategy and ignores the
   compression;
4. **parameter references in property values** — 1.24 *escapes* every `#{` while
   instantiating a snippet, with or without a bound context: a working
   `#{param}` lands as the literal `##{param}`. This one is the nastiest, since
   work's flows are parameter-driven, and it was silently unwiring them.

`_push_in_place` now prints all of it **before** it empties the live group
(`_warn_template_limits`), then repairs it afterwards: `_reconcile_in_place`
diffs the live group against the model and re-applies the settings/property
residue one change at a time with `PlanApplier` (the `push --update` engine).
Anything it cannot repair is logged as a loud residual difference. Not a
refusal: everything above is recoverable over REST, and refusing would leave a
1.24 user with no way to push at all ("1.24 must always work"). A refusal is
still the right answer for anything *unrecoverable* — nothing in the model is,
today.

Acceptance (live): `flows/torture.py` + a parameter context + root/child
variables, version-controlled on 1.24, pushed in place → same group id,
`LOCALLY_MODIFIED`, and the live snapshot shows 3 funnels, the label with its
text/size, the DISABLED processor, CRON+PRIMARY+concurrency 4+penalty/yield/
bulletin, the retry block, all four tuned queues (0 and 1 000 000 thresholds,
10 TB, 5 min expiration, both prioritizer lists, ROUND_ROBIN +
COMPRESS_ATTRIBUTES_AND_CONTENT, PARTITION_BY_ATTRIBUTE `code`), and both
parameter-context bindings + both variable maps — then **`niflow plan`
converges to zero**. Fuzz: 6 XML signatures → 0 (`niflow fuzz --tier 1
--count 0`; only the two known `emit_json` KeyErrors remain).

Bonus, same mechanism: the **2.x paste path** had its own silent losses —
`scheduledState=DISABLED` and a *child* group's parameter-context binding do not
survive `PUT /paste`. `_reconcile_in_place` runs on both lines now, so 2.7.2
also converges (only 1.x-only `variables` remain, which 2.x has no registry
for).

### Recommendation: retire templates for the 1.x in-place push
**Yes — and it is proven feasible on 1.24.** Templates cost us an entire second
emitter held to snapshot fidelity, and two of the four losses above (parameter
escaping, load-balance compression) are NiFi behaviours we can only paper over.
`_instantiate_template` exists because 1.x snapshot import always creates a
*new child group*, and the in-place contract needs the components to land inside
the existing (version-controlled) group. There is a 1.x-native way to do that:

1. import `flow.to_json()` as a temporary child group (existing
   `_create_from_snapshot`);
2. pre-create the target group's own controller services and remap references —
   *exactly* the `_recreate_group_services`/`_remap_service_refs` dance the 2.x
   paste path already does (`PUT /snippets/{id}` refuses the move otherwise:
   *"references a service that is not available in the destination Process
   Group"*);
3. `POST /snippets` over the temp group's contents, then
   `PUT /snippets/{id}` with `parentGroupId` = the target group;
4. delete the temp group.

Probed live on 1.24 (scratch `probe_snippet.py`): the move returns 200 and lands
processors, connections, funnels, labels, the nested group **with its
variables**, DISABLED state, the tuned queue **including
COMPRESS_ATTRIBUTES_ONLY**, and — the point — `#{p1}` **unescaped**. That
removes the whole class of bug rather than patching an emitter, and makes both
lines share one code shape (snapshot + pre-created services + a group-level
"inject" call). Costs: a temp group and per-component revision juggling, and
`to_xml` still has to stay honest for `niflow convert` (XML is a supported
export format), so the fidelity work above is not wasted either way.

## T7 — trace/follow hardened against NiFi 1.24 (2026-08-19)

Adversarial pass over the live stepper on the **1.24.0** container, with every
1.x/2.x payload difference checked against 2.7.2 side by side. New fixture:
`flows/labyrinth.py` (tracked — added to the `.gitignore` negation list;
endpoint-free, standard-NAR only, nothing started, safe on a dev NiFi). It is
built to break the debugger rather than the emitter: four levels of nested
groups joined by input/output ports, a three-way fan-in, a 50-way SplitJson
into a 50-way MergeContent, a followable 2-way split/merge, a
route-to-`failure` lane that records no provenance, a self-loop for long
journeys, and a 200-file batch that overflows NiFi's queue listing.
Live tests: `tests/test_follow_live.py` (10, `-m integration`, expected green
on both lines).

### Fixed — ranked by how much they hurt at work

- [x] **1. Follow could not cross a process-group boundary at all.** The
      user's tree is 4-5 groups deep and every level is entered through an
      input port, so the *first or second* step of a real journey dead-ended
      with `terminal`. run-once does not exist for ports, and quiesce had
      stopped them. `step()` now recognises INPUT_PORT/OUTPUT_PORT/FUNNEL and
      **carries the file across**: start the port, watch our uuid leave the
      queue, stop the port again (~0.3s on 1.24), then synthesise a `CROSS`
      hop — ports and funnels emit **no provenance event** (verified on 1.24
      *and* 2.7.2). Live: DeepGen → four input ports down → four processors →
      three output ports back up, 14 steps, no dead end. Honest caveat in the
      docstring: a running port drains the *whole* queue, not just our file
      (the group is quiesced, so they only move one hop and stop).
- [x] **2. RUN_ONCE on an invalid processor silently wedges it — forever on
      1.24.** NiFi returns **200** and does nothing, so the old "blocked"
      path (which only fired when NiFi *refused*) never triggered and the
      step reported `stalled`, blaming the provenance index. Worse: the
      processor is then stuck in `RUN_ONCE`/`VALIDATING` and **nothing clears
      it on 1.24** — stop-group, `DELETE /processors/{id}/threads`,
      run-status STOPPED and a full component PUT all return 200 and change
      nothing; it has to be deleted and recreated. (2.7.2 clears it with a
      group-level stop, but *refuses config changes while wedged* — 400
      "Cannot modify configuration … while the Processor is running" — so the
      property that caused it cannot be fixed in place.) The stepper, as
      shipped, would run-once up to 8 times against an invalid destination:
      it could brick a work processor. `step()` now calls the new
      `processor_validation(proc_id)` (one GET, not a group walk) **before**
      the first run and returns `blocked` with the validation errors quoted,
      `runs == 0`. It also names the wedge when it finds one already there.
- [x] **3. NiFi's provenance `maxResults` does not mean "the newest N".**
      Measured on 1.24 against a component with 800 matching events: asking
      for 10 returned event ids 932-1071 — **from the previous day** — while
      the newest was 133249; asking for 250 returned a set with a hole in the
      middle. Same on 2.7.2 (asking for 3 of 1200 returned a scattered
      subset). The cap is applied per index shard, so a capped answer is an
      arbitrary sample **presented as the whole story**. Consequences before
      the fix: `recent_events` (the GUI's "view data provenance" replacement)
      showed ancient events; `trace_flowfile` on a long journey showed an
      arbitrary 100 hops with gaps; and — the one that explains the "1.24
      provenance lag" the stepper was papering over with retries — an
      incremental `flowfile_events_since` poll on a file with a long lineage
      could come back full of events *below* the cursor and look like a
      stall that no amount of re-polling fixes. Fixed with
      `_provenance_newest`: NiFi flags a capped result as `total: "N+"`, so
      the cap is escalated (×10, ceiling 5000) until the answer is complete,
      then the newest N are taken locally where the ordering is knowable.
      Live: `recent_events(5)` now returns the actual newest five, in 0.0s.
- [x] **4. A FlowFile past position 100 was reported as dropped or expired.**
      NiFi caps a queue listing at 100 and **will not raise it** — a request
      body asking for `maxResults: 500` still comes back `maxResults: 100`,
      on 1.24 *and* 2.7.2. Work queues hold thousands. `_find` now notices a
      short listing (`len(files) < queue["queued"]`) and settles it with a
      targeted `GET /flowfile-queues/{id}/flowfiles/{uuid}`, which resolves a
      FlowFile **at any depth** on both lines (new `locate_flowfile`).
      `_locate_many` does the same, so a 50-way fanout into a deep queue
      still finds its children. Live-verified against a 200-file queue.
- [x] **5. A hop with no provenance event was reported as a stall.** A plain
      `session.transfer` writes nothing to the provenance repository:
      verified on 1.24 that SplitJson routing to `failure` produces **zero**
      events, and the file's whole trace is just its CREATE. The step saw the
      file leave the queue, found no event, and said "1.24 can lag indexing:
      retry" — advice for a poll that can never succeed. New status
      **`moved`**: it re-locates the file, synthesises a `MOVED` hop and says
      "X moved the FlowFile to `A -> B` but recorded no provenance event".
      Not retryable, because there is nothing to retry.
- [x] **6. A queue with files ahead exhausted a fixed 8-run budget.** run-once
      serves ONE FlowFile from ONE inbound queue, so a file at position 20
      needs 20 runs; the old fixed 8 gave up and called it `stalled`. The
      budget is now `files ahead + run_attempts` (cap 500 runs / 60s), and
      while files are known to be ahead the loop skips the provenance poll and
      just watches the position fall. It reports how far it got instead of
      pretending the file vanished. Also fixed: `position` is **1-based** on
      both lines, so "19 FlowFile(s) ahead of it" was always one too many.
- [x] **7. `FlowFileUUID` is a lineage query, not a uuid filter — so trace
      attributed other FlowFiles' events to yours.** Verified on 1.24 and
      2.7.2: querying a split child returns the *parent's* FORK (whose 50
      `childUuids` are the child's **siblings**) and the *merged file's* JOIN
      (a different uuid, a different size, a different attribute set). It is
      one hop of attribution in each direction, not transitive. Before: the
      child's trace rendered "spawned 50 children"; `annotate_hops` diffed the
      child's 4 B against the merged file's 1030 B and produced a wall of
      nonsense; a merged file's DROP could end the wrong branch; and
      `_register_children` would have adopted 49 siblings as children. Now
      every hop carries `flowfile_uuid` and `own`, `lineage_note()` labels a
      relative's event ("this FlowFile was merged into 7269ef63… here,
      together with 1 other(s)"), foreign hops are neither diffed nor used as
      the diff baseline, only *our* DROP ends a branch, and the one relative's
      event that does count — the JOIN naming us among its parents — still
      registers the merged file as the branch the journey continues in.
- [x] **8. Following a FlowFile through a merge now works end to end.** Live
      on 1.24: PairGen → 2 children → the JOIN is labelled, the merged file
      appears as a branch with relationship `merged` and destination
      `PairSink`, the sibling is correctly "consumed", and the merged file is
      walked to its DROP. Also: `gone` now polls provenance once before
      shrugging, so a consumed file's final events are rendered instead of
      lost.
- [x] **9. `--restore` never restarted ports.** `quiesce` stop-groups (which
      stops ports too) but only remembered RUNNING *processors*, so a
      debugging session handed the group back with every port stopped — a
      flow that silently stops moving data. Ports are now remembered and
      restored.
- [x] **10. Cheaper, and 403/408 named.** The journey query asks for
      un-summarized events, so one round trip returns attributes, parent/child
      uuids and content availability instead of one query plus a GET per event
      (with a fallback for a server that summarizes anyway). A provenance 403
      now names the *'query provenance' global policy* and points at
      `niflow queues` as the fallback; a 408 says the repository is busy and
      to re-poll rather than re-run.

### Proved on 1.24 that was previously assumed

- Port hops cross in ~0.3s and emit **no** provenance event (same on 2.7.2);
  the run-status body 1.x wants is accepted unchanged by 2.7.2, and
  `disconnectedNodeAcknowledged` turns out to be optional on both.
- `FlowFileSummaryDTO`/`FlowFileDTO` are key-identical across the lines and
  both carry `penalized`/`penaltyExpiresIn`; `position` is 1-based and exists
  **only** in the listing, never in the single-FlowFile DTO.
- A ROUTE event does carry its `relationship` through the un-summarized query
  (`RouteOnAttribute.Route: hot`, live).
- An unknown uuid — or a malformed one, or an unknown search-term key —
  returns `total: "0"` and an empty list on both lines, never an error.
- Splitting to 50 children costs one queue scan and lands 50 branches in 1.2s.

### Written up, not fixed

- [ ] **T7a — `recent_events`/trace can still be incomplete above the
      escalation ceiling.** `_PROV_RESULT_CEILING = 5000`: a component with
      more matching events than that falls back to NiFi's arbitrary subset.
      The real fix is `startDate`/`endDate` on the provenance request (the
      DTO supports both) to bound "recent" by time instead of by count.
      Deliberately not done here: it changes what "recent events" means.
- [ ] **T7b — 50 branches is technically fine and practically unusable.** The
      50-way split registers all 50 correctly and quickly, but the CLI branch
      table then prints 50 rows and `next_live` walks them one at a time.
      Wants grouping by (relationship, destination) with a count, and a
      "follow N of them" sample — a UX design job, not a bug.
- [ ] **T7c — a merge point stalls until its bin fills.** Following one child
      into a 50-entry MergeContent bin, the step run-onces the merger and
      nothing happens (correctly — 49 files are missing) and reports
      `stalled`. It should recognise a merging destination and say "this
      processor is binning: it needs N more FlowFiles before it will emit",
      which means reading `Minimum Number of Entries`/`Max Bin Age` and the
      queue depth. Needs a design decision about how much processor-specific
      knowledge the stepper is allowed.
- [ ] **T7d — MergeContent emits one fewer hop per input on 2.7.2.** The same
      50-way merge gives `50 × ATTRIBUTES_MODIFIED + 1 JOIN + 50 DROP` on
      1.24 but `1 JOIN + 50 DROP` on 2.7.2 (no ATTRIBUTES_MODIFIED). Any
      test asserting hop counts across a merge differs by line; the live
      tests here deliberately assert shape, not counts.
- [ ] **T7e — empty-`searchTerms` provenance queries are unreliable on 2.x.**
      Observed on 2.7.2: events belonging to since-deleted process groups are
      *counted* in `totalCount` but never returned, so an unfiltered "what
      happened recently" query answered `total: '0'` at maxResults 100 and
      500, then `106` at 1000. Always pass a search term. niflow does not
      currently issue an unfiltered query; this is a landmine for anything
      that adds one.
- [x] **T7f — webgui's `hopCard()` renders `lineage`.** *(done 2026-08-20)*
      The JS twin now makes the same choice `format_hop` does: a hop carrying
      `lineage` shows the ⤳ note **instead of** a diff table (a relative's
      event diffed against this file is nonsense, so its diff is deliberately
      empty and the table would have read "no attribute changes" where the
      explanation belongs), plus the CLI's "continues as <uuid>" jump for each
      child. A synthetic hop (`CROSS`/`MOVED` — a port crossing or a transfer
      NiFi recorded no event for) drops the time/size stamp, because "0 B"
      there reads as an empty FlowFile, and draws dashed (`.hop.synth`) so it
      is visibly not a real event. Both tabs share the one renderer.
- [x] **T7g — a capped trace says so.** *(done 2026-08-20)* The unknown-uuid
      case already printed "wrong UUID, or the events have aged out"; what was
      missing was `truncated`. `niflow trace` now prints "Showing the newest N
      hops of a longer journey — the file's earlier hops are not below" and
      takes `--max-events N`, and the Trace tab says "hop #1 below is not
      where this FlowFile began". Hop #1 of a capped journey being read as the
      origin is exactly the wrong conclusion to let someone draw.
- [ ] **T7h — not testable locally.** No cluster, so: primary-node-only
      scheduling, load-balanced connections actually redistributing, and
      `disconnectedNodeAcknowledged` semantics are all untested. Nor is real
      load (the provenance findings above were measured on a container with
      one client), a rolled-over/rebuilding provenance repository, back
      pressure engaged hard enough to block a port crossing, FlowFile
      expiry firing mid-journey, or work's own NARs.

### Found while here — not trace/follow, for whoever owns those files

- [x] **`max-bin-age` was never a rename — and nothing caught it.** *(fixed
      2026-08-20)* Both lines key it `Max Bin Age` (2.7.2 and 1.24.0 agree;
      the map's MergeContent renames are `Header/Footer/Demarcator File`,
      `Maximum number of Bins` and `mergecontent-metadata-strategy`), so
      `max-bin-age` is a property of *neither* namespace — NiFi files any
      unrecognised key under dynamic properties, does nothing with it, and
      marks the processor invalid. No rename could have helped; the missing
      piece was a local check.
      `rules.near_miss_properties()` flags a key that is a property of neither
      line but normalises (case, `-`, `_`, space — deliberately **not** `.`,
      so attribute-style dynamic keys are never touched) onto exactly one real
      property of the same type, in both directions. `validate` reports it:
      "property 'max-bin-age' is not a property of this type — did you mean
      'Max Bin Age'?". Deliberately a report, not a silent rewrite: this repo
      does not guess values onto properties.
      While there: **validate never looked at controller services at all** —
      the same blind spot the cross-version work found — so required
      properties, allowable values and near-miss keys now run on services too
      (`_typed_entry` already covered both catalogs). Verified: a
      DBCPConnectionPool written with `database-connection-url` now names
      `Database Connection URL`.
- [x] **`push --update` reported removing a property and did not remove it.**
      *(fixed 2026-08-20, verified live on 1.24.0)* Not the emitter — NiFi
      **merges** the properties map on `PUT /processors/{id}` and
      `PUT /controller-services/{id}`: a key the request does not mention is
      left exactly as it was, and only a key sent as `null` is removed (a
      dynamic property) or reset to its default (a real one). The applier sent
      the model's properties, which by definition no longer contain the
      dropped key, so every removal was a silent no-op.
      `PlanApplier._property_removals()` now derives the nulls from the plan's
      own `properties[...]` fields — so what apply sends cannot drift from what
      the plan promised — and puts them back through `properties_for_target`,
      because the plan speaks the catalog namespace and a 1.x server may not.
      Controller services had the identical bug and the identical fix.
      Live repro on 1.24: push with `max-bin-age`, drop it, `push --update` —
      the property is gone from the server and
      `'max-bin-age' … is not a supported property` is gone with it. Unit
      cover in tests/test_apply_unit.py (processor, service, and "a property
      the flow still sets is never nulled").
