# Backup and rollback

Every mutating push takes a snapshot first. Nothing to configure.

```bash
niflow backup "Prod Flow"                 # snapshot on demand
niflow rollback "Prod Flow" --list        # what's available
niflow rollback "Prod Flow"               # restore the newest
niflow rollback "Prod Flow" --file .niflow-backups/Prod_Flow-20260819-140233.json
niflow rollback "Prod Flow" --start -y    # restore, start, no prompt
```

Backups are the group's `VersionedFlowSnapshot` JSON, written to
`.niflow-backups/` (override with `NIFLOW_BACKUP_DIR`), timestamped per group.
The directory is git-ignored — it is a copy of live flows.

**What restores exactly:** the flow structure — components, wiring, properties,
queue settings, nested groups.

**What does not:** sensitive parameter *values*. NiFi never exports them, so
they come from `.niflow-secrets.env` on restore, the same as on any push
(`--secrets` points elsewhere).

## When to reach for it

* an apply failed part-way — the error says what had already been done, and
  rollback puts the group back;
* a bad edit landed and you would rather rewind than re-derive;
* someone changed the canvas and you want the last known-good shape back.

For "what changed since then?", `niflow diff` and [`niflow plan`](plan-and-apply.md)
answer against the live group; a backup file is a plain snapshot, so
`niflow pull --format json` output and a backup are the same shape and diff
against each other with ordinary tools.

## Registry, if you have one

Where a group is under NiFi Registry version control, a full push rebuilds it
**in place**: the group id and registry linkage survive, so the push shows up
as local changes to review and commit rather than a new, unlinked group.

```bash
niflow commit "Prod Flow" -m "tuned batch size"
```

That is a different safety net from `.niflow-backups/` — the Registry keeps
versions of what you *meant*, backups keep what was actually live a moment ago.
Use both.
