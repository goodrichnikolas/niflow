"""The niflow CLI — pull, edit, push.

The day-to-day loop against a live NiFi (1.x or 2.x)::

    niflow list                                # see the canvas tree + ids
    niflow copy "My Flow"                      # detached working copy to play with
    niflow pull "My Flow (copy)" -o flows/my_flow.py
    # ... edit flows/my_flow.py ...
    niflow validate flows/my_flow.py [--live]  # rulebook check (+ NiFi's own, sandboxed)
    niflow plan flows/my_flow.py               # semantic "what will change"
    niflow test flows/my_flow.py               # inject FlowFiles, assert what comes out
    niflow trace <flowfile-uuid>               # replay one file's journey (attr diffs)
    niflow follow "My Flow (copy)"             # live-step a file, one run-once per hop
    niflow push flows/my_flow.py --update      # apply only the delta in place
    niflow rollback "My Flow (copy)"           # undo from the automatic backup
    niflow diff flows/my_flow.py               # raw JSON diff vs the live group
    niflow push flows/my_flow.py --env prod    # full replace, prod parameter overlay
    niflow commit "My Flow" -m "msg"           # save a versioned group to the Registry

Whole-instance workflows::

    niflow pull --all -o flows/                # mirror every top-level group
    niflow drift                               # ok/DRIFT per flow; exit 1 on any
    niflow watch                               # alert when a healthy component breaks
    niflow push --all flows/ --update          # reconcile the directory
    niflow diagram flows/my_flow.py -o doc.md  # Mermaid flowchart for PR review

Connection settings come from env vars (``NIFLOW_NIFI_HOST`` / ``_USER`` /
``_PASSWORD`` / ``_VERIFY_SSL``) or the local-Docker defaults; see
:mod:`niflow.config`. Sensitive parameter values are read from
``.niflow-secrets.env`` (or ``--secrets``) at push time.

Also available as ``python -m niflow``.
"""
from __future__ import annotations

import argparse
import difflib
import json
import sys
from pathlib import Path
from typing import Optional

from niflow.client import NiFiClient
from niflow.config import NiFiConfig
from niflow.core import Flow


def _client() -> NiFiClient:
    return NiFiClient(NiFiConfig.from_env())


def _load_flow_py(path: str, var: str = "flow") -> Flow:
    from niflow.convert import _load_python_flow

    return _load_python_flow(path, var)


# ----------------------------------------------------------------- commands


def cmd_version(args: argparse.Namespace) -> int:
    client = _client()
    print(f"NiFi {client.version()} at {client.base}")
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    client = _client()
    print(f"root ({client.root_id()})")
    for path, comp in client.walk_groups():
        depth = path.count("/") + 1
        print(f"{'  ' * depth}{comp['name']}  ({comp['id']})")
    return 0


def cmd_pull(args: argparse.Namespace) -> int:
    client = _client()
    if args.all:
        from niflow.sync import pull_all

        reports = pull_all(client, args.output or "flows", parent=args.parent)
        for r in reports:
            note = f"  ({len(r['warnings'])} pull warning(s))" if r["warnings"] else ""
            print(f"Pulled {r['name']!r} -> {r['file']}{note}")
            for warning in r["warnings"]:
                print(f"    ! {warning}", file=sys.stderr)
        print(f"{len(reports)} group(s) mirrored.")
        return 0
    if not args.group:
        print("error: give a group name, or use --all", file=sys.stderr)
        return 2
    flow = client.pull_flow(args.group)
    if args.format == "json":
        text = flow.to_json()
    else:
        text = flow.to_python(
            module_docstring=f"Pulled from NiFi group {args.group!r} — edit and `niflow push` to apply."
        )
    if args.output:
        Path(args.output).write_text(text)
        print(f"Pulled {flow.name!r} -> {args.output}")
    else:
        sys.stdout.write(text)
    if flow.pull_warnings:
        print(
            f"\nWARNING: pull of {flow.name!r} is LOSSY "
            f"({len(flow.pull_warnings)} issue(s)):",
            file=sys.stderr,
        )
        for warning in flow.pull_warnings:
            print(f"  ! {warning}", file=sys.stderr)
        print(
            "  Pushing this file back will drop the components above.",
            file=sys.stderr,
        )
    return 0


def cmd_validate(args: argparse.Namespace) -> int:
    """Statically validate a flow .py — no NiFi connection required.

    Catches the errors NiFi would reject on push (unhandled relationships,
    missing required properties, bad values) using the harvested rulebook, so
    you can fix them before pushing. Exit code 1 if any issues are found.

    The **compatibility baseline** is checked by default and with no flag:
    every flow is judged against the oldest NiFi line the estate runs
    (``NIFLOW_MIN_NIFI_VERSION`` in ``.niflow.env``, default 1.24), and a
    property that cannot land there is a named failure, not a note. It fails
    the command deliberately — on the server that failure is *silent* (NiFi
    files an unknown key away as an inert dynamic property and runs the real
    one at its default), and a warning in a wall of output is exactly how that
    class of bug reaches production.

    ``--target-version 2.7.2`` swaps the baseline for an ad-hoc check against
    some other line; ``--no-compat-check`` (or a baseline of ``none``) turns
    the cross-version check off for someone who only cares about 2.x.
    """
    flow = _load_flow_py(args.file, args.var)
    target_version = getattr(args, "target_version", None)
    baseline = not getattr(args, "no_compat_check", False)
    if target_version:
        from niflow.compat import describe_target, parse_major

        target_major = parse_major(target_version)
        if target_major is None:
            print(f"--target-version {target_version!r} is not a NiFi version "
                  f"(try 1.24 or 2.7.2)")
            return 2
        print(f"Target: {describe_target(target_major)}")
    elif baseline:
        from niflow.compat import describe_baseline

        print(f"Checking {describe_baseline()}")
    issues = flow.validate(target_version, baseline=baseline)
    if issues:
        print(f"{flow.name!r} has {len(issues)} static validation issue(s):")
        for issue in issues:
            print(f"  • {issue['component']}: {issue['message']}")
    else:
        print(f"{flow.name!r} passes static validation.")

    live_errors = []
    if args.live:
        live_errors = _client().validate_flow_live(flow)
        if live_errors:
            print(f"NiFi reports {len(live_errors)} invalid component(s) (live dry run):")
            for err in live_errors:
                where = f"{err['path']}/{err['name']}".lstrip("/")
                for message in err["errors"]:
                    print(f"  • {where}: {message}")
        else:
            print("Live dry run: NiFi reports every component valid.")
    return 1 if (issues or live_errors) else 0


def cmd_push(args: argparse.Namespace) -> int:
    client = _client()
    if args.all:
        from niflow.sync import push_all

        reports = push_all(
            client, args.file, update=args.update, start=args.start,
            secrets=args.secrets, env=args.env, var=args.var,
        )
        for r in reports:
            if r["changes"] is None:
                print(f"{r['name']!r}: rebuilt in full (id={r['id']}).")
            elif r["changes"]:
                print(f"{r['name']!r}: applied {len(r['changes'])} change(s).")
            else:
                print(f"{r['name']!r}: already in sync.")
        return 0
    flow = _load_flow_py(args.file, args.var)
    if args.update:
        changes = client.push_update(flow, start=args.start, secrets=args.secrets, env=args.env)
        if changes:
            from niflow.plan import format_plan

            print(format_plan(changes))
            print(f"Applied to {flow.name!r} (id={flow.nifi_id}).")
        else:
            print(f"{flow.name!r} already matches the model — nothing to do.")
        return 0
    new_id = client.push_flow(flow, start=args.start, secrets=args.secrets, env=args.env)
    state = "started" if args.start else "stopped"
    print(f"Pushed {flow.name!r} (id={new_id}, {state}).")
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    """Diagnose connectivity/auth against the configured NiFi."""
    from niflow.doctor import FAIL, format_checks, run_checks

    checks = run_checks(NiFiConfig.from_env(args.config))
    print(format_checks(checks))
    return 1 if any(c.status == FAIL for c in checks) else 0


def cmd_plan(args: argparse.Namespace) -> int:
    """Show what `push --update` would change, without touching NiFi."""
    from niflow.plan import format_plan

    client = _client()
    if args.all:
        from niflow.sync import plan_all

        drifted = 0
        for r in plan_all(client, args.file, var=args.var):
            if not r["exists"]:
                print(f"== {r['name']} ({r['file']}): group does not exist yet")
            elif not r["changes"]:
                print(f"== {r['name']}: in sync")
                continue
            else:
                print(f"== {r['name']} ({r['file']}):")
            print(format_plan(r["changes"]))
            drifted += 1
        return 1 if drifted else 0
    flow = _load_flow_py(args.file, args.var)
    pg_id, _live, changes = client.plan_flow(flow)
    if pg_id is None:
        print(f"Group {flow.name!r} does not exist yet — everything below is new.")
    print(format_plan(changes))
    return 1 if changes else 0


def cmd_diagram(args: argparse.Namespace) -> int:
    """Render flow module(s) as Mermaid markdown (GitHub draws it inline)."""
    from niflow.mermaid import to_markdown

    if args.all:
        from niflow.sync import load_flows

        docs = [to_markdown(f) for _, f in load_flows(args.file, var=args.var)]
        text = "\n".join(docs)
    else:
        text = to_markdown(_load_flow_py(args.file, args.var))
    if args.output:
        Path(args.output).write_text(text)
        print(f"Wrote {args.output}")
    else:
        sys.stdout.write(text)
    return 0


def cmd_drift(args: argparse.Namespace) -> int:
    """One line per flow module: in sync or drifted. Exit 1 on any drift."""
    from niflow.sync import plan_all

    client = _client()
    drifted = 0
    for r in plan_all(client, args.dir, var=args.var):
        if not r["exists"]:
            print(f"DRIFT  {r['name']}: group missing from NiFi")
            drifted += 1
        elif r["changes"]:
            counts = {"add": 0, "remove": 0, "update": 0}
            for c in r["changes"]:
                counts[c.op] += 1
            print(
                f"DRIFT  {r['name']}: +{counts['add']} ~{counts['update']} -{counts['remove']}"
                f"  (niflow plan {r['file']})"
            )
            drifted += 1
        else:
            print(f"ok     {r['name']}")
    return 1 if drifted else 0


def cmd_copy(args: argparse.Namespace) -> int:
    client = _client()
    new_id = client.copy_group(args.group, new_name=args.name, parent=args.parent)
    print(f"Copied {args.group!r} -> id={new_id} (detached from version control).")
    return 0


def _normalised_json(flow: Flow) -> str:
    """Flow JSON normalised for diffing: components sorted, cosmetics dropped.

    NiFi returns components in arbitrary order and canvas positions are
    meaningless to flow logic — without this every diff drowns in
    reorder/position noise.
    """
    data = json.loads(flow.to_json())

    def scrub(node):
        if isinstance(node, dict):
            for key in ("position", "bends", "width", "height", "zIndex", "labelIndex"):
                node.pop(key, None)
            for key, value in node.items():
                if isinstance(value, list) and value and isinstance(value[0], dict):
                    value.sort(key=lambda d: (d.get("name") or "", d.get("identifier") or ""))
                scrub(value)
        elif isinstance(node, list):
            for item in node:
                scrub(item)

    scrub(data)
    return json.dumps(data, indent=2, sort_keys=True)


def cmd_diff(args: argparse.Namespace) -> int:
    """Diff a local flow .py against the live group with the same name."""
    flow = _load_flow_py(args.file, args.var)
    client = _client()
    try:
        live = client.pull_flow(args.group or flow.name)
    except ValueError:
        print(f"No live group named {flow.name!r} — everything is new.")
        return 1

    # Compare via the deterministic JSON emission: UUID5 identifiers are
    # path-seeded, so structurally identical flows serialise identically.
    local_json = _normalised_json(flow).splitlines(keepends=True)
    live_json = _normalised_json(live).splitlines(keepends=True)
    delta = list(
        difflib.unified_diff(live_json, local_json, fromfile="live", tofile="local")
    )
    if not delta:
        print(f"{flow.name!r} is in sync with NiFi.")
        return 0
    sys.stdout.writelines(delta)
    return 1


def cmd_test(args: argparse.Namespace) -> int:
    """Run a flow module's `tests` cases against a live-NiFi sandbox."""
    import importlib.util

    from niflow.testing import FlowTester, as_cases, format_results

    p = Path(args.file).resolve()
    spec = importlib.util.spec_from_file_location(p.stem, p)
    if spec is None or spec.loader is None:
        raise SystemExit(f"Cannot import {args.file!r} as a Python module")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    flow = getattr(module, args.var, None)
    if not isinstance(flow, Flow):
        raise SystemExit(f"Expected a niflow.Flow at {args.var!r} in {args.file!r}")
    raw_cases = getattr(module, args.tests_var, None)
    if not raw_cases:
        raise SystemExit(
            f"No test cases: define `{args.tests_var} = [TestCase(...)]` in {args.file!r} "
            "(from niflow.testing import TestCase)"
        )
    cases = as_cases(list(raw_cases))

    client = _client()
    with FlowTester(
        client, flow, sandbox_name=args.sandbox, keep=args.keep, secrets=args.secrets
    ) as tester:
        results = tester.run(cases)
        if args.keep:
            print(f"Sandbox kept: {client.ui_url(tester.pg_id)}", file=sys.stderr)
    print(format_results(results))
    return 0 if all(r.passed for r in results) else 1


def cmd_backup(args: argparse.Namespace) -> int:
    path = _client().backup_group(args.group)
    print(f"Backed up {args.group!r} -> {path}")
    return 0


def cmd_rollback(args: argparse.Namespace) -> int:
    from niflow.backup import backup_dir, list_backups

    if args.list:
        backups = list_backups(args.group)
        if not backups:
            print(f"No backups for {args.group!r} in {backup_dir()}/")
            return 1
        for path in backups:
            print(path)
        return 0

    client = _client()
    target = args.file
    if target is None:
        from niflow.backup import latest_backup

        target = latest_backup(args.group)
        if target is None:
            print(f"error: no backups for {args.group!r} in {backup_dir()}/", file=sys.stderr)
            return 2
    if not args.yes:
        answer = input(f"Replace live group {args.group!r} with {target}? [y/N] ")
        if answer.strip().lower() not in ("y", "yes"):
            print("Aborted.")
            return 1
    pg_id = client.rollback(args.group, target, start=args.start, secrets=args.secrets)
    print(f"Rolled back {args.group!r} from {target} (id={pg_id}).")
    return 0


def cmd_commit(args: argparse.Namespace) -> int:
    info = _client().commit_version(args.group, args.message or "")
    print(
        f"Committed {args.group!r} as version {info.get('version', '?')} "
        f"(state: {info.get('state', '?')})."
    )
    return 0


def cmd_delete(args: argparse.Namespace) -> int:
    client = _client()
    pg_id = client.resolve_group(args.group)
    if not args.yes:
        entity = client._pg_entity(pg_id)["component"]
        answer = input(f"Delete group {entity['name']!r} ({pg_id})? [y/N] ")
        if answer.strip().lower() not in ("y", "yes"):
            print("Aborted.")
            return 1
    client.delete_group(pg_id)
    print(f"Deleted {args.group!r}.")
    return 0


def cmd_start(args: argparse.Namespace) -> int:
    client = _client()
    pg_id = client.resolve_group(args.group)
    client.enable_services(pg_id)
    client.start_group(pg_id)
    print(f"Started {args.group!r}.")
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    _client().stop_group(args.group)
    print(f"Stopped {args.group!r}.")
    return 0


def cmd_explain(args: argparse.Namespace) -> int:
    """Write plain-English walkthrough docs for a live group (needs an LLM)."""
    from niflow.llm import llm_config

    config = llm_config()
    # Say which backend is about to be billed/driven — for the Claude Code
    # provider there is no URL to show, hence describe() rather than .url.
    print(f"LLM: {config.describe() if config else 'not configured'}")
    depth = 0 if args.all else args.depth

    def confirm(plan: dict) -> bool:
        """Count first, spend second — a deep canvas is hundreds of docs."""
        level = "all levels" if depth <= 0 else f"depth {depth}"
        print(f"{plan['documents']} document(s) in scope at {level}: "
              f"{plan['llm_calls']} LLM call(s), "
              f"{plan['documents'] - plan['llm_calls']} already current "
              f"(or hand-written and left alone).")
        if plan["summarised_groups"]:
            print(f"{plan['summarised_groups']} deeper group(s) get one summary "
                  "line each inside their parent — use --depth N or --all to "
                  "give them documents of their own.")
        if not plan["llm_calls"] or args.yes or not sys.stdin.isatty():
            return True
        return input("Generate? [y/N] ").strip().lower() in ("y", "yes")

    results = _client().explain_group(
        args.group, docs_dir=args.docs_dir,
        depth=depth, force=args.force, confirm=confirm,
    )
    if not results:  # confirm() said no
        print("Aborted.")
        return 1
    for r in results:
        print(f"{r['status']:>9}  {r['group']}  ({r['path']})")
    written = sum(r["status"] == "generated" for r in results)
    print(f"{written} document(s) written, {len(results) - written} untouched.")
    return 0


def cmd_trace(args: argparse.Namespace) -> int:
    """Print one FlowFile's provenance journey, hop by hop."""
    from niflow.follow import annotate_hops, format_hop

    trace = _client().trace_flowfile(args.uuid, max_events=args.max_events)
    hops = trace["hops"]
    if not hops:
        print(f"No provenance events for {args.uuid} — wrong UUID, or the "
              "events have aged out of the provenance repository.")
        return 1
    # A capped journey is the newest N events, not the first N — saying so
    # matters, because the hop numbered 1 is then NOT where the file began and
    # reading it as the origin is the wrong conclusion.
    if trace.get("truncated"):
        print(f"Showing the newest {len(hops)} hops of a longer journey — "
              f"the file's earlier hops are not below "
              f"(raise --max-events to see further back).\n")
    # Same annotation the stepper applies, so trace and follow render
    # identically (added/changed/removed + content changes).
    annotate_hops(hops)
    for i, hop in enumerate(hops, 1):
        print(format_hop(i, hop, full=args.full))
    return 0


def cmd_watch(args: argparse.Namespace) -> int:
    """Background health watcher: alert when a healthy component starts failing."""
    from niflow.watch import watch_command

    return watch_command(
        _client(), args.group,
        interval=args.interval, baseline=args.baseline, once=args.once,
        list_only=args.list, as_json=args.json, include_warnings=args.warnings,
        probe=not args.no_probe, no_stop_alerts=args.no_stop_alerts,
        ack=args.ack, clear=args.clear,
    )


def cmd_follow(args: argparse.Namespace) -> int:
    """Quiesce a group and live-step one FlowFile through it via run-once."""
    from niflow.follow import follow_command

    return follow_command(
        _client(), args.group,
        uuid=args.uuid, queue=args.queue, source=args.source,
        start=args.start, list_only=args.list, mute=args.mute or [],
        resume=args.resume, auto=args.auto, max_hops=args.max_hops,
        restore=args.restore, full=args.full,
    )


def cmd_fuzz(args: argparse.Namespace) -> int:
    """Generate thousands of micro-flows and hunt niflow's own defects."""
    from niflow.fuzz import (
        KINDS,
        NIFLOW_BUG,
        SweepConfig,
        format_report,
        replay,
        sweep,
    )

    config = SweepConfig(
        tier=args.tier,
        count=args.count,
        seed=args.seed,
        kinds=tuple(args.kinds.split(",")) if args.kinds else KINDS,
        type_pattern=args.types,
        out_dir=Path(args.out),
        resume=args.resume,
        max_repros_per_signature=args.repros_per_bug,
        max_failures=args.max_failures,
        keep_sandboxes=args.keep_sandboxes,
    )
    client = _client() if config.tier >= 2 else None

    if args.replay:
        result = replay(args.replay, config, client)
        print(f"{result.case.case_id} [{result.case.kind}] -> {result.status}")
        print(json.dumps(result.case.spec, indent=2, ensure_ascii=False))
        for finding in result.findings:
            print(f"\n### {finding.check} [{finding.classification}] {finding.signature}")
            print(f"  {finding.message}")
            if finding.detail:
                print(finding.detail)
        return 1 if result.status == NIFLOW_BUG else 0

    def progress(index: int, total: int, _result) -> None:
        if index % 100 == 0 or index == total:
            print(f"  ... {index}/{total}", file=sys.stderr, flush=True)

    report = sweep(config, client, progress=None if args.quiet else progress)
    print(format_report(report))
    print(f"\nFull results: {config.out_dir}/results.jsonl")
    return 1 if report.counts[NIFLOW_BUG] else 0


def cmd_tidy(args: argparse.Namespace) -> int:
    layout = "vertical" if args.vertical else "horizontal"
    moved = _client().tidy_group(
        args.group, layout=layout, recurse=not args.no_recurse
    )
    print(f"Tidied {args.group!r} ({layout}): moved {moved} component(s).")
    return 0


# --------------------------------------------------------------------- main


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="niflow",
        description="Pull NiFi process groups into Python, edit, and push back (NiFi 1.x and 2.x).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("version", help="Show the NiFi server version")
    p.set_defaults(func=cmd_version)

    p = sub.add_parser("list", help="List the process-group tree with ids")
    p.set_defaults(func=cmd_list)

    p = sub.add_parser("pull", help="Pull a process group into a Python module")
    p.add_argument("group", nargs="?", help="Process group name, path (a/b), or id")
    p.add_argument("-o", "--output", help="Output file (default: stdout); a directory with --all (default: flows/)")
    p.add_argument("--format", choices=("py", "json"), default="py")
    p.add_argument("--all", action="store_true",
                   help="Mirror EVERY child group of --parent into the output directory")
    p.add_argument("--parent", default="root", help="Parent group to mirror with --all (default: root)")
    p.set_defaults(func=cmd_pull)

    p = sub.add_parser("push", help="Push a flow .py (or, with --all, a directory of them) to NiFi")
    p.add_argument("file", help="Python module exposing a top-level Flow; a directory with --all")
    p.add_argument("--all", action="store_true", help="Push every flow module in the directory")
    p.add_argument("--var", default="flow", help="Flow variable name (default: flow)")
    p.add_argument("--start", action="store_true", help="Enable services and start after push")
    p.add_argument("--secrets", help="Secrets file for sensitive parameters (default: .niflow-secrets.env)")
    p.add_argument("--env", help=(
        "Environment overlay: parameter values from .niflow-params.<ENV>.env "
        "override the model's (and .niflow-secrets.<ENV>.env becomes the "
        "default secrets file)"
    ))
    p.add_argument(
        "--update",
        action="store_true",
        help="Apply only the diff with targeted calls (queues/state survive) "
        "instead of rebuilding the group",
    )
    p.set_defaults(func=cmd_push)

    p = sub.add_parser(
        "doctor", help="Diagnose connection/auth to the configured NiFi"
    )
    p.add_argument("--config", help="Config file to test (default: $NIFLOW_CONFIG / ./.niflow.env / ~/.niflow.env)")
    p.set_defaults(func=cmd_doctor)

    p = sub.add_parser(
        "plan", help="Show what `push --update` would change (read-only)"
    )
    p.add_argument("file", help="Python module exposing a top-level Flow; a directory with --all")
    p.add_argument("--all", action="store_true", help="Plan every flow module in the directory")
    p.add_argument("--var", default="flow")
    p.set_defaults(func=cmd_plan)

    p = sub.add_parser(
        "diagram", help="Render a flow .py as a Mermaid flowchart (markdown)"
    )
    p.add_argument("file", help="Flow module; a directory with --all")
    p.add_argument("--all", action="store_true", help="Render every flow module in the directory")
    p.add_argument("-o", "--output", help="Output .md file (default: stdout)")
    p.add_argument("--var", default="flow")
    p.set_defaults(func=cmd_diagram)

    p = sub.add_parser(
        "drift",
        help="One line per flow module vs live NiFi; exit 1 on drift (cron/CI friendly)",
    )
    p.add_argument("dir", nargs="?", default="flows", help="Directory of flow modules (default: flows/)")
    p.add_argument("--var", default="flow")
    p.set_defaults(func=cmd_drift)

    p = sub.add_parser(
        "validate", help="Statically validate a flow .py (no NiFi connection needed)"
    )
    p.add_argument("file", help="Python module exposing a top-level Flow")
    p.add_argument("--live", action="store_true", help=(
        "Also dry-run against the live NiFi: push a throwaway sandbox, collect "
        "the server's own validation errors, delete it"
    ))
    p.add_argument("--target-version", metavar="VER", default=None, help=(
        "Check against this NiFi line instead of the configured baseline "
        "(e.g. --target-version 2.7.2). Offline — uses the generated "
        "cross-version map — so you can find out at home what breaks at work"
    ))
    p.add_argument("--no-compat-check", action="store_true", help=(
        "Skip the cross-version check entirely. By default every flow is "
        "checked against the compatibility baseline NIFLOW_MIN_NIFI_VERSION "
        "(default 1.24) and fails validate if a property cannot land there"
    ))
    p.add_argument("--var", default="flow")
    p.set_defaults(func=cmd_validate)

    p = sub.add_parser(
        "fuzz",
        help="Generate thousands of micro-flows and hunt niflow's own bugs "
        "(tier 1 needs no NiFi)",
    )
    p.add_argument("--tier", "--level", type=int, choices=(1, 2, 3), default=1,
                   dest="tier", help=(
                       "1: offline emit/parse/plan round trips (default); "
                       "2: + NiFi's own validation in a sandbox; "
                       "3: + live push/pull/plan convergence"))
    p.add_argument("--count", type=int, default=0,
                   help="Cases to run (0 = every generated case)")
    p.add_argument("--seed", type=int, default=0, help="Generator seed (default: 0)")
    p.add_argument("--kinds", help="Comma-separated case kinds "
                   "(solo,props,pair,service,shape)")
    p.add_argument("--types", help="Regex filter on processor type")
    p.add_argument("-o", "--out", default=".niflow-fuzz",
                   help="Output directory for results + repros (default: .niflow-fuzz)")
    p.add_argument("--resume", action="store_true",
                   help="Skip cases already recorded in the output directory")
    p.add_argument("--replay", metavar="CASE_ID",
                   help="Re-run one case by id and print everything it found")
    p.add_argument("--repros-per-bug", type=int, default=3,
                   help="Standalone repro files to write per root-cause "
                        "signature (default: 3)")
    p.add_argument("--max-failures", type=int, default=0,
                   help="Stop after this many failing cases (0 = never)")
    p.add_argument("--keep-sandboxes", action="store_true",
                   help="Leave the live sandbox groups behind for autopsy "
                        "(tiers 2/3 delete every 'niflow-fuzz *' group when done)")
    p.add_argument("--quiet", action="store_true", help="No progress output")
    p.set_defaults(func=cmd_fuzz)

    p = sub.add_parser("copy", help="Clone a group as a detached working copy")
    p.add_argument("group", help="Source group name, path, or id")
    p.add_argument("--name", help="Name for the copy (default: '<name> (copy)')")
    p.add_argument("--parent", help="Parent group for the copy (default: same as source)")
    p.set_defaults(func=cmd_copy)

    p = sub.add_parser("diff", help="Diff a local flow .py against the live group")
    p.add_argument("file", help="Python module exposing a top-level Flow")
    p.add_argument("--group", help="Live group to compare against (default: the flow's name)")
    p.add_argument("--var", default="flow")
    p.set_defaults(func=cmd_diff)

    p = sub.add_parser(
        "test",
        help="Push a sandbox copy, inject FlowFiles, assert what comes out",
    )
    p.add_argument("file", help="Flow module that also defines `tests = [TestCase(...)]`")
    p.add_argument("--var", default="flow")
    p.add_argument("--tests-var", default="tests", help="Variable holding the cases")
    p.add_argument("--sandbox", help="Sandbox group name (default: '<flow> (niflow-test)')")
    p.add_argument("--keep", action="store_true", help="Leave the sandbox group for debugging")
    p.add_argument("--secrets", help="Secrets file for sensitive parameters")
    p.set_defaults(func=cmd_test)

    p = sub.add_parser("backup", help="Snapshot a live group to .niflow-backups/")
    p.add_argument("group", help="Group name, path, or id")
    p.set_defaults(func=cmd_backup)

    p = sub.add_parser(
        "rollback",
        help="Rebuild a group from its newest backup (mutating pushes back up automatically)",
    )
    p.add_argument("group", help="Group name (as used when it was backed up)")
    p.add_argument("--file", help="Specific backup file (default: newest for the group)")
    p.add_argument("--list", action="store_true", help="List backups for the group and exit")
    p.add_argument("--start", action="store_true", help="Start the group after restoring")
    p.add_argument("--secrets", help="Secrets file for sensitive parameters")
    p.add_argument("-y", "--yes", action="store_true", help="Skip confirmation")
    p.set_defaults(func=cmd_rollback)

    p = sub.add_parser(
        "commit",
        help="Commit a versioned group's local changes to NiFi Registry",
    )
    p.add_argument("group", help="Group name, path, or id (must be under version control)")
    p.add_argument("-m", "--message", help="Commit message shown in the Registry")
    p.set_defaults(func=cmd_commit)

    p = sub.add_parser("delete", help="Stop, drain, and delete a process group")
    p.add_argument("group", help="Group name, path, or id")
    p.add_argument("-y", "--yes", action="store_true", help="Skip confirmation")
    p.set_defaults(func=cmd_delete)

    p = sub.add_parser("start", help="Enable services and start a process group")
    p.add_argument("group")
    p.set_defaults(func=cmd_start)

    p = sub.add_parser(
        "explain",
        help="Write a plain-English walkthrough of a live group to "
        "docs/explanations/ (needs an LLM — an installed Claude Code CLI "
        "is enough; otherwise GOOGLE_API_KEY or NIFLOW_LLM_URL + _MODEL)",
    )
    p.add_argument("group", nargs="?", default="root",
                   help="Group name, a/b path, id, or 'root' (default: root)")
    p.add_argument("--docs-dir", default="docs/explanations",
                   help="Where the .md documents live (default: docs/explanations)")
    p.add_argument("--force", action="store_true",
                   help="Regenerate even when the doc is up to date")
    p.add_argument("--depth", type=int, default=1, metavar="N",
                   help="How many levels get their own document (default: 1 — "
                        "just this group, with nested groups summarised in one "
                        "line each); 2 adds the immediate children, and so on")
    p.add_argument("--all", action="store_true",
                   help="Document the whole subtree: one file and one LLM call "
                        "per nested group, however deep (same as --depth 0)")
    p.add_argument("-y", "--yes", action="store_true",
                   help="Skip the 'N documents, N LLM calls' confirmation")
    p.set_defaults(func=cmd_explain)

    p = sub.add_parser(
        "tidy",
        help="Auto-arrange a live group's canvas along its connections",
    )
    p.add_argument("group", help="Group name, a/b path, id, or 'root'")
    p.add_argument(
        "--vertical", action="store_true",
        help="Flow top-to-bottom instead of left-to-right",
    )
    p.add_argument(
        "--no-recurse", action="store_true",
        help="Only this group's canvas, not nested groups",
    )
    p.set_defaults(func=cmd_tidy)

    p = sub.add_parser("stop", help="Stop a process group")
    p.add_argument("group")
    p.set_defaults(func=cmd_stop)

    p = sub.add_parser(
        "trace",
        help="Follow one FlowFile's provenance journey with per-hop attribute diffs",
    )
    p.add_argument("uuid", help="FlowFile UUID (from a queue listing, bulletin, or log)")
    p.add_argument("--full", action="store_true",
                   help="Show every attribute at every hop, not just what changed")
    p.add_argument("--max-events", type=int, default=1000, metavar="N",
                   help="Cap the journey at the newest N provenance events "
                        "(default 1000; a capped trace says so)")
    p.set_defaults(func=cmd_trace)

    p = sub.add_parser(
        "follow",
        help="Quiesce a group and step one FlowFile through it live, "
        "one run-once at a time",
    )
    p.add_argument("group", help="Group name, a/b path, id, or 'root'")
    p.add_argument("--uuid", help="Follow this queued FlowFile "
                   "(default: the front of the first non-empty queue)")
    p.add_argument("--queue", help="Connection id to take the FlowFile from")
    p.add_argument("--source", help="Run this source processor once first "
                   "to mint the FlowFile to follow")
    p.add_argument("--start", help="Start point to begin at: a number from "
                   "--list, a connection/processor id, or 'kind:id'")
    p.add_argument("--list", action="store_true",
                   help="List the plausible start points and exit "
                   "(read-only: the group is not quiesced)")
    p.add_argument("--mute", action="append", metavar="SPEC",
                   help="Do not follow a branch: a relationship name, "
                   "'rel:failure', 'dest:PutFile', 'queue:<id>' or a child "
                   "UUID. Repeatable. View-only — NiFi keeps running it")
    p.add_argument("--resume", action="store_true",
                   help="Re-attach to the last saved session for this group "
                   "instead of starting a new journey")
    p.add_argument("--auto", action="store_true",
                   help="Step without prompting until the file reaches a "
                   "terminal state (dropped/sent/port)")
    p.add_argument("--max-hops", type=int, default=50,
                   help="Safety cap on run-once steps (default: 50)")
    p.add_argument("--restore", action="store_true",
                   help="Afterwards restart the processors that were "
                   "running before the quiesce")
    p.add_argument("--full", action="store_true",
                   help="Show every attribute at every hop, not just what changed")
    p.set_defaults(func=cmd_follow)

    p = sub.add_parser(
        "watch",
        help="Watch for components that were healthy and started failing, "
        "and say whether the cause looks external (cron/CI friendly)",
    )
    p.add_argument("group", nargs="?", default="root",
                   help="Group name, a/b path, id, or 'root' (default: root)")
    p.add_argument("--interval", type=float, default=15.0,
                   help="Seconds between health polls (default: 15)")
    p.add_argument("--baseline", type=float, default=120.0,
                   help="Seconds a component must look healthy before a "
                        "failure counts as 'it WAS working' (default: 120)")
    p.add_argument("--once", action="store_true",
                   help="Poll once and exit — the cron shape; exit 1 if any "
                        "alert is active")
    p.add_argument("--list", action="store_true",
                   help="Print the recorded alerts and exit (no polling)")
    p.add_argument("--json", action="store_true",
                   help="One JSON object per alert event, for piping")
    p.add_argument("--warnings", action="store_true",
                   help="Treat WARNING bulletins as failures too, not just ERROR")
    p.add_argument("--no-probe", action="store_true",
                   help="Skip the provenance probe that recovers the HTTP "
                        "status / URL when an alert fires")
    p.add_argument("--no-stop-alerts", action="store_true",
                   help="Don't alert when a running processor becomes stopped")
    p.add_argument("--ack", metavar="ALERT_ID",
                   help="Acknowledge one alert so it stops shouting")
    p.add_argument("--clear", action="store_true",
                   help="Forget every resolved alert")
    p.set_defaults(func=cmd_watch)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except Exception as exc:  # surface clean one-line errors, not tracebacks
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
