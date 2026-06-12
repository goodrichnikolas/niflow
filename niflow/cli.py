"""The niflow CLI — pull, edit, push.

The day-to-day loop against a live NiFi (1.x or 2.x)::

    niflow list                                # see the canvas tree + ids
    niflow copy "My Flow"                      # detached working copy to play with
    niflow pull "My Flow (copy)" -o flows/my_flow.py
    # ... edit flows/my_flow.py ...
    niflow diff flows/my_flow.py               # what changed vs the live group?
    niflow push flows/my_flow.py               # replace the live group
    niflow push flows/my_flow.py --start       # ... and start it

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
    return 0


def cmd_push(args: argparse.Namespace) -> int:
    flow = _load_flow_py(args.file, args.var)
    client = _client()
    new_id = client.push_flow(flow, start=args.start, secrets=args.secrets)
    state = "started" if args.start else "stopped"
    print(f"Pushed {flow.name!r} (id={new_id}, {state}).")
    return 0


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
    p.add_argument("group", help="Process group name, path (a/b), or id")
    p.add_argument("-o", "--output", help="Output file (default: stdout)")
    p.add_argument("--format", choices=("py", "json"), default="py")
    p.set_defaults(func=cmd_pull)

    p = sub.add_parser("push", help="Push a flow .py to NiFi (delete-and-recreate)")
    p.add_argument("file", help="Python module exposing a top-level Flow")
    p.add_argument("--var", default="flow", help="Flow variable name (default: flow)")
    p.add_argument("--start", action="store_true", help="Enable services and start after push")
    p.add_argument("--secrets", help="Secrets file for sensitive parameters (default: .niflow-secrets.env)")
    p.set_defaults(func=cmd_push)

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

    p = sub.add_parser("delete", help="Stop, drain, and delete a process group")
    p.add_argument("group", help="Group name, path, or id")
    p.add_argument("-y", "--yes", action="store_true", help="Skip confirmation")
    p.set_defaults(func=cmd_delete)

    p = sub.add_parser("start", help="Enable services and start a process group")
    p.add_argument("group")
    p.set_defaults(func=cmd_start)

    p = sub.add_parser("stop", help="Stop a process group")
    p.add_argument("group")
    p.set_defaults(func=cmd_stop)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except Exception as exc:  # surface clean one-line errors, not tracebacks
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
