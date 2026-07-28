"""Whole-instance sync: a directory of flow modules mirroring live NiFi.

"Point at the top and the repo *is* the instance":

    niflow pull --all -o flows/            # every top-level group -> flows/<slug>.py
    niflow drift                           # anything changed on either side? (exit 1)
    niflow plan --all flows/               # full per-flow change plans
    niflow push --all flows/ --update      # reconcile every flow incrementally

Each ``.py`` is a normal niflow module (top-level ``flow`` variable), so the
single-flow commands keep working file by file. ``--parent`` scopes the sync
to one group's children instead of the root canvas — useful when only part of
a shared instance is yours.
"""
from __future__ import annotations

import importlib.util
import logging
import re
from pathlib import Path
from typing import Any, List, Optional, Tuple

from niflow.core import Flow

logger = logging.getLogger("niflow")


def _slug(name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").lower()
    return slug or "flow"


def pull_all(client: Any, out_dir: str, parent: str = "root") -> List[dict]:
    """Pull every child group of ``parent`` into ``out_dir`` as flow modules.

    Returns one report dict per group: ``name``, ``file``, ``warnings``.
    Filenames are slugged group names; collisions get a numeric suffix.
    """
    directory = Path(out_dir)
    directory.mkdir(parents=True, exist_ok=True)
    parent_id = client.resolve_group(parent)
    reports: List[dict] = []
    used: set = set()
    for child in client._child_groups(parent_id):
        flow = client.pull_flow(child["id"])
        slug = _slug(flow.name)
        candidate, n = slug, 2
        while candidate in used:
            candidate, n = f"{slug}_{n}", n + 1
        used.add(candidate)
        path = directory / f"{candidate}.py"
        path.write_text(
            flow.to_python(
                module_docstring=(
                    f"Pulled from NiFi group {flow.name!r} by `niflow pull --all` — "
                    "edit and `niflow push --all --update` to apply."
                )
            )
        )
        logger.info("Pulled %r -> %s", flow.name, path)
        reports.append({"name": flow.name, "file": str(path), "warnings": list(flow.pull_warnings)})
    return reports


def load_flows(directory: str, var: str = "flow") -> List[Tuple[Path, Flow]]:
    """Load every flow module in ``directory`` (sorted; ``_``-prefixed skipped)."""
    root = Path(directory)
    if not root.is_dir():
        raise ValueError(f"{directory!r} is not a directory")
    out: List[Tuple[Path, Flow]] = []
    for path in sorted(root.glob("*.py")):
        if path.name.startswith("_"):
            continue
        spec = importlib.util.spec_from_file_location(f"niflow_sync_{path.stem}", path)
        if spec is None or spec.loader is None:
            raise ValueError(f"cannot import {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        flow = getattr(module, var, None)
        if not isinstance(flow, Flow):
            raise ValueError(f"{path} does not define a niflow.Flow named {var!r}")
        out.append((path, flow))
    if not out:
        raise ValueError(f"no flow modules (*.py with a `{var}` Flow) in {directory!r}")
    return out


def plan_all(client: Any, directory: str, var: str = "flow") -> List[dict]:
    """Plan every flow module against the live instance.

    Returns per-flow dicts: ``file``, ``name``, ``exists``, ``changes``.
    """
    results = []
    for path, flow in load_flows(directory, var):
        pg_id, _live, changes = client.plan_flow(flow)
        results.append({
            "file": str(path), "name": flow.name,
            "exists": pg_id is not None, "changes": changes,
        })
    return results


def push_all(
    client: Any,
    directory: str,
    *,
    update: bool = True,
    start: bool = False,
    secrets: Any = None,
    env: Optional[str] = None,
    var: str = "flow",
) -> List[dict]:
    """Push every flow module; incremental by default. Returns per-flow reports."""
    results = []
    for path, flow in load_flows(directory, var):
        if update:
            changes = client.push_update(flow, start=start, secrets=secrets, env=env)
            results.append({
                "file": str(path), "name": flow.name,
                "changes": changes, "id": flow.nifi_id,
            })
        else:
            new_id = client.push_flow(flow, start=start, secrets=secrets, env=env)
            results.append({"file": str(path), "name": flow.name, "changes": None, "id": new_id})
    return results
