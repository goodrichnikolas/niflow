"""Pre-apply safety nets: local snapshots of live groups, and rollback.

Every mutating push (``push --update``, or a full push that replaces an
existing group) first downloads the live group's ``VersionedFlowSnapshot``
into ``.niflow-backups/`` (override with ``NIFLOW_BACKUP_DIR``). If an apply
goes sideways — half-applied plan, bad edit, wrong file — ``niflow rollback``
rebuilds the group from the newest backup.

Backups are plain snapshot JSON: they restore the flow *structure* exactly,
but NiFi never exports sensitive values, so secret parameters still come
from ``.niflow-secrets.env`` on restore.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger("niflow")

_STAMP = "%Y%m%d-%H%M%S"


def backup_dir() -> Path:
    return Path(os.environ.get("NIFLOW_BACKUP_DIR", ".niflow-backups"))


def _slug(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_") or "group"


def save_backup(name: str, snapshot: dict) -> Path:
    """Write ``snapshot`` as ``<name>-<utc-stamp>.json``; returns the path."""
    directory = backup_dir()
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime(_STAMP)
    path = directory / f"{_slug(name)}-{stamp}.json"
    # Same-second pushes must not overwrite each other.
    counter = 1
    while path.exists():
        path = directory / f"{_slug(name)}-{stamp}-{counter}.json"
        counter += 1
    path.write_text(json.dumps(snapshot, indent=2))
    logger.info("Backed up %r to %s", name, path)
    return path


def list_backups(name: Optional[str] = None) -> List[Path]:
    """All backups (for ``name`` if given), newest first."""
    directory = backup_dir()
    if not directory.is_dir():
        return []
    pattern = f"{_slug(name)}-*.json" if name else "*.json"
    return sorted(directory.glob(pattern), key=lambda p: p.name, reverse=True)


def latest_backup(name: str) -> Optional[Path]:
    backups = list_backups(name)
    return backups[0] if backups else None
