"""Lifecycle, registry version control, and backup/rollback operations."""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import List, Optional, Union

from niflow.rest.common import (
    _POLL_INTERVAL_S,
    _POLL_TIMEOUT_S,
    NiFiApiError,
    _strip_version_control,
    logger,
)


# Canvas component kinds tidy_group re-positions: flow-listing key ->
# (REST endpoint, canvas box size in px — how the NiFi UI draws them).
# Listed in the lane tie-break order place() should prefer.
_CANVAS_KINDS = (
    ("inputPorts", ("input-ports", (240.0, 48.0))),
    ("processors", ("processors", (352.0, 128.0))),
    ("funnels", ("funnels", (48.0, 48.0))),
    ("processGroups", ("process-groups", (384.0, 176.0))),
    ("remoteProcessGroups", ("remote-process-groups", (384.0, 176.0))),
    ("outputPorts", ("output-ports", (240.0, 48.0))),
)


class OpsMixin:
    def tidy_group(
        self, group: str, layout: str = "horizontal", recurse: bool = True
    ) -> int:
        """Auto-arrange a live group's canvas along its connection graph.

        One-click de-spaghettifier: every component is re-ranked by longest
        path from the sources and rewritten over REST — ``"horizontal"``
        marches left-to-right, ``"vertical"`` top-to-bottom — and connection
        bend points are cleared so the lines re-route straight. Hand-placed
        positions are deliberately overridden; that is the point. Returns the
        number of components moved.
        """
        from niflow.layout import place

        moved = 0
        todo = [self.resolve_group(group)]
        while todo:
            gid = todo.pop()
            flow = self._get_json(f"/flow/process-groups/{gid}")[
                "processGroupFlow"
            ]["flow"]

            entities = {}  # component id -> (REST endpoint, entity)
            nodes = []
            sizes = {}
            for key, (endpoint, size) in _CANVAS_KINDS:
                for ent in flow.get(key, []):
                    entities[ent["id"]] = (endpoint, ent)
                    nodes.append(ent["id"])
                    sizes[ent["id"]] = size
            loose = []
            for ent in flow.get("labels", []):
                entities[ent["id"]] = ("labels", ent)
                loose.append(ent["id"])
                comp = ent.get("component") or {}
                sizes[ent["id"]] = (comp.get("width") or 352.0,
                                    comp.get("height") or 128.0)

            def end_node(end: dict) -> str:
                # A nested (or remote) group's port stands in for the group
                # itself: its owning groupId is a node here, its port id isn't.
                owner = end.get("groupId")
                return end.get("id", "") if owner in (gid, None) else owner

            edges = []
            for conn in flow.get("connections", []):
                comp = conn.get("component") or {}
                edges.append(
                    (end_node(comp.get("source") or {}),
                     end_node(comp.get("destination") or {}))
                )

            for nid, (x, y) in place(nodes, edges, layout, loose, sizes).items():
                endpoint, ent = entities[nid]
                current = ent.get("position") or {}
                if current.get("x") == x and current.get("y") == y:
                    continue
                self._request(
                    "PUT",
                    f"/{endpoint}/{ent['id']}",
                    json={
                        "revision": ent["revision"],
                        "component": {"id": ent["id"],
                                      "position": {"x": x, "y": y}},
                        "disconnectedNodeAcknowledged": False,
                    },
                )
                moved += 1

            for conn in flow.get("connections", []):
                if (conn.get("component") or {}).get("bends"):
                    self._request(
                        "PUT",
                        f"/connections/{conn['id']}",
                        json={
                            "revision": conn["revision"],
                            "component": {"id": conn["id"], "bends": []},
                        },
                    )

            if recurse:
                todo.extend(ent["id"] for ent in flow.get("processGroups", []))
        return moved

    def explain_status(self, group: str = "root",
                       docs_dir: str = "docs/explanations") -> dict:
        """Is the group's plain-English doc missing, current, or outdated?

        See :mod:`niflow.explain`; no LLM needed for the check itself.
        """
        from niflow.explain import explanation_status

        return explanation_status(self, group, docs_dir=docs_dir)

    def explain_group(self, group: str = "root",
                      docs_dir: str = "docs/explanations",
                      recurse: bool = True, force: bool = False) -> List[dict]:
        """Write plain-English walkthrough docs for a live group's subtree.

        See :mod:`niflow.explain`; needs an LLM (``NIFLOW_LLM_URL``).
        """
        from niflow.explain import explain_group

        return explain_group(self, group, docs_dir=docs_dir,
                             recurse=recurse, force=force)

    def backup_group(self, group: str, name: Optional[str] = None) -> Path:
        """Snapshot a live group to ``.niflow-backups/`` (see :mod:`niflow.backup`).

        Called automatically before every mutating push; also available as
        ``niflow backup`` for a manual save point.
        """
        return self._backup(self.resolve_group(group), name)

    def _backup(self, pg_id: str, name: Optional[str] = None) -> Path:
        from niflow.backup import save_backup

        if name is None:
            name = self._pg_entity(pg_id)["component"]["name"]
        return save_backup(name, self.download_snapshot(pg_id))

    def rollback(
        self,
        group: str,
        backup: Union[None, str, Path] = None,
        *,
        start: bool = False,
        secrets: Union[None, dict, str, Path] = None,
    ) -> str:
        """Rebuild ``group`` from a backup file (newest for the name if omitted).

        The current live state is itself backed up first (by the push path),
        so a rollback can be rolled back. Returns the group id.
        """
        from niflow.backup import backup_dir, latest_backup
        from niflow.formats import from_json

        if backup is None:
            backup = latest_backup(group)
            if backup is None:
                raise ValueError(f"no backups found for {group!r} in {backup_dir()}")
        snapshot = json.loads(Path(backup).read_text())
        flow = from_json(snapshot)
        if not re.fullmatch(r"[0-9a-f-]{36}", group):
            flow.name = group.split("/")[-1]  # a name or a/b path: restore under it

        # Land it back where the live group sits today (if it still exists).
        try:
            pg_id = self.resolve_group(flow.name)
            parent_id = self._pg_entity(pg_id)["component"].get("parentGroupId")
            if parent_id and parent_id != self.root_id():
                flow.parent_pg = self._pg_entity(parent_id)["component"]["name"]
        except ValueError:
            pass  # group is gone entirely — restore under its recorded parent
        logger.info("Rolling back %r from %s", flow.name, backup)
        return self.push_flow(flow, start=start, secrets=secrets)

    def create_registry_client(
        self, name: str, url: str, description: str = ""
    ) -> str:
        """Register a NiFi Registry with this NiFi; returns the registry-client id.

        The request body differs across lines: NiFi 1.x takes a flat ``uri``;
        NiFi 2.x turned registry clients into extensions, so it wants a ``type``
        plus a ``url`` *property*. If a client for ``url`` already exists we
        reuse it rather than erroring on a duplicate.
        """
        for existing in self._get_json("/controller/registry-clients").get(
            "registries", []
        ):
            comp = existing.get("component", {})
            if comp.get("uri") == url or (comp.get("properties") or {}).get("url") == url:
                return comp["id"]

        if self._major_version() >= 2:
            component = {
                "name": name,
                "type": "org.apache.nifi.registry.flow.NifiRegistryFlowRegistryClient",
                "properties": {"url": url},
                "description": description,
            }
        else:
            component = {"name": name, "uri": url, "description": description}
        resp = self._request(
            "POST",
            "/controller/registry-clients",
            json={"revision": {"version": 0, "clientId": "niflow"}, "component": component},
        )
        return resp.json()["component"]["id"]

    def list_registry_buckets(self, registry_id: str) -> List[dict]:
        """Buckets visible through ``registry_id``, as ``{id, name}`` dicts.

        NiFi proxies this to the registry, so it works without talking to the
        registry's own API. (Creating a bucket is *not* proxied — do that
        against the registry directly.)
        """
        entity = self._get_json(f"/flow/registries/{registry_id}/buckets")
        out = []
        for b in entity.get("buckets", []):
            bucket = b.get("bucket") or b.get("component") or b
            out.append({"id": bucket.get("id") or b.get("id"), "name": bucket.get("name", "")})
        return out

    def start_version_control(
        self,
        pg_id: str,
        registry_id: str,
        bucket_id: str,
        flow_name: str,
        comment: str = "",
    ) -> dict:
        """Place ``pg_id`` under version control in the given bucket.

        Returns the resulting ``versionControlInformation``. Use this to put a
        group under control from code (the integration tests need a versioned
        group to push against).
        """
        revision = self._pg_entity(pg_id)["revision"]
        body = {
            "processGroupRevision": {
                "version": revision["version"],
                "clientId": "niflow",
            },
            "versionedFlow": {
                "registryId": registry_id,
                "bucketId": bucket_id,
                "flowName": flow_name,
                "comments": comment,
                "description": "",
                # Required on both lines ("Action is required" 400 without it).
                "action": "COMMIT",
            },
        }
        resp = self._request("POST", f"/versions/process-groups/{pg_id}", json=body)
        return resp.json().get("versionControlInformation", {})

    def commit_version(self, group: str, message: str = "") -> dict:
        """Save a versioned group's local changes as a new registry version.

        The push side of the review loop: an in-place push leaves the group
        ``LOCALLY_MODIFIED``; this commits those changes (same endpoint as
        starting version control, but re-using the group's existing registry
        coordinates). Returns the updated ``versionControlInformation``.
        """
        pg_id = self.resolve_group(group)
        entity = self._pg_entity(pg_id)
        vci = entity.get("component", {}).get("versionControlInformation")
        if not vci:
            raise ValueError(
                f"{group!r} is not under version control — nothing to commit"
            )
        body = {
            "processGroupRevision": {
                "version": entity["revision"]["version"],
                "clientId": "niflow",
            },
            "versionedFlow": {
                "registryId": vci["registryId"],
                "bucketId": vci["bucketId"],
                "flowId": vci["flowId"],
                "flowName": vci.get("flowName", ""),
                "comments": message,
                "description": "",
                "action": "COMMIT",
            },
        }
        resp = self._request("POST", f"/versions/process-groups/{pg_id}", json=body)
        info = resp.json().get("versionControlInformation", {})
        logger.info(
            "Committed %r as version %s", group, info.get("version", "?")
        )
        return info

    def version_control_state(self, pg_id: str) -> Optional[str]:
        """The registry sync state of ``pg_id`` (``UP_TO_DATE``,
        ``LOCALLY_MODIFIED``, ``STALE``, …), or ``None`` if not versioned."""
        info = self._get_json(f"/versions/process-groups/{pg_id}").get(
            "versionControlInformation"
        )
        return info.get("state") if info else None

    def copy_group(
        self, group: str, new_name: Optional[str] = None, parent: Optional[str] = None
    ) -> str:
        """Clone a process group as a detached working copy; returns the new id.

        The clone is stripped of registry version-control coordinates, so it's
        safe to edit and replace without touching the original — this is the
        manual "copy, disconnect version control" dance in one step.
        """
        src_id = self.resolve_group(group)
        snapshot = self.download_snapshot(src_id)
        _strip_version_control(snapshot.get("flowContents", {}))

        src_entity = self._pg_entity(src_id)["component"]
        name = new_name or f"{src_entity['name']} (copy)"
        parent_id = (
            self.resolve_group(parent) if parent else src_entity.get("parentGroupId", self.root_id())
        )
        pos = src_entity.get("position") or {"x": 0.0, "y": 0.0}
        new_pos = {"x": float(pos.get("x", 0.0)) + 480.0, "y": float(pos.get("y", 0.0))}
        new_id = self._create_from_snapshot(parent_id, name, snapshot, new_pos)
        logger.info("Copied %r -> %r (%s)", src_entity["name"], name, new_id)
        return new_id

    def _set_group_state(self, pg_id: str, state: str) -> None:
        self._request(
            "PUT", f"/flow/process-groups/{pg_id}", json={"id": pg_id, "state": state}
        )

    def start_group(self, group: str) -> None:
        self._set_group_state(self.resolve_group(group), "RUNNING")

    def stop_group(self, group: str) -> None:
        self._set_group_state(self.resolve_group(group), "STOPPED")

    def quiesce_group(self, group: str = "root") -> str:
        """Fully quiesce ``group``: stop processors, disable services, drain queues.

        Leaves the group in the exact state NiFi requires before it can be
        deleted (nothing running, no enabled services, no queued FlowFiles) —
        without deleting it. Returns NiFi's drop summary, e.g. ``"12 / 4.2 KB"``.
        """
        pg_id = self.resolve_group(group)
        self._set_group_state(pg_id, "STOPPED")
        self._disable_services(pg_id)
        dropped = self._empty_queues(pg_id)
        logger.info("Quiesced group %r (%s dropped)", group, dropped)
        return dropped

    def enable_services(self, group: str) -> None:
        pg_id = self.resolve_group(group)
        self._request(
            "PUT",
            f"/flow/process-groups/{pg_id}/controller-services",
            json={"id": pg_id, "state": "ENABLED"},
        )

    def _disable_services(self, pg_id: str) -> None:
        self._request(
            "PUT",
            f"/flow/process-groups/{pg_id}/controller-services",
            json={"id": pg_id, "state": "DISABLED"},
        )

    def purge_queues(self, group: str = "root") -> str:
        """Drop the contents of every queue under ``group`` (recursive).

        Returns NiFi's summary of what was dropped, e.g. ``"12 / 4.2 KB"``.
        """
        dropped = self._empty_queues(self.resolve_group(group))
        logger.info("Purged queues under %r: %s dropped", group, dropped)
        return dropped

    def _has_connections(self, pg_id: str) -> bool:
        """Whether any connection exists under ``pg_id`` (recursive)."""
        flow = self._get_json(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
        if flow.get("connections"):
            return True
        return any(
            self._has_connections(child["component"]["id"])
            for child in flow.get("processGroups", [])
        )

    def _empty_queues(self, pg_id: str) -> str:
        # NiFi 1.x returns 500 from empty-all-connections-requests when the group
        # has no connections at all; skip the request rather than trip on it.
        if not self._has_connections(pg_id):
            return ""
        req = self._request(
            "POST", f"/process-groups/{pg_id}/empty-all-connections-requests"
        ).json()["dropRequest"]
        req_id = req["id"]
        try:
            deadline = time.monotonic() + _POLL_TIMEOUT_S
            while not req.get("finished"):
                if time.monotonic() > deadline:
                    raise NiFiApiError(408, f"emptying queues of {pg_id} timed out")
                time.sleep(_POLL_INTERVAL_S)
                req = self._get_json(
                    f"/process-groups/{pg_id}/empty-all-connections-requests/{req_id}"
                )["dropRequest"]
        finally:
            self._request(
                "DELETE", f"/process-groups/{pg_id}/empty-all-connections-requests/{req_id}"
            )
        return req.get("dropped", "")

    def delete_group(self, group: str) -> None:
        """Stop, disable, drain, and delete a process group."""
        self._teardown(self.resolve_group(group))

    def _teardown(self, pg_id: str) -> None:
        self._set_group_state(pg_id, "STOPPED")
        self._disable_services(pg_id)
        self._empty_queues(pg_id)
        version = self._pg_entity(pg_id)["revision"]["version"]
        self._request(
            "DELETE",
            f"/process-groups/{pg_id}",
            params={"version": version, "clientId": "niflow", "disconnectedNodeAcknowledged": "false"},
        )
        logger.info("Deleted group %s", pg_id)
