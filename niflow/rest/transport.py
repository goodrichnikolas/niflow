"""Session, auth, and canvas inventory — the base layer of NiFiClient."""
from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional, Tuple

from niflow.config import NiFiConfig
from niflow.rest.common import (
    _UUID_RE,
    NiFiApiError,
    logger,
)


# Two shapes NiFi answers with on a cluster whose membership is incomplete.
# Both are accurate and neither says what to do about it, and both are far
# more likely to be met at work (behind a load balancer, mid-rolling-restart)
# than on a dev box — so the hint is attached once, here, and every command
# gets it: push, plan, follow, test, both GUIs.
_DISCONNECTED_SELF = "node is disconnected from its configured cluster"
_DISCONNECTED_OTHER = "nodes are not connected"


def _cluster_hint(body: str) -> str:
    """An actionable sentence for NiFi's two disconnected-node refusals."""
    lowered = (body or "").lower()
    if _DISCONNECTED_SELF in lowered:
        return ("\n  → the NiFi you are talking to has fallen out of its own "
                "cluster, so it refuses changes that the rest of the cluster "
                "would never see. Point niflow at a connected node (or wait "
                "for this one to rejoin); `niflow doctor` names them. niflow "
                "deliberately does not set disconnectedNodeAcknowledged — "
                "that means 'apply this knowing a node will not get it', "
                "which is not a tool's decision to make.")
    if _DISCONNECTED_OTHER in lowered:
        return ("\n  → a cluster node is disconnected, and NiFi will not let a "
                "component be deleted while a node that still has it cannot "
                "hear about it. (Creates and updates are still allowed — it is "
                "deletion that cannot be un-done on the missing node.) "
                "Reconnect the node, or remove it from the cluster, then "
                "retry; `niflow doctor` shows the node states.")
    return ""


class TransportMixin:
    def __init__(self, config: Optional[NiFiConfig] = None, session: Any = None):
        self.config = config or NiFiConfig.from_env()
        self.base = self.config.host.rstrip("/")
        # ONE stored trust decision, pinned onto every request (see _request).
        # A CA bundle path beats the verify boolean (work servers have real
        # certs signed by an internal CA; export it and point
        # NIFLOW_NIFI_CA_BUNDLE at the PEM).
        self._verify: Any = self.config.ca_bundle or self.config.verify_ssl
        if session is None:
            import requests

            session = requests.Session()
            session.verify = self._verify
            if self.config.client_cert:
                session.cert = (
                    (self.config.client_cert, self.config.client_key)
                    if self.config.client_key
                    else self.config.client_cert
                )
            if not session.verify and self.config.suppress_ssl_warnings:
                import urllib3

                urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        elif hasattr(session, "verify"):
            # A caller-supplied session (tests, the GUI's per-thread pool)
            # brings its own trust material — adopt it instead of overriding,
            # so what we pin per request matches what the session was given.
            self._verify = session.verify
        self.session = session
        self._token: Optional[str] = None
        self._root_id: Optional[str] = None
        self._bundle_index: Optional[Dict[str, dict]] = None

    def login(self) -> None:
        """Fetch a bearer token (single-user and LDAP both use /access/token).

        Skipped under mTLS (the client certificate IS the identity — servers
        configured for cert auth don't even expose /access/token) and for
        anonymous/plain-HTTP servers (no password configured).
        """
        if self.config.client_cert:
            logger.info("Authenticating with client certificate %s", self.config.client_cert)
            return
        if not self.config.password:
            logger.info("No password configured; using anonymous access")
            return
        resp = self.session.request(
            "POST",
            f"{self.base}/access/token",
            data={"username": self.config.username, "password": self.config.password},
            timeout=30,
            verify=self._verify,
        )
        token = resp.text.strip()
        # NiFi can return an HTML error page (e.g. host-header rejection) with
        # a 2xx status; a real token is a single JWT with no whitespace or '<'.
        if resp.status_code >= 400 or not token or "<" in token or any(c.isspace() for c in token):
            raise NiFiApiError(
                resp.status_code,
                f"login failed for user {self.config.username!r} at {self.base}: {resp.text}",
            )
        self._token = token
        logger.info("Authenticated to %s as %r", self.base, self.config.username)

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        if self._token is None and self.config.password and not self.config.client_cert:
            self.login()
        headers = dict(kwargs.pop("headers", {}))
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        kwargs.setdefault("timeout", 60)
        # Pin the trust material on every call. requests merges *environment*
        # settings into each request, and with trust_env on (the default)
        # REQUESTS_CA_BUNDLE / CURL_CA_BUNDLE replace session.verify whenever
        # the call itself doesn't pass verify= — so a corporate image that
        # exports one silently beats NIFLOW_NIFI_CA_BUNDLE, and even flips
        # NIFLOW_NIFI_VERIFY_SSL=false back on. Passing verify= explicitly is
        # what makes the session-level setting stick. (`niflow doctor` reports
        # when such a variable is set.) Proxy env vars are left alone —
        # corporate proxies are usually how you reach the server at all.
        kwargs.setdefault("verify", self._verify)
        resp = self.session.request(method, f"{self.base}{path}", headers=headers, **kwargs)
        if resp.status_code == 401 and self._token is not None:
            # Token expired mid-session — refresh once and retry.
            self.login()
            headers["Authorization"] = f"Bearer {self._token}"
            resp = self.session.request(method, f"{self.base}{path}", headers=headers, **kwargs)
        if resp.status_code >= 400:
            raise NiFiApiError(
                resp.status_code,
                f"{method} {path}: {resp.text}{_cluster_hint(resp.text)}")
        return resp

    def probe(self, path: str, method: str = "GET", **kwargs: Any) -> Any:
        """Raw request with our trust material — no login, no error raising.

        `niflow doctor` needs to see the bare response (or the TLS exception)
        *before* authentication enters the picture, but it must still use the
        same CA bundle / client cert / verify setting the real calls use.
        """
        kwargs.setdefault("timeout", 30)
        kwargs.setdefault("verify", self._verify)
        return self.session.request(method, f"{self.base}{path}", **kwargs)

    def _get_json(self, path: str) -> dict:
        return self._request("GET", path).json()

    def version(self) -> str:
        """The server version string, e.g. ``"1.24.0"`` or ``"2.7.2"``."""
        about = self._get_json("/flow/about").get("about", {})
        return about.get("version", "unknown")

    def ui_url(self, group_id: str = "", component_id: str = "") -> str:
        """A deep link into the NiFi *UI* that selects a component on the canvas.

        Derives the UI base from the API base (``…/nifi-api`` -> ``…/nifi``) and
        appends ``?processGroupId=…&componentIds=…`` so opening it drops you
        right on the processor — no drilling through nested groups by hand.
        """
        ui = self.base
        if ui.endswith("/nifi-api"):
            ui = ui[: -len("/nifi-api")] + "/nifi"
        params = []
        if group_id:
            params.append(f"processGroupId={group_id}")
        if component_id:
            params.append(f"componentIds={component_id}")
        return ui + ("/?" + "&".join(params) if params else "")

    def root_id(self) -> str:
        if self._root_id is None:
            self._root_id = self._get_json("/flow/process-groups/root")[
                "processGroupFlow"
            ]["id"]
        return self._root_id

    def _child_groups(self, pg_id: str) -> List[dict]:
        flow = self._get_json(f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]
        return [g["component"] for g in flow.get("processGroups", [])]

    def _recursive_status(self, pg_id: str) -> Optional[dict]:
        """The recursive status snapshot for a group, or ``None`` if unavailable.

        ``GET /flow/process-groups/{id}/status?recursive=true`` returns the
        whole subtree — groups, connections, queue depths — in ONE call
        (works on 1.x and 2.x). The per-group walks use it as a fast path:
        a 5-level work tree costs one round trip instead of one per group.
        """
        try:
            entity = self._get_json(
                f"/flow/process-groups/{pg_id}/status?recursive=true"
            )
            return entity["processGroupStatus"]["aggregateSnapshot"]
        except Exception:  # any failure -> per-group fallback, never an error
            return None

    def walk_groups(self) -> Iterator[Tuple[str, dict]]:
        """Yield ``(path, component)`` for every process group, depth-first.

        Fast path: one recursive-status call. Components carry ``id`` and
        ``name`` (the status snapshot has no positions — use
        ``_child_groups`` when canvas geometry matters). Falls back to the
        one-call-per-group walk if the status endpoint is unavailable.
        """
        snapshot = self._recursive_status(self.root_id())
        if snapshot is not None:
            def visit_snap(snap: dict, prefix: str) -> Iterator[Tuple[str, dict]]:
                for wrapper in snap.get("processGroupStatusSnapshots", []):
                    child = wrapper.get("processGroupStatusSnapshot") or {}
                    path = f"{prefix}/{child['name']}" if prefix else child["name"]
                    yield path, {"id": child["id"], "name": child["name"]}
                    yield from visit_snap(child, path)

            yield from visit_snap(snapshot, "")
            return

        def visit(pg_id: str, prefix: str) -> Iterator[Tuple[str, dict]]:
            for comp in self._child_groups(pg_id):
                path = f"{prefix}/{comp['name']}" if prefix else comp["name"]
                yield path, comp
                yield from visit(comp["id"], path)

        yield from visit(self.root_id(), "")

    def resolve_group(self, group: str) -> str:
        """Resolve a process-group reference (UUID, name, or ``a/b`` path) to an id."""
        if group == "root":
            return self.root_id()
        if _UUID_RE.match(group):
            return group
        matches = [
            (path, comp)
            for path, comp in self.walk_groups()
            if comp["name"] == group or path == group
        ]
        if not matches:
            raise ValueError(f"No process group named {group!r} found")
        if len(matches) > 1:
            paths = ", ".join(repr(p) for p, _ in matches)
            raise ValueError(
                f"Process group name {group!r} is ambiguous ({paths}); "
                "use the full path or the id"
            )
        return matches[0][1]["id"]

    def _pg_entity(self, pg_id: str) -> dict:
        return self._get_json(f"/process-groups/{pg_id}")

    def _major_version(self) -> int:
        """Leading integer of the server version (``1`` for ``"1.24.0"``)."""
        try:
            return int(self.version().split(".", 1)[0])
        except (ValueError, AttributeError):
            return 0
