"""Browser-based NiFlow helper — the PyQt GUI's features over plain HTTP.

Zero extra dependencies: a stdlib :mod:`http.server` serving a single
embedded page plus a small JSON API bridging to :class:`NiFiClient`. Start
with ``niflow-web`` (or ``make webgui``); it binds to 127.0.0.1 and opens
your default browser (the *Windows* browser under WSL).

Why both GUIs: this one needs no PyQt/display server and works anywhere a
browser can reach ``localhost``; the desktop helper (``niflow-gui``)
remains for those who prefer it. Feature set here:

* processor list with filter, state, run-once / start / stop per row,
  a top-level-flow dropdown and starred rows pinned to the top (both
  remembered per browser via localStorage)
* queues with live counts -> click through to FlowFiles -> attributes+content,
  purge one queue or every queue in the selected flow
* Trace tab: one FlowFile's provenance journey hop by hop — attribute
  diffs, relationship taken, payload per processor (``niflow trace``)
* Follow tab: the live stepper (``niflow follow``) as a debugger — pick a
  start point, hit Step, watch changed/added/removed attributes flash at
  each hop, and mute the fork branches you don't care about
* group-wide start / stop / stop+drain / purge
* one-click "Tidy canvas": auto-arrange the selected flow (or everything)
  along its connections, left-to-right or top-to-bottom (``niflow tidy``)
* Explain tab: plain-English walkthrough per group (``niflow explain``) —
  generated once via the configured LLM, saved under ``docs/explanations/``,
  flagged outdated when the flow's logic changes; high-level by default
  (nested groups get one summary line), with a depth select and a
  documents/LLM-calls count shown before you spend them
* Alerts tab: a background health watcher (``niflow.watch``) that
  baselines every component and shouts when one goes healthy -> failing —
  "CallOrdersApi was healthy for 42m, broke at 14:02, api-frontiers refused
  the connection". The badge and the red banner are updated from every tab,
  because nobody is sitting on the Alerts tab when it happens
* bulletins and validation-error panels; every processor/connection named
  anywhere on the page is a link that opens NiFi on that component
* auto-refresh (3s), on by default, paused while you're mid-interaction
* flow files under ``flows/``: semantic plan preview and incremental push
* ``--reload`` (what ``make webgui`` uses): the server restarts itself when
  any niflow source file changes and open pages reload themselves — no
  manual restart while hacking on the GUI

All NiFi calls are serialised through one lock — ``requests.Session`` is
not thread-safe and the HTTP server is threading.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from niflow.client import NiFiClient
from niflow.config import NiFiConfig
from niflow.utils import get_logger, open_url

logger = get_logger()

# New value on every (re)start. The page compares it against what it loaded
# with and reloads itself when it changes — that's what makes --reload "live".
BOOT = uuid.uuid4().hex
RELOAD = False  # set by serve(reload=True); tells the page to watch BOOT


# ------------------------------------------------------------------ follow

# The Follow tab drives ONE stepping session at a time, kept between requests
# (this GUI is single-user by construction, and the session is also written to
# .niflow-follow/ so a page refresh or a server restart can re-attach).
_FOLLOW: dict = {"follower": None}


def _branch_rows(follower) -> List[dict]:
    """Branch records for the page: hop counts, not the hops themselves."""
    rows = []
    for branch in follower.branches():
        row = {k: v for k, v in branch.items()
               if k not in ("hops", "baseline", "baseline_size")}
        row["hop_count"] = len(branch.get("hops") or [])
        rows.append(row)
    return rows


def _follow_payload(follower, **extra: Any) -> dict:
    """The whole tab state: branches, the current branch's hops, mutes.

    The watch table is computed here rather than in the page: it is the same
    :func:`niflow.follow.watch_rows` the CLI prints, so both views agree about
    which hop changed which attribute.
    """
    from niflow.follow import watch_rows

    session = follower.session
    columns, rows = watch_rows(session.history(), session.watches)
    payload = {
        "active": True,
        "group": session.group,
        "session_id": session.id,
        "current": session.current,
        "mutes": session.mutes.describe(),
        "mute_rules": [f"{kind}:{value}"
                       for kind, values in session.mutes.to_dict().items()
                       for value in values],
        "branches": _branch_rows(follower),
        "hops": session.history(),
        "restorable": len(session.prior_running),
        "watches": list(session.watches),
        "watch_columns": columns,
        "watch_rows": rows,
        "fixture": session.fixture,
        "runs": len(session.runs),
        "injector": (session.injector or {}).get("label"),
    }
    payload.update(extra)
    return payload


def _follow_route(client: NiFiClient, method: str, path: str,
                  q, body: dict) -> Tuple[int, Any]:
    """``/api/follow/*`` — the stepper's routes (see :mod:`niflow.follow`).

    Muting routes never touch NiFi: they change what is followed and drawn,
    nothing else.
    """
    from niflow.follow import (
        FollowSession,
        FlowFollower,
        compare_runs,
        entry_points,
        format_run_comparison,
    )

    follower = _FOLLOW["follower"]
    if method == "GET" and path == "/api/follow/entrypoints":
        group = q("group") or "root"
        return 200, {"group": group, "entries": entry_points(client, group)}
    if method == "GET" and path == "/api/follow/session":
        if follower is not None:
            return 200, _follow_payload(follower)
        saved = FollowSession.latest(q("group") or None)
        return 200, {"active": False,
                     "resumable": {"id": saved.id, "group": saved.group,
                                   "current": saved.current,
                                   "hops": len(saved.history())}
                     if saved else None}
    if method == "POST" and path == "/api/follow/start":
        group = body.get("group") or "root"
        if body.get("resume"):
            session = FollowSession.latest(group)
            if session is None:
                return 404, {"error": f"no saved session for {group!r}"}
            follower = FlowFollower(client, group, session=session)
            stopped = follower.quiesce(remember=False)
        else:
            session = FollowSession.open(group, client.resolve_group(group))
            follower = FlowFollower(client, group, session=session)
            stopped = follower.quiesce()
            for spec in body.get("mutes") or []:
                follower.mute(spec)
            for spec in body.get("watches") or []:
                follower.watch(spec)
            fixture = body.get("inject") or None
            if fixture:
                follower.inject(fixture.get("target") or "",
                                content=fixture.get("content") or "",
                                attributes=fixture.get("attributes") or {})
            else:
                follower.start_from(body.get("entry") or {})
        _FOLLOW["follower"] = follower
        return 200, _follow_payload(follower, stopped=stopped)
    if follower is None:
        return 409, {"error": "no stepping session — pick a start point first"}
    if method == "POST" and path == "/api/follow/step":
        outcome = follower.step()
    elif method == "POST" and path == "/api/follow/repoll":
        outcome = follower.repoll()
    elif method == "POST" and path == "/api/follow/mute":
        outcome = dict(follower.mute(body.get("spec") or ""), status="muted")
    elif method == "POST" and path == "/api/follow/unmute":
        outcome = dict(follower.unmute(body.get("spec") or ""), status="unmuted")
    elif method == "POST" and path == "/api/follow/replay":
        picked = follower.replay()
        outcome = {"status": "replayed", "run": picked["run"],
                   "uuid": picked["uuid"],
                   "message": f"run {picked['run']} injected at "
                              f"{picked['injected']}"}
    elif method == "POST" and path == "/api/follow/watch":
        spec = (body.get("spec") or "").strip()
        if not spec:
            follower.session.watches = []
            follower._save()
        elif body.get("remove"):
            follower.unwatch(spec)
        else:
            follower.watch(spec)
        outcome = {"status": "watching"}
    elif method == "GET" and path == "/api/follow/compare":
        runs = follower.session.runs
        if not runs:
            return 200, {"text": "Only one run so far — replay the fixture, "
                                 "then compare."}
        which = int(q("run") or len(runs))
        if not 1 <= which <= len(runs):
            return 404, {"error": f"no run {which} (there are {len(runs)})"}
        rows = compare_runs(follower.session.run_hops(which),
                            follower.session.flat_hops())
        return 200, {"run": which,
                     "text": format_run_comparison(which, len(runs) + 1, rows)}
    elif method == "POST" and path == "/api/follow/switch":
        follower.switch_to(body.get("uuid") or "")
        outcome = {"status": "switched", "uuid": follower.uuid}
    elif method == "POST" and path == "/api/follow/next":
        nxt = follower.next_live()
        if nxt:
            follower.switch_to(nxt)
        outcome = {"status": "switched" if nxt else "none", "uuid": nxt}
    elif method == "POST" and path == "/api/follow/stop":
        restored = follower.restore() if body.get("restore") else 0
        # Removing the injector drains its connection, and an unstepped
        # fixture IS that queue — so it is left behind rather than dropped.
        kept = bool(follower.session.injector) and follower.injector_holds_file()
        if not kept:
            follower.cleanup_injector()
        _FOLLOW["follower"] = None
        return 200, {"active": False, "restored": restored,
                     "injector_kept": kept,
                     "session": str(follower.session.path or "")}
    else:
        return 404, {"error": f"no route for {method} {path}"}
    # Hops the caller should flash: the ones this action just produced.
    return 200, _follow_payload(
        follower, outcome={k: v for k, v in outcome.items() if k != "hops"},
        fresh=[h.get("event_id") for h in outcome.get("hops") or []])


# ------------------------------------------------------------------ watch

# The background health watcher (see :mod:`niflow.watch`). One per server
# process, started at boot so something is *always* checking: the whole point
# of the Alerts tab is catching a break while you are looking at another tab.
# Its state lives in .niflow-watch/, so restarting the GUI does not lose the
# "this was healthy for three hours" baseline.
_WATCH: dict = {"watcher": None, "thread": None, "stop": None,
                "interval": 15.0, "group": "root"}


def _watch_start(client: NiFiClient, lock: threading.Lock, *, group: str = "root",
                 interval: float = 15.0, baseline: Optional[float] = None,
                 include_warnings: bool = False) -> Any:
    """(Re)start the background watcher thread; returns the Watcher."""
    from niflow.watch import DEFAULT_BASELINE_SECONDS, Watcher

    _watch_stop()
    watcher = Watcher(
        client, group,
        baseline_seconds=DEFAULT_BASELINE_SECONDS if baseline is None else baseline,
        include_warnings=include_warnings,
    )
    stop = threading.Event()

    def loop() -> None:
        while not stop.is_set():
            try:
                with lock:  # NiFi calls are serialised with the page's
                    watcher.tick()
            except Exception as exc:  # noqa: BLE001 - a dead watcher is useless
                watcher.last_error = str(exc)
                logger.warning("watch tick failed: %s", exc)
            if stop.wait(interval):
                return

    thread = threading.Thread(target=loop, daemon=True, name="niflow-watch")
    _WATCH.update(watcher=watcher, thread=thread, stop=stop,
                  interval=interval, group=group)
    thread.start()
    return watcher


def _watch_stop() -> None:
    if _WATCH.get("stop") is not None:
        _WATCH["stop"].set()
    _WATCH.update(thread=None, stop=None)


def _watch_payload(watcher: Any) -> dict:
    return {
        "running": _WATCH.get("thread") is not None,
        "interval": _WATCH.get("interval"),
        "summary": watcher.summary(),
        "alerts": watcher.alerts(),
        "state_file": str(watcher.store.path),
    }


def _watch_route(client: NiFiClient, lock: threading.Lock, method: str,
                 path: str, q, body: dict) -> Tuple[int, Any]:
    """``/api/alerts/*`` — read alerts, ack/dismiss them, steer the watcher.

    Reads are served from the watcher's in-memory state and touch NiFi not at
    all, which is what makes the badge safe to poll on the page's 3s tick
    from every tab.
    """
    watcher = _WATCH.get("watcher")
    if watcher is None:
        watcher = _watch_start(client, lock, group=_WATCH.get("group") or "root",
                               interval=_WATCH.get("interval") or 15.0)
    if method == "GET" and path == "/api/alerts":
        return 200, _watch_payload(watcher)
    if method == "GET" and path == "/api/alerts/summary":
        return 200, {"running": _WATCH.get("thread") is not None,
                     **watcher.summary()}
    if method == "POST" and path == "/api/alerts/ack":
        ok = watcher.acknowledge(body.get("id") or "", body.get("on", True))
        return (200 if ok else 404), {"ok": ok, **_watch_payload(watcher)}
    if method == "POST" and path == "/api/alerts/dismiss":
        ok = watcher.dismiss(body.get("id") or "")
        return (200 if ok else 404), {"ok": ok, **_watch_payload(watcher)}
    if method == "POST" and path == "/api/alerts/clear":
        return 200, {"ok": True, "cleared": watcher.clear_resolved(),
                     **_watch_payload(watcher)}
    if method == "POST" and path == "/api/alerts/check":
        watcher.tick()  # "check now" — one poll, on the request thread
        return 200, _watch_payload(watcher)
    if method == "POST" and path == "/api/alerts/watch":
        if body.get("on", True):
            watcher = _watch_start(
                client, lock,
                group=body.get("group") or _WATCH.get("group") or "root",
                interval=float(body.get("interval") or _WATCH.get("interval") or 15.0),
                baseline=body.get("baseline"),
                include_warnings=bool(body.get("warnings")),
            )
        else:
            _watch_stop()
        return 200, _watch_payload(watcher)
    return 404, {"error": f"no route for {method} {path}"}


# ---------------------------------------------------------------- dispatch


def dispatch(
    client: NiFiClient,
    lock: threading.Lock,
    method: str,
    path: str,
    query: dict,
    body: dict,
    flows_dir: Path,
) -> Tuple[int, Any]:
    """Route one API call; returns ``(status, json-serialisable payload)``.

    Pure enough to unit-test without sockets.
    """
    def q(name: str) -> Optional[str]:
        values = query.get(name)
        return values[0] if values else None

    try:
        with lock:
            if method == "GET" and path == "/api/about":
                return 200, {
                    "version": client.version(),
                    "base": client.base,
                    # UI base for the page's NiFi deep links (see compLink()).
                    "ui": client.ui_url(),
                    "auth": client.config.auth_mode,
                    "boot": BOOT,
                    "reload": RELOAD,
                }
            if method == "GET" and path == "/api/processors":
                return 200, client.find_processors()
            if method == "GET" and path == "/api/groups":
                # Every process group's path+id — feeds the top-level-flow
                # dropdown and maps bulletin group ids back to a flow.
                return 200, [
                    {"id": comp["id"], "path": group_path}
                    for group_path, comp in client.walk_groups()
                ]
            if method == "GET" and path == "/api/queues":
                return 200, client.list_queues()
            if method == "GET" and path == "/api/bulletins":
                return 200, client.bulletins()
            if method == "GET" and path == "/api/errors":
                return 200, client.validation_errors()
            if method == "GET" and path == "/api/flowfiles":
                return 200, client.list_flowfiles(q("connection_id"))
            if method == "GET" and path == "/api/flowfile":
                return 200, client.flowfile_detail(q("connection_id"), q("uuid"))
            if method == "GET" and path == "/api/trace":
                from niflow.follow import annotate_hops

                trace = client.trace_flowfile(q("uuid"))
                # Same annotation the stepper applies, so the Trace and Follow
                # tabs render one hop the same way (added/changed/removed).
                annotate_hops(trace["hops"])
                return 200, trace
            if method == "GET" and path == "/api/trace/content":
                return 200, {"content": client.event_content(
                    q("event_id"), q("direction") or "output")}
            if path.startswith("/api/follow"):
                return _follow_route(client, method, path, q, body)
            if path.startswith("/api/alerts"):
                return _watch_route(client, lock, method, path, q, body)

            if method == "POST" and path.startswith("/api/processors/"):
                _, _, _, proc_id, action = path.split("/", 4)
                if action == "run-once":
                    client.run_processor_once(proc_id)
                elif action == "start":
                    client.start_processor(proc_id)
                elif action == "stop":
                    client.stop_processor(proc_id)
                else:
                    return 404, {"error": f"unknown processor action {action!r}"}
                return 200, {"ok": True}

            if method == "POST" and path.startswith("/api/queues/"):
                _, _, _, conn_id, action = path.split("/", 4)
                if action in ("run-source-once", "run-destination-once"):
                    which = action.split("-")[1]
                    return 200, {"ok": True,
                                 "ran": client.run_queue_endpoint_once(conn_id, which)}
                if action == "purge":
                    # One queue only — the drop-request reports what it dropped.
                    return 200, {"ok": True,
                                 "dropped": client.drain_connection(conn_id)}
                return 404, {"error": f"unknown queue action {action!r}"}

            if method == "POST" and path.startswith("/api/group/"):
                action = path.rsplit("/", 1)[-1]
                if action == "start":
                    client.enable_services("root")
                    client.start_group("root")
                elif action == "stop":
                    client.stop_group("root")
                elif action == "drain":
                    return 200, {"ok": True, "dropped": client.quiesce_group("root")}
                elif action == "purge":
                    # Scoped to the page's selected flow when it sends one;
                    # "root" (everything) only when it doesn't.
                    group = body.get("group") or "root"
                    return 200, {"ok": True, "group": group,
                                 "dropped": client.purge_queues(group)}
                else:
                    return 404, {"error": f"unknown group action {action!r}"}
                return 200, {"ok": True}

            if method == "POST" and path == "/api/tidy":
                # One-click canvas de-spaghettifier (see NiFiClient.tidy_group).
                return 200, {"ok": True, "moved": client.tidy_group(
                    body.get("group") or "root",
                    layout=body.get("layout") or "horizontal",
                    recurse=body.get("recurse", True),
                )}

            if method == "GET" and path == "/api/explain":
                # Doc + freshness for one group, plus what a generate at this
                # depth would cost (documents/llm_calls); no LLM call here.
                # depth 0 is meaningful ("everything below"), so no `or 1`.
                depth = q("depth")
                return 200, client.explain_status(
                    q("group") or "root", depth=1 if depth is None else int(depth))
            if method == "POST" and path == "/api/explain":
                depth = body.get("depth")
                results = client.explain_group(
                    body.get("group") or "root",
                    depth=1 if depth is None else int(depth),
                    force=body.get("force", False),
                )
                return 200, {"ok": True, "results": results,
                             **client.explain_status(
                                 body.get("group") or "root",
                                 depth=1 if depth is None else int(depth))}

            if method == "GET" and path == "/api/flows":
                flows = sorted(str(p) for p in flows_dir.glob("*.py"))
                return 200, flows
            if method == "GET" and path == "/api/plan":
                flow = _load_flow(q("file"))
                from niflow.plan import format_plan

                pg_id, _live, changes = client.plan_flow(flow)
                return 200, {
                    "exists": pg_id is not None,
                    "changes": len(changes),
                    "plan": format_plan(changes),
                    "issues": flow.validate(),
                }
            if method == "POST" and path == "/api/push":
                flow = _load_flow(body.get("file"))
                start = bool(body.get("start"))
                if body.get("update", True):
                    changes = client.push_update(flow, start=start)
                    return 200, {"ok": True, "applied": len(changes), "id": flow.nifi_id}
                new_id = client.push_flow(flow, start=start)
                return 200, {"ok": True, "applied": None, "id": new_id}

        return 404, {"error": f"no route for {method} {path}"}
    except Exception as exc:  # surface as JSON, keep the server alive
        logger.warning("webgui %s %s failed: %s", method, path, exc)
        return 500, {"error": str(exc)}


def _load_flow(path: Optional[str]):
    if not path:
        raise ValueError("missing 'file'")
    from niflow.convert import _load_python_flow

    return _load_python_flow(path, "flow")


# ------------------------------------------------------------- HTTP shell


class _Handler(BaseHTTPRequestHandler):
    server_version = "niflow-web"
    client_ref: NiFiClient = None  # type: ignore[assignment]
    lock: threading.Lock = None  # type: ignore[assignment]
    flows_dir: Path = Path("flows")

    def log_message(self, fmt, *args):  # quiet request spam
        pass

    def _reply(self, status: int, payload: Any, content_type="application/json") -> None:
        data = (
            payload.encode() if isinstance(payload, str)
            else json.dumps(payload, default=str).encode()
        )
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
            self._reply(200, PAGE, content_type="text/html; charset=utf-8")
            return
        status, payload = dispatch(
            self.client_ref, self.lock, "GET", parsed.path,
            parse_qs(parsed.query), {}, self.flows_dir,
        )
        self._reply(status, payload)

    def do_POST(self):
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            self._reply(400, {"error": "invalid JSON body"})
            return
        status, payload = dispatch(
            self.client_ref, self.lock, "POST", parsed.path,
            parse_qs(parsed.query), body, self.flows_dir,
        )
        self._reply(status, payload)


def _watch_and_reexec(argv: List[str], interval: float = 1.0) -> None:
    """Re-exec this process when any ``niflow/**.py`` changes (dev mode).

    execv keeps the pid and the terminal; the listening socket is
    close-on-exec, so the restarted process rebinds the same port. The page
    notices the new BOOT id via its /api/about poll and reloads itself.
    """
    package = Path(__file__).resolve().parent

    def snapshot() -> float:
        newest = 0.0
        for p in package.rglob("*.py"):
            try:
                newest = max(newest, p.stat().st_mtime)
            except OSError:  # transient: editors replace files on save
                pass
        return newest

    baseline = snapshot()
    while True:
        time.sleep(interval)
        if snapshot() != baseline:
            logger.info("source changed — restarting webgui")
            os.execv(sys.executable, [sys.executable, "-m", "niflow.webgui", *argv])


def serve(
    config: Optional[NiFiConfig] = None,
    host: str = "127.0.0.1",
    port: int = 7777,
    flows_dir: str = "flows",
    open_browser: bool = True,
    reload: bool = False,
    watch: bool = True,
    watch_group: str = "root",
    watch_interval: float = 15.0,
) -> None:
    client = NiFiClient(config or NiFiConfig.from_env())
    lock = threading.Lock()
    handler = type("BoundHandler", (_Handler,), {
        "client_ref": client,
        "lock": lock,
        "flows_dir": Path(flows_dir),
    })
    if watch:
        # Always-on background health check: the Alerts badge has to light up
        # while you are on some other tab, which means the polling cannot be
        # the page's job.
        _WATCH["group"], _WATCH["interval"] = watch_group, watch_interval
        _watch_start(client, lock, group=watch_group, interval=watch_interval)
        logger.info("watching %r for health transitions every %ss "
                    "(Alerts tab)", watch_group, watch_interval)
    if reload:
        # NB: the restart re-reads config from the environment/.niflow.env —
        # a `config` object passed in programmatically won't survive it.
        global RELOAD
        RELOAD = True
        argv = ["--host", host, "--port", str(port), "--flows-dir", str(flows_dir),
                "--reload", "--no-browser"]  # never pop a second tab on restart
        threading.Thread(target=_watch_and_reexec, args=(argv,), daemon=True).start()
    server = ThreadingHTTPServer((host, port), handler)
    url = f"http://{host}:{port}/"
    logger.info("NiFlow web helper on %s (Ctrl-C to stop)%s", url,
                " — live reload on" if reload else "")
    if open_browser:
        open_url(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("bye")
    finally:
        server.server_close()


def main() -> None:
    parser = argparse.ArgumentParser(description="NiFlow web helper (browser GUI)")
    parser.add_argument("--port", type=int, default=7777)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--flows-dir", default="flows", help="Directory of flow .py files")
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument("--reload", action="store_true",
                        help="restart on niflow source changes; open pages reload themselves")
    parser.add_argument("--no-watch", action="store_true",
                        help="don't run the background health watcher (Alerts tab)")
    parser.add_argument("--watch-group", default="root",
                        help="group the health watcher polls (default: root)")
    parser.add_argument("--watch-interval", type=float, default=15.0,
                        help="seconds between health polls (default: 15)")
    args = parser.parse_args()
    serve(host=args.host, port=args.port, flows_dir=args.flows_dir,
          open_browser=not args.no_browser, reload=args.reload,
          watch=not args.no_watch, watch_group=args.watch_group,
          watch_interval=args.watch_interval)


# ------------------------------------------------------------------- page


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NiFlow Helper</title>
<style>
  :root { --bg:#fff; --fg:#1a1a1a; --muted:#667; --line:#dfe3e8; --accent:#0b6bcb;
          --ok:#127a3d; --warn:#a15c07; --bad:#b42318; --chip:#f2f4f7; --pin:#fdf6e3;
          --flash:#fff3bf; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#101418; --fg:#e6e9ec; --muted:#98a2b3; --line:#2a3138;
            --accent:#559ee8; --ok:#4cc38a; --warn:#e2b03a; --bad:#f97066; --chip:#1c232b;
            --pin:#241f12; --flash:#3d3313; }
  }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--fg);
         font:14px/1.45 system-ui, "Segoe UI", sans-serif; }
  header { display:flex; gap:1rem; align-items:center; padding:.6rem 1rem;
           border-bottom:1px solid var(--line); flex-wrap:wrap; }
  header h1 { font-size:1rem; margin:0; }
  header .meta { color:var(--muted); font-size:.85rem; }
  nav { display:flex; gap:.25rem; padding:.4rem 1rem 0; border-bottom:1px solid var(--line); }
  nav button { border:1px solid var(--line); border-bottom:none; background:var(--chip);
               color:var(--fg); padding:.4rem .9rem; cursor:pointer;
               border-radius:.5rem .5rem 0 0; }
  nav button.active { background:var(--bg); font-weight:600; }
  main { padding:1rem; }
  .bar { display:flex; gap:.5rem; align-items:center; margin-bottom:.8rem; flex-wrap:wrap; }
  .bar input[type=text] { padding:.35rem .5rem; border:1px solid var(--line);
                          border-radius:.4rem; background:var(--bg); color:var(--fg); min-width:16rem; }
  .bar select { padding:.35rem .5rem; border:1px solid var(--line); border-radius:.4rem;
                background:var(--bg); color:var(--fg); max-width:18rem; }
  td.star { cursor:pointer; user-select:none; width:1.6rem; text-align:center;
            color:var(--muted); font-size:1rem; }
  td.star.on { color:var(--warn); }
  tr.pinned td { background:var(--pin); }
  tr.gap td { border-bottom:none; height:.7rem; padding:0; }
  button.op { border:1px solid var(--line); background:var(--chip); color:var(--fg);
              padding:.3rem .7rem; border-radius:.4rem; cursor:pointer; }
  button.op:hover { border-color:var(--accent); }
  button.danger { color:var(--bad); border-color:var(--bad); }
  button.danger:hover { background:var(--bad); border-color:var(--bad); color:#fff; }
  a.nifi { color:var(--accent); text-decoration:none; }
  a.nifi:hover { text-decoration:underline; }
  table { border-collapse:collapse; width:100%; }
  th, td { text-align:left; padding:.35rem .55rem; border-bottom:1px solid var(--line); }
  th { color:var(--muted); font-weight:600; font-size:.8rem; text-transform:uppercase; }
  tr.click { cursor:pointer; } tr.click:hover td { background:var(--chip); }
  .state-RUNNING { color:var(--ok); } .state-STOPPED { color:var(--muted); }
  .state-DISABLED { color:var(--warn); } .state-INVALID { color:var(--bad); }
  .muted { color:var(--muted); }
  pre { background:var(--chip); padding: .8rem; border-radius:.5rem;
        overflow-x:auto; white-space:pre-wrap; word-break:break-word; }
  .split { display:grid; grid-template-columns:1fr 1fr; gap:1rem; }
  @media (max-width: 900px) { .split { grid-template-columns:1fr; } }
  #status { margin-left:auto; color:var(--muted); font-size:.85rem; }
  article.doc { max-width:52rem; line-height:1.55; }
  article.doc h2 { border-bottom:1px solid var(--line); padding-bottom:.2rem; }
  article.doc code { background:var(--chip); padding:.05rem .3rem; border-radius:.25rem; }
  .pill { background:var(--chip); border-radius:1rem; padding:.1rem .6rem; font-size:.8rem; }
  .hop { border:1px solid var(--line); border-radius:.5rem; padding:.6rem .8rem;
         margin-bottom:.7rem; }
  .hop .hophead { display:flex; gap:.6rem; align-items:center; flex-wrap:wrap;
                  margin-bottom:.4rem; }
  .hop table { width:auto; min-width:50%; }
  /* A hop with no event behind it (a port crossing, a transfer NiFi did not
     record) or one belonging to a relative on the same lineage: dashed, so it
     is visibly not something that happened to this FlowFile at this second. */
  .hop.synth { border-style:dashed; }
  .hop .lineage { color:var(--muted); margin:.2rem 0 .3rem; }
  .hop .lineage .arrow { color:var(--accent); margin-right:.35rem; }
  /* The stepper's headline: what changed at this hop has to catch the eye. */
  .hop.flash { animation: hopflash 1.6s ease-out; }
  @keyframes hopflash { 0%, 35% { background:var(--flash); } 100% { background:transparent; } }
  .hop.flash table.diff tr { animation: hopflash 2s ease-out; }
  table.diff td.mark { width:1.4rem; text-align:center; font-weight:700; }
  tr.d-added td.mark { color:var(--ok); }
  tr.d-changed td.mark { color:var(--accent); }
  tr.d-removed td.mark { color:var(--bad); }
  tr.d-removed td:not(.mark) { text-decoration:line-through; color:var(--muted); }
  .chip { display:inline-block; background:var(--chip); border-radius:.4rem;
          padding:.1rem .5rem; font-size:.8rem; margin-top:.3rem; }
  tr.branch-muted td { opacity:.5; }
  tr.branch-current td { background:var(--pin); }
  button.op.primary { border-color:var(--accent); color:var(--accent); font-weight:600; }
  /* Alerts: a break has to be visible from whatever tab you are on, so the
     banner lives above the tab bar and the tab itself carries a count. */
  #alertbar { display:none; padding:.55rem 1rem; border-bottom:1px solid var(--line);
              background:var(--bad); color:#fff; align-items:center; gap:.6rem;
              flex-wrap:wrap; cursor:pointer; }
  #alertbar.on { display:flex; animation: alertpulse 1.2s ease-out 3; }
  #alertbar.ext { background:var(--bad); }
  #alertbar.int { background:var(--warn); }
  #alertbar.unk { background:var(--muted); }
  @keyframes alertpulse { 0% { filter:brightness(1.5); } 100% { filter:none; } }
  nav button .count { display:inline-block; margin-left:.4rem; min-width:1.1rem;
                      padding:0 .35rem; border-radius:1rem; background:var(--bad);
                      color:#fff; font-size:.75rem; font-weight:700; }
  .alert { border:1px solid var(--line); border-left:.35rem solid var(--muted);
           border-radius:.5rem; padding:.6rem .8rem; margin-bottom:.7rem; }
  .alert.external { border-left-color:var(--bad); }
  .alert.internal { border-left-color:var(--warn); }
  .alert.unknown  { border-left-color:var(--muted); }
  .alert.resolved { opacity:.55; }
  .alert.acked { opacity:.65; }
  .alert h3 { margin:0 0 .2rem; font-size:.98rem; }
  .alert .why { margin:.35rem 0; font-size:1rem; }
  .alert .hint { color:var(--muted); }
  .alert .ev { color:var(--muted); font-size:.82rem; white-space:pre-wrap;
               word-break:break-word; margin-top:.35rem; }
</style>
</head>
<body>
<header>
  <h1>NiFlow Helper</h1>
  <span class="meta" id="about">connecting…</span>
  <label class="meta" title="re-reads the current tab every 3s; pauses while you type,
while a drill-down or dialog is open, and on the Trace/Explain/Flows tabs">
    <input type="checkbox" id="auto" checked> auto-refresh (3s)</label>
  <span id="status"></span>
</header>
<div id="alertbar" onclick="gotoAlerts()"></div>
<nav id="tabs"></nav>
<main id="view"></main>
<script>
const $ = (s, el=document) => el.querySelector(s);
const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const api = async (path, opts) => {
  const r = await fetch(path, opts);
  const j = await r.json();
  if (!r.ok) throw new Error(j.error || r.status);
  return j;
};
let inflight = 0;   // mutating requests in flight (auto-refresh backs off)
let modal = false;  // a confirm() is on screen (ditto)
// Every destructive confirm goes through here so a 3s tick can't re-render
// the page out from under the dialog.
function ask(msg) { modal = true; try { return confirm(msg); } finally { modal = false; } }
const post = async (path, body) => {
  inflight++;  // auto-refresh holds off while a mutation is in flight
  try {
    return await api(path, {method:"POST", headers:{"Content-Type":"application/json"},
                            body: body ? JSON.stringify(body) : "{}"});
  } finally { inflight--; }
};
const status = msg => { $("#status").textContent = msg; };

// --- NiFi deep links -------------------------------------------------
// Same shape as NiFiClient.ui_url(): the canvas opens on `groupId` with
// `compId` selected. The UI base arrives with /api/about; without it (not
// connected yet) links degrade to plain text rather than going nowhere.
function uiUrl(groupId, compId) {
  if (!window._uiBase || (!groupId && !compId)) return "";
  const p = [];
  if (groupId) p.push("processGroupId=" + encodeURIComponent(groupId));
  if (compId) p.push("componentIds=" + encodeURIComponent(compId));
  return window._uiBase + "/?" + p.join("&");
}
// Queue rows name their endpoints, but NiFi's status snapshot carries no
// endpoint ids (checked live on 1.24 and 2.7.2), so match the names against
// the processor listing to get id + group. Cached: ids only move when a flow
// is rebuilt, and the queues tab re-renders every few seconds.
async function procIndex() {
  if (window._procIx) return window._procIx;
  const procs = await api("/api/processors");
  window._procIx = {};
  for (const p of procs) window._procIx[p.path + "\u0000" + p.name] = p;
  return window._procIx;
}
function endpointLink(path, name) {
  const p = (window._procIx || {})[path + "\u0000" + name];
  // No match (a port, a funnel, a component in another group) -> plain text
  // rather than a link that lands somewhere it didn't promise.
  return p ? compLink(p.group_id, p.id, name, "open " + name + " in NiFi") : esc(name);
}

// One anchor for every processor / connection / group mention on the page.
// New tab, and stopPropagation so clicking the name never triggers the row's
// own drill-down or the buttons beside it.
function compLink(groupId, compId, label, title) {
  const url = uiUrl(groupId, compId);
  if (!url) return esc(label ?? "");
  return `<a class="nifi" href="${esc(url)}" target="_blank" rel="noopener"` +
         ` title="${esc(title || "open in NiFi")}"` +
         ` onclick="event.stopPropagation()">${esc(label ?? "")}</a>`;
}

// --- one hop, drawn the same way wherever it appears -----------------
// The Trace and Follow tabs are two views of the same journey, so they share
// this renderer the way the CLI's trace/follow share format_hop(). Hops that
// have been through annotate_hops() carry a cross-hop `diff` (added/changed/
// removed) and a `content_change`; anything older falls back to NiFi's own
// per-event `changes` list.
const DIFF_MARK = {added: "+", changed: "~", removed: "−"};
function diffTable(h) {
  const entries = h.diff
    ? h.diff.map(d => [d.status, d.name, d.before, d.after])
    : (h.changes || []).map(c => [c.before === null ? "added" : "changed",
                                  c.name, c.before, c.after]);
  const rank = {changed: 0, added: 1, removed: 2};
  entries.sort((a, b) => (rank[a[0]] - rank[b[0]]) || String(a[1]).localeCompare(b[1]));
  const cc = h.content_change;
  const content = cc ? `<div class="chip">content ${
      cc.before === null || cc.before === cc.after
        ? "rewritten (" + esc(cc.after) + " B)"
        : esc(cc.before) + " B → " + esc(cc.after) + " B"}</div>` : "";
  if (!entries.length)
    return content || `<span class="muted">no attribute changes</span>`;
  return `<table class="diff">
    <tr><th></th><th>Attribute</th><th>Before</th><th>After</th></tr>
    ${entries.map(([st, name, before, after]) => `
      <tr class="d-${st}"><td class="mark">${DIFF_MARK[st] || ""}</td>
        <td>${esc(name)}</td>
        <td class="muted">${before === null || before === undefined
            ? "<i>(new)</i>" : esc(before)}</td>
        <td>${after === null || after === undefined
            ? "<i>(removed)</i>" : "<b>" + esc(after) + "</b>"}</td></tr>`).join("")}
    </table>${content}`;
}
function hopCard(h, i, flash) {
  const kin = (label, uuids) => (uuids || []).map(u =>
    `<div class="muted">${label} ${esc(u)}
       <button class="op" onclick="traceJump('${esc(u)}')">trace</button></div>`).join("");
  // The CLI's format_hop() twin. A hop carrying `lineage` is either synthetic
  // (a port crossing / an unrecorded transfer: no event, so no time and no
  // size — printing "0 B" there reads as an empty FlowFile) or an event on a
  // relative's FlowFile that the lineage query returned. Either way its diff
  // is deliberately empty, so the note replaces the table instead of sitting
  // above an unhelpful "no attribute changes".
  const synthetic = h.synthetic || h.lineage;
  const stamp = h.synthetic ? "" :
    `<span class="muted">${esc(h.time)} · ${esc(h.size)} B</span>`;
  const note = h.lineage
    ? `<div class="lineage"><span class="arrow">⤳</span>${esc(h.lineage)}</div>` : "";
  const continues = h.lineage
    ? (h.children || []).map(u =>
        `<div class="muted">continues as ${esc(u)}
           <button class="op" onclick="traceJump('${esc(u)}')">trace</button></div>`).join("")
    : "";
  return `<div class="hop${flash ? " flash" : ""}${synthetic ? " synth" : ""}">
    <div class="hophead">
      <b>#${i + 1} ${compLink(h.group_id, h.component_id, h.component || "(flow)")}</b>
      <span class="pill">${esc(h.event_type)}${h.relationship ? " → " + esc(h.relationship) : ""}</span>
      ${stamp}
      <span style="flex:1"></span>
      <button class="op" onclick="hopAttrs(${i})">Attributes</button>
      ${h.input_available ? `<button class="op" onclick="hopContent(${i},'input')">Content in</button>` : ""}
      ${h.output_available ? `<button class="op" onclick="hopContent(${i},'output')">Content out</button>` : ""}
    </div>
    ${note}
    ${h.lineage ? continues : diffTable(h) +
        kin("joined from", h.parents) + kin("spawned", h.children)}
    <pre id="hopx${i}" style="display:none"></pre>
  </div>`;
}

// One top-level-flow filter shared by every server-data tab; the pick is
// remembered (localStorage) across tabs and sessions.
const FLOWKEY = "niflow.group";
const topOf = p => p ? p.split("/")[0] : "";
async function flowTops() {
  const groups = await api("/api/groups");
  window._groupPaths = Object.fromEntries(groups.map(g => [g.id, g.path]));
  return [...new Set(["", ...groups.map(g => topOf(g.path))])]
         .sort((a, b) => a.localeCompare(b));
}
function currentFlow(tops) {
  const g = localStorage.getItem(FLOWKEY) ?? "*";
  return g !== "*" && !tops.includes(g) ? "*" : g; // flow gone — show all
}
function flowSelect(tops, sel) {
  return `<select id="pg" title="show only one top-level flow">
      <option value="*">All flows</option>
      ${tops.map(g => `<option value="${esc(g)}"${g === sel ? " selected" : ""}>` +
                      `${g ? esc(g) : "(top level)"}</option>`).join("")}
    </select>`;
}
function bindFlowSelect() {
  const pg = $("#pg");
  if (pg) pg.onchange = e => { localStorage.setItem(FLOWKEY, e.target.value); render(); };
}

const TABS = [
  ["processors", "Processors"], ["queues", "Queues & FlowFiles"],
  ["trace", "Trace"], ["follow", "Follow"],
  ["alerts", "Alerts"],
  ["bulletins", "Bulletins"], ["errors", "Errors"],
  ["explain", "Explain"], ["flows", "Flows"],
];
let active = "processors";
let inspector = null;   // {connId, groupId, label, uuid} queues-tab drill-down
let traceUuid = null;   // FlowFile the Trace tab is following (survives tab hops)
let followFlash = [];   // event ids the last step produced -> those hops flash

function nav() {
  const n = (window._alertCount || 0);
  $("#tabs").innerHTML = TABS.map(([k, label]) =>
    `<button data-tab="${k}" class="${k===active?'active':''}">${label}` +
    (k === "alerts" && n ? `<span class="count">${n}</span>` : "") +
    `</button>`).join("");
  document.querySelectorAll("#tabs button").forEach(b =>
    b.onclick = () => { active = b.dataset.tab; inspector = null; nav(); render(); });
}
function gotoAlerts() { active = "alerts"; inspector = null; nav(); render(); }

async function render() {
  const view = $("#view");
  try {
    if (active === "processors") {
      const [procs, tops] = await Promise.all([api("/api/processors"), flowTops()]);
      const filter = window._pf || "";
      const group = currentFlow(tops);
      const stars = new Set(JSON.parse(localStorage.getItem("niflow.stars") || "[]"));
      const key = p => (p.path ? p.path + "/" : "") + p.name;
      const rows = procs.filter(p =>
        (group === "*" || topOf(p.path) === group) &&
        (`${p.path}/${p.name} ${p.type}`).toLowerCase().includes(filter.toLowerCase()));
      rows.sort((a, b) => stars.has(key(b)) - stars.has(key(a))); // pinned first, order kept
      const pinned = rows.filter(p => stars.has(key(p))).length;
      window._rowKeys = rows.map(key);
      view.innerHTML = `
        <div class="bar">
          ${flowSelect(tops, group)}
          <input type="text" id="pf" placeholder="filter by name / path / type" value="${esc(filter)}">
          <span class="pill">${rows.length} / ${procs.length}</span>
          <span style="flex:1"></span>
          <select id="td" title="tidy direction">
            <option value="horizontal"${tidyDir() === "horizontal" ? " selected" : ""}>Left → right</option>
            <option value="vertical"${tidyDir() === "vertical" ? " selected" : ""}>Top ↓ bottom</option>
          </select>
          <button class="op" title="auto-arrange the ${group === "*" ? "whole canvas" : group === "" ? "top-level canvas" : esc(group) + " canvas"} along its connections" onclick="tidyCanvas()">Tidy canvas</button>
          <button class="op" onclick="groupOp('start')">Start All</button>
          <button class="op" onclick="groupOp('stop')">Stop All</button>
          <button class="op" onclick="groupOp('drain')">Stop &amp; Drain</button>
          <button class="op danger" title="drop every FlowFile queued in the selected flow" onclick="purgeFlow()">Purge Queues</button>
        </div>
        <table><tr><th></th><th>Processor</th><th>Type</th><th>State</th><th></th></tr>
        ${rows.map((p, i) => `${i === pinned && pinned ? '<tr class="gap"><td colspan="5"></td></tr>' : ""}<tr class="${i < pinned ? "pinned" : ""}">
          <td class="star ${stars.has(key(p)) ? "on" : ""}" onclick="toggleStar(${i})"
              title="${stars.has(key(p)) ? "unpin" : "pin to top"}">${stars.has(key(p)) ? "★" : "☆"}</td>
          <td>${esc(p.path ? p.path + "/" : "")}<b>${compLink(p.group_id, p.id, p.name, "open " + p.name + " in NiFi")}</b></td>
          <td class="muted">${esc(p.type.split(".").pop())}</td>
          <td class="state-${esc(p.state)}">${esc(p.state)}</td>
          <td>
            <button class="op" onclick="procOp('${p.id}','run-once')">Run once</button>
            <button class="op" onclick="procOp('${p.id}','start')">Start</button>
            <button class="op" onclick="procOp('${p.id}','stop')">Stop</button>
          </td></tr>`).join("")}
        </table>`;
      const pf = $("#pf");
      pf.oninput = () => { window._pf = pf.value; render().then(() => { const el=$("#pf"); el.focus(); el.setSelectionRange(el.value.length, el.value.length); }); };
      $("#td").onchange = e => localStorage.setItem("niflow.tidydir", e.target.value);
      bindFlowSelect();
    }

    if (active === "queues") {
      if (inspector && inspector.uuid) {
        const d = await api(`/api/flowfile?connection_id=${inspector.connId}&uuid=${inspector.uuid}`);
        view.innerHTML = `
          <div class="bar"><button class="op" onclick="inspector.uuid=null;render()">← back to ${esc(inspector.label)}</button>
            <button class="op" title="how did this file get here? provenance journey with attribute diffs"
                onclick="traceJump('${inspector.uuid}')">Trace journey</button>
            <span class="muted">${compLink(inspector.groupId, inspector.connId, "show this queue in NiFi")}</span></div>
          <div class="split">
            <div><h3>Attributes</h3><pre>${esc(JSON.stringify(d.attributes ?? d, null, 2))}</pre></div>
            <div><h3>Content</h3><pre>${esc(d.content ?? "(no content view)")}</pre></div>
          </div>`;
      } else if (inspector) {
        const files = await api(`/api/flowfiles?connection_id=${inspector.connId}`);
        view.innerHTML = `
          <div class="bar"><button class="op" onclick="inspector=null;render()">← all queues</button>
            <span class="pill">${esc(inspector.label)} — ${files.length} FlowFile(s)</span>
            <span class="muted">${compLink(inspector.groupId, inspector.connId, "show in NiFi")}</span>
            <span style="flex:1"></span>
            <button class="op" title="feeds this queue"
                onclick="queueOp(inspector.connId,'source')">Run source once</button>
            <button class="op" title="consumes this queue"
                onclick="queueOp(inspector.connId,'destination')">Run destination once</button>
            <button class="op danger" title="drop every FlowFile in this queue"
                onclick="purgeQueue(-1)">Purge queue</button></div>
          <table><tr><th>UUID</th><th>Filename</th><th>Size</th><th>Queued</th></tr>
          ${files.map(f => `<tr class="click" onclick="inspector.uuid='${f.uuid}';render()">
            <td class="muted">${esc(f.uuid)}</td><td>${esc(f.filename ?? "")}</td>
            <td>${esc(f.size ?? "")}</td><td>${esc(f.queued_duration ?? "")}</td></tr>`).join("")}
          </table>`;
      } else {
        const [queues, tops] = await Promise.all([api("/api/queues"), flowTops(), procIndex()]);
        const group = currentFlow(tops);
        const qf = localStorage.getItem("niflow.qfilter") || "all";
        const rows = queues.filter(c =>
          (group === "*" || topOf(c.path) === group) &&
          (qf === "all" || (qf === "full" ? c.queued > 0 : !c.queued)));
        view.innerHTML = `
          <div class="bar">${flowSelect(tops, group)}
            <select id="qf" title="filter by queue contents">
              <option value="all"${qf === "all" ? " selected" : ""}>All queues</option>
              <option value="full"${qf === "full" ? " selected" : ""}>With FlowFiles</option>
              <option value="empty"${qf === "empty" ? " selected" : ""}>Empty</option>
            </select>
            <span class="pill">${rows.length} / ${queues.length}</span>
            <span style="flex:1"></span>
            <button class="op danger" title="drop every FlowFile queued in ${group === "*" ? "this NiFi" : group === "" ? "the top-level canvas" : esc(group)}"
                onclick="purgeFlow()">Purge ${group === "*" || group === "" ? "all queues" : "this flow's queues"}</button>
          </div>
          <table><tr><th>Queue</th><th>Path</th><th>Queued</th><th></th></tr>
          ${rows.map((c, i) => `<tr class="click" onclick="openQueue(${i})">
            <td>${c.source_id ? compLink(c.source_group_id || c.group_id, c.source_id, c.source)
                              : endpointLink(c.path, c.source)} →
                ${c.destination_id ? compLink(c.destination_group_id || c.group_id, c.destination_id, c.destination)
                                   : endpointLink(c.path, c.destination)}</td>
            <td class="muted">${compLink(c.group_id, c.id, c.path || "(top level)", "show this connection in NiFi")}</td>
            <td>${c.queued ? `<b>${esc(c.queued_label || c.queued)}</b>` : '<span class="muted">empty</span>'}</td>
            <td>
              <button class="op" title="run ${esc(c.source)} once (feeds this queue)"
                  onclick="event.stopPropagation();queueOp('${c.id}','source')">Src once</button>
              <button class="op" title="run ${esc(c.destination)} once (consumes this queue)"
                  onclick="event.stopPropagation();queueOp('${c.id}','destination')">Dest once</button>
              <button class="op danger" title="drop every FlowFile queued here"
                  onclick="event.stopPropagation();purgeQueue(${i})">Purge</button>
            </td>
          </tr>`).join("")}
          </table>`;
        // Row index -> row: keeps hostile names out of onclick attributes.
        window._qRows = rows;
        bindFlowSelect();
        $("#qf").onchange = e => { localStorage.setItem("niflow.qfilter", e.target.value); render(); };
      }
    }

    if (active === "trace") {
      // Post-hoc debugger: replay one FlowFile's provenance journey. Each hop
      // shows what that processor did to the file — attribute before/after,
      // relationship taken, payload on demand — so "where did it go wrong"
      // is a read, not a log hunt.
      const t = traceUuid
        ? await api(`/api/trace?uuid=${encodeURIComponent(traceUuid)}`) : null;
      window._hops = t ? t.hops : [];
      view.innerHTML = `
        <div class="bar">
          <input type="text" id="tu" placeholder="FlowFile UUID" value="${esc(traceUuid || "")}">
          <button class="op" onclick="traceGo()">Trace</button>
          <span class="muted">paste a UUID, or open a FlowFile under Queues → “Trace journey”</span>
        </div>` + (!t ? "" : !t.hops.length
        ? `<p class="muted">No provenance events for this UUID — wrong id, or the
             events have aged out of the provenance repository.</p>`
        : (t.truncated
            ? `<p class="muted">Showing the newest ${t.hops.length} hops of a
                 longer journey — hop #1 below is not where this FlowFile
                 began.</p>` : "")
          + t.hops.map((h, i) => hopCard(h, i, false)).join(""));
      const tu = $("#tu");
      tu.onkeydown = e => { if (e.key === "Enter") traceGo(); };
    }

    if (active === "follow") {
      // The live stepper: pick a start point, hit Step, watch the attribute
      // diff flash at every hop. Muting a branch is a VIEW decision — NiFi
      // keeps running it (same contract as `niflow follow --mute`).
      const st = await api("/api/follow/session");
      window._follow = st;
      if (!st.active) {
        const tops = await flowTops();
        const sel = currentFlow(tops);
        const g = (sel === "*" || sel === "") ? "root" : sel;
        let entries = [];
        let err = "";
        try { entries = (await api(`/api/follow/entrypoints?group=${encodeURIComponent(g)}`)).entries; }
        catch (e) { err = e.message; }
        window._entries = entries;
        const resume = st.resumable ? `<button class="op" onclick="followStart(-1)">
            Resume ${esc(st.resumable.id)} (${esc(st.resumable.hops)} hop(s))</button>` : "";
        view.innerHTML = `
          <div class="bar">
            ${flowSelect(tops, sel)}
            <input type="text" id="premute" placeholder="mute up front, e.g. failure"
                   value="${esc(localStorage.getItem("niflow.premute") || "")}">
            ${resume}
            <span class="muted">Starting quiesces ${esc(g)} — the group is stopped so
              nothing races the stepper; end the session with “restore” to put it back.</span>
          </div>` + (err ? `<p class="muted">${esc(err)}</p>` : "") + (!entries.length
          ? `<p class="muted">No start points in ${esc(g)}: no queued FlowFiles, no
               source processors, no input ports.</p>`
          : `<table>
              <tr><th>Kind</th><th>Where</th><th>Group</th><th></th><th></th></tr>
              ${entries.map((e, i) => `<tr>
                <td><span class="pill">${esc(e.kind)}</span></td>
                <td>${compLink(e.group_id, e.id, e.label)}</td>
                <td class="muted">${esc(e.path || "(top level)")}</td>
                <td class="muted">${esc(e.detail || "")}</td>
                <td><button class="op primary" onclick="followStart(${i})">Start here</button></td>
              </tr>`).join("")}
             </table>`)
          + `<h3>…or inject your own FlowFile</h3>
             <p class="muted">A temporary GenerateFlowFile mints exactly the file you
               describe at the component you name, and the stepper follows that — the
               debugger's own input, instead of waiting for the flow to produce one.
               It is removed when the session ends.</p>
             <div class="bar">
               <input type="text" id="ftarget" placeholder="inject at: processor name, Group/Name or id">
               <input type="text" id="fattrs" placeholder="attributes: k=v, k2=v2">
               <button class="op primary" onclick="followInject()">Inject &amp; start</button>
             </div>
             <textarea id="fcontent" rows="4" style="width:100%"
                       placeholder="content of the injected FlowFile (optional)"></textarea>`;
        bindFlowSelect();
        const pm = $("#premute");
        if (pm) pm.onchange = e => localStorage.setItem("niflow.premute", e.target.value);
      } else {
        window._hops = st.hops;
        const flash = new Set(followFlash);
        const branches = st.branches;
        window._branches = branches;
        view.innerHTML = `
          <div class="bar">
            <button class="op primary" onclick="followStep()" title="advance this FlowFile one processor">▶ Step</button>
            <button class="op" onclick="followAct('repoll')"
                    title="re-ask provenance without running anything (1.24 can lag)">Retry poll</button>
            <button class="op" onclick="followAct('next')" title="follow the next live branch">Next branch</button>
            <span class="pill">${st.hops.length} hop(s)</span>
            <span class="muted">${esc(st.group)} · following ${esc(st.current || "")}</span>
            <span style="flex:1"></span>
            <input type="text" id="mspec" placeholder="mute: failure / dest:PutFile / uuid">
            <button class="op" onclick="followMute()">Mute</button>
            <button class="op" onclick="followStop(true)">End &amp; restore</button>
            <button class="op danger" onclick="followStop(false)">End (leave stopped)</button>
          </div>
          <div class="bar">
            <span class="muted">Muted: ${st.mute_rules.length
              ? st.mute_rules.map(r => `<button class="op" onclick="followUnmute('${esc(r)}')"
                    title="unmute">${esc(r)} ✕</button>`).join(" ")
              : "nothing"} — muted branches keep running in NiFi, they are just not followed.</span>
          </div>
          <table>
            <tr><th>Branch</th><th>From</th><th>Queue</th><th>State</th><th>Hops</th><th></th></tr>
            ${branches.map((b, i) => `<tr class="${b.current ? "branch-current" : ""} ${b.state === "muted" ? "branch-muted" : ""}">
              <td>${b.current ? "▶ " : ""}${esc(b.uuid)}</td>
              <td class="muted">${esc(b.origin || "start")}${b.relationship ? " → " + esc(b.relationship) : ""}</td>
              <td class="muted">${esc(b.queue || "-")}</td>
              <td>${esc(b.state)}${b.muted_by ? " (" + esc(b.muted_by) + ")" : ""}</td>
              <td>${esc(b.hop_count)}</td>
              <td>
                ${b.current ? "" : `<button class="op" onclick="followSwitch(${i})">Follow</button>`}
                ${b.state === "muted"
                  ? `<button class="op" onclick="followUnmute('uuid:' + window._branches[${i}].uuid)">Unmute</button>`
                  : `<button class="op" onclick="followMuteBranch(${i})">Mute</button>`}
                <button class="op" onclick="traceJump(window._branches[${i}].uuid)">Trace</button>
              </td></tr>`).join("")}
          </table>
          <div class="bar">
            <input type="text" id="wspec" placeholder="watch an attribute: filename, http.*, @size">
            <button class="op" onclick="followWatch()">Watch</button>
            <span class="muted">${st.watches.length
              ? st.watches.map(w => `<button class="op" onclick="followUnwatch('${esc(w)}')"
                    title="stop watching">${esc(w)} ✕</button>`).join(" ")
              : "watching nothing yet"}</span>
            <span style="flex:1"></span>
            ${st.fixture ? `<button class="op" onclick="followReplay()"
                 title="re-inject the same FlowFile and step it again">↻ Replay fixture</button>` : ""}
            ${st.runs ? `<button class="op" onclick="followCompare()"
                 title="what changed since the previous run">Compare runs</button>` : ""}
          </div>
          ${st.fixture ? `<p class="muted">Fixture: ${st.fixture.content.length} byte(s) at
             ${esc(st.fixture.label || st.fixture.target)}${st.runs ? ` · run ${st.runs + 1}` : ""}</p>` : ""}
          <pre id="cmpout" class="muted" style="display:none;white-space:pre-wrap"></pre>
          ${watchTable(st)}
          <h3>Hops on this branch</h3>` + (st.hops.length
            ? st.hops.map((h, i) => hopCard(h, i, flash.has(h.event_id))).join("")
            : `<p class="muted">No hops yet — hit Step.</p>`);
      }
    }

    if (active === "alerts") {
      // Everything here is served from the watcher's in-memory state — no
      // NiFi call — so this tab is cheap to leave open and cheap to poll.
      const st = await api("/api/alerts");
      const s = st.summary || {};
      const showAll = localStorage.getItem("niflow.alertsall") === "1";
      const rows = (st.alerts || []).filter(a => showAll || a.state === "active");
      const last = s.last_tick ? new Date(s.last_tick * 1000).toLocaleTimeString() : "never";
      view.innerHTML = `
        <div class="bar">
          <span class="pill" style="color:${st.running ? "var(--ok)" : "var(--muted)"}">
            ${st.running ? "watching" : "paused"} ${esc(s.watching || "root")} — checked ${esc(last)}</span>
          <span class="pill" title="components with an established healthy baseline">
            ${s.established || 0} / ${s.tracked || 0} baselined</span>
          ${s.chronic ? `<span class="pill" title="failing since before the watcher started — no baseline, so no alert">${s.chronic} chronic</span>` : ""}
          <label class="meta"><input type="checkbox" id="aall" ${showAll ? "checked" : ""}> show resolved</label>
          <span style="flex:1"></span>
          <button class="op" onclick="alertsCheck()" title="run one health poll right now">Check now</button>
          <button class="op" onclick="alertsWatch(${st.running ? "false" : "true"})">${st.running ? "Pause" : "Resume"} watching</button>
          <button class="op" onclick="alertsClear()" title="forget resolved alerts">Clear resolved</button>
        </div>
        ${s.error ? `<p class="muted">last watcher error: ${esc(s.error)}</p>` : ""}
        ${rows.length ? rows.map(alertCard).join("") : `<p class="muted">
           Nothing has broken since the watcher started. It is baselining
           ${s.tracked || 0} component(s) — a component has to look healthy for
           ${esc(fmtDur(s.baseline_seconds))} before a failure counts as
           "it <i>was</i> working". State lives in <code>${esc(st.state_file || "")}</code>.</p>`}`;
      $("#aall").onchange = e => {
        localStorage.setItem("niflow.alertsall", e.target.checked ? "1" : "0"); render(); };
    }

    if (active === "bulletins") {
      const [items, tops] = await Promise.all([api("/api/bulletins"), flowTops()]);
      const group = currentFlow(tops);
      // A bulletin only carries its group id; map it back to a flow. Unknown
      // ids (root, controller-level) count as top level.
      const rows = items.filter(b =>
        group === "*" || topOf(window._groupPaths[b.group_id] ?? "") === group);
      view.innerHTML = `
        <div class="bar">${flowSelect(tops, group)}
          <span class="pill">${rows.length} / ${items.length}</span></div>` + (rows.length ? `
        <table><tr><th>When</th><th>Level</th><th>Source</th><th>Message</th></tr>
        ${rows.map(b => `<tr><td class="muted">${esc(b.time ?? b.timestamp ?? "")}</td>
          <td class="state-${b.level === "ERROR" ? "INVALID" : "DISABLED"}">${esc(b.level ?? "")}</td>
          <td>${compLink(b.group_id, b.source_id, b.source ?? b.source_name ?? "")}</td>
          <td>${esc(b.message ?? "")}</td></tr>`).join("")}
        </table>` : `<p class="muted">No bulletins${group === "*" ? "" : " for this flow"}.</p>`);
      bindFlowSelect();
    }

    if (active === "errors") {
      const [items, tops] = await Promise.all([api("/api/errors"), flowTops()]);
      const group = currentFlow(tops);
      const rows = items.filter(e => group === "*" || topOf(e.path) === group);
      view.innerHTML = `
        <div class="bar">${flowSelect(tops, group)}
          <span class="pill">${rows.length} / ${items.length}</span></div>` + (rows.length ? `
        <table><tr><th>Component</th><th>Problem</th></tr>
        ${rows.map(e => `<tr><td>${esc(e.path ? e.path + "/" : "")}<b>${compLink(e.group_id, e.id, e.name ?? "", "open " + (e.name ?? "") + " in NiFi")}</b></td>
          <td>${esc((e.errors ?? [e.message]).join("; "))}</td></tr>`).join("")}
        </table>` : `<p class="muted">No validation errors${group === "*" ? " — every processor is happy" : " for this flow"}.</p>`);
      bindFlowSelect();
    }

    if (active === "explain") {
      // One doc per group down to the chosen depth (default: just the
      // selected group, nested groups summarised in a line each), generated
      // by the configured LLM and saved to docs/explanations/ — the
      // fingerprint tells fresh from stale, and the plan pill says how many
      // documents/LLM calls the button is about to spend.
      const groups = await api("/api/groups");
      const paths = ["", ...groups.map(g => g.path)];
      let sel = localStorage.getItem("niflow.explaingroup") ?? "";
      if (!paths.includes(sel)) sel = "";
      const depth = localStorage.getItem("niflow.explaindepth") ?? "1";
      const st = await api(`/api/explain?group=${encodeURIComponent(sel || "root")}&depth=${encodeURIComponent(depth)}`);
      const badge = !st.exists
        ? `<span class="pill">not generated yet</span>`
        : st.outdated
          ? `<span class="pill" style="color:var(--warn)">flow changed since ${esc(st.generated || "?")} — regenerate?</span>`
          : `<span class="pill" style="color:var(--ok)">up to date (${esc(st.generated || "")})</span>`;
      const fresh = st.exists && !st.outdated;
      view.innerHTML = `
        <div class="bar">
          <select id="eg" title="which group to explain">
            ${paths.map(p => `<option value="${esc(p)}"${p === sel ? " selected" : ""}>${p ? esc(p) : "(root canvas)"}</option>`).join("")}
          </select>
          <select id="ed" title="how deep to document (deeper = one file and one LLM call per nested group)">
            ${[["1", "this group only"], ["2", "+ 1 level down"], ["3", "+ 2 levels down"], ["0", "everything below"]]
              .map(([v, t]) => `<option value="${v}"${v === depth ? " selected" : ""}>${t}</option>`).join("")}
          </select>
          ${badge}
          <span class="pill" title="what the button will spend">${st.documents} doc(s), ${st.llm_calls} LLM call(s)${st.summarised_groups ? ` — ${st.summarised_groups} deeper group(s) summarised in one line` : ""}</span>
          ${st.backend ? `<span class="pill" title="which LLM generates these docs">${esc(st.backend)}</span>` : ""}
          <span style="flex:1"></span>
          ${st.configured ? `<button class="op" onclick="explainGen(${fresh}, ${st.documents}, ${st.llm_calls})">${st.exists ? "Regenerate" : "Generate explanation"}</button>` : ""}
        </div>
        ${st.configured ? "" : `<p class="muted">LLM off — with the <b>Claude Code</b> CLI installed and logged in,
           no key is needed: niflow finds <code>claude</code> on PATH (pin it with
           <code>NIFLOW_LLM_PROVIDER=claude-code</code>). With a key instead: put <code>GOOGLE_API_KEY=…</code> in
           <code>.env</code> (git-ignored) and niflow uses Gemini's cheapest model. Or point
           <code>NIFLOW_LLM_URL</code> + <code>NIFLOW_LLM_MODEL</code> at any endpoint (local Ollama:
           <code>http://localhost:11434/v1</code>).</p>`}
        ${st.doc ? `<article class="doc">${md(st.doc)}</article>`
                 : st.configured ? `<p class="muted">No explanation for this group yet — a saved plain-English
                     walkthrough appears here once you generate it.</p>` : ""}`;
      $("#eg").onchange = e => { localStorage.setItem("niflow.explaingroup", e.target.value); render(); };
      $("#ed").onchange = e => { localStorage.setItem("niflow.explaindepth", e.target.value); render(); };
    }

    if (active === "flows") {
      const flows = await api("/api/flows");
      view.innerHTML = `
        <p class="muted">Flow modules found in <code>flows/</code>. Plan is read-only;
           Push applies the plan incrementally (queues and state survive).</p>
        <table><tr><th>File</th><th></th></tr>
        ${flows.map(f => `<tr><td>${esc(f)}</td><td>
            <button class="op" onclick="planFlow('${esc(f)}')">Plan</button>
            <button class="op" onclick="pushFlow('${esc(f)}')">Push (update)</button>
          </td></tr>`).join("")}
        </table>
        <div id="planout"></div>`;
      if (!flows.length) view.innerHTML = `<p class="muted">No .py files in flows/.</p>`;
    }
  } catch (err) {
    view.innerHTML = `<pre>Error: ${esc(err.message)}</pre>`;
  }
}

function openQueue(i) {
  const c = window._qRows[i];
  inspector = {connId: c.id, groupId: c.group_id, uuid: null,
               label: `${c.source} → ${c.destination}`};
  render();
}

// Purge one queue. `i` indexes the rendered queue rows; -1 means "the queue
// the inspector is currently showing".
async function purgeQueue(i) {
  const c = i < 0 ? {id: inspector.connId, label: inspector.label} : window._qRows[i];
  const label = c.label || `${c.source} → ${c.destination}`;
  if (!ask(`Drop every FlowFile queued in ${label}?\n\nThis cannot be undone.`)) return;
  status(`purging ${label}…`);
  try { const r = await post(`/api/queues/${c.id}/purge`);
        status(`purged ${label} ✓ — dropped ${r.dropped || "nothing"}`); }
  catch (e) { status(`purge failed: ${e.message}`); }
  render();
}

// Purge every queue in the flow the dropdown has selected — never the whole
// instance while the user is looking at one flow.
async function purgeFlow() {
  const g = localStorage.getItem(FLOWKEY) ?? "*";
  const whole = g === "*" || g === "";
  const scope = whole ? "EVERY queue in this NiFi" : `every queue in ${g}`;
  if (!ask(`Drop the contents of ${scope}?\n\nThis cannot be undone.`)) return;
  status(`purging ${whole ? "all queues" : g}…`);
  try { const r = await post("/api/group/purge", {group: whole ? "root" : g});
        status(`purge ✓ — dropped ${r.dropped || "nothing"}`); }
  catch (e) { status(`purge failed: ${e.message}`); }
  render();
}

async function queueOp(connId, which) {
  status(`run ${which} once…`);
  try { const r = await post(`/api/queues/${connId}/run-${which}-once`);
        status(`ran ${r.ran || which} once ✓`); }
  catch (e) { status(`run ${which} once failed: ${e.message}`); }
  render();
}
function traceGo() { traceUuid = $("#tu").value.trim() || null; render(); }
function traceJump(u) { traceUuid = u; active = "trace"; inspector = null; nav(); render(); }
function hopAttrs(i) {
  // Toggle the hop's full attribute map (the table above shows only changes).
  const pre = $(`#hopx${i}`);
  if (pre.dataset.kind === "attrs" && pre.style.display !== "none") {
    pre.style.display = "none"; return;
  }
  pre.dataset.kind = "attrs";
  pre.textContent = JSON.stringify(window._hops[i].attributes, null, 2);
  pre.style.display = "block";
}
async function hopContent(i, dir) {
  status(`${dir} content…`);
  try {
    const r = await api(`/api/trace/content?event_id=${window._hops[i].event_id}&direction=${dir}`);
    const pre = $(`#hopx${i}`);
    pre.dataset.kind = dir;
    pre.textContent = r.content || "(empty)";
    pre.style.display = "block";
    status("");
  } catch (e) { status(`content failed: ${e.message}`); }
}
// --- Follow tab actions ----------------------------------------------
// Every one of them posts, reports the outcome in the status line, and
// re-renders; the hops the action produced are flashed (followFlash).
async function followAct(what, body) {
  status(`${what}…`);
  try {
    const r = await post(`/api/follow/${what}`, body || {});
    followFlash = r.fresh || [];
    const o = r.outcome || {};
    const bits = [];
    if (o.status) bits.push(o.status);
    if (o.runs > 1) bits.push(`ran ${o.runs}x`);
    if (o.message) bits.push(o.message);
    status(bits.join(" — ") || `${what} ✓`);
  } catch (e) { status(`${what} failed: ${e.message}`); }
  render();
}
function followStep() { followAct("step"); }
async function followStart(i) {
  const tops = await flowTops();
  const sel = currentFlow(tops);
  const group = (sel === "*" || sel === "") ? "root" : sel;
  if (i < 0) return followAct("start", {group, resume: true});
  const entry = window._entries[i];
  const premute = ($("#premute") || {}).value || "";
  if (!ask(`Start stepping at ${entry.label}?\n\n${group} will be STOPPED so nothing
races the stepper (End & restore puts it back).`)) return;
  followAct("start", {group, entry,
                      mutes: premute.split(",").map(x => x.trim()).filter(Boolean)});
}
function watchTable(st) {
  // The same hop x attribute table `w` prints in the CLI: rows are hops,
  // columns are the watched attributes, and a cell says whether this hop
  // added (+), changed (~) or removed (-) it.
  if (!st.watch_columns.length) return "";
  if (!st.watch_rows.length) return `<p class="muted">Nothing to tabulate yet — hit Step.</p>`;
  const mark = {changed: "~", added: "+", removed: "-"};
  return `<h3>Watching</h3><table>
    <tr><th>Hop</th><th>Component</th>${st.watch_columns.map(c => `<th>${esc(c)}</th>`).join("")}</tr>
    ${st.watch_rows.map(r => `<tr>
      <td>${esc(r.hop)}</td><td class="muted">${esc(r.component)}</td>
      ${st.watch_columns.map(c => {
        const cell = r.cells[c];
        const m = mark[cell.status] || "";
        return `<td class="${cell.status === "same" ? "muted" : ""}"
                    title="${esc(cell.status)}">${esc(m)}${cell.value === null
                      ? "·" : esc(cell.value)}</td>`;
      }).join("")}
    </tr>`).join("")}
  </table>`;
}
async function followInject() {
  const tops = await flowTops();
  const sel = currentFlow(tops);
  const group = (sel === "*" || sel === "") ? "root" : sel;
  const target = ($("#ftarget") || {}).value.trim();
  if (!target) { status("name the processor (or nested input port) to inject at"); return; }
  const attributes = {};
  for (const pair of (($("#fattrs") || {}).value || "").split(",")) {
    const [k, ...rest] = pair.split("=");
    if (k.trim() && rest.length) attributes[k.trim()] = rest.join("=");
  }
  const premute = ($("#premute") || {}).value || "";
  if (!ask(`Inject a FlowFile at ${target} and start stepping?\n\n${group} will be
STOPPED so nothing races the stepper (End & restore puts it back).`)) return;
  followAct("start", {group,
                      inject: {target, content: ($("#fcontent") || {}).value || "", attributes},
                      mutes: premute.split(",").map(x => x.trim()).filter(Boolean)});
}
function followWatch() {
  const spec = ($("#wspec") || {}).value.trim();
  if (spec) followAct("watch", {spec});
}
function followUnwatch(spec) { followAct("watch", {spec, remove: true}); }
function followReplay() {
  if (!ask("Re-inject the same FlowFile and start the journey again?\n\nThe run so far is kept, so Compare can show what changed.")) return;
  followAct("replay");
}
async function followCompare() {
  status("comparing…");
  try {
    const r = await api("/api/follow/compare");
    const box = $("#cmpout");
    box.style.display = "block";
    box.textContent = r.text;
    status("compare ✓");
  } catch (e) { status(`compare failed: ${e.message}`); }
}
function followSwitch(i) { followAct("switch", {uuid: window._branches[i].uuid}); }
function followMuteBranch(i) { followAct("mute", {spec: "uuid:" + window._branches[i].uuid}); }
function followUnmute(spec) { followAct("unmute", {spec}); }
function followMute() {
  const spec = ($("#mspec") || {}).value.trim();
  if (spec) followAct("mute", {spec});
}
async function followStop(restore) {
  status("ending session…");
  try {
    const r = await post("/api/follow/stop", {restore});
    const kept = r.injector_kept ? " (the injector stays: its FlowFile has not moved)" : "";
    status((restore ? `session ended — restarted ${r.restored} processor(s)`
                    : "session ended — the group is left stopped") + kept);
  } catch (e) { status(`stop failed: ${e.message}`); }
  render();
}

function tidyDir() { return localStorage.getItem("niflow.tidydir") || "horizontal"; }
async function tidyCanvas() {
  // Selected flow -> tidy that flow (and its children). "All flows" -> the
  // whole instance from root down. "(top level)" -> just the root canvas.
  const g = localStorage.getItem(FLOWKEY) ?? "*";
  const target = g === "*" || g === "" ? "root" : g;
  const scope = g === "*" ? "the whole canvas" : g === "" ? "the top-level canvas" : g;
  if (!ask(`Auto-arrange ${scope}? Hand-placed positions will be overwritten.`)) return;
  status(`tidying ${scope}…`);
  try { const r = await post("/api/tidy", {group: target, layout: tidyDir(), recurse: g !== ""});
        status(`tidy ✓ moved ${r.moved} component(s) — refresh the NiFi canvas to see it`); }
  catch (e) { status(`tidy failed: ${e.message}`); }
}
function md(src) {
  // Tiny renderer for the explanation docs: headings, lists, bold/italic,
  // inline code, hr. Everything is escaped first — the doc is LLM output.
  const inline = s => esc(s)
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/\*\*([^*]+)\*\*/g, "<b>$1</b>")
    .replace(/\*([^*]+)\*/g, "<i>$1</i>");
  const out = []; let list = false; let para = [];
  const flush = () => { if (para.length) { out.push(`<p>${inline(para.join(" "))}</p>`); para = []; } };
  const endList = () => { if (list) { out.push("</ul>"); list = false; } };
  for (const raw of src.split("\n")) {
    const line = raw.replace(/<!--[^>]*-->/g, "").trimEnd();
    if (!line.trim()) { flush(); endList(); continue; }
    const h = line.match(/^(#{1,4})\s+(.*)/);
    if (h) { flush(); endList(); const n = h[1].length + 1; out.push(`<h${n}>${inline(h[2])}</h${n}>`); continue; }
    if (/^\s*---+\s*$/.test(line)) { flush(); endList(); out.push("<hr>"); continue; }
    const li = line.match(/^\s*[-*]\s+(.*)/);
    if (li) { flush(); if (!list) { out.push("<ul>"); list = true; } out.push(`<li>${inline(li[1])}</li>`); continue; }
    para.push(line.trim());
  }
  flush(); endList();
  return out.join("\n");
}
async function explainGen(fresh, docs, calls) {
  // Missing/outdated -> regenerate whatever's stale in scope; already up to
  // date -> force a rewrite of every document in scope. Scope is the depth
  // select, and anything past it is a one-line summary, not a document —
  // confirm the count first, because deep canvases used to spider.
  const g = localStorage.getItem("niflow.explaingroup") || "";
  const d = localStorage.getItem("niflow.explaindepth") ?? "1";
  const n = fresh ? docs : calls;  // forcing rewrites the current ones too
  if (n > 1 && !confirm(`Generate ${n} document(s) with ${n} LLM call(s)?`)) return;
  status(`generating explanation… (${n} LLM call(s) — this can take a minute)`);
  try {
    const r = await post("/api/explain", {group: g || "root", force: fresh, depth: Number(d)});
    const done = r.results.filter(x => x.status === "generated").length;
    status(`explanation ✓ — ${done} document(s) written`);
  } catch (e) { status(`explain failed: ${e.message}`); }
  render();
}
function toggleStar(i) {
  // Keys are path/name (stable across pushes and rebuilds, unlike ids); the
  // row index -> key table dodges quoting hostile names into onclick attrs.
  const k = window._rowKeys[i];
  const stars = new Set(JSON.parse(localStorage.getItem("niflow.stars") || "[]"));
  stars.has(k) ? stars.delete(k) : stars.add(k);
  localStorage.setItem("niflow.stars", JSON.stringify([...stars]));
  render();
}
async function procOp(id, op) {
  status(`${op}…`);
  try { await post(`/api/processors/${id}/${op}`); status(`${op} ✓`); }
  catch (e) { status(`${op} failed: ${e.message}`); }
  render();
}
async function groupOp(op) {
  status(`${op}…`);
  try { const r = await post(`/api/group/${op}`);
        status(`${op} ✓${r.dropped ? " — dropped " + r.dropped : ""}`); }
  catch (e) { status(`${op} failed: ${e.message}`); }
  render();
}
async function planFlow(file) {
  status("planning…");
  try {
    const r = await api(`/api/plan?file=${encodeURIComponent(file)}`);
    const issues = r.issues.length
      ? `<h3>Validation issues</h3><pre>${esc(r.issues.map(i => `${i.component}: ${i.message}`).join("\n"))}</pre>` : "";
    $("#planout").innerHTML = `<h3>Plan for ${esc(file)}</h3><pre>${esc(r.plan)}</pre>${issues}`;
    status("plan ready");
  } catch (e) { status(`plan failed: ${e.message}`); }
}
async function pushFlow(file) {
  if (!ask(`Apply changes from ${file} to the live group?`)) return;
  status("pushing…");
  window._procIx = null;  // a push can renumber components — drop the id cache
  try { const r = await post("/api/push", {file, update: true});
        status(`push ✓ — ${r.applied} change(s) applied`); }
  catch (e) { status(`push failed: ${e.message}`); }
}

// --- alerts ----------------------------------------------------------
// The sentence these cards exist to say: "this was healthy, you changed
// nothing, it broke at 14:02, and the cause was outside NiFi."
function fmtDur(sec) {
  if (sec == null) return "?";
  sec = Math.max(0, Math.round(sec));
  if (sec < 60) return sec + "s";
  if (sec < 3600) return Math.floor(sec / 60) + "m";
  if (sec < 86400) return Math.floor(sec / 3600) + "h" + String(Math.floor(sec % 3600 / 60)).padStart(2, "0") + "m";
  return Math.floor(sec / 86400) + "d";
}
const clock = ts => ts ? new Date(ts * 1000).toLocaleTimeString() : "?";
function alertCard(a) {
  const cls = `${a.category || "unknown"}${a.state !== "active" ? " resolved" : ""}${a.acknowledged ? " acked" : ""}`;
  const badge = {external: "EXTERNAL — not NiFi, not your flow",
                 internal: "INTERNAL — something on our side",
                 unknown: "UNKNOWN — not enough evidence to say"}[a.category] || a.category;
  const was = a.healthy_for
    ? `was healthy for <b>${esc(a.healthy_for)}</b>` : "was healthy";
  const proc = a.last_processed
    ? ` (last processed ${clock(a.last_processed)})`
    : (a.ever_processed ? "" : " (running, but no FlowFiles seen through it)");
  return `<div class="alert ${cls}">
    <h3>${compLink(a.group_id, a.component_id, a.component || "(component)", "open in NiFi")}
      <span class="muted">${esc(a.path ? a.path + " · " : "")}${esc(a.component_type || "")}</span>
      <span class="pill">${esc(badge)}</span>
      ${a.state !== "active" ? `<span class="pill" style="color:var(--ok)">recovered ${clock(a.resolved_at)}${a.down_for ? " after " + esc(a.down_for) : ""}</span>` : ""}
      ${a.occurrences > 1 ? `<span class="pill">${a.occurrences}×</span>` : ""}
    </h3>
    <div>${was}${esc(proc)}, <b>broke at ${clock(a.broke_at)}</b></div>
    <div class="why">${esc(a.summary || "")}</div>
    ${a.hint ? `<div class="hint">→ ${esc(a.hint)}</div>` : ""}
    ${a.confidence && a.confidence !== "high"
      ? `<div class="hint">confidence: ${esc(a.confidence)}${a.pattern ? ` (pattern ${esc(a.pattern)})` : ""} — signal: ${esc(a.signal || "")}</div>` : ""}
    ${(a.evidence || []).length ? `<div class="ev">${(a.evidence || []).map(esc).join("\n")}</div>` : ""}
    <div class="bar" style="margin:.5rem 0 0">
      <button class="op" onclick="alertAck('${esc(a.id)}',${a.acknowledged ? "false" : "true"})">
        ${a.acknowledged ? "Un-acknowledge" : "Acknowledge"}</button>
      <button class="op" onclick="alertDismiss('${esc(a.id)}')">Dismiss</button>
    </div>
  </div>`;
}
async function alertAck(id, on) {
  try { await post("/api/alerts/ack", {id, on}); } catch (e) { status(e.message); }
  alertPoll(); render();
}
async function alertDismiss(id) {
  if (!ask("Dismiss this alert? (it can fire again if the component breaks anew)")) return;
  try { await post("/api/alerts/dismiss", {id}); } catch (e) { status(e.message); }
  alertPoll(); render();
}
async function alertsClear() {
  try { const r = await post("/api/alerts/clear", {}); status(`cleared ${r.cleared} resolved alert(s)`); }
  catch (e) { status(e.message); }
  render();
}
async function alertsCheck() {
  status("checking…");
  try { await post("/api/alerts/check", {}); status("checked"); } catch (e) { status(e.message); }
  alertPoll(); render();
}
async function alertsWatch(on) {
  try { await post("/api/alerts/watch", {on}); } catch (e) { status(e.message); }
  render();
}
// The badge/banner poll runs on EVERY tab (and even with auto-refresh off):
// nobody sits on the Alerts tab waiting for something to break. It is a
// read of the watcher's in-memory counters, not a NiFi call.
async function alertPoll() {
  try {
    const s = await api("/api/alerts/summary");
    const n = s.unacknowledged || 0;
    const bar = $("#alertbar");
    if (n) {
      const cat = s.external ? "ext" : s.internal ? "int" : "unk";
      bar.className = "on " + cat;
      bar.innerHTML = `<b>${n} alert${n > 1 ? "s" : ""}</b>` +
        `<span>${esc(s.newest_component || "")}: ${esc(s.newest_summary || "")}</span>` +
        `<span style="flex:1"></span><span>click to open Alerts</span>`;
    } else {
      bar.className = "";
      bar.innerHTML = "";
    }
    if (n !== window._alertCount) { window._alertCount = n; nav(); }
  } catch (e) { /* server restarting — try again next tick */ }
}

// Auto-refresh: on unless this browser turned it off, and skipped whenever a
// tick would fight the user or pointlessly hammer NiFi.
const AUTOKEY = "niflow.autorefresh";
const AUTO_MS = 3000;
// Tabs that poll badly: Trace runs a provenance query per tick (create →
// poll → delete, plus one fetch per event), Explain re-fingerprints the flow
// to date its doc, and Flows is a static directory listing. All three stay
// on-demand; the cheap status reads (processors, queues, bulletins, errors)
// are what the 3s tick is for.
// Follow joins them: stepping is deliberate (one run-once per click) and a
// 3s re-render would re-play the attribute flash the user is reading.
const NO_POLL = new Set(["trace", "follow", "explain", "flows"]);
function autoOn() { return (localStorage.getItem(AUTOKEY) ?? "1") === "1"; }
function autoTick() {
  if (!autoOn() || document.hidden) return;
  if (inflight || modal) return;          // mid-mutation / mid-confirm
  if (NO_POLL.has(active)) return;        // expensive or static tabs
  if (inspector) return;                  // don't yank a drill-down away
  const el = document.activeElement;      // mid-typing / dropdown open
  if (el && ["INPUT", "SELECT", "TEXTAREA"].includes(el.tagName)) return;
  render();
}

(async () => {
  try {
    const a = await api("/api/about");
    window._uiBase = a.ui || "";  // deep links need this before the first paint
    $("#about").textContent = `NiFi ${a.version} @ ${a.base} (auth: ${a.auth})`;
    if (a.reload) setInterval(async () => {   // dev mode: follow server restarts
      try { if ((await api("/api/about")).boot !== a.boot) location.reload(); }
      catch (e) {} // server mid-restart — try again next tick
    }, 1500);
  } catch (e) { $("#about").textContent = `not connected: ${e.message}`; }
  $("#auto").checked = autoOn();
  $("#auto").onchange = e =>
    localStorage.setItem(AUTOKEY, e.target.checked ? "1" : "0");
  nav(); render();
  setInterval(autoTick, AUTO_MS);
  alertPoll();
  setInterval(alertPoll, AUTO_MS);   // badge + banner, every tab, always on
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
