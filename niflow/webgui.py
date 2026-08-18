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
* queues with live counts -> click through to FlowFiles -> attributes+content
* Trace tab: one FlowFile's provenance journey hop by hop — attribute
  diffs, relationship taken, payload per processor (``niflow trace``)
* group-wide start / stop / stop+drain / purge
* one-click "Tidy canvas": auto-arrange the selected flow (or everything)
  along its connections, left-to-right or top-to-bottom (``niflow tidy``)
* Explain tab: plain-English walkthrough per group (``niflow explain``) —
  generated once via the configured LLM, saved under ``docs/explanations/``,
  flagged outdated when the flow's logic changes
* bulletins and validation-error panels
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
                return 200, client.trace_flowfile(q("uuid"))
            if method == "GET" and path == "/api/trace/content":
                return 200, {"content": client.event_content(
                    q("event_id"), q("direction") or "output")}

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
                    return 200, {"ok": True, "dropped": client.purge_queues("root")}
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
                # Doc + freshness for one group; no LLM call happens here.
                return 200, client.explain_status(q("group") or "root")
            if method == "POST" and path == "/api/explain":
                results = client.explain_group(
                    body.get("group") or "root",
                    recurse=body.get("recurse", True),
                    force=body.get("force", False),
                )
                return 200, {"ok": True, "results": results,
                             **client.explain_status(body.get("group") or "root")}

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
) -> None:
    handler = type("BoundHandler", (_Handler,), {
        "client_ref": NiFiClient(config or NiFiConfig.from_env()),
        "lock": threading.Lock(),
        "flows_dir": Path(flows_dir),
    })
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
    args = parser.parse_args()
    serve(host=args.host, port=args.port, flows_dir=args.flows_dir,
          open_browser=not args.no_browser, reload=args.reload)


# ------------------------------------------------------------------- page


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NiFlow Helper</title>
<style>
  :root { --bg:#fff; --fg:#1a1a1a; --muted:#667; --line:#dfe3e8; --accent:#0b6bcb;
          --ok:#127a3d; --warn:#a15c07; --bad:#b42318; --chip:#f2f4f7; --pin:#fdf6e3; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#101418; --fg:#e6e9ec; --muted:#98a2b3; --line:#2a3138;
            --accent:#559ee8; --ok:#4cc38a; --warn:#e2b03a; --bad:#f97066; --chip:#1c232b;
            --pin:#241f12; }
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
  button.danger { color:var(--bad); }
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
</style>
</head>
<body>
<header>
  <h1>NiFlow Helper</h1>
  <span class="meta" id="about">connecting…</span>
  <label class="meta"><input type="checkbox" id="auto"> auto-refresh (3s)</label>
  <span id="status"></span>
</header>
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
const post = (path, body) => api(path, {method:"POST", headers:{"Content-Type":"application/json"},
                                        body: body ? JSON.stringify(body) : "{}"});
const status = msg => { $("#status").textContent = msg; };

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
  ["trace", "Trace"], ["bulletins", "Bulletins"], ["errors", "Errors"],
  ["explain", "Explain"], ["flows", "Flows"],
];
let active = "processors";
let inspector = null;   // {connId, label, uuid} drill-down state on the queues tab
let traceUuid = null;   // FlowFile the Trace tab is following (survives tab hops)

function nav() {
  $("#tabs").innerHTML = TABS.map(([k, label]) =>
    `<button data-tab="${k}" class="${k===active?'active':''}">${label}</button>`).join("");
  document.querySelectorAll("#tabs button").forEach(b =>
    b.onclick = () => { active = b.dataset.tab; inspector = null; nav(); render(); });
}

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
          <button class="op danger" onclick="groupOp('purge')">Purge Queues</button>
        </div>
        <table><tr><th></th><th>Processor</th><th>Type</th><th>State</th><th></th></tr>
        ${rows.map((p, i) => `${i === pinned && pinned ? '<tr class="gap"><td colspan="5"></td></tr>' : ""}<tr class="${i < pinned ? "pinned" : ""}">
          <td class="star ${stars.has(key(p)) ? "on" : ""}" onclick="toggleStar(${i})"
              title="${stars.has(key(p)) ? "unpin" : "pin to top"}">${stars.has(key(p)) ? "★" : "☆"}</td>
          <td>${esc(p.path ? p.path + "/" : "")}<b>${esc(p.name)}</b></td>
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
                onclick="traceJump('${inspector.uuid}')">Trace journey</button></div>
          <div class="split">
            <div><h3>Attributes</h3><pre>${esc(JSON.stringify(d.attributes ?? d, null, 2))}</pre></div>
            <div><h3>Content</h3><pre>${esc(d.content ?? "(no content view)")}</pre></div>
          </div>`;
      } else if (inspector) {
        const files = await api(`/api/flowfiles?connection_id=${inspector.connId}`);
        view.innerHTML = `
          <div class="bar"><button class="op" onclick="inspector=null;render()">← all queues</button>
            <span class="pill">${esc(inspector.label)} — ${files.length} FlowFile(s)</span>
            <span style="flex:1"></span>
            <button class="op" title="feeds this queue"
                onclick="queueOp(inspector.connId,'source')">Run source once</button>
            <button class="op" title="consumes this queue"
                onclick="queueOp(inspector.connId,'destination')">Run destination once</button></div>
          <table><tr><th>UUID</th><th>Filename</th><th>Size</th><th>Queued</th></tr>
          ${files.map(f => `<tr class="click" onclick="inspector.uuid='${f.uuid}';render()">
            <td class="muted">${esc(f.uuid)}</td><td>${esc(f.filename ?? "")}</td>
            <td>${esc(f.size ?? "")}</td><td>${esc(f.queued_duration ?? "")}</td></tr>`).join("")}
          </table>`;
      } else {
        const [queues, tops] = await Promise.all([api("/api/queues"), flowTops()]);
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
            <span class="pill">${rows.length} / ${queues.length}</span></div>
          <table><tr><th>Queue</th><th>Path</th><th>Queued</th><th></th></tr>
          ${rows.map(c => `<tr class="click"
              onclick="inspector={connId:'${c.id}',label:'${esc(c.source)} → ${esc(c.destination)}',uuid:null};render()">
            <td>${esc(c.source)} → ${esc(c.destination)}</td>
            <td class="muted">${esc(c.path)}</td>
            <td>${c.queued ? `<b>${esc(c.queued_label || c.queued)}</b>` : '<span class="muted">empty</span>'}</td>
            <td>
              <button class="op" title="run ${esc(c.source)} once (feeds this queue)"
                  onclick="event.stopPropagation();queueOp('${c.id}','source')">Src once</button>
              <button class="op" title="run ${esc(c.destination)} once (consumes this queue)"
                  onclick="event.stopPropagation();queueOp('${c.id}','destination')">Dest once</button>
            </td>
          </tr>`).join("")}
          </table>`;
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
      const changes = h => h.changes.length ? `
        <table><tr><th>Attribute</th><th>Before</th><th>After</th></tr>
        ${h.changes.map(c => `<tr><td>${esc(c.name)}</td>
          <td class="muted">${c.before === null ? "<i>(new)</i>" : esc(c.before)}</td>
          <td><b>${esc(c.after ?? "")}</b></td></tr>`).join("")}
        </table>` : `<span class="muted">no attribute changes</span>`;
      const kin = (label, uuids) => uuids.map(u => `<div class="muted">${label}
          ${esc(u)} <button class="op" onclick="traceJump('${esc(u)}')">trace</button></div>`).join("");
      view.innerHTML = `
        <div class="bar">
          <input type="text" id="tu" placeholder="FlowFile UUID" value="${esc(traceUuid || "")}">
          <button class="op" onclick="traceGo()">Trace</button>
          <span class="muted">paste a UUID, or open a FlowFile under Queues → “Trace journey”</span>
        </div>` + (!t ? "" : !t.hops.length
        ? `<p class="muted">No provenance events for this UUID — wrong id, or the
             events have aged out of the provenance repository.</p>`
        : t.hops.map((h, i) => `
          <div class="hop">
            <div class="hophead"><b>#${i + 1} ${esc(h.component || "(flow)")}</b>
              <span class="pill">${esc(h.event_type)}${h.relationship ? " → " + esc(h.relationship) : ""}</span>
              <span class="muted">${esc(h.time)} · ${esc(h.size)} B${h.content_equal === false ? " · content changed" : ""}</span>
              <span style="flex:1"></span>
              <button class="op" onclick="hopAttrs(${i})">Attributes</button>
              ${h.input_available ? `<button class="op" onclick="hopContent(${i},'input')">Content in</button>` : ""}
              ${h.output_available ? `<button class="op" onclick="hopContent(${i},'output')">Content out</button>` : ""}
            </div>
            ${changes(h)}
            ${kin("joined from", h.parents)}${kin("spawned", h.children)}
            <pre id="hopx${i}" style="display:none"></pre>
          </div>`).join(""));
      const tu = $("#tu");
      tu.onkeydown = e => { if (e.key === "Enter") traceGo(); };
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
          <td>${esc(b.source ?? b.source_name ?? "")}</td><td>${esc(b.message ?? "")}</td></tr>`).join("")}
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
        ${rows.map(e => `<tr><td>${esc(e.path ? e.path + "/" : "")}${esc(e.name ?? "")}</td>
          <td>${esc((e.errors ?? [e.message]).join("; "))}</td></tr>`).join("")}
        </table>` : `<p class="muted">No validation errors${group === "*" ? " — every processor is happy" : " for this flow"}.</p>`);
      bindFlowSelect();
    }

    if (active === "explain") {
      // One doc per group (any depth), generated by the configured LLM and
      // saved to docs/explanations/ — the fingerprint tells fresh from stale.
      const groups = await api("/api/groups");
      const paths = ["", ...groups.map(g => g.path)];
      let sel = localStorage.getItem("niflow.explaingroup") ?? "";
      if (!paths.includes(sel)) sel = "";
      const st = await api(`/api/explain?group=${encodeURIComponent(sel || "root")}`);
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
          ${badge}
          <span style="flex:1"></span>
          ${st.configured ? `<button class="op" onclick="explainGen(${fresh})">${st.exists ? "Regenerate" : "Generate explanation"}</button>` : ""}
        </div>
        ${st.configured ? "" : `<p class="muted">LLM off — easiest: put <code>GOOGLE_API_KEY=…</code> in <code>.env</code>
           (git-ignored) and niflow uses Gemini's cheapest model. Or point <code>NIFLOW_LLM_URL</code> +
           <code>NIFLOW_LLM_MODEL</code> at any endpoint (local Ollama: <code>http://localhost:11434/v1</code>).</p>`}
        ${st.doc ? `<article class="doc">${md(st.doc)}</article>`
                 : st.configured ? `<p class="muted">No explanation for this group yet — a saved plain-English
                     walkthrough appears here once you generate it.</p>` : ""}`;
      $("#eg").onchange = e => { localStorage.setItem("niflow.explaingroup", e.target.value); render(); };
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
function tidyDir() { return localStorage.getItem("niflow.tidydir") || "horizontal"; }
async function tidyCanvas() {
  // Selected flow -> tidy that flow (and its children). "All flows" -> the
  // whole instance from root down. "(top level)" -> just the root canvas.
  const g = localStorage.getItem(FLOWKEY) ?? "*";
  const target = g === "*" || g === "" ? "root" : g;
  const scope = g === "*" ? "the whole canvas" : g === "" ? "the top-level canvas" : g;
  if (!confirm(`Auto-arrange ${scope}? Hand-placed positions will be overwritten.`)) return;
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
async function explainGen(fresh) {
  // Missing/outdated -> regenerate whatever's stale in the subtree; already
  // up to date -> force just this group's doc (children summaries reused).
  const g = localStorage.getItem("niflow.explaingroup") || "";
  status("generating explanation… (LLM call — this can take a minute)");
  try {
    const r = await post("/api/explain", {group: g || "root", force: fresh, recurse: !fresh});
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
  if ((op === "purge") && !confirm("Drop the contents of EVERY queue?")) return;
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
  if (!confirm(`Apply changes from ${file} to the live group?`)) return;
  status("pushing…");
  try { const r = await post("/api/push", {file, update: true});
        status(`push ✓ — ${r.applied} change(s) applied`); }
  catch (e) { status(`push failed: ${e.message}`); }
}

let timer = null;
$("#auto").onchange = e => {
  if (e.target.checked) timer = setInterval(render, 3000);
  else { clearInterval(timer); timer = null; }
};

(async () => {
  nav(); render();
  try {
    const a = await api("/api/about");
    $("#about").textContent = `NiFi ${a.version} @ ${a.base} (auth: ${a.auth})`;
    if (a.reload) setInterval(async () => {   // dev mode: follow server restarts
      try { if ((await api("/api/about")).boot !== a.boot) location.reload(); }
      catch (e) {} // server mid-restart — try again next tick
    }, 1500);
  } catch (e) { $("#about").textContent = `not connected: ${e.message}`; }
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
