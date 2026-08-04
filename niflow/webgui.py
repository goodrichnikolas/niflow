"""Browser-based NiFlow helper — the click-reduction debug UI for nested flows.

Deep flows make trivial actions expensive in the NiFi UI: re-triggering a
GenerateFlowFile buried four groups down means drilling in, stopping it,
running it, and drilling back out — every single iteration. This helper keeps
those actions one click away, no matter how deep the component lives.

Zero extra dependencies: a stdlib :mod:`http.server` serving a single
embedded page plus a small JSON API bridging to :class:`NiFiClient`. Start
with ``niflow-web`` (or ``make webgui``); it binds to 127.0.0.1 and opens
your default browser (the *Windows* browser under WSL). It needs no display
server and works anywhere a browser can reach ``localhost``. Feature set:

* processor list with filter, state, run-once / start / stop per row
* queues with live counts -> click through to FlowFiles -> attributes+content
* group-wide start / stop / stop+drain / purge
* bulletins and validation-error panels
* flow files under ``flows/``: semantic plan preview and incremental push

All NiFi calls are serialised through one lock — ``requests.Session`` is
not thread-safe and the HTTP server is threading.
"""
from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from niflow.client import NiFiClient
from niflow.config import NiFiConfig
from niflow.utils import get_logger, open_url

logger = get_logger()


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
                }
            if method == "GET" and path == "/api/processors":
                return 200, client.find_processors()
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


def serve(
    config: Optional[NiFiConfig] = None,
    host: str = "127.0.0.1",
    port: int = 7777,
    flows_dir: str = "flows",
    open_browser: bool = True,
) -> None:
    handler = type("BoundHandler", (_Handler,), {
        "client_ref": NiFiClient(config or NiFiConfig.from_env()),
        "lock": threading.Lock(),
        "flows_dir": Path(flows_dir),
    })
    server = ThreadingHTTPServer((host, port), handler)
    url = f"http://{host}:{port}/"
    logger.info("NiFlow web helper on %s (Ctrl-C to stop)", url)
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
    args = parser.parse_args()
    serve(host=args.host, port=args.port, flows_dir=args.flows_dir,
          open_browser=not args.no_browser)


# ------------------------------------------------------------------- page


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NiFlow Helper</title>
<style>
  :root { --bg:#fff; --fg:#1a1a1a; --muted:#667; --line:#dfe3e8; --accent:#0b6bcb;
          --ok:#127a3d; --warn:#a15c07; --bad:#b42318; --chip:#f2f4f7; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#101418; --fg:#e6e9ec; --muted:#98a2b3; --line:#2a3138;
            --accent:#559ee8; --ok:#4cc38a; --warn:#e2b03a; --bad:#f97066; --chip:#1c232b; }
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
  .pill { background:var(--chip); border-radius:1rem; padding:.1rem .6rem; font-size:.8rem; }
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

const TABS = [
  ["processors", "Processors"], ["queues", "Queues & FlowFiles"],
  ["bulletins", "Bulletins"], ["errors", "Errors"], ["flows", "Flows"],
];
let active = "processors";
let inspector = null;   // {connId, label, uuid} drill-down state on the queues tab

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
      const procs = await api("/api/processors");
      const filter = window._pf || "";
      const rows = procs.filter(p => (`${p.path}/${p.name} ${p.type}`).toLowerCase()
                                     .includes(filter.toLowerCase()));
      view.innerHTML = `
        <div class="bar">
          <input type="text" id="pf" placeholder="filter by name / path / type" value="${esc(filter)}">
          <span class="pill">${rows.length} / ${procs.length}</span>
          <span style="flex:1"></span>
          <button class="op" onclick="groupOp('start')">Start All</button>
          <button class="op" onclick="groupOp('stop')">Stop All</button>
          <button class="op" onclick="groupOp('drain')">Stop &amp; Drain</button>
          <button class="op danger" onclick="groupOp('purge')">Purge Queues</button>
        </div>
        <table><tr><th>Processor</th><th>Type</th><th>State</th><th></th></tr>
        ${rows.map(p => `<tr>
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
    }

    if (active === "queues") {
      if (inspector && inspector.uuid) {
        const d = await api(`/api/flowfile?connection_id=${inspector.connId}&uuid=${inspector.uuid}`);
        view.innerHTML = `
          <div class="bar"><button class="op" onclick="inspector.uuid=null;render()">← back to ${esc(inspector.label)}</button></div>
          <div class="split">
            <div><h3>Attributes</h3><pre>${esc(JSON.stringify(d.attributes ?? d, null, 2))}</pre></div>
            <div><h3>Content</h3><pre>${esc(d.content ?? "(no content view)")}</pre></div>
          </div>`;
      } else if (inspector) {
        const files = await api(`/api/flowfiles?connection_id=${inspector.connId}`);
        view.innerHTML = `
          <div class="bar"><button class="op" onclick="inspector=null;render()">← all queues</button>
            <span class="pill">${esc(inspector.label)} — ${files.length} FlowFile(s)</span></div>
          <table><tr><th>UUID</th><th>Filename</th><th>Size</th><th>Queued</th></tr>
          ${files.map(f => `<tr class="click" onclick="inspector.uuid='${f.uuid}';render()">
            <td class="muted">${esc(f.uuid)}</td><td>${esc(f.filename ?? "")}</td>
            <td>${esc(f.size ?? "")}</td><td>${esc(f.queued_duration ?? "")}</td></tr>`).join("")}
          </table>`;
      } else {
        const queues = await api("/api/queues");
        view.innerHTML = `
          <table><tr><th>Queue</th><th>Path</th><th>Queued</th></tr>
          ${queues.map(c => `<tr class="click"
              onclick="inspector={connId:'${c.id}',label:'${esc(c.source)} → ${esc(c.destination)}',uuid:null};render()">
            <td>${esc(c.source)} → ${esc(c.destination)}</td>
            <td class="muted">${esc(c.path)}</td>
            <td>${c.queued ? `<b>${esc(c.queued_label || c.queued)}</b>` : '<span class="muted">empty</span>'}</td>
          </tr>`).join("")}
          </table>`;
      }
    }

    if (active === "bulletins") {
      const items = await api("/api/bulletins");
      view.innerHTML = items.length ? `
        <table><tr><th>When</th><th>Level</th><th>Source</th><th>Message</th></tr>
        ${items.map(b => `<tr><td class="muted">${esc(b.timestamp ?? "")}</td>
          <td class="state-${b.level === "ERROR" ? "INVALID" : "DISABLED"}">${esc(b.level ?? "")}</td>
          <td>${esc(b.source_name ?? b.sourceName ?? "")}</td><td>${esc(b.message ?? "")}</td></tr>`).join("")}
        </table>` : `<p class="muted">No bulletins.</p>`;
    }

    if (active === "errors") {
      const items = await api("/api/errors");
      view.innerHTML = items.length ? `
        <table><tr><th>Component</th><th>Problem</th></tr>
        ${items.map(e => `<tr><td>${esc(e.path ? e.path + "/" : "")}${esc(e.name ?? "")}</td>
          <td>${esc((e.errors ?? [e.message]).join("; "))}</td></tr>`).join("")}
        </table>` : `<p class="muted">No validation errors — every processor is happy.</p>`;
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
  } catch (e) { $("#about").textContent = `not connected: ${e.message}`; }
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
