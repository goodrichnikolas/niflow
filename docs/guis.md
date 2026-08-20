# The two GUIs

Both are optional; everything they do has a CLI equivalent.

```bash
make webgui        # browser helper  (niflow-web)  — no extra dependencies
make gui           # desktop helper  (niflow-gui)  — needs PyQt
```

## Browser helper (`niflow-web`)

A stdlib HTTP server on `127.0.0.1:7777` serving one embedded page plus a small
JSON API. It opens your default browser (the *Windows* browser under WSL). No
PyQt, no display server, works anywhere a browser can reach localhost.

What is on it:

* **Processors** — filter, state, run-once / start / stop per row, a
  top-level-flow dropdown, starred rows pinned to the top (remembered per
  browser).
* **Queues** — live counts, click through to FlowFiles and their
  attributes/content, **purge one queue** or every queue in the selected flow.
  Purging is confirmed and reports what was dropped; the flow-wide purge is
  scoped to the flow you picked, never root behind your back.
* **Trace** — [`niflow trace`](trace-and-follow.md) as a tab: one FlowFile's
  journey, attribute diffs per hop, payload on demand.
* **Follow** — the live stepper as a debugger: pick a start point, hit Step,
  watch changed/added/removed attributes flash, mute the branches you do not
  care about (muting is a view decision — NiFi keeps running them).
* **Explain** — [`niflow explain`](explain.md) per group, with the
  documents/LLM-calls count shown before you spend them.
* **Alerts** — the [`watch`](watch.md) health watcher, with a badge and banner
  that update from every tab.
* **Errors / Bulletins** — including controller services, which is the
  commonest reason a whole flow sits idle.
* **Flows** — the modules under `flows/`: semantic plan preview and incremental
  push.
* One-click **Tidy canvas**, group-wide start / stop / stop+drain / purge.

Two things make it pleasant to live in:

* **Auto-refresh (3s) is on by default**, and skipped while a mutation is in
  flight, while a confirm dialog is up, while an input has focus, while the
  FlowFile drill-down is open, on a hidden tab, and on the expensive tabs
  (Trace/Explain/Flows).
* **Every component mention is a link** that opens NiFi centred on that
  component — processors, errors, bulletins, queue endpoints, trace hops.

`--reload` (what `make webgui` uses) restarts the server when any niflow source
file changes and open pages reload themselves.

All NiFi calls are serialised through one lock: `requests.Session` is not
thread-safe and the HTTP server is threaded.

## Desktop helper (`niflow-gui`)

The original PyQt app, for people who prefer a window to a tab. Same idea —
collapse the click-marathons of a deep flow:

* **Run File** — trigger any `GenerateFlowFile` in the whole tree once, from a
  dropdown, no matter how deep it lives.
* **Start All / Stop All / Stop & Drain All** — the whole tree at once,
  including the quiesced state a group must reach before it can be deleted.
* **Inspect FlowFiles** — queues → items → attributes + payload in three panes,
  plus a per-queue purge.
* **Find Processor** — type-to-search by name or type, opens it in the browser.
* **Pull / Push / Undo** — pull a group to a `.py`, push one back (validated
  and plan-previewed first), undo via the backup.
