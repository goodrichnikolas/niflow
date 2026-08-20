# Watch — "it was working, nothing changed, it broke at 14:02"

The 45-minute hunt this exists to end: something outside NiFi breaks — an
endpoint starts 404ing, a broker refuses a connection, a certificate expires —
and the flow just… stops producing. NiFi tells you *that* a processor is
unhappy (that is the bulletin board). It never tells you the sentence that
actually shortens the hunt:

```
CallOrdersApi was healthy for 42m (last processed 13:58:11), broke at 14:02:07
external: api-frontiers returned HTTP 404 for http://api-frontiers:9099/v1/orders
```

```bash
niflow watch                          # poll the whole canvas every 15s
niflow watch "Prod Flow"              # one group
niflow watch --once                   # cron shape: poll once, exit 1 if anything is alerting
niflow watch --list                   # what has been recorded, no polling
niflow watch --json                   # one JSON object per alert event, for piping
niflow watch --ack <alert-id>         # stop one alert shouting
niflow watch --clear                  # forget resolved alerts
```

The web GUI has the same thing as an **Alerts** tab, with a badge and a banner
that update from every tab — because nobody is sitting on the Alerts tab when
it happens.

## How it decides something broke

Three jobs per tick, on one recursive status read plus one bulletin read:

1. **Baseline.** Per component: does it look healthy, and is it doing work?
   Persisted under `.niflow-watch/`, so "healthy for three hours" survives a
   restart of the watcher and means something over days.
2. **Transition.** An alert fires on healthy → failing, and only for a
   component whose health was *established* (healthy continuously for
   `--baseline` seconds, default 120). Something that was already broken when
   you started watching is not news; it is recorded as chronic.
3. **Attribution.** The bulletin's message is matched against a pattern table:
   **external** (the endpoint, the broker, the host, the certificate),
   **internal** (our flow, our config, our disk), or an honest **unknown**. A
   confidently wrong attribution sends the analyst down exactly the wrong path,
   so "unknown" is what you get when the evidence does not separate the two.

## Why four signals and not just bulletins

Bulletins are the richest signal and they are **not sufficient**: on NiFi 1.24
an `InvokeHTTP` that starts getting HTTP 404 emits *no bulletin at all* — a 404
is a normal routing decision to the "No Retry" relationship. Verified live. So
the watcher also watches:

* `runStatus == "Invalid"` — a component that went yellow (internal);
* running → stopped — but not when several components in a group stop in the
  same tick, because that is a deliberate mass stop, not a break;
* **a failure route opening** — a connection whose relationships look like an
  error path ("failure", "No Retry", "retry", "unmatched") that carried nothing
  for the whole healthy baseline and is suddenly carrying FlowFiles. That is
  the silent-404 detector, and it is the one signal that catches a break NiFi
  never logs.

Only when an alert actually fires does it spend an expensive call: a provenance
probe of the offending component, which is where `invokehttp.status.code` and
`invokehttp.request.url` come from. `--no-probe` skips it.

## Teaching it your own errors

Work will have processors and error strings that cannot be seen from here. Drop
a JSON file at `.niflow-watch/patterns.json` (or point `NIFLOW_WATCH_PATTERNS`
at one):

```json
[{"name": "acme-gateway", "category": "external", "kind": "gateway",
  "regex": "AcmeGatewayException: (?P<code>\\w+)",
  "summary": "the Acme gateway rejected the call ({code})",
  "hint": "check the Acme status page", "confidence": "high"}]
```

User patterns are tried **before** the built-ins, so they can override them. A
malformed file is ignored rather than breaking the watcher.

`.niflow-watch/` holds real component names and error text from your flows, so
it is git-ignored, like `flows/`.
