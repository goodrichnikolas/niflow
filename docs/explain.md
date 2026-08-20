# Explain

`niflow explain` writes a plain-English walkthrough of a live group and saves it
under `docs/explanations/`. It is for the flow nobody remembers building.

```bash
niflow explain "Prod Flow"                 # this group, nested ones summarised
niflow explain "Prod Flow" --depth 2       # + a document per immediate child
niflow explain "Prod Flow" --all           # one document per nested group, however deep
niflow explain --force                     # regenerate even if up to date
```

It prints how many documents and LLM calls it is about to spend and asks first
(`-y` skips). **High level by default** — a deep group used to spider into
hundreds of documents, which is not what "explain this flow" means.

## What it writes

Per group: what the flow does end to end, what each stage contributes, where
data comes from and goes, and a **Gotchas** section — dead ends,
auto-terminated failure paths, mismatched schedules, primary-node-only
processors with incoming connections.

Run state is deliberately *not* part of it. Whether something is stopped is
operations, not flow logic, and including it invalidated every document on
every start/stop.

Each document records a fingerprint of the flow's *logic*, so the GUI can mark
it outdated when the flow actually changes rather than when someone restarts a
processor.

## Which LLM

Resolution order, first hit wins:

1. `NIFLOW_LLM_PROVIDER` if you set it (`claude-code`, `openai`, `anthropic`);
2. an explicit `NIFLOW_LLM_URL` + `NIFLOW_LLM_MODEL` (any OpenAI-compatible
   endpoint, including a local Ollama);
3. an API key — `GOOGLE_API_KEY`, or `NIFLOW_LLM_KEY`;
4. **the local Claude Code CLI**, if `claude` is on your PATH.

That last one is the point at work, where there is no API key but Claude Code
is installed. It runs headless with no session persistence, so it does not
litter `~/.claude` with transcripts of work flows. `NIFLOW_LLM_CLAUDE_BIN`
points at the binary when it is not on PATH.

Nothing here is required: with no LLM configured, `explain` says so and every
other niflow command carries on working.

## Where the output goes

`docs/explanations/`, one `.md` per group. That directory is git-ignored except
for the repo's own example flows — an explanation describes whatever flow you
pointed it at, which is the same exposure as `flows/` and one step less
obvious.

## Related

* `niflow diagram flows/prod.py -o doc.md` renders a Mermaid flowchart from a
  flow module — no LLM involved, good for a PR.
* `niflow tidy "Prod Flow"` auto-arranges the live canvas along its
  connections, which makes a flow readable before you ask anyone (human or
  model) to read it.
