"""Plain-English flow documentation: ``niflow explain`` and the webgui button.

Turns a live process group into a Markdown walkthrough an ETL engineer can
read in two minutes — where data enters, how often it runs, what each step
does to the FlowFile, where the flow splits, and where every branch ends.
The narration comes from the LLM configured in :mod:`niflow.llm`; without
one, status checks still work but generation raises
:class:`~niflow.llm.LLMUnavailable`.

One document per group (``docs/explanations/<path>.md``), but only down to
``depth`` levels (default 1: the group you asked about and nothing else).
Groups below the cut are not documents — they become one structural line
each under the parent's "## Nested groups", derived from their digest, so a
high-level explanation still says what every branch is without a per-child
LLM call. Deeper documents are an explicit opt-in (``--depth N`` / ``--all``)
and both front ends count the documents and LLM calls before spending them:
pointing explain at a deep canvas used to spider into hundreds of files.

Every document embeds a fingerprint of the flow *logic* it was written from
(positions, ids, and run state excluded, nested fingerprints included), so
the helper can tell "up to date" from "flow changed — regenerate?".
Regeneration is always an explicit request, never automatic.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

DOCS_DIR = "docs/explanations"

_META_RE = re.compile(r"<!--\s*niflow-explanation\s+([^>]*?)-->")
_ATTR_RE = re.compile(r'(\w+)="([^"]*)"')
_SUMMARY_RE = re.compile(r"\*\*Summary:?\*\*:?\s*(.+)")

_SYSTEM = """\
You explain Apache NiFi flows to ETL engineers who have never seen them.
You get a JSON digest of ONE process group: its processors (type, schedule,
configuration properties, auto-terminated relationships), the connections
between them (with the relationships each carries), input/output ports,
controller services, canvas labels, and one-line summaries of nested groups.

Write GitHub Markdown with exactly these sections, using exactly these
heading texts (never append anything to a heading):
- First line: "**Summary:**" followed by one sentence saying what the group
  does end to end.
- "## Walkthrough": narrate the flow in order, from where data enters (or is
  generated) to where each branch ends. Say how often sources run, what each
  step does to the FlowFile in plain words (attributes set or rewritten,
  content converted, NiFi Expression Language decoded into English), where
  the flow splits and what each branch is for, and how every branch
  terminates (a sink, a log, auto-terminated, or a port to another group).
- "## Nested groups" — only if there are any: one line per nested group,
  saying what it does and how it fits into the flow, based strictly on the
  summary given for it. Summaries marked "(structure only)" come from the
  child's configuration rather than a written document: describe those at
  that level and invent nothing beyond them.
- "## Services" — only if there are controller services: one line each on
  what it provides and which processors need it.
- "## Gotchas" — only when something would genuinely surprise or bite the
  reader: dead ends and relationships that go nowhere, relationships
  auto-terminated where the data looks important (failure especially),
  contradictory or dangerous configuration, a schedule that does not match
  the data rate the flow implies, primary-node-only processors fed by an
  incoming connection, back pressure or expiry settings that will silently
  drop data, credentials or endpoints hard-coded where a parameter belongs.
  Say nothing about which processors are running or stopped — that is
  today's operational state, not a property of the flow. Omit the section
  entirely when nothing qualifies; never pad it.

Rules: never invent details the digest does not contain — if something is
unclear, say so. Refer to processors by their names, not their types. Keep
it tight; a reader should finish in under two minutes."""


# ------------------------------------------------------------------ digest


def _short_type(qualified: str) -> str:
    return (qualified or "").rsplit(".", 1)[-1]


def _trimmed(properties: Optional[dict], limit: int = 200) -> Dict[str, str]:
    """Non-empty properties with long values clipped (sensitive ones come
    back from NiFi as null, so they drop out here on their own)."""
    out = {}
    for key, value in (properties or {}).items():
        if value is None or value == "":
            continue
        text = str(value)
        out[key] = text if len(text) <= limit else text[:limit] + "…"
    return out


def digest_tree(client, pg_id: str) -> dict:
    """The logic-only digest of a group and its whole subtree.

    Returns ``{"path", "digest", "fingerprint", "legacy_fingerprint",
    "children": [same shape]}``. The digest is everything an explanation
    needs and nothing cosmetic — canvas positions, bends, and ids stay out so
    moving boxes around never invalidates a document. Run state (RUNNING /
    STOPPED / DISABLED, and a service's ENABLED) is left out too: half a
    canvas is stopped at any moment, that is operations rather than logic,
    and digesting it both invalidated every document on a start/stop and
    tempted the model into listing stopped processors as "gotchas".

    ``legacy_fingerprint`` is the same hash *with* run state, i.e. the
    fingerprint niflow wrote before state was dropped; a document carrying
    it is still treated as current so the change did not mass-invalidate
    the docs already on disk. Safe to delete once they have all been
    regenerated (see :func:`_is_current`).

    A child's fingerprint feeds its parent's, so a change deep in the tree
    marks every enclosing document outdated.
    """
    pgf = client._get_json(f"/flow/process-groups/{pg_id}")["processGroupFlow"]
    flow = pgf.get("flow") or {}
    comp = client._get_json(f"/process-groups/{pg_id}")["component"]

    crumbs, bc = [], pgf.get("breadcrumb")
    while bc:
        crumbs.append((bc.get("breadcrumb") or {}).get("name", ""))
        bc = bc.get("parentBreadcrumb")
    path = "/".join(reversed(crumbs[:-1]))  # drop the root crumb

    children = []
    child_name: Dict[str, str] = {}
    for ent in flow.get("processGroups", []):
        c = ent["component"]
        child_name[c["id"]] = c.get("name", "")
        children.append(digest_tree(client, c["id"]))
    children.sort(key=lambda n: n["digest"]["group"])

    def end(ref: dict) -> str:
        name = ref.get("name") or (ref.get("type") or "").replace("_", " ").lower()
        gid = ref.get("groupId")
        if gid in child_name:  # a port on a nested group
            return f"{child_name[gid]} :: {name}"
        return name

    stateful_processors = []  # run state kept only for the legacy fingerprint
    for ent in flow.get("processors", []):
        c = ent["component"]
        cfg = c.get("config") or {}
        stateful_processors.append({
            "name": c.get("name", ""),
            "type": _short_type(c.get("type", "")),
            "state": c.get("state", ""),
            "schedule": {
                "strategy": cfg.get("schedulingStrategy", ""),
                "period": cfg.get("schedulingPeriod", ""),
            },
            "auto_terminated": sorted(cfg.get("autoTerminatedRelationships") or []),
            "properties": _trimmed(cfg.get("properties")),
            "comments": cfg.get("comments") or None,
        })
    stateful_processors.sort(key=lambda p: p["name"])
    processors = [{k: v for k, v in p.items() if k != "state"}
                  for p in stateful_processors]

    connections = []
    for ent in flow.get("connections", []):
        c = ent["component"]
        connections.append({
            "from": end(c.get("source") or {}),
            "to": end(c.get("destination") or {}),
            "relationships": sorted(c.get("selectedRelationships") or []),
        })
    connections.sort(key=lambda c: (c["from"], c["to"], c["relationships"]))

    stateful_services = []
    try:
        listing = client._get_json(f"/flow/process-groups/{pg_id}/controller-services")
        for ent in listing.get("controllerServices", []):
            svc = ent.get("component") or {}
            if svc.get("parentGroupId") != pg_id:
                continue  # inherited from an ancestor — documented there
            stateful_services.append({
                "name": svc.get("name", ""),
                "type": _short_type(svc.get("type", "")),
                "state": svc.get("state", ""),
                "properties": _trimmed(svc.get("properties")),
            })
    except Exception:  # older servers without the endpoint: fine, skip
        pass
    stateful_services.sort(key=lambda s: s["name"])
    services = [{k: v for k, v in s.items() if k != "state"}
                for s in stateful_services]

    ctx = (comp.get("parameterContext") or {}).get("component") or {}
    digest = {
        "group": comp.get("name", ""),
        "comments": comp.get("comments") or None,
        "parameter_context": ctx.get("name") or None,
        "processors": processors,
        "connections": connections,
        "input_ports": sorted(
            e["component"].get("name", "") for e in flow.get("inputPorts", [])),
        "output_ports": sorted(
            e["component"].get("name", "") for e in flow.get("outputPorts", [])),
        "funnels": len(flow.get("funnels", [])),
        "labels": sorted(
            (e["component"].get("label") or "") for e in flow.get("labels", [])),
        "services": services,
        "children": [
            {"name": n["digest"]["group"], "fingerprint": n["fingerprint"]}
            for n in children
        ],
    }
    legacy = dict(
        digest,
        processors=stateful_processors,
        services=stateful_services,
        children=[{"name": n["digest"]["group"],
                   "fingerprint": n["legacy_fingerprint"]} for n in children],
    )
    return {"path": path, "digest": digest,
            "fingerprint": _hash(digest),
            "legacy_fingerprint": _hash(legacy),
            "children": children}


def _hash(digest: dict) -> str:
    return hashlib.sha256(
        json.dumps(digest, sort_keys=True).encode()).hexdigest()[:12]


# --------------------------------------------------------------- documents


def doc_path(docs_dir, group_path: str) -> Path:
    name = re.sub(r"[^\w.\- ()']+", "_", (group_path or "root").replace("/", "__"))
    return Path(docs_dir) / f"{name}.md"


def _read_doc(path: Path) -> Tuple[Optional[dict], Optional[str]]:
    """(metadata, full text) — metadata is ``None`` for a file we didn't
    write (no niflow-explanation comment), so it never gets clobbered."""
    if not path.exists():
        return None, None
    text = path.read_text()
    match = _META_RE.search(text)
    return (dict(_ATTR_RE.findall(match.group(1))) if match else None), text


def _summary_of(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    match = _SUMMARY_RE.search(text)
    if match:
        return match.group(1).strip()
    for line in text.splitlines():  # model skipped the Summary line: improvise
        line = line.strip()
        if line and not line.startswith(("#", "<!--", "-", "*")):
            return line[:200]
    return None


def _is_current(meta: Optional[dict], node: dict) -> bool:
    """Does an existing document's fingerprint still describe this flow?

    ``legacy_fingerprint`` is accepted so that dropping run state from the
    digest didn't mark every document ever written as "flow changed" (see
    :func:`digest_tree`); drop that half once the old docs have turned over.
    """
    recorded = (meta or {}).get("fingerprint")
    return recorded in (node["fingerprint"], node["legacy_fingerprint"])


def _outline(node: dict) -> str:
    """A one-line summary of a group derived from its digest alone.

    What the parent's "## Nested groups" section gets for a child we are not
    documenting — enough to say what the branch is (size, the processor
    types that characterise it, how data gets in and out, whether it nests
    further) without spending an LLM call per child.
    """
    digest = node["digest"]
    bits = []
    types: List[str] = []
    for proc in digest["processors"]:
        if proc["type"] not in types:
            types.append(proc["type"])
    if digest["processors"]:
        shown = ", ".join(types[:5]) + ("…" if len(types) > 5 else "")
        bits.append(f"{len(digest['processors'])} processors ({shown})")
    if digest["input_ports"]:
        bits.append("input ports: " + ", ".join(digest["input_ports"]))
    if digest["output_ports"]:
        bits.append("output ports: " + ", ".join(digest["output_ports"]))
    if digest["children"]:
        bits.append(f"{len(digest['children'])} nested group(s)")
    if digest["comments"]:
        bits.append(f"comment: {digest['comments'][:120]}")
    return "; ".join(bits) or "no processors, ports, or nested groups"


def _prompt(node: dict, child_summaries: List[Tuple[str, Optional[str], bool]]) -> str:
    parts = ["Process group digest (JSON):", json.dumps(node["digest"], indent=2)]
    if child_summaries:
        parts.append("\nOne-line summaries of the nested groups. A line marked "
                     "(structure only) lists what the group contains rather "
                     "than what a document says about it — summarise it at "
                     "that level and add nothing:")
        for name, summary, documented in child_summaries:
            if documented and summary:
                parts.append(f"- {name}: {summary}")
            else:
                parts.append(f"- {name}: (structure only) {summary}")
    return "\n".join(parts)


# --------------------------------------------------------------- top level


def _walk_plan(node: dict, docs_dir, depth: int, force: bool) -> List[dict]:
    """What a generate at this depth would touch, in generation order.

    Same statuses :func:`explain_group` reports (``generate`` becomes
    ``generated``), decided from fingerprints alone — no LLM, no writes.
    """
    entries: List[dict] = []

    def walk(n: dict, remaining: int) -> bool:
        """Returns whether this node's document would be (re)written."""
        wrote_child = False
        for child in n["children"]:
            if remaining <= 0 or remaining > 1:  # <=0 means "all the way down"
                wrote_child |= walk(child, remaining - 1)
        path = doc_path(docs_dir, n["path"])
        meta, text = _read_doc(path)
        if text is not None and meta is None:
            status = "skipped"  # hand-written file: we never touch it
        elif text is not None and not force and not wrote_child \
                and _is_current(meta, n):
            status = "current"
        else:
            # ``wrote_child``: a child that has just gained (or refreshed) a
            # document changes what this document should say about it, and
            # that is invisible to the fingerprint — deepening a run would
            # otherwise leave the parent quoting structure-only summaries.
            status = "generate"
        entries.append({"group": n["path"] or "(root)", "status": status,
                        "path": str(path)})
        return status == "generate"

    walk(node, depth)
    return entries


def _count_groups(node: dict) -> int:
    return 1 + sum(_count_groups(child) for child in node["children"])


def _plan(node: dict, docs_dir, depth: int, force: bool) -> dict:
    """The count-before-you-spend summary both front ends show."""
    entries = _walk_plan(node, docs_dir, depth, force)
    return {
        "depth": depth,
        "plan": entries,
        "documents": len(entries),
        "llm_calls": sum(e["status"] == "generate" for e in entries),
        # Groups below the depth cut: one line inside their parent's doc.
        "summarised_groups": _count_groups(node) - len(entries),
    }


def explanation_status(client, group: str = "root", docs_dir=DOCS_DIR,
                       depth: int = 1) -> dict:
    """Is the group's explanation missing, current, or outdated? (No LLM.)

    Also costs out a generate at ``depth`` (``documents``, ``llm_calls``,
    ``summarised_groups``, and the per-document ``plan``) so the CLI and the
    web GUI can say what they are about to spend before spending it.
    """
    from niflow.llm import llm_config

    node = digest_tree(client, client.resolve_group(group))
    path = doc_path(docs_dir, node["path"])
    meta, text = _read_doc(path)
    config = llm_config()
    return {
        "group": node["path"] or "(root)",
        "configured": config is not None,
        # Human-readable backend, not a URL: the Claude Code provider has no
        # endpoint at all, so displays must show this rather than config.url.
        "backend": config.describe() if config else None,
        "exists": text is not None,
        "outdated": text is not None and not _is_current(meta, node),
        "generated": (meta or {}).get("generated"),
        "model": (meta or {}).get("model"),
        "path": str(path),
        "doc": text,
        **_plan(node, docs_dir, depth, force=False),
    }


def explain_group(
    client,
    group: str = "root",
    docs_dir=DOCS_DIR,
    depth: int = 1,
    force: bool = False,
    complete: Optional[Callable[[str, str], str]] = None,
    confirm: Optional[Callable[[dict], bool]] = None,
) -> List[dict]:
    """Generate/refresh the explanation docs for a group (children first).

    ``depth`` is how many levels get their own document: 1 (the default)
    means the selected group only, 2 adds its immediate children, and 0 or
    less means the whole subtree. Groups below the cut are summarised in one
    line inside their parent's document — from their own document when one
    happens to exist already, otherwise from their digest (:func:`_outline`),
    so a high-level run never fans out into an LLM call per nested group.

    ``confirm`` is called once with the :func:`_plan` summary before anything
    is written; returning ``False`` aborts and yields an empty list. Returns
    one ``{"group", "status", "path"}`` per document otherwise — ``status``
    is ``generated``, ``current`` (fingerprint matched, LLM not called), or
    ``skipped`` (a file we didn't write sits at that path). ``complete``
    overrides the configured LLM (tests); the default is
    :func:`niflow.llm.complete`.
    """
    node = digest_tree(client, client.resolve_group(group))
    if confirm is not None and not confirm(_plan(node, docs_dir, depth, force)):
        return []
    results: List[dict] = []
    _generate(node, Path(docs_dir), depth, force, complete, results)
    return results


def _generate(node: dict, docs_dir: Path, depth: int, force: bool,
              complete: Optional[Callable[[str, str], str]],
              results: List[dict]) -> Optional[str]:
    """Write one node's document (bottom-up); returns its one-line summary."""
    child_summaries: List[Tuple[str, Optional[str], bool]] = []
    written = len(results)
    for child in node["children"]:
        if depth <= 0 or depth > 1:  # <=0 means "all the way down"
            summary = _generate(child, docs_dir, depth - 1, force, complete,
                                results)
        else:
            _, text = _read_doc(doc_path(docs_dir, child["path"]))
            summary = _summary_of(text)
        documented = summary is not None
        if summary is None:  # no document to lean on: describe the shape
            summary = _outline(child)
        child_summaries.append((child["digest"]["group"], summary, documented))

    display = node["path"] or "(root)"
    path = doc_path(docs_dir, node["path"])
    meta, text = _read_doc(path)
    if text is not None and meta is None:
        results.append({"group": display, "status": "skipped", "path": str(path)})
        return _summary_of(text)
    # A child that just gained or refreshed a document changes what this
    # document should say about it, and no fingerprint sees that — without
    # this, deepening a run leaves the parent quoting structure-only lines.
    wrote_child = any(r["status"] == "generated" for r in results[written:])
    if text is not None and not force and not wrote_child \
            and _is_current(meta, node):
        results.append({"group": display, "status": "current", "path": str(path)})
        return _summary_of(text)

    if complete is None:
        from niflow import llm

        complete = llm.complete
        config = llm.llm_config()
        model = config.model if config else "?"
    else:
        model = "custom"
    body = complete(_SYSTEM, _prompt(node, child_summaries)).strip()

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    doc = (
        f'<!-- niflow-explanation group="{display.replace(chr(34), chr(39))}" '
        f'fingerprint="{node["fingerprint"]}" generated="{stamp}" '
        f'model="{model}" -->\n'
        f"# {display}\n\n{body}\n\n---\n"
        f"*Generated {stamp} by {model} via `niflow explain` — regenerate "
        f"after the flow changes.*\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(doc)
    results.append({"group": display, "status": "generated", "path": str(path)})
    return _summary_of(body)
