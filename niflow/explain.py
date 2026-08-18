"""Plain-English flow documentation: ``niflow explain`` and the webgui button.

Turns a live process group into a Markdown walkthrough an ETL engineer can
read in two minutes — where data enters, how often it runs, what each step
does to the FlowFile, where the flow splits, and where every branch ends.
The narration comes from the LLM configured in :mod:`niflow.llm`; without
one, status checks still work but generation raises
:class:`~niflow.llm.LLMUnavailable`.

One document per group (``docs/explanations/<path>.md``): a nested group
gets its own file, and the parent's document summarises each child in one
line. Every document embeds a fingerprint of the flow logic it was written
from (positions and other cosmetics excluded, nested fingerprints included),
so the helper can tell "up to date" from "flow changed — regenerate?".
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
- "## Nested groups" — only if there are any: one line per nested group from
  the provided summaries; deeper detail lives in that group's own document.
- "## Services" — only if there are controller services: one line each on
  what it provides and which processors need it.
- "## Gotchas" — only when genuinely warranted: stopped or disabled
  processors, dead ends, surprising schedules, misconfigurations you can see.

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

    Returns ``{"path", "digest", "fingerprint", "children": [same shape]}``.
    The digest is everything an explanation needs and nothing cosmetic —
    canvas positions, bends, and ids stay out so moving boxes around never
    invalidates a document. A child's fingerprint feeds its parent's, so a
    change deep in the tree marks every enclosing document outdated.
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

    processors = []
    for ent in flow.get("processors", []):
        c = ent["component"]
        cfg = c.get("config") or {}
        processors.append({
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
    processors.sort(key=lambda p: p["name"])

    connections = []
    for ent in flow.get("connections", []):
        c = ent["component"]
        connections.append({
            "from": end(c.get("source") or {}),
            "to": end(c.get("destination") or {}),
            "relationships": sorted(c.get("selectedRelationships") or []),
        })
    connections.sort(key=lambda c: (c["from"], c["to"], c["relationships"]))

    services = []
    try:
        listing = client._get_json(f"/flow/process-groups/{pg_id}/controller-services")
        for ent in listing.get("controllerServices", []):
            svc = ent.get("component") or {}
            if svc.get("parentGroupId") != pg_id:
                continue  # inherited from an ancestor — documented there
            services.append({
                "name": svc.get("name", ""),
                "type": _short_type(svc.get("type", "")),
                "state": svc.get("state", ""),
                "properties": _trimmed(svc.get("properties")),
            })
    except Exception:  # older servers without the endpoint: fine, skip
        pass
    services.sort(key=lambda s: s["name"])

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
    fingerprint = hashlib.sha256(
        json.dumps(digest, sort_keys=True).encode()
    ).hexdigest()[:12]
    return {"path": path, "digest": digest, "fingerprint": fingerprint,
            "children": children}


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


def _prompt(node: dict, child_summaries: List[Tuple[str, Optional[str]]]) -> str:
    parts = ["Process group digest (JSON):", json.dumps(node["digest"], indent=2)]
    if child_summaries:
        parts.append("\nOne-line summaries of the nested groups (each has its "
                     "own explanation document):")
        parts += [f"- {name}: {summary or '(not documented yet)'}"
                  for name, summary in child_summaries]
    return "\n".join(parts)


# --------------------------------------------------------------- top level


def explanation_status(client, group: str = "root",
                       docs_dir=DOCS_DIR) -> dict:
    """Is the group's explanation missing, current, or outdated? (No LLM.)"""
    from niflow.llm import llm_config

    node = digest_tree(client, client.resolve_group(group))
    path = doc_path(docs_dir, node["path"])
    meta, text = _read_doc(path)
    return {
        "group": node["path"] or "(root)",
        "configured": llm_config() is not None,
        "exists": text is not None,
        "outdated": text is not None
        and (meta or {}).get("fingerprint") != node["fingerprint"],
        "generated": (meta or {}).get("generated"),
        "model": (meta or {}).get("model"),
        "path": str(path),
        "doc": text,
    }


def explain_group(
    client,
    group: str = "root",
    docs_dir=DOCS_DIR,
    recurse: bool = True,
    force: bool = False,
    complete: Optional[Callable[[str, str], str]] = None,
) -> List[dict]:
    """Generate/refresh the explanation docs for a group (children first).

    Returns one ``{"group", "status", "path"}`` per document — ``status`` is
    ``generated``, ``current`` (fingerprint matched, LLM not called), or
    ``skipped`` (a file we didn't write sits at that path). ``recurse=False``
    touches only the target's own document; child summaries are then read
    from whatever child documents already exist. ``complete`` overrides the
    configured LLM (tests); the default is :func:`niflow.llm.complete`.
    """
    node = digest_tree(client, client.resolve_group(group))
    results: List[dict] = []
    _generate(node, Path(docs_dir), recurse, force, complete, results)
    return results


def _generate(node: dict, docs_dir: Path, recurse: bool, force: bool,
              complete: Optional[Callable[[str, str], str]],
              results: List[dict]) -> Optional[str]:
    """Write one node's document (bottom-up); returns its one-line summary."""
    child_summaries: List[Tuple[str, Optional[str]]] = []
    for child in node["children"]:
        if recurse:
            summary = _generate(child, docs_dir, recurse, force, complete, results)
        else:
            _, text = _read_doc(doc_path(docs_dir, child["path"]))
            summary = _summary_of(text)
        child_summaries.append((child["digest"]["group"], summary))

    display = node["path"] or "(root)"
    path = doc_path(docs_dir, node["path"])
    meta, text = _read_doc(path)
    if text is not None and meta is None:
        results.append({"group": display, "status": "skipped", "path": str(path)})
        return _summary_of(text)
    if (text is not None and not force
            and meta.get("fingerprint") == node["fingerprint"]):
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
