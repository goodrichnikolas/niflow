"""Render a Flow as a Mermaid flowchart — flows you can SEE in a PR.

GitHub (and most Markdown renderers) draw ```mermaid fences natively, so
``niflow diagram flows/my_flow.py -o docs/my_flow.md`` gives reviewers the
picture without opening NiFi: processors as boxes, ports as stadiums,
funnels as circles, nested process groups as subgraphs, and connections as
relationship-labelled arrows.
"""
from __future__ import annotations

from typing import Dict, List

from niflow.core import Connection, Flow, Funnel, Port, ProcessGroup


def to_mermaid(flow: Flow) -> str:
    """The flow as a ``flowchart`` definition (no markdown fence)."""
    direction = "TD" if flow.layout == "vertical" else "LR"
    lines: List[str] = [f"flowchart {direction}"]
    ids: Dict[int, str] = {}
    counter = [0]

    def nid(component) -> str:
        if id(component) not in ids:
            ids[id(component)] = f"n{counter[0]}"
            counter[0] += 1
        return ids[id(component)]

    def esc(text: str) -> str:
        return text.replace('"', "&quot;")

    def emit_group(group: ProcessGroup, indent: str, top: bool) -> None:
        if not top:
            lines.append(f'{indent}subgraph {nid(group)}["{esc(group.name)}"]')
            indent += "    "
        for proc in group.processors:
            simple = proc.type.rsplit(".", 1)[-1]
            lines.append(f'{indent}{nid(proc)}["{esc(proc.name)}<br/><i>{esc(simple)}</i>"]')
        for port in list(group.input_ports) + list(group.output_ports):
            arrow = "&gt;" if port.port_type == "INPUT_PORT" else ""
            lines.append(f'{indent}{nid(port)}(["{arrow}{esc(port.name)}"])')
        for funnel in group.funnels:
            lines.append(f"{indent}{nid(funnel)}((+))")
        for child in group.process_groups:
            emit_group(child, indent, top=False)
        if not top:
            lines.append(f"{indent[:-4]}end")

    def emit_connections(group: ProcessGroup) -> None:
        for conn in group.connections:
            lines.append(_edge(conn, nid))
        for child in group.process_groups:
            emit_connections(child)

    emit_group(flow, "    ", top=True)
    emit_connections(flow)
    return "\n".join(lines) + "\n"


def _edge(conn: Connection, nid) -> str:
    src, dst = nid(conn.source), nid(conn.target)
    if isinstance(conn.source, (Port, Funnel)):
        return f"    {src} --> {dst}"
    label = ",".join(conn.relationships)
    return f"    {src} -->|{label}| {dst}"


def to_markdown(flow: Flow) -> str:
    """A markdown document: title, optional comment, fenced diagram."""
    parts = [f"# {flow.name}", ""]
    if flow.comment:
        parts += [flow.comment, ""]
    parts += ["```mermaid", to_mermaid(flow).rstrip(), "```", ""]
    return "\n".join(parts)
