"""Emit a :class:`~niflow.core.Flow` as an idiomatic Python source module.

The output is the inverse of importing one of the example flow scripts: given a
``Flow`` constructed in memory, :func:`to_python` returns a string that — when
written to a file and executed — rebuilds an equivalent ``Flow`` using the
public niflow API (``Flow(...)``, ``Processor(...)``, ``>>`` connections, the
``with flow.process_group(...) as ...`` context manager, etc.).

The module is self-contained: no nipyapi or NiFi access is required. The emitter
walks the in-memory tree, assigns each named component a unique Python variable
name, decides which classes are actually referenced (so the ``from niflow
import ...`` line is minimal), and renders each group's components in the same
order the deployer creates them (controller services -> ports -> processors ->
nested groups -> connections) so the source reads naturally top-to-bottom.

Only stdlib is used (``io``, ``keyword``, ``re``, ``typing``). The function is
pure — it does not mutate the input flow.
"""
from __future__ import annotations

import io
import keyword
import re
from typing import Dict, List, Optional, Set

from niflow.core import (
    Connection,
    ControllerService,
    Flow,
    Funnel,
    InputPort,
    Label,
    OutputPort,
    ParameterContext,
    Port,
    Processor,
    ProcessGroup,
)

# --- name sanitisation ------------------------------------------------------

_IDENT_RE = re.compile(r"[^0-9a-zA-Z_]")


def _safe_var(raw: str, used: Set[str]) -> str:
    """Coerce *raw* into a fresh, valid, non-keyword Python identifier.

    Lower-cases the result, replaces non-identifier characters with ``_``,
    prefixes with ``_`` if it starts with a digit, suffixes ``_`` for keywords,
    and appends a counter on collision against *used*. The chosen name is added
    to *used* before returning so subsequent calls see it.
    """
    base = _IDENT_RE.sub("_", raw).strip("_").lower()
    if not base:
        base = "x"
    if base[0].isdigit():
        base = "_" + base
    if keyword.iskeyword(base):
        base += "_"
    candidate = base
    n = 2
    while candidate in used:
        candidate = f"{base}_{n}"
        n += 1
    used.add(candidate)
    return candidate


# --- tree walking -----------------------------------------------------------

def _assign_var_names(flow: Flow) -> Dict[int, str]:
    """Walk *flow* once and assign every named component a unique var name.

    Returns a mapping from ``id(component)`` to its chosen identifier. The flow
    itself is assigned the name ``flow`` (or a sanitised fallback if that
    collides — unlikely in practice). Connections are anonymous: they are
    emitted inline via the ``>>``/``.to()`` sugar, so no var is needed.
    """
    used: Set[str] = set()
    mapping: Dict[int, str] = {}

    # Reserve "flow" for the top-level flow itself.
    mapping[id(flow)] = "flow"
    used.add("flow")

    def visit(pg: ProcessGroup) -> None:
        ctx = pg.parameter_context
        if ctx is not None and id(ctx) not in mapping:
            # Contexts may be shared between groups — name them once.
            mapping[id(ctx)] = _safe_var(ctx.name + "_params", used)
        for svc in pg.controller_services:
            mapping[id(svc)] = _safe_var(svc.name, used)
        for port in pg.input_ports:
            mapping[id(port)] = _safe_var(port.name, used)
        for port in pg.output_ports:
            mapping[id(port)] = _safe_var(port.name, used)
        for proc in pg.processors:
            mapping[id(proc)] = _safe_var(proc.name, used)
        for funnel in pg.funnels:
            mapping[id(funnel)] = _safe_var(funnel.name or "funnel", used)
        for label in pg.labels:
            mapping[id(label)] = _safe_var(label.name or "label", used)
        for child in pg.process_groups:
            mapping[id(child)] = _safe_var(child.name, used)
            visit(child)

    visit(flow)
    return mapping


def _collect_used_classes(flow: Flow) -> Set[str]:
    """Return the set of niflow class names referenced anywhere in *flow*.

    ``Flow`` is always present. ``ProcessGroup`` is not emitted directly (nested
    groups use the ``with flow.process_group(...)`` context-manager form), so it
    is never reported.
    """
    used: Set[str] = {"Flow"}

    def visit(pg: ProcessGroup) -> None:
        if pg.controller_services:
            used.add("ControllerService")
        if pg.processors:
            used.add("Processor")
            if any(p.bundle is not None for p in pg.processors):
                used.add("Bundle")
        if any(s.bundle is not None for s in pg.controller_services):
            used.add("Bundle")
        if pg.input_ports:
            used.add("InputPort")
        if pg.output_ports:
            used.add("OutputPort")
        if pg.funnels:
            used.add("Funnel")
        if pg.labels:
            used.add("Label")
        if pg.parameter_context is not None:
            used.add("ParameterContext")
            if pg.parameter_context.parameters:
                used.add("Parameter")
        for child in pg.process_groups:
            visit(child)

    visit(flow)
    return used


# --- value rendering --------------------------------------------------------

def _render_value(value: object, varnames: Dict[int, str]) -> str:
    """Render *value* as the Python literal that reproduces it.

    Controller-service instances become a bare variable reference (so the
    emitted module re-establishes the service-ref relationship), everything
    else falls back to :func:`repr` — which gives correct quoting for strings
    (including raw-ish strings via Python's default repr) and the right
    literals for bools, numbers, ``None``, lists, and nested dicts.
    """
    if isinstance(value, ControllerService):
        return varnames[id(value)]
    return repr(value)


def _render_properties(properties: dict, varnames: Dict[int, str], indent: str) -> str:
    """Render a properties dict as a multi-line Python literal.

    One key per line, indented one level deeper than the kwarg itself. For an
    empty dict the caller is expected to skip the kwarg entirely.
    """
    inner = indent + "    "
    lines = ["{"]
    for key, value in properties.items():
        lines.append(f"{inner}{key!r}: {_render_value(value, varnames)},")
    lines.append(indent + "}")
    return "\n".join(lines)


# --- per-component emitters -------------------------------------------------

def _emit_processor(
    proc: Processor, varnames: Dict[int, str], indent: str, out: io.StringIO
) -> None:
    """Emit ``var = Processor(name=..., type=..., ...)`` for *proc*.

    Skips any kwarg that matches the field default so the emitted constructor
    is minimal and reads like the curated examples.
    """
    var = varnames[id(proc)]
    out.write(f"{indent}{var} = Processor(\n")
    out.write(f"{indent}    name={proc.name!r},\n")
    out.write(f"{indent}    type={proc.type!r},\n")
    if proc.properties:
        rendered = _render_properties(proc.properties, varnames, indent + "    ")
        out.write(f"{indent}    properties={rendered},\n")
    if proc.scheduling_period != "0 sec":
        out.write(f"{indent}    scheduling_period={proc.scheduling_period!r},\n")
    if proc.scheduling_strategy != "TIMER_DRIVEN":
        out.write(f"{indent}    scheduling_strategy={proc.scheduling_strategy!r},\n")
    if proc.concurrent_tasks != 1:
        out.write(f"{indent}    concurrent_tasks={proc.concurrent_tasks!r},\n")
    if proc.auto_terminate:
        out.write(f"{indent}    auto_terminate={proc.auto_terminate!r},\n")
    if proc.position is not None:
        out.write(f"{indent}    position={proc.position!r},\n")
    if proc.comments:
        out.write(f"{indent}    comments={proc.comments!r},\n")
    if proc.penalty_duration != "30 sec":
        out.write(f"{indent}    penalty_duration={proc.penalty_duration!r},\n")
    if proc.yield_duration != "1 sec":
        out.write(f"{indent}    yield_duration={proc.yield_duration!r},\n")
    if proc.bulletin_level != "WARN":
        out.write(f"{indent}    bulletin_level={proc.bulletin_level!r},\n")
    if proc.run_duration_millis:
        out.write(f"{indent}    run_duration_millis={proc.run_duration_millis!r},\n")
    if proc.execution_node != "ALL":
        out.write(f"{indent}    execution_node={proc.execution_node!r},\n")
    if proc.scheduled_state != "ENABLED":
        out.write(f"{indent}    scheduled_state={proc.scheduled_state!r},\n")
    if proc.retry_count != 10:
        out.write(f"{indent}    retry_count={proc.retry_count!r},\n")
    if proc.retried_relationships:
        out.write(f"{indent}    retried_relationships={proc.retried_relationships!r},\n")
    if proc.backoff_mechanism != "PENALIZE_FLOWFILE":
        out.write(f"{indent}    backoff_mechanism={proc.backoff_mechanism!r},\n")
    if proc.max_backoff_period != "10 mins":
        out.write(f"{indent}    max_backoff_period={proc.max_backoff_period!r},\n")
    if proc.bundle is not None:
        b = proc.bundle
        out.write(
            f"{indent}    bundle=Bundle(group={b.group!r}, artifact={b.artifact!r}, version={b.version!r}),\n"
        )
    out.write(f"{indent})\n")


def _emit_service(
    svc: ControllerService, varnames: Dict[int, str], indent: str, out: io.StringIO
) -> None:
    """Emit ``var = ControllerService(name=..., type=..., ...)`` for *svc*."""
    var = varnames[id(svc)]
    out.write(f"{indent}{var} = ControllerService(\n")
    out.write(f"{indent}    name={svc.name!r},\n")
    out.write(f"{indent}    type={svc.type!r},\n")
    if svc.properties:
        rendered = _render_properties(svc.properties, varnames, indent + "    ")
        out.write(f"{indent}    properties={rendered},\n")
    if not svc.enabled:
        out.write(f"{indent}    enabled=False,\n")
    if svc.comments:
        out.write(f"{indent}    comments={svc.comments!r},\n")
    if svc.bundle is not None:
        b = svc.bundle
        out.write(
            f"{indent}    bundle=Bundle(group={b.group!r}, artifact={b.artifact!r}, version={b.version!r}),\n"
        )
    out.write(f"{indent})\n")


def _emit_port(
    port: Port, varnames: Dict[int, str], indent: str, out: io.StringIO
) -> None:
    """Emit ``var = InputPort("name")`` or ``var = OutputPort("name")``."""
    var = varnames[id(port)]
    cls = "InputPort" if isinstance(port, InputPort) else "OutputPort"
    out.write(f"{indent}{var} = {cls}({port.name!r})\n")


def _emit_funnel(
    funnel: Funnel, varnames: Dict[int, str], indent: str, out: io.StringIO
) -> None:
    var = varnames[id(funnel)]
    if funnel.position is not None:
        out.write(f"{indent}{var} = Funnel(position={funnel.position!r})\n")
    else:
        out.write(f"{indent}{var} = Funnel()\n")


def _emit_label(
    label: Label, varnames: Dict[int, str], indent: str, out: io.StringIO
) -> None:
    var = varnames[id(label)]
    kwargs = []
    if label.position is not None:
        kwargs.append(f"position={label.position!r}")
    if label.width != 150.0:
        kwargs.append(f"width={label.width!r}")
    if label.height != 150.0:
        kwargs.append(f"height={label.height!r}")
    tail = (", " + ", ".join(kwargs)) if kwargs else ""
    out.write(f"{indent}{var} = Label({label.text!r}{tail})\n")


def _emit_parameter_context(
    ctx: ParameterContext, varnames: Dict[int, str], out: io.StringIO
) -> None:
    """Emit a top-level ``ctx = ParameterContext(...)`` definition.

    Sensitive parameters are emitted without a value (NiFi never exports
    them); supply real values at push time via the secrets file.
    """
    var = varnames[id(ctx)]
    out.write(f"{var} = ParameterContext(\n")
    out.write(f"    {ctx.name!r},\n")
    if ctx.description:
        out.write(f"    description={ctx.description!r},\n")
    if ctx.parameters:
        out.write("    parameters=[\n")
        for p in ctx.parameters:
            args = [repr(p.name)]
            # Sensitive values must never land in a git-tracked file; they are
            # supplied at push time via the secrets file instead.
            if p.value is not None and not p.sensitive:
                args.append(f"value={p.value!r}")
            if p.sensitive:
                args.append("sensitive=True")
            if p.description:
                args.append(f"description={p.description!r}")
            out.write(f"        Parameter({', '.join(args)}),\n")
        out.write("    ],\n")
    if ctx.inherited_contexts:
        out.write(f"    inherited_contexts={ctx.inherited_contexts!r},\n")
    out.write(")\n")


def _emit_connection(
    conn: Connection,
    varnames: Dict[int, str],
    pg_var: str,
    indent: str,
    out: io.StringIO,
) -> None:
    """Emit a single ``pg_var.add_connection(...)`` call for *conn*.

    Prefers the ``a >> b`` sugar when everything is default (relationships
    ``["success"]``, or a port/funnel source which has none, and stock queue
    settings); falls back to ``a.to(b, ...)`` with explicit kwargs otherwise.
    """
    src = varnames[id(conn.source)]
    tgt = varnames[id(conn.target)]
    is_unnamed_source = isinstance(conn.source, (Port, Funnel))
    default_rel = conn.relationships == ["success"]

    kwargs = []
    if conn.name:
        kwargs.append(f"name={conn.name!r}")
    if not is_unnamed_source and not default_rel:
        kwargs.append(f"relationships={conn.relationships!r}")
    if conn.back_pressure_object_threshold != 10000:
        kwargs.append(f"back_pressure_object_threshold={conn.back_pressure_object_threshold!r}")
    if conn.back_pressure_data_size_threshold != "1 GB":
        kwargs.append(f"back_pressure_data_size_threshold={conn.back_pressure_data_size_threshold!r}")
    if conn.flowfile_expiration != "0 sec":
        kwargs.append(f"flowfile_expiration={conn.flowfile_expiration!r}")
    if conn.prioritizers:
        kwargs.append(f"prioritizers={conn.prioritizers!r}")
    if conn.load_balance_strategy != "DO_NOT_LOAD_BALANCE":
        kwargs.append(f"load_balance_strategy={conn.load_balance_strategy!r}")
    if conn.partitioning_attribute:
        kwargs.append(f"partitioning_attribute={conn.partitioning_attribute!r}")
    if conn.load_balance_compression != "DO_NOT_COMPRESS":
        kwargs.append(f"load_balance_compression={conn.load_balance_compression!r}")

    if kwargs:
        body = f"{src}.to({tgt}, {', '.join(kwargs)})"
    else:
        body = f"{src} >> {tgt}"

    out.write(f"{indent}{pg_var}.add_connection({body})\n")


# --- process-group emitter --------------------------------------------------

def _emit_register_calls(
    pg_var: str,
    items: List[object],
    method: str,
    varnames: Dict[int, str],
    indent: str,
    out: io.StringIO,
) -> None:
    """Emit one ``pg_var.method(a, b, c)`` call grouping the given items.

    A no-op when *items* is empty so callers can call unconditionally.
    """
    if not items:
        return
    refs = ", ".join(varnames[id(item)] for item in items)
    out.write(f"{indent}{pg_var}.{method}({refs})\n")


def _emit_group_body(
    pg: ProcessGroup,
    pg_var: str,
    varnames: Dict[int, str],
    indent: str,
    out: io.StringIO,
    is_top_level: bool,
) -> None:
    """Emit every component in *pg* and its descendants, in deploy order.

    Layout per group: controller services -> ports -> processors -> registration
    calls -> nested groups (each wrapped in a ``with pg.process_group(...)``
    block) -> connections. The top-level flow doesn't get a ``with`` wrapper —
    its assignment is emitted separately by :func:`to_python` — but its body
    follows the same shape.
    """
    blocks_written = 0

    def sep() -> None:
        nonlocal blocks_written
        if blocks_written:
            out.write("\n")
        blocks_written += 1

    # Definitions: services, ports, processors.
    if pg.controller_services:
        sep()
        for svc in pg.controller_services:
            _emit_service(svc, varnames, indent, out)

    if pg.input_ports or pg.output_ports:
        sep()
        for port in pg.input_ports:
            _emit_port(port, varnames, indent, out)
        for port in pg.output_ports:
            _emit_port(port, varnames, indent, out)

    if pg.processors:
        sep()
        for proc in pg.processors:
            _emit_processor(proc, varnames, indent, out)

    if pg.funnels or pg.labels:
        sep()
        for funnel in pg.funnels:
            _emit_funnel(funnel, varnames, indent, out)
        for label in pg.labels:
            _emit_label(label, varnames, indent, out)

    # Registration calls grouped by kind.
    if (
        pg.controller_services or pg.input_ports or pg.output_ports
        or pg.processors or pg.funnels or pg.labels
    ):
        sep()
        _emit_register_calls(
            pg_var, list(pg.controller_services), "add_controller_service",
            varnames, indent, out,
        )
        # add_port handles both directions; emit a single call with all ports.
        all_ports: List[object] = list(pg.input_ports) + list(pg.output_ports)
        _emit_register_calls(pg_var, all_ports, "add_port", varnames, indent, out)
        _emit_register_calls(
            pg_var, list(pg.processors), "add_processor", varnames, indent, out,
        )
        _emit_register_calls(pg_var, list(pg.funnels), "add_funnel", varnames, indent, out)
        _emit_register_calls(pg_var, list(pg.labels), "add_label", varnames, indent, out)

    # Group-level settings: comment (children get theirs as a process_group
    # kwarg), variables, and the bound parameter context.
    if (is_top_level and pg.comment) or pg.variables or pg.parameter_context is not None:
        sep()
        if is_top_level and pg.comment:
            out.write(f"{indent}{pg_var}.comment = {pg.comment!r}\n")
        if pg.variables:
            out.write(f"{indent}{pg_var}.variables = {pg.variables!r}\n")
        if pg.parameter_context is not None:
            ctx_var = varnames[id(pg.parameter_context)]
            out.write(f"{indent}{pg_var}.parameter_context = {ctx_var}\n")

    # Nested groups: each in its own ``with ... as ...`` block.
    for child in pg.process_groups:
        sep()
        child_var = varnames[id(child)]
        extra = ""
        if child.comment:
            extra += f", comment={child.comment!r}"
        if child.position is not None:
            extra += f", position={child.position!r}"
        if child.layout != "horizontal":
            extra += f", layout={child.layout!r}"
        out.write(f"{indent}with {pg_var}.process_group({child.name!r}{extra}) as {child_var}:\n")
        _emit_group_body(
            child, child_var, varnames, indent + "    ", out, is_top_level=False
        )

    # Connections last so all endpoints are defined (including ports of nested
    # groups, which the parent flow's connections may reference).
    if pg.connections:
        sep()
        for conn in pg.connections:
            _emit_connection(conn, varnames, pg_var, indent, out)


# --- public entrypoint ------------------------------------------------------

def to_python(flow: Flow, *, module_docstring: Optional[str] = None) -> str:
    """Emit *flow* as a runnable Python module using the niflow API.

    Output structure (in order):

    1. Optional module docstring.
    2. ``from __future__ import annotations``.
    3. ``from niflow import ...`` — only the classes actually used.
    4. ``flow = Flow(...)`` plus one assignment per named component, registered
       into its containing group via ``add_*`` calls.
    5. ``if __name__ == "__main__":`` block that deploys the flow.

    The returned string is guaranteed to parse with :func:`ast.parse` and, when
    ``exec``'d, produce a ``flow`` global whose Pydantic dump matches *flow*'s
    (modulo the excluded runtime fields ``nifi_id``/``nifi_entity``).
    """
    varnames = _assign_var_names(flow)
    used_classes = _collect_used_classes(flow)

    out = io.StringIO()

    # 1. Optional module docstring.
    if module_docstring:
        out.write(f'"""{module_docstring}"""\n')

    # 2. Future import for forward-reference-friendly annotations.
    out.write("from __future__ import annotations\n\n")

    # 3. Minimal niflow import — only the classes the body actually mentions.
    # Stable ordering: Flow first, then alphabetical for predictability.
    ordered = ["Flow"] + sorted(c for c in used_classes if c != "Flow")
    out.write(f"from niflow import {', '.join(ordered)}\n\n")

    # 4. Parameter contexts first — group bodies reference them by variable.
    contexts: List[ParameterContext] = []
    seen: Set[int] = set()

    def collect_contexts(pg: ProcessGroup) -> None:
        ctx = pg.parameter_context
        if ctx is not None and id(ctx) not in seen:
            seen.add(id(ctx))
            contexts.append(ctx)
        for child in pg.process_groups:
            collect_contexts(child)

    collect_contexts(flow)
    for ctx in contexts:
        _emit_parameter_context(ctx, varnames, out)
        out.write("\n")

    # 5. The Flow itself, then its body.
    flow_var = varnames[id(flow)]
    flow_args = [repr(flow.name)]
    if flow.parent_pg != "root":
        flow_args.append(f"parent_pg={flow.parent_pg!r}")
    if flow.layout != "horizontal":
        flow_args.append(f"layout={flow.layout!r}")
    out.write(f"{flow_var} = Flow({', '.join(flow_args)})\n")

    _emit_group_body(flow, flow_var, varnames, indent="", out=out, is_top_level=True)

    # 6. Push entrypoint — delete-and-recreate via flow-definition upload,
    # which works on NiFi 1.x and 2.x alike.
    out.write("\n\n")
    out.write('if __name__ == "__main__":\n')
    out.write(f"    {flow_var}.push()\n")
    out.write(
        f'    print(f"Pushed {{{flow_var}.name!r}} (id={{{flow_var}.nifi_id}}).")\n'
    )

    return out.getvalue()
