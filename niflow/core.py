"""Core declarative models for NiFlow.

These Pydantic models describe a NiFi flow purely in memory; nothing here talks
to NiFi. :func:`Flow.deploy` hands the tree to :mod:`niflow.deployment`, which
does the nipyapi orchestration.

Design notes
------------
* Every component subclasses :class:`NiFiComponent`, which accepts the ``name``
  positionally (``Flow("MyFlow")``, ``InputPort("in")``) for ergonomic flows.
* ``a >> b`` builds a :class:`Connection` (default relationship ``"success"``).
  It does **not** auto-register; you pass it to ``add_connection`` so the wiring
  stays explicit and visible.
* Runtime fields (``nifi_id``/``nifi_entity``) are populated during deploy. The
  *same* object instance is shared between a process group and any connection
  referencing it, so connections can read a component's assigned NiFi entity.
"""
from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Bundle(BaseModel):
    """NAR coordinates for a processor/service type.

    Only needed for components from non-standard NARs; when ``None`` the
    formats emit sensible standard-bundle defaults and NiFi resolves the
    actual NAR on import.
    """

    group: str = "org.apache.nifi"
    artifact: str = "nifi-standard-nar"
    version: str = ""


def nifi_property_value(value: Any, allowable: Any = None) -> Any:
    """One property value in the form NiFi actually stores it: a string.

    NiFi's component property map is a ``Map<String,String>``, so anything a
    Python author naturally writes — ``{"Batch Size": 10}``, ``{"Pretty
    Print": True}`` — comes back from the server as ``"10"`` / ``"true"`` and
    would otherwise register as drift on every single plan. Booleans use
    NiFi's lower-case spelling (``str(True)`` would produce ``"True"``, which
    lands as an unrecognised value rather than an error); everything else is
    ``str()``.

    ``allowable`` (a descriptor's allowable-value list, when known) settles
    the spelling: the handful of properties whose enum really is
    ``["True", "False"]`` get the capitalised form back. ``None`` means
    "unset" and is preserved, as are strings and
    :class:`ControllerService` references (resolved to ids at emit time).
    """
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, bool):
        text = "true" if value else "false"
    elif isinstance(value, (int, float)):
        text = str(value)
    else:
        return value
    for candidate in allowable or ():
        if isinstance(candidate, str) and candidate.lower() == text.lower():
            return candidate
    return text


def nifi_property_values(type_str: str, properties: dict) -> dict:
    """:func:`nifi_property_value` over a whole property map of *type_str*."""
    if not properties or all(
        v is None or isinstance(v, str) for v in properties.values()
    ):
        return properties
    try:
        from niflow.processors.rules import descriptors_for

        descriptors = descriptors_for(type_str) or {}
    except Exception:  # pragma: no cover - catalog is optional
        descriptors = {}
    return {
        key: nifi_property_value(value, (descriptors.get(key) or {}).get("allowable"))
        for key, value in properties.items()
    }


class NiFiComponent(BaseModel):
    """Base for everything that ends up on the NiFi canvas."""

    # arbitrary_types_allowed lets ``nifi_entity`` hold opaque nipyapi objects.
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str

    # --- runtime state, filled in by the deployer (not part of the definition) ---
    nifi_id: Optional[str] = Field(default=None, exclude=True)
    nifi_entity: Any = Field(default=None, exclude=True, repr=False)

    # Property names THIS SERVER says are sensitive, read off the snapshot's
    # own propertyDescriptors by from_json. It is knowledge about the type,
    # not part of the flow's definition — hence exclude=True, and hence no
    # emitter, differ or identity seed looks at it.
    #
    # The catalogs answer the same question for every type Apache ships; this
    # answers it for the ones it does not, which is what a custom NAR is. A
    # sensitive property reads back empty however it was set, so without this
    # a work flow with a custom processor's password re-planned that change
    # forever and `niflow drift` cried wolf on it every run.
    sensitive_keys: List[str] = Field(default_factory=list, exclude=True,
                                      repr=False)

    def __init__(self, name: Optional[str] = None, **data: Any) -> None:
        # Allow the name to be passed positionally: Flow("x"), InputPort("in").
        if name is not None:
            data["name"] = name
        super().__init__(**data)

    def __rshift__(self, other: "NiFiComponent") -> "Connection":
        """``a >> b`` -> a Connection from ``a`` to ``b`` on the "success" relationship."""
        return Connection(source=self, target=other)

    def to(
        self,
        target: "NiFiComponent",
        relationships: Optional[List[str]] = None,
        name: str = "",
        **settings: Any,
    ) -> "Connection":
        """Explicit connection builder for non-default relationships/settings.

        Example::

            split.to(handle_failure, relationships=["failure"])
            slow.to(sink, back_pressure_object_threshold=0)
        """
        return Connection(
            name=name,
            source=self,
            target=target,
            relationships=relationships or ["success"],
            **settings,
        )


class Parameter(BaseModel):
    """A single parameter inside a :class:`ParameterContext`.

    Sensitive parameter *values* never leave NiFi via the API, so a pulled
    flow carries ``value=None`` for them; supply the real value at push time
    through the secrets file (see :mod:`niflow.client`).
    """

    name: str
    value: Optional[str] = None
    sensitive: bool = False
    description: str = ""

    def __init__(self, name: Optional[str] = None, value: Optional[str] = None, **data: Any) -> None:
        if name is not None:
            data["name"] = name
        if value is not None:
            data["value"] = value
        super().__init__(**data)


class ParameterContext(NiFiComponent):
    """A named set of parameters referenced from properties as ``#{name}``.

    Attach to any group via ``group.parameter_context = ctx``. Contexts are
    matched/created **by name** on push; ``inherited_contexts`` lists parent
    context names (NiFi 1.15+ inheritance) and is preserved on round-trip.
    """

    description: str = ""
    parameters: List[Parameter] = Field(default_factory=list)
    inherited_contexts: List[str] = Field(default_factory=list)

    def add_parameter(self, *params: Parameter) -> "ParameterContext":
        self.parameters.extend(params)
        return self


class ControllerService(NiFiComponent):
    """A shared service (e.g. a record reader/writer) referenced by processors.

    Pass an instance directly as a processor property value and NiFlow swaps in
    its NiFi UUID at deploy time::

        reader = JsonTreeReader("Reader")
        ConvertRecord("Convert", properties={"Record Reader": reader})

    ``enabled`` controls whether deploy enables the service after creating it.
    Set ``False`` to just create the service on the canvas without enabling —
    useful when you only want to verify creation (the catalog test does this),
    or when properties aren't filled in yet so enable would fail validation.

    It is a *deploy intent*, and only a stated one is diffed: NiFi imports
    every service of a pushed flow DISABLED whatever the snapshot asks for, so
    diffing the mere default would report drift on every service-bearing flow
    forever (activation is what ``push --start`` / ``niflow start`` is for).
    Write ``enabled`` explicitly — either value — and it becomes an assertion
    the plan diffs and ``push --update`` applies (it really does enable the
    live service). Note NiFi's flow-definition download reports every service
    as DISABLED whatever its runtime state, so a *stated* ``enabled=True``
    keeps planning until that live read learns to ask
    ``/flow/process-groups/{id}/controller-services``. See
    :func:`niflow.plan._diff_service_fields`.
    """

    type: str
    properties: dict = Field(default_factory=dict)
    enabled: bool = True
    comments: str = ""
    # NAR coordinates; None means "standard bundle" (resolved by NiFi on import).
    bundle: Optional[Bundle] = None

    @model_validator(mode="after")
    def _nifi_property_values(self) -> "ControllerService":
        # Values are strings to NiFi; normalise once, at construction, so a
        # model written in Python never drifts against its own pushed state.
        self.properties = nifi_property_values(self.type, self.properties)
        return self


class Processor(NiFiComponent):
    """A NiFi processor: a typed unit of work with properties and scheduling.

    Field defaults mirror NiFi's own defaults, so a constructor that only sets
    ``name``/``type``/``properties`` behaves exactly like a freshly dragged
    processor — and the formats can skip emitting default values.
    """

    type: str
    properties: dict = Field(default_factory=dict)
    scheduling_period: str = "0 sec"
    scheduling_strategy: Literal["TIMER_DRIVEN", "CRON_DRIVEN"] = "TIMER_DRIVEN"
    concurrent_tasks: int = 1
    # Relationships to auto-terminate explicitly. Unused relationships are
    # auto-terminated automatically at deploy unless that behaviour is disabled.
    auto_terminate: List[str] = Field(default_factory=list)
    position: Optional[Tuple[float, float]] = None

    # --- fidelity fields (NiFi defaults; only emitted when changed) ----------
    comments: str = ""
    penalty_duration: str = "30 sec"
    yield_duration: str = "1 sec"
    bulletin_level: str = "WARN"
    run_duration_millis: int = 0
    execution_node: Literal["ALL", "PRIMARY"] = "ALL"
    # DISABLED survives round-trips so a deliberately disabled processor in a
    # pulled flow stays disabled when pushed back.
    scheduled_state: Literal["ENABLED", "DISABLED", "RUNNING"] = "ENABLED"
    retry_count: int = 10
    retried_relationships: List[str] = Field(default_factory=list)
    backoff_mechanism: Literal["PENALIZE_FLOWFILE", "YIELD_PROCESSOR"] = "PENALIZE_FLOWFILE"
    max_backoff_period: str = "10 mins"
    bundle: Optional[Bundle] = None

    @model_validator(mode="after")
    def _sorted_relationship_sets(self) -> "Processor":
        # These lists are *sets* to NiFi; keeping them sorted means declaration
        # order can never masquerade as drift (in plans or emitted JSON).
        self.auto_terminate = sorted(self.auto_terminate)
        self.retried_relationships = sorted(self.retried_relationships)
        return self

    @model_validator(mode="after")
    def _nifi_property_values(self) -> "Processor":
        # Property values are strings to NiFi; ``{"Batch Size": 10}`` is the
        # natural thing to write in Python, so normalise it here (same reason
        # as the sorting above: declaration form must never look like drift).
        self.properties = nifi_property_values(self.type, self.properties)
        return self


class Funnel(NiFiComponent):
    """A canvas funnel — merges any number of connections into one stream.

    Funnels have no name in NiFi; ``name`` defaults to ``""`` and is ignored
    on emit. Use one as a connection endpoint like any other component.
    """

    name: str = ""
    position: Optional[Tuple[float, float]] = None


class Label(NiFiComponent):
    """A canvas annotation box. Purely cosmetic but preserved on round-trip."""

    name: str = ""
    text: str = ""
    position: Optional[Tuple[float, float]] = None
    width: float = 150.0
    height: float = 150.0

    def __init__(self, text: Optional[str] = None, **data: Any) -> None:
        # Labels read naturally as Label("some note") — text positionally.
        if text is not None:
            data["text"] = text
        super().__init__(**data)


class Port(NiFiComponent):
    """Base for input/output ports. Use :class:`InputPort` / :class:`OutputPort`."""

    port_type: Literal["INPUT_PORT", "OUTPUT_PORT"]
    position: Optional[Tuple[float, float]] = None


class InputPort(Port):
    port_type: Literal["INPUT_PORT", "OUTPUT_PORT"] = "INPUT_PORT"


class OutputPort(Port):
    port_type: Literal["INPUT_PORT", "OUTPUT_PORT"] = "OUTPUT_PORT"


class Connection(NiFiComponent):
    """A directed link from ``source`` to ``target`` on one or more relationships.

    When ``source`` is a :class:`Port`, relationships are ignored (ports have
    none) and the deployer connects them directly.
    """

    name: str = ""
    source: NiFiComponent
    target: NiFiComponent
    relationships: List[str] = Field(default_factory=lambda: ["success"])

    # --- queue settings (NiFi defaults; only emitted when changed) -----------
    back_pressure_object_threshold: int = 10000
    back_pressure_data_size_threshold: str = "1 GB"
    flowfile_expiration: str = "0 sec"
    prioritizers: List[str] = Field(default_factory=list)
    load_balance_strategy: Literal[
        "DO_NOT_LOAD_BALANCE",
        "PARTITION_BY_ATTRIBUTE",
        "ROUND_ROBIN",
        "SINGLE_NODE",
    ] = "DO_NOT_LOAD_BALANCE"
    partitioning_attribute: str = ""
    load_balance_compression: Literal[
        "DO_NOT_COMPRESS",
        "COMPRESS_ATTRIBUTES_ONLY",
        "COMPRESS_ATTRIBUTES_AND_CONTENT",
    ] = "DO_NOT_COMPRESS"

    @model_validator(mode="after")
    def _no_relationships_for_unnamed_sources(self) -> "Connection":
        # Ports and funnels have no named relationships; normalising here keeps
        # ``port >> x`` and a parsed NiFi export structurally identical.
        if isinstance(self.source, (Port, Funnel)) and self.relationships:
            self.relationships = []
        return self


class ProcessGroup(NiFiComponent):
    """A container of components; may nest other process groups."""

    comment: str = ""
    position: Optional[Tuple[float, float]] = None
    # Direction for auto-placing components that have no explicit position:
    # connected chains march right ("horizontal") or down ("vertical").
    layout: Literal["horizontal", "vertical"] = "horizontal"
    processors: List[Processor] = Field(default_factory=list)
    controller_services: List[ControllerService] = Field(default_factory=list)
    input_ports: List[InputPort] = Field(default_factory=list)
    output_ports: List[OutputPort] = Field(default_factory=list)
    connections: List[Connection] = Field(default_factory=list)
    process_groups: List["ProcessGroup"] = Field(default_factory=list)
    funnels: List[Funnel] = Field(default_factory=list)
    labels: List[Label] = Field(default_factory=list)
    # NiFi 1.x variable registry entries scoped to this group.
    variables: Dict[str, str] = Field(default_factory=dict)
    # Parameter context bound to this group. Share one instance across groups
    # to bind them to the same context; matching on push is by context *name*.
    parameter_context: Optional[ParameterContext] = None

    # --- builder API ---------------------------------------------------------
    def add_processor(self, *processors: Processor) -> "ProcessGroup":
        self.processors.extend(processors)
        return self

    def add_controller_service(self, *services: ControllerService) -> "ProcessGroup":
        self.controller_services.extend(services)
        return self

    def add_connection(self, *connections: Connection) -> "ProcessGroup":
        self.connections.extend(connections)
        return self

    def add_port(self, *ports: Port) -> "ProcessGroup":
        for port in ports:
            if isinstance(port, OutputPort) or port.port_type == "OUTPUT_PORT":
                self.output_ports.append(port)  # type: ignore[arg-type]
            else:
                self.input_ports.append(port)  # type: ignore[arg-type]
        return self

    def add_funnel(self, *funnels: Funnel) -> "ProcessGroup":
        self.funnels.extend(funnels)
        return self

    def add_label(self, *labels: Label) -> "ProcessGroup":
        self.labels.extend(labels)
        return self

    def add(self, *components: NiFiComponent) -> "ProcessGroup":
        """Smart-dispatch any component(s) to the right collection."""
        for c in components:
            if isinstance(c, Connection):
                self.add_connection(c)
            elif isinstance(c, ControllerService):
                self.add_controller_service(c)
            elif isinstance(c, Port):
                self.add_port(c)
            elif isinstance(c, Funnel):
                self.add_funnel(c)
            elif isinstance(c, Label):
                self.add_label(c)
            elif isinstance(c, ParameterContext):
                self.parameter_context = c
            elif isinstance(c, ProcessGroup):
                self.process_groups.append(c)
            elif isinstance(c, Processor):
                self.add_processor(c)
            else:  # pragma: no cover - defensive
                raise TypeError(f"Cannot add component of type {type(c).__name__}")
        return self

    def process_group(self, name: str, **kwargs: Any) -> "ProcessGroup":
        """Create, register, and return a child process group.

        Pairs with the ``with`` form for readable nesting::

            with flow.process_group("Ingest") as ingest:
                ingest.add_processor(...)
        """
        child = ProcessGroup(name=name, **kwargs)
        self.process_groups.append(child)
        return child

    # --- context manager sugar for nested groups -----------------------------
    def __enter__(self) -> "ProcessGroup":
        return self

    def __exit__(self, *exc: Any) -> Literal[False]:
        return False


class Flow(ProcessGroup):
    """Top-level, deployable process group.

    ``parent_pg`` is the NiFi process group this flow is created under — the
    string ``"root"`` (the canvas root) or the name of an existing group.
    """

    parent_pg: str = "root"
    # Filled by from_json when a live snapshot contained things the model
    # cannot represent (remote process groups, external service references).
    # Never serialised; surfaced by `niflow pull` and the GUI so a lossy pull
    # is loud instead of silent.
    pull_warnings: List[str] = Field(default_factory=list, exclude=True)

    def deploy(self, config: Any = None, start: bool = False, auto_terminate_unused: bool = True) -> "Flow":
        """Deploy this flow to NiFi component-by-component via nipyapi (legacy, 2.x only).

        Prefer :meth:`push`, which works on NiFi 1.x and 2.x and round-trips
        parameter contexts, funnels, and labels. Imported lazily so importing
        :mod:`niflow.core` never requires nipyapi.
        """
        from niflow.deployment import deploy as _deploy

        return _deploy(self, config=config, start=start, auto_terminate_unused=auto_terminate_unused)

    def push(self, config: Any = None, start: bool = False, secrets: Any = None) -> "Flow":
        """Push this flow to NiFi as a flow definition (works on 1.x and 2.x).

        Delete-and-recreate: an existing group with the same name under
        ``parent_pg`` is stopped, emptied, and replaced. ``secrets`` maps
        sensitive parameter names to values (see :mod:`niflow.client`).
        """
        from niflow.client import NiFiClient

        client = NiFiClient(config)
        client.push_flow(self, start=start, secrets=secrets)
        return self

    @classmethod
    def pull(cls, group: str, config: Any = None) -> "Flow":
        """Pull a live process group (by name or id) into a :class:`Flow`."""
        from niflow.client import NiFiClient

        return NiFiClient(config).pull_flow(group)

    # --- format conveniences ------------------------------------------------
    # Lazy imports so :mod:`niflow.core` stays free of format dependencies.
    @classmethod
    def from_json(cls, source: Any) -> "Flow":
        """Parse a NiFi 2.x flow-definition (JSON) into a :class:`Flow`."""
        from niflow.formats import from_json as _from_json

        return _from_json(source)

    def to_json(self, *, indent: int = 2, target_major: Optional[int] = None) -> str:
        """Emit this flow as a NiFi ``VersionedFlowSnapshot`` JSON string.

        ``target_major`` translates property keys to that NiFi line's
        namespace (see :func:`niflow.formats.json_format.to_json`); leave it
        ``None`` for offline emission.
        """
        from niflow.formats import to_json as _to_json

        return _to_json(self, indent=indent, target_major=target_major)

    def to_python(self, *, module_docstring: Optional[str] = None) -> str:
        """Emit this flow as a runnable Python module using the niflow API."""
        from niflow.formats import to_python as _to_python

        return _to_python(self, module_docstring=module_docstring)

    def validate(self, target_version: Any = None, *, baseline: bool = True) -> List[dict]:
        """Static pre-push checks; ``[]`` if clean. See :mod:`niflow.validate`.

        With no ``target_version`` the flow is also checked against the
        declared compatibility baseline (``NIFLOW_MIN_NIFI_VERSION``, default
        NiFi 1.24) — the oldest line it has to keep working on. Pass
        ``target_version`` (e.g. ``"1.24"``) to check some other line instead,
        or ``baseline=False`` to skip the cross-version check entirely. No
        server required either way.
        """
        from niflow.validate import validate_flow

        return validate_flow(self, target_version, baseline=baseline)

    @classmethod
    def from_xml(cls, source: Any) -> "Flow":
        """Parse a NiFi 1.x template (``<template><snippet>...``) into a :class:`Flow`."""
        from niflow.formats import from_xml as _from_xml

        return _from_xml(source)

    def to_xml(self, *, indent: int = 4) -> str:
        """Emit this flow as a NiFi 1.x ``<template>`` XML string."""
        from niflow.formats import to_xml as _to_xml

        return _to_xml(self, indent=indent)


# Resolve the forward reference in ProcessGroup.process_groups.
ProcessGroup.model_rebuild()
Flow.model_rebuild()


def find_unregistered_components(group: ProcessGroup) -> List[Tuple[str, str]]:
    """Components that are *wired* into a flow but were never added to it.

    Two hand-editing mistakes with the same ugly ending — a bare ``KeyError``
    carrying a memory address, thrown from the middle of an emit, after the
    push has already started:

    * a :class:`ControllerService` used as a property value but never
      ``add_controller_service``'d, so it has no identifier to reference;
    * a connection whose source or destination was never added to its group,
      so the endpoint points at a component the snapshot does not contain.

    Registration is checked across the **whole tree**, not per group: NiFi
    resolves a controller service from any ancestor group, and a connection
    between groups is a separate (real) problem that
    :func:`niflow.validate.validate_flow` reports on its own.

    Returns ``(group_path, message)`` pairs, empty when everything is wired to
    something the flow actually contains.
    """
    registered: set = set()
    services: set = set()

    def collect(g: ProcessGroup) -> None:
        registered.add(id(g))
        for member in (list(g.processors) + list(g.input_ports)
                       + list(g.output_ports) + list(g.funnels)):
            registered.add(id(member))
        for service in g.controller_services:
            registered.add(id(service))
            services.add(id(service))
        for child in g.process_groups:
            collect(child)

    collect(group)
    problems: List[Tuple[str, str]] = []

    def visit(g: ProcessGroup, path: str) -> None:
        owners = [("processor", p) for p in g.processors]
        owners += [("controller service", s) for s in g.controller_services]
        for kind, owner in owners:
            for key, value in (owner.properties or {}).items():
                if isinstance(value, ControllerService) and id(value) not in services:
                    problems.append((
                        path,
                        f"{kind} {owner.name!r} property {key!r} references "
                        f"controller service {value.name!r}, which was never "
                        f"added to the flow — add_controller_service() it "
                        f"(in this group or an ancestor)",
                    ))
        for conn in g.connections:
            for role, end in (("source", conn.source), ("destination", conn.target)):
                if id(end) not in registered:
                    problems.append((
                        path,
                        f"connection {role} {getattr(end, 'name', end)!r} is not "
                        f"part of the flow — add it to a group before "
                        f"connecting it",
                    ))
        for child in g.process_groups:
            visit(child, f"{path}/{child.name}")

    visit(group, group.name or ".")
    return problems


def find_identity_collisions(group: ProcessGroup) -> List[Tuple[str, str]]:
    """Same-kind name duplicates that break niflow's name-based identity.

    niflow identifies components by name within their group (deterministic
    UUID5 seeds on push, name matching in plans), so two processors named
    "Worker" in one group — or two sibling groups both called "Stage" — are
    indistinguishable and would silently merge or clobber each other.
    Returns ``(group_path, message)`` pairs; empty list means safe.
    NiFi itself allows duplicate names; niflow requires them unique per
    (group, kind). Duplicate connections and funnels are fine.
    """
    collisions: List[Tuple[str, str]] = []

    def visit(g: ProcessGroup, path: str) -> None:
        kinds = (
            ("processor", g.processors),
            ("input port", g.input_ports),
            ("output port", g.output_ports),
            ("controller service", g.controller_services),
            ("child group", g.process_groups),
        )
        for kind, members in kinds:
            seen: Dict[str, int] = {}
            for m in members:
                seen[m.name] = seen.get(m.name, 0) + 1
            for name, count in seen.items():
                if count > 1:
                    collisions.append((
                        path,
                        f"{count} {kind}s named {name!r} — rename all but one "
                        f"(niflow identity is name-based per group)",
                    ))
        for child in g.process_groups:
            visit(child, f"{path}/{child.name}")

    visit(group, group.name or ".")
    return collisions
