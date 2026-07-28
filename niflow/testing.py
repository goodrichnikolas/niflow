"""Flow testing: push to a sandbox, inject a FlowFile, assert what comes out.

The debugging loop NiFi makes painful — "what does this flow actually DO to a
file?" — as a test harness::

    from niflow.testing import TestCase

    tests = [
        TestCase(
            name="urgent rows reach the audit log as JSON",
            inject_at="Stamp",                      # any processor or input port
            content="id,priority\\n1,urgent\\n",
            expect_at="Audit",                      # a processor; its input queue is inspected
            expect_attributes={"mime.type": "application/json"},
            expect_content_contains='"priority"',
        ),
    ]

``niflow test flows/my_flow.py`` then:

1. pushes the flow to a throwaway sandbox group (the real group is never touched);
2. starts it, but **stops every source processor** (the test injects its own
   data) and every ``expect_at`` collector (so results queue up instead of
   being consumed);
3. injects each case's FlowFile — a temporary ``GenerateFlowFile`` wired to
   ``inject_at`` and triggered exactly once (dynamic properties carry the
   case's attributes);
4. waits for FlowFiles to arrive in the queues feeding ``expect_at``, then
   checks the expected attributes/content against every one collected;
5. cleans up (``--keep`` leaves the sandbox on the canvas for autopsy).

Works on NiFi 1.x and 2.x — everything here is plain REST the client already
speaks. ``inject_at``/``expect_at`` accept a bare name or a ``Group/Sub/Name``
path when names repeat across groups.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from niflow.core import Flow

logger = logging.getLogger("niflow")

_POLL_S = 1.0


@dataclass
class TestCase:
    """One injected FlowFile and what is expected to come out."""

    name: str
    inject_at: str
    expect_at: str
    content: str = ""
    attributes: Dict[str, str] = field(default_factory=dict)
    expect_attributes: Dict[str, str] = field(default_factory=dict)
    expect_content: Optional[str] = None
    expect_content_contains: Optional[str] = None
    expect_count: int = 1
    timeout: float = 30.0


@dataclass
class CaseResult:
    case: TestCase
    passed: bool
    failures: List[str]
    flowfiles: List[dict]  # each: {"attributes": {...}, "content": "..."}

    def __str__(self) -> str:  # pragma: no cover - convenience
        mark = "✓" if self.passed else "✗"
        lines = [f"{mark} {self.case.name}"]
        lines += [f"    {f}" for f in self.failures]
        return "\n".join(lines)


class FlowTester:
    """Runs :class:`TestCase` s against a sandbox copy of a flow."""

    def __init__(
        self,
        client: Any,
        flow: Flow,
        *,
        sandbox_name: Optional[str] = None,
        keep: bool = False,
        secrets: Any = None,
    ) -> None:
        self.client = client
        self.flow = flow
        self.sandbox_name = sandbox_name or f"{flow.name} (niflow-test)"
        self.keep = keep
        self.secrets = secrets
        self.pg_id: Optional[str] = None

    # ------------------------------------------------------------- lifecycle

    def __enter__(self) -> "FlowTester":
        sandbox = self.flow.model_copy(deep=True)
        sandbox.name = self.sandbox_name
        self.pg_id = self.client.push_flow(sandbox, secrets=self.secrets)
        logger.info("Sandbox %r pushed (%s)", self.sandbox_name, self.pg_id)
        return self

    def __exit__(self, *exc: Any) -> None:
        if self.pg_id is None:
            return
        if self.keep:
            url = ""
            if hasattr(self.client, "ui_url"):
                url = f" — {self.client.ui_url(self.pg_id)}"
            logger.info("Keeping sandbox %r%s", self.sandbox_name, url)
            return
        self.client.delete_group(self.pg_id)

    def run(self, cases: List[TestCase]) -> List[CaseResult]:
        assert self.pg_id, "use FlowTester as a context manager"
        return [self._run_case(case) for case in cases]

    # ------------------------------------------------------------- one case

    def _run_case(self, case: TestCase) -> CaseResult:
        failures: List[str] = []
        collected: List[dict] = []
        temp_proc = temp_conn = None
        try:
            target = self._resolve(case.inject_at, kinds=("processor", "input_port"))
            self._resolve_expect(case.expect_at)  # fail fast on a bad name

            self.client.enable_services(self.pg_id)
            self._start_flow_except_sources_and_collectors(case)

            temp_proc, temp_conn = self._inject(case, target)
            collected = self._await_results(case)
            failures = _check(case, collected)
        finally:
            self._cleanup_case(temp_proc, temp_conn)
        return CaseResult(case, passed=not failures, failures=failures, flowfiles=collected)

    # ------------------------------------------------------------- topology

    def _components(self) -> List[dict]:
        """Processors and ports in the sandbox: kind/id/name/path/group_id."""
        out: List[dict] = []

        def visit(pg_id: str, prefix: str, parent_id: Optional[str]) -> None:
            flow = self.client._get_json(f"/flow/process-groups/{pg_id}")[
                "processGroupFlow"
            ]["flow"]
            for kind, key in (("processor", "processors"),
                              ("input_port", "inputPorts"),
                              ("output_port", "outputPorts")):
                for entity in flow.get(key, []):
                    comp = entity["component"]
                    out.append({
                        "kind": kind, "id": comp["id"], "name": comp.get("name", ""),
                        "path": prefix, "group_id": pg_id, "parent_group_id": parent_id,
                    })
            for child in flow.get("processGroups", []):
                comp = child["component"]
                path = f"{prefix}/{comp['name']}" if prefix else comp["name"]
                visit(comp["id"], path, pg_id)

        visit(self.pg_id, "", None)
        return out

    def _resolve(self, spec: str, kinds: Tuple[str, ...]) -> dict:
        matches = [
            c for c in self._components()
            if c["kind"] in kinds and _spec_matches(spec, c["path"], c["name"])
        ]
        if not matches:
            raise ValueError(f"no {' or '.join(kinds)} named {spec!r} in the flow")
        if len(matches) > 1:
            paths = ", ".join(f"{m['path']}/{m['name']}".lstrip("/") for m in matches)
            raise ValueError(f"{spec!r} is ambiguous ({paths}); use a Group/Name path")
        return matches[0]

    def _resolve_expect(self, spec: str) -> dict:
        return self._resolve(spec, kinds=("processor",))

    def _expect_queues(self, case: TestCase) -> List[dict]:
        return [
            q for q in self.client.list_queues(self.pg_id)
            if _spec_matches(case.expect_at, q["path"], q["destination"])
        ]

    def _start_flow_except_sources_and_collectors(self, case: TestCase) -> None:
        """Start components one by one, never the sources or the collector.

        Components are started individually — NOT via start-group — because a
        source (e.g. a GenerateFlowFile on a timer) fires the instant it
        starts: start-everything-then-stop-sources races the test with the
        flow's own data. Sources stay stopped (the test injects its data);
        the collector stays stopped so results queue up instead of being
        consumed. Verified the hard way: the group-start variant produced a
        phantom FlowFile per case on the first live run.
        """
        destinations = {
            (q["path"], q["destination"]) for q in self.client.list_queues(self.pg_id)
        }
        for comp in self._components():
            if comp["kind"] == "processor":
                is_source = (comp["path"], comp["name"]) not in destinations
                is_collector = _spec_matches(case.expect_at, comp["path"], comp["name"])
                if is_source or is_collector:
                    continue
                try:
                    self.client.start_processor(comp["id"])
                except Exception as exc:  # disabled/invalid processors refuse
                    logger.warning("Could not start %s: %s", comp["name"], exc)
            else:
                try:
                    self.client.set_port_state(comp["kind"], comp["id"], "RUNNING")
                except Exception as exc:
                    logger.warning("Could not start port %s: %s", comp["name"], exc)

    # ------------------------------------------------------------- injection

    def _inject(self, case: TestCase, target: dict) -> Tuple[str, str]:
        properties: Dict[str, str] = {"File Size": "0B"}
        if case.content:
            # 2.x renamed the property; direct REST creates are NOT migrated
            # the way snapshot imports are, so pick the real per-line name
            # (verified in tests/fixtures/real/): the 1.x machine name would
            # silently become a dynamic property on 2.x -> empty content.
            key = "Custom Text" if self.client._major_version() >= 2 else "generate-ff-custom-text"
            properties[key] = case.content
        properties.update(case.attributes)  # dynamic props -> FlowFile attributes

        if target["kind"] == "input_port":
            # A group's input port is fed from OUTSIDE: the generator lives in
            # the parent group and the connection crosses the boundary.
            gen_group = target["parent_group_id"] or self.pg_id
            dest_type = "INPUT_PORT"
        else:
            gen_group = target["group_id"]
            dest_type = "PROCESSOR"

        proc_id = self.client.create_processor(
            gen_group,
            {
                "type": "org.apache.nifi.processors.standard.GenerateFlowFile",
                "name": "niflow-test-inject",
                "position": {"x": -420.0, "y": -160.0},
                "config": {"properties": properties, "schedulingPeriod": "1 hour"},
            },
        )
        conn_id = self.client.create_connection(
            gen_group,
            source={"id": proc_id, "groupId": gen_group, "type": "PROCESSOR"},
            destination={"id": target["id"], "groupId": target["group_id"], "type": dest_type},
            relationships=["success"],
        )
        if target["kind"] == "processor":
            # The target may have been stopped as a source; it must consume.
            self.client.start_processor(target["id"])
        self.client.run_processor_once(proc_id)
        return proc_id, conn_id

    # ------------------------------------------------------------- collection

    def _await_results(self, case: TestCase) -> List[dict]:
        deadline = time.monotonic() + case.timeout
        while True:
            queues = self._expect_queues(case)
            if sum(q["queued"] for q in queues) >= case.expect_count:
                break
            if time.monotonic() > deadline:
                break
            time.sleep(_POLL_S)

        collected = []
        for queue in self._expect_queues(case):
            if not queue["queued"]:
                continue
            for summary in self.client.list_flowfiles(queue["id"]):
                detail = self.client.flowfile_detail(queue["id"], summary["uuid"])
                collected.append(
                    {"attributes": detail["attributes"], "content": detail["content"]}
                )
        return collected

    # ------------------------------------------------------------- cleanup

    def _cleanup_case(self, temp_proc: Optional[str], temp_conn: Optional[str]) -> None:
        try:
            self.client._set_group_state(self.pg_id, "STOPPED")
            if temp_conn:
                self.client.drain_connection(temp_conn)
                self.client._delete_component("connections", temp_conn)
            if temp_proc:
                self.client._delete_component("processors", temp_proc)
            self.client.purge_queues(self.pg_id)
        except Exception as exc:  # cleanup must never mask the case's outcome
            logger.warning("Sandbox cleanup issue (continuing): %s", exc)


# ------------------------------------------------------------------ checking


def _check(case: TestCase, collected: List[dict]) -> List[str]:
    failures: List[str] = []
    if len(collected) < case.expect_count:
        failures.append(
            f"expected {case.expect_count} FlowFile(s) at {case.expect_at!r} "
            f"within {case.timeout:.0f}s, got {len(collected)}"
        )
        return failures

    for i, ff in enumerate(collected):
        where = f"FlowFile {i + 1}"
        for key, want in case.expect_attributes.items():
            got = ff["attributes"].get(key)
            if got != want:
                failures.append(f"{where}: attribute {key!r} = {got!r}, expected {want!r}")
        if case.expect_content is not None and ff["content"] != case.expect_content:
            failures.append(
                f"{where}: content {_trim(ff['content'])!r}, expected {_trim(case.expect_content)!r}"
            )
        if (
            case.expect_content_contains is not None
            and case.expect_content_contains not in ff["content"]
        ):
            failures.append(
                f"{where}: content {_trim(ff['content'])!r} does not contain "
                f"{case.expect_content_contains!r}"
            )
    return failures


def _trim(text: str, limit: int = 120) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _spec_matches(spec: str, path: str, name: str) -> bool:
    """``"Name"`` matches anywhere; ``"Group/Sub/Name"`` pins the group path;
    a leading slash (``"/Name"``) pins the flow's root group."""
    if "/" not in spec:
        return name == spec
    spec_path, _, spec_name = spec.rpartition("/")
    return name == spec_name and path == spec_path


def as_cases(raw: List[Any]) -> List[TestCase]:
    """Accept `tests = [TestCase(...)]` or plain dicts from a flow module."""
    return [c if isinstance(c, TestCase) else TestCase(**c) for c in raw]


def format_results(results: List[CaseResult]) -> str:
    lines = []
    for r in results:
        lines.append(f"{'✓' if r.passed else '✗'} {r.case.name}")
        lines.extend(f"    {f}" for f in r.failures)
    passed = sum(1 for r in results if r.passed)
    lines.append(f"{passed}/{len(results)} case(s) passed.")
    return "\n".join(lines)
