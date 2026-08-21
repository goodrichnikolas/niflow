"""Bulk bug-hunting: thousands of generated micro-flows, hunting *niflow's* defects.

``flows/torture.py`` is one hand-built adversarial flow, and it paid for itself
(see the torture-flow findings in todo.md). This module generalises it: it
generates a very large number of *small* flows — mostly a single processor, or
one ``A -> B`` hop, plus property and shape variations — and runs each through
niflow's own pipeline looking for niflow misbehaving.

**NiFi rejecting a nonsensical combination is a pass, not a failure.** The
signal we hunt is niflow's fault:

* a crash (traceback) anywhere in emission, parsing, planning, or apply;
* a round trip that does not converge — ``to_json -> from_json -> to_json``
  must be byte-stable, and the re-parsed flow must plan to *zero* changes
  against the model it came from;
* a property that silently lands under the wrong key, or as an inert dynamic
  property, on the target server's namespace (the exact NiFi 1.24 bug in
  todo.md's "Cross-version property fidelity");
* identity collisions that merge or drop components;
* a ``validate`` verdict that disagrees with what the live server says;
* lossy emission that drops components without saying so.

Tiers, cheapest first — most of the value needs no NiFi at all::

    niflow fuzz                        # tier 1: pure offline, whole catalog
    niflow fuzz --tier 2 --count 200   # + NiFi's own validation, sandboxed
    niflow fuzz --tier 3 --count 50    # + push/pull/plan convergence, live

Every failing case writes a **standalone repro** under ``<out>/repro/<case-id>/``
(a runnable flow module plus the command that fails), the run is seeded so it
replays exactly, results stream to ``results.jsonl`` so a multi-hour sweep is
resumable, and the final report groups findings by *root-cause signature* so
500 failures from one bug read as one bug.

Re-run a single case with ``niflow fuzz --replay <case-id>``.
"""
from niflow.fuzz.cases import (
    HOSTILE_NAMES,
    HOSTILE_VALUES,
    KINDS,
    SECRET_VALUE,
    NIFI_REJECTED,
    DEFAULT_OUT_DIR,
    NIFLOW_BUG,
    PASSED,
    SANDBOX_PREFIX,
    SHAPES,
    Case,
    build_case_flow,
    generate_cases,
    processor_types,
    service_types,
)
from niflow.fuzz.checks import (
    CaseResult,
    Finding,
    _classify_live_error,
    _server_normalised,
    check_live_roundtrip,
    check_live_validate,
    check_apply_faults,
    check_offline,
    check_plan_sensitivity,
    check_secret_containment,
    normalise_message,
)
from niflow.fuzz.fakeserver import FakeServer
from niflow.fuzz.runner import (
    Report,
    SweepConfig,
    cleanup_sandboxes,
    find_case,
    format_report,
    replay,
    run_case,
    sweep,
    write_repro,
)

__all__ = [
    "PASSED", "NIFLOW_BUG", "NIFI_REJECTED", "KINDS", "SHAPES", "SECRET_VALUE",
    "HOSTILE_VALUES", "HOSTILE_NAMES", "DEFAULT_OUT_DIR",
    "Case", "build_case_flow", "generate_cases", "processor_types", "service_types",
    "Finding", "CaseResult", "normalise_message", "check_offline",
    "check_plan_sensitivity", "check_live_validate", "check_live_roundtrip",
    "check_apply_faults", "check_secret_containment", "FakeServer",
    "SweepConfig", "Report", "run_case", "sweep", "replay", "find_case",
    "write_repro", "format_report", "cleanup_sandboxes", "SANDBOX_PREFIX",
]
