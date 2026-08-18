"""Torture flow: adversarial constructs, no external endpoints.

Round one of the niflow stress campaign. Everything here is legal NiFi but
hostile to IaC tooling assumptions:

* cycles — an A->B->A retry loop and a true self-loop;
* parallel edges — several connections between the same pair, including the
  same relationship carried by two differently named connections;
* colliding-ish names — a port sharing its name with a processor in the same
  group (legal: kinds are distinct). Same-kind duplicates ("Worker" twice in
  one group, two sibling "Stage" groups) are now *rejected* by niflow before
  anything touches the server — see tests/test_identity.py;
* hostile strings — emoji, quotes, newlines, escaped EL (``$${...}``) and
  escaped parameter syntax (``##{...}``), regex backslashes, trailing
  whitespace in multi-line property values;
* dynamic relationships — a RouteOnAttribute with 24 dynamic properties,
  each its own relationship, split between wired and auto-terminated;
* funnel chain — funnel -> funnel -> funnel;
* config extremes — CRON + primary node, per-relationship retry/backoff,
  a DISABLED processor, zero and huge backpressure, prioritizers, load
  balancing with compression, flowfile expiration.

Only standard-NAR, endpoint-free processors are used, and nothing is ever
started, so pushing this to a dev NiFi is safe.
"""
from __future__ import annotations

from niflow import Flow, InputPort, OutputPort
from niflow.core import Funnel, Label, Processor

UPDATE_ATTR = "org.apache.nifi.processors.attributes.UpdateAttribute"
ROUTE_ATTR = "org.apache.nifi.processors.standard.RouteOnAttribute"
LOG_ATTR = "org.apache.nifi.processors.standard.LogAttribute"

flow = Flow("NiflowTorture")
flow.comment = (
    'Adversarial fixture — "quotes", a\nliteral newline, escaped EL $${not.el} '
    "and escaped param ##{not.a.param}. 🔥"
)

flow.add_label(
    Label(
        "Torture zone 🔥\nline two has trailing spaces   \n\ttab-indented line with 'quotes' and <angle brackets>",
        width=360.0,
        height=80.0,
    )
)

# --- source: hostile multi-line content with escapes and trailing blanks -----
gen = Processor(
    name="Gen 🚀 'source'",
    type="org.apache.nifi.processors.standard.GenerateFlowFile",
    properties={
        "File Size": "0B",
        "generate-ff-custom-text": (
            "id,code,note   \n"
            "17-alpha,42-beta,keep $${this.literal} intact\n"
            "  indented line with ##{escaped.param} and a backslash \\d+ \n"
            "\n"
        ),
    },
    scheduling_period="60 sec",
    comments="multi-line comment:\nsecond line with \"double\" and 'single' quotes",
)

# --- cycle 1: A -> B -> A retry loop ------------------------------------------
retry_router = Processor(
    name="RetryRouter",
    type=ROUTE_ATTR,
    properties={"give-up": "${retry.count:ge(3)}"},
)
bump = Processor(
    name="Bump",
    type=UPDATE_ATTR,
    properties={"retry.count": "${retry.count:plus(1)}"},
)

# --- cycle 2: true self-loop ---------------------------------------------------
ouroboros = Processor(
    name="Ouroboros",
    type=UPDATE_ATTR,
    properties={"seen": "true"},
)

sink_log = Processor(
    name="Sink 'log' <ω>",
    type=LOG_ATTR,
    properties={"Log Level": "info"},
    auto_terminate=["success"],
)

# --- parallel edges + duplicate processor names --------------------------------
fork = Processor(
    name="Fork",
    type=ROUTE_ATTR,
    properties={
        "hot": "${code:contains('alpha')}",
        "cold": "${code:contains('beta')}",
    },
    auto_terminate=["unmatched"],
)
worker1 = Processor(name="Worker", type=UPDATE_ATTR, properties={"lane": "one"})
worker2 = Processor(name="Worker 2", type=UPDATE_ATTR, properties={"lane": "two"})

# --- regex-heavy ReplaceText with per-relationship retry -----------------------
rewrite = Processor(
    name='Rewrite \\d+ "regex"',
    type="org.apache.nifi.processors.standard.ReplaceText",
    properties={
        "Regular Expression": "(\\d{2,4})-(\\w+)\\s*$",
        "Replacement Value": "$2_$1\\n",
        "Evaluation Mode": "Line-by-Line",
    },
    auto_terminate=["failure"],
    retry_count=2,
    retried_relationships=["failure"],
    backoff_mechanism="YIELD_PROCESSOR",
    max_backoff_period="2 mins",
)

# --- CRON + primary-node + concurrency extremes --------------------------------
cron_log = Processor(
    name="Cron 'audit'",
    type=LOG_ATTR,
    properties={"Log Level": "warn"},
    scheduling_strategy="CRON_DRIVEN",
    scheduling_period="0 0/5 * * * ?",
    execution_node="PRIMARY",
    concurrent_tasks=4,
    penalty_duration="90 sec",
    yield_duration="5 sec",
    bulletin_level="ERROR",
    auto_terminate=["success"],
)

# --- 24-way dynamic-relationship fanout ----------------------------------------
fanout_props = {f"r{i:02d}": f"${{fan:equals('{i}')}}" for i in range(24)}
fanout = Processor(
    name="Fanout",
    type=ROUTE_ATTR,
    properties=fanout_props,
    # half the dynamic relationships terminate here, half are wired below
    auto_terminate=["unmatched"] + [f"r{i:02d}" for i in range(16, 24)],
)

# --- funnel chain ---------------------------------------------------------------
f1, f2, f3 = Funnel(), Funnel(), Funnel()

flow.add_processor(
    gen, retry_router, bump, ouroboros, sink_log, fork, worker1, worker2,
    rewrite, cron_log, fanout,
)
flow.add_funnel(f1, f2, f3)

# --- sibling stage groups with identically named ports --------------------------
with flow.process_group("Stage") as stage1:
    s1_in = InputPort("in")
    s1_out = OutputPort("out")
    # deliberately DISABLED; must survive round-trip as DISABLED
    s1_work = Processor(
        name="Worker",
        type=UPDATE_ATTR,
        properties={"stage": "first"},
        scheduled_state="DISABLED",
    )
    stage1.add(s1_in, s1_out, s1_work)
    stage1.add_connection(s1_in >> s1_work, s1_work >> s1_out)

with flow.process_group("Stage 2") as stage2:
    s2_in = InputPort("in")
    s2_out = OutputPort("out")
    # processor named "in", colliding with the port beside it
    s2_work = Processor(name="in", type=UPDATE_ATTR, properties={"stage": "second"})
    stage2.add(s2_in, s2_out, s2_work)
    stage2.add_connection(s2_in >> s2_work, s2_work >> s2_out)

# --- wiring ----------------------------------------------------------------------
flow.add_connection(
    # gen fans out to three consumers off the same success relationship
    gen >> retry_router,
    gen >> ouroboros,
    gen.to(rewrite, name="to rewrite"),
    gen >> fork,
    # cycle 1: RetryRouter -> Bump -> RetryRouter
    retry_router.to(sink_log, relationships=["give-up"]),
    retry_router.to(bump, relationships=["unmatched"], name="try again"),
    bump >> retry_router,
    # cycle 2: self-loop plus an exit edge
    ouroboros.to(ouroboros, name="ouroboros eats itself"),
    ouroboros >> sink_log,
    # parallel edges Fork -> Worker(1): hot twice under different names, cold once
    fork.to(worker1, relationships=["hot"], name="hot path",
            load_balance_strategy="PARTITION_BY_ATTRIBUTE",
            partitioning_attribute="code"),
    fork.to(worker1, relationships=["hot"], name="hot path clone"),
    fork.to(worker1, relationships=["cold"], name="cold path"),
    fork.to(worker2, relationships=["cold"], name="cold to second worker"),
    # workers into the duplicate-named stages, with tuned queues
    worker1.to(s1_in, back_pressure_object_threshold=0, flowfile_expiration="5 min"),
    worker2.to(s2_in,
               load_balance_strategy="ROUND_ROBIN",
               load_balance_compression="COMPRESS_ATTRIBUTES_AND_CONTENT",
               prioritizers=["org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer"]),
    # stage outputs converge on the funnel chain
    s1_out >> f1,
    s2_out >> f1,
    # rewrite feeds the fanout; wired half of the dynamic relationships
    rewrite >> fanout,
    *[fanout.to(f1 if i % 2 == 0 else f2, relationships=[f"r{i:02d}"]) for i in range(16)],
    # funnel -> funnel -> funnel, ending in the CRON logger
    f1 >> f2,
    f2 >> f3,
    f3.to(cron_log,
          back_pressure_object_threshold=1_000_000,
          back_pressure_data_size_threshold="10 TB",
          prioritizers=[
              "org.apache.nifi.prioritizer.OldestFlowFileFirstPrioritizer",
              "org.apache.nifi.prioritizer.FirstInFirstOutPrioritizer",
          ]),
)


if __name__ == "__main__":
    flow.push()
    print(f"Pushed {flow.name!r} (id={flow.nifi_id}).")
