"""Labyrinth: an adversarial fixture for the trace/follow stepper (T7).

Where ``torture.py`` attacks the *emitter* (hostile names, cycles, parallel
edges), this one attacks the *runtime debugger*: every construct a FlowFile
journey can take that the stepper has to survive.

* **Deep nesting** — a four-level group chain (``L1/L2/L3/L4``) entered and
  left through input/output ports, which is what work's real trees look like.
* **Fan-in** — three sources feeding one destination, so run-once has three
  inbound queues to choose between.
* **Split** — one FlowFile into fifty children via SplitJson.
* **Merge** — those fifty collapsing back into one via MergeContent: the
  followed uuid is *consumed*, and its journey continues under a new uuid.
* **Failure** — SplitJson over non-JSON, routed (not auto-terminated).
* **Volume** — a batch of 200 FlowFiles so the followed one is nowhere near
  the front of its queue.

Endpoint-free (standard NAR only) and nothing is started, so it is safe on a
dev NiFi. Sized to run on 1.24 as well as 2.x: no 2.x-only properties.
"""
from __future__ import annotations

import json

from niflow import Flow, InputPort, OutputPort
from niflow.core import Processor

GEN = "org.apache.nifi.processors.standard.GenerateFlowFile"
UPDATE = "org.apache.nifi.processors.attributes.UpdateAttribute"
LOG = "org.apache.nifi.processors.standard.LogAttribute"
SPLIT_JSON = "org.apache.nifi.processors.standard.SplitJson"
MERGE = "org.apache.nifi.processors.standard.MergeContent"
ROUTE = "org.apache.nifi.processors.standard.RouteOnAttribute"

CHILDREN = 50
BATCH = 200

flow = Flow("NiflowLabyrinth")
flow.comment = "Adversarial fixture for trace/follow (T7)."


def _log(name: str) -> Processor:
    return Processor(name=name, type=LOG, properties={"Log Level": "info"},
                     auto_terminate=["success"])


# --------------------------------------------------------------- deep nesting
# Gen -> L1.in -> (L1) Mark1 -> L2.in -> ... -> L4 Mark4 -> L4.out -> ... -> Bottom
deep_gen = Processor(
    name="DeepGen", type=GEN,
    properties={"File Size": "0B", "generate-ff-custom-text": "deep"},
    scheduling_period="60 sec",
)
deep_sink = _log("DeepSink")

l1_in, l1_out = InputPort("in"), OutputPort("out")
l2_in, l2_out = InputPort("in"), OutputPort("out")
l3_in, l3_out = InputPort("in"), OutputPort("out")
l4_in, l4_out = InputPort("in"), OutputPort("out")
mark1 = Processor(name="Mark1", type=UPDATE, properties={"depth": "1"})
mark2 = Processor(name="Mark2", type=UPDATE, properties={"depth": "2"})
mark3 = Processor(name="Mark3", type=UPDATE, properties={"depth": "3"})
mark4 = Processor(name="Mark4", type=UPDATE, properties={"depth": "4"})
back3 = Processor(name="Back3", type=UPDATE, properties={"leaving": "3"})
back2 = Processor(name="Back2", type=UPDATE, properties={"leaving": "2"})
back1 = Processor(name="Back1", type=UPDATE, properties={"leaving": "1"})

with flow.process_group("L1") as g1:
    with g1.process_group("L2") as g2:
        with g2.process_group("L3") as g3:
            with g3.process_group("L4") as g4:
                g4.add(l4_in, l4_out, mark4)
                g4.add_connection(l4_in >> mark4, mark4 >> l4_out)
            g3.add(l3_in, l3_out, mark3, back3)
            g3.add_connection(l3_in >> mark3, mark3 >> l4_in,
                              l4_out >> back3, back3 >> l3_out)
        g2.add(l2_in, l2_out, mark2, back2)
        g2.add_connection(l2_in >> mark2, mark2 >> l3_in,
                          l3_out >> back2, back2 >> l2_out)
    g1.add(l1_in, l1_out, mark1, back1)
    g1.add_connection(l1_in >> mark1, mark1 >> l2_in,
                      l2_out >> back1, back1 >> l1_out)

flow.add_processor(deep_gen, deep_sink)
flow.add_connection(deep_gen >> l1_in, l1_out >> deep_sink)

# -------------------------------------------------------------------- fan-in
fan_a = Processor(name="FanA", type=GEN,
                  properties={"File Size": "0B", "generate-ff-custom-text": "a"},
                  scheduling_period="60 sec")
fan_b = Processor(name="FanB", type=GEN,
                  properties={"File Size": "0B", "generate-ff-custom-text": "b"},
                  scheduling_period="60 sec")
fan_c = Processor(name="FanC", type=GEN,
                  properties={"File Size": "0B", "generate-ff-custom-text": "c"},
                  scheduling_period="60 sec")
collector = Processor(name="Collector", type=UPDATE, properties={"collected": "yes"})
collector_sink = _log("CollectorSink")
flow.add_processor(fan_a, fan_b, fan_c, collector, collector_sink)
flow.add_connection(fan_a >> collector, fan_b >> collector, fan_c >> collector,
                    collector >> collector_sink)

# --------------------------------------------------------------- split/merge
_array = json.dumps([{"i": i, "v": f"row-{i}"} for i in range(CHILDREN)])
split_gen = Processor(
    name="SplitGen", type=GEN,
    properties={"File Size": "0B", "generate-ff-custom-text": _array},
    scheduling_period="60 sec",
)
splitter = Processor(
    name="Splitter", type=SPLIT_JSON,
    properties={"JsonPath Expression": "$.*"},
    auto_terminate=["original"],
)
tally = Processor(name="Tally", type=UPDATE, properties={"tallied": "yes"})
merger = Processor(
    name="Merger", type=MERGE,
    properties={
        "Minimum Number of Entries": str(CHILDREN),
        "Maximum Number of Entries": str(CHILDREN),
        "Merge Format": "Binary Concatenation",
    },
    auto_terminate=["original"],
)
merged_sink = _log("MergedSink")
split_fail = _log("SplitFailure")
flow.add_processor(split_gen, splitter, tally, merger, merged_sink, split_fail)
flow.add_connection(
    split_gen >> splitter,
    splitter.to(tally, relationships=["split"]),
    splitter.to(split_fail, relationships=["failure"]),
    tally >> merger,
    merger.to(merged_sink, relationships=["merged"]),
    merger.to(split_fail, relationships=["failure"]),
)

# ------------------------------------------------------------------- failure
bad_gen = Processor(
    name="BadGen", type=GEN,
    properties={"File Size": "0B", "generate-ff-custom-text": "this is not json"},
    scheduling_period="60 sec",
)
bad_split = Processor(
    name="BadSplit", type=SPLIT_JSON,
    properties={"JsonPath Expression": "$.*"},
    auto_terminate=["original", "split"],
)
bad_sink = _log("BadSink")
flow.add_processor(bad_gen, bad_split, bad_sink)
flow.add_connection(bad_gen >> bad_split,
                    bad_split.to(bad_sink, relationships=["failure"]))

# -------------------------------------------------------------------- volume
bulk_gen = Processor(
    name="BulkGen", type=GEN,
    properties={"File Size": "0B", "generate-ff-custom-text": "bulk",
                "Batch Size": str(BATCH)},
    scheduling_period="60 sec",
)
bulk_mark = Processor(name="BulkMark", type=UPDATE, properties={"bulk": "yes"})
bulk_sink = _log("BulkSink")
flow.add_processor(bulk_gen, bulk_mark, bulk_sink)
flow.add_connection(bulk_gen >> bulk_mark, bulk_mark >> bulk_sink)

# ------------------------------------------------- a followable 2-way merge
# The 50-way merge above is realistic but needs 50 steps to fill a bin; this
# lane merges two children, so a stepper can actually walk a FlowFile *into*
# a merge and out the other side.
_pair = json.dumps([{"i": 0}, {"i": 1}])
pair_gen = Processor(
    name="PairGen", type=GEN,
    properties={"File Size": "0B", "generate-ff-custom-text": _pair},
    scheduling_period="60 sec",
)
pair_split = Processor(
    name="PairSplit", type=SPLIT_JSON,
    properties={"JsonPath Expression": "$.*"},
    auto_terminate=["original", "failure"],
)
pair_merge = Processor(
    name="PairMerge", type=MERGE,
    properties={
        "Minimum Number of Entries": "2",
        "Maximum Number of Entries": "2",
        "Merge Format": "Binary Concatenation",
    },
    auto_terminate=["original", "failure"],
)
pair_sink = _log("PairSink")
flow.add_processor(pair_gen, pair_split, pair_merge, pair_sink)
flow.add_connection(
    pair_gen >> pair_split,
    pair_split.to(pair_merge, relationships=["split"]),
    pair_merge.to(pair_sink, relationships=["merged"]),
)

# ------------------------------------------------------------ long journey
# A self-loop with an escape hatch: run it for a second and one FlowFile
# accumulates hundreds of provenance events, which is how you find out what
# `max_events` really does to a trace.
loop_gen = Processor(
    name="LoopGen", type=GEN,
    properties={"File Size": "0B", "generate-ff-custom-text": "loop"},
    scheduling_period="60 sec",
)
looper = Processor(name="Looper", type=UPDATE,
                   properties={"looped": "${looped:isEmpty():ifElse('1','2')}"})
flow.add_processor(loop_gen, looper)
flow.add_connection(loop_gen >> looper, looper.to(looper, name="again"))

# ---------------------------------------------------------------- routing
# A wired (not auto-terminated) relationship, so the journey carries a real
# ROUTE provenance event with a relationship name on it.
route_gen = Processor(
    name="RouteGen", type=GEN,
    properties={"File Size": "0B", "generate-ff-custom-text": "route"},
    scheduling_period="60 sec",
)
router = Processor(
    name="Router", type=ROUTE,
    properties={"hot": "${filename:isEmpty():not()}"},
    auto_terminate=["unmatched"],
)
route_sink = _log("RouteSink")
flow.add_processor(route_gen, router, route_sink)
flow.add_connection(route_gen >> router, router.to(route_sink, relationships=["hot"]))
