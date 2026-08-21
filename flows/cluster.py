"""Cluster fixture: the constructs that only mean something on a cluster (T7h).

A standalone NiFi accepts every one of these and quietly makes them no-ops.
It takes a real cluster for them to *do* anything, which is why they went
untested for so long — and why this file exists next to a compose profile that
brings one up (``make cluster-up``).

* **Primary-node-only scheduling** — ``ListFTP`` is annotated
  ``@PrimaryNodeOnly``, so NiFi forces ``executionNode=PRIMARY`` whatever the
  model asks for. The model default is ``ALL``, which is what used to make
  these processors drift forever (fixed by harvesting the annotation; this is
  the fixture that proves it on a server where PRIMARY is not a formality).
* **Load balancing** — ``ROUND_ROBIN`` on one connection and
  ``PARTITION_BY_ATTRIBUTE`` on another. On a cluster NiFi genuinely moves
  FlowFiles between nodes over the load-balance port; on a standalone server
  the setting round-trips and nothing moves.
* **Compression on a load-balanced queue** — a separate field from the
  strategy, and the one most likely to be silently dropped by an emitter.
* **A plain lane** — so a test can tell "nothing moved because load balancing
  is broken" from "nothing moved at all".

Endpoint-free (standard NAR only, no host it can actually reach) and nothing
is started, so it is safe to push anywhere. 1.24-clean: no 2.x-only property.
"""
from __future__ import annotations

from niflow import Flow
from niflow.core import Processor

GEN = "org.apache.nifi.processors.standard.GenerateFlowFile"
UPDATE = "org.apache.nifi.processors.attributes.UpdateAttribute"
LOG = "org.apache.nifi.processors.standard.LogAttribute"
LIST_FTP = "org.apache.nifi.processors.standard.ListFTP"

#: Enough files that a round-robin split across two nodes is unambiguous —
#: 1-of-20 landing on the other node would be luck, 10 is the design.
BATCH = 20

flow = Flow("NiflowCluster")
flow.comment = "Cluster-only constructs: primary-node scheduling, load balancing."

# --------------------------------------------------------- primary node only
# NiFi forces executionNode=PRIMARY on this type. It is deliberately left with
# the model's own default (ALL) so a plan against a live cluster proves the
# effective-value rule rather than the model simply agreeing with the server.
lister = Processor(
    name="ListOnPrimary", type=LIST_FTP,
    properties={"Hostname": "ftp.invalid", "Username": "nobody",
                "Remote Path": "/"},
    scheduling_period="60 sec",
    auto_terminate=["success"],
)

# ------------------------------------------------------------ load balancing
source = Processor(
    name="Source", type=GEN,
    properties={"File Size": "0B", "Batch Size": str(BATCH),
                "generate-ff-custom-text": "cluster"},
    scheduling_period="60 sec",
)
spread = Processor(name="Spread", type=UPDATE, properties={"seen": "yes"})
keyed = Processor(name="Keyed", type=UPDATE, properties={"partition": "a"})
sink = Processor(name="Sink", type=LOG, properties={"Log Level": "info"},
                 auto_terminate=["success"])

flow.add_processor(lister, source, spread, keyed, sink)

# Round robin: every node gets its share, in turn.
balanced = source >> spread
balanced.load_balance_strategy = "ROUND_ROBIN"
balanced.load_balance_compression = "COMPRESS_ATTRIBUTES_ONLY"

# Partition by attribute: same value -> same node, always. Every FlowFile here
# carries the same 'partition', so the whole queue must end up on ONE node —
# which is a sharper assertion than "some spread happened".
partitioned = spread >> keyed
partitioned.load_balance_strategy = "PARTITION_BY_ATTRIBUTE"
partitioned.partitioning_attribute = "partition"

# The control lane: no load balancing at all.
plain = keyed >> sink

flow.add_connection(balanced, partitioned, plain)

__all__ = ["flow", "BATCH"]
