# Clusters

Everything niflow does works against a clustered NiFi, but a cluster changes
what several things *mean* — and one class of call flatly requires extra
information that a standalone server does not. This page is what a real
two-node cluster taught us; every claim on it was measured on one.

Run one locally:

```bash
make cluster-up cluster-wait     # 2 × NiFi 1.24 + ZooKeeper, unsecured HTTP
make test-cluster                # the cluster-only suite
make cluster-down
```

Nodes land on `http://localhost:8180/nifi` and `:8181`. The profile is 1.x and
plain HTTP on purpose: node-to-node traffic has to be mutually trusted, and a
secured cluster means a cert per node plus a shared truststore — a lot of
moving parts for a fixture. NiFi 1.x still allows `nifi.web.http.port`; 2.x
removed it. 1.24/1.28 is also the line work runs.

> One non-obvious, fatal detail if you build your own: a **clustered** 1.x node
> refuses to launch without an explicit shared `nifi.sensitive.props.key`
> (`NIFI_SENSITIVE_PROPS_KEY`), and every node needs the *same* one. Standalone
> containers generate one each, so this only bites on a cluster.

## Is this a cluster, and is it whole?

```bash
niflow doctor
```

```
✓ cluster: 2/2 nodes connected. cluster-n1 = Primary Node/Cluster Coordinator.
  Primary-node-only processors run on the primary only; load-balanced
  connections redistribute between nodes
```

From the API: `client.cluster_summary()`, `client.cluster_nodes()` (address,
status, elected roles) and `client.disconnected_nodes()`.

## When a node drops out

This is the one that costs an afternoon, because reads keep working.

* **Talking to a node that fell out of its own cluster** — every read succeeds
  and every *change* is refused with
  `400 … This node is disconnected from its configured cluster`. Behind a load
  balancer you may not even know which node you got. `niflow doctor` now fails
  loudly on it and tells you to point somewhere else.
* **Talking to a connected node while another is disconnected** — creates,
  updates and starts still work; **deletes are refused**
  (`409 Cannot delete component because the following Nodes are not connected`).
  That is enough to fail a full `niflow push`, which replaces the group, and
  any plan with a removal in it. `niflow plan` and `push --update` with only
  adds/updates go through.

Both refusals now carry a sentence saying what to do; it is attached at the
transport layer, so every command gets it — CLI, both GUIs, `niflow test`, the
stepper.

niflow always sends `disconnectedNodeAcknowledged: false`. Acknowledging it
means "apply this change knowing that node will never see it", which is not a
decision a tool should make on your behalf.

## Primary-node-only processors

NiFi forces `executionNode=PRIMARY` on types annotated `@PrimaryNodeOnly`
(ListFTP, ListSFTP, ListS3, QueryDatabaseTable, the SaaS pollers — 26 types on
2.7.2, and three more that exist only on 1.x). The model's default is `ALL`, so
without care every plan proposes a change the server will never accept. niflow
harvests the annotation into the catalog and treats PRIMARY as the effective
value on both sides, so `push` then `plan` converges to zero on a cluster.

If a flow uses a primary-node-only type that only exists on 1.x, run
`make catalog-v1` so the 1.x catalog carries its own list.

## Load-balanced connections

`load_balance_strategy` / `partitioning_attribute` / `load_balance_compression`
round-trip through push, pull and plan. On a cluster they actually move
FlowFiles between nodes:

```python
edge = source >> target
edge.load_balance_strategy = "ROUND_ROBIN"
edge.load_balance_compression = "COMPRESS_ATTRIBUTES_ONLY"
```

To *verify* redistribution, read the queue listing, not the status snapshot.
Each listed FlowFile carries the node holding it (`node_address`), while the
nodewise status counts come from heartbeats and can disagree with themselves
mid-transfer — two nodes reporting from different instants is how you get
"80 + 40 = 80".

```bash
niflow-web    # Queues tab -> a queue -> the Node column
```

## A queue is per-node

The consequence that broke things quietly: **NiFi will not hand over one
FlowFile without being told which node holds it.**
`GET /flowfile-queues/{id}/flowfiles/{uuid}` answers
`400 The id of the node in the cluster` — so, before this was fixed, on every
clustered NiFi:

* the queue browser's attribute/content drill-down failed with a raw 400 (both
  GUIs);
* `niflow test` could not collect its results at all;
* the stepper's lookup past the 100-FlowFile listing cap reported "not there"
  for a file that was right there.

`list_flowfiles` now returns `node_id`/`node_address` per file, and
`locate_flowfile`/`flowfile_detail` take an optional `node_id` — pass the one
from the listing when you have it; without it every connected node is asked and
the wrong ones cleanly 404.

## Run-once fires on every node

One `RUN_ONCE` trigger runs the processor **on each node**, so a source that
mints one FlowFile mints *N* on an N-node cluster. `niflow follow` says so:

```
Injected a fixture FlowFile at 'Stamp' on cluster-n1:8080 (7 byte(s) of content, 1 attribute(s)).
Cluster: run-once fires on EVERY node, so it minted 2 FlowFile(s) — one per
node. Following the one above; the rest stay queued and go when the injector does.
```

Stepping itself is unaffected: the stepper follows the uuid it picked, across
nodes and port crossings, exactly as it does on a standalone server.

## Still untested

A local two-node cluster does not reproduce everything. Real load, a rolled-over
or rebuilding provenance repository, and work's own NARs remain open — see
T7h in `todo.md`.
