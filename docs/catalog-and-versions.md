# The catalog, and the two NiFi lines

NiFi has no "describe type X" endpoint. Relationships, property descriptors,
required flags, allowable values — none of it exists until a component is
instantiated. So niflow **harvests** it: spin up a throwaway group, create one
of every type, read the responses back, delete the group, and generate Python
modules from what it saw.

That harvest is what lets `validate` and `plan` reason about a flow with no
server in the room.

## The generated modules

| Module | What it holds | Regenerate |
|---|---|---|
| `niflow/processors/catalog.py` | 2.x processor rulebook: types, relationships (incl. conditional/dynamic), descriptors, property names, primary-node-only, restricted/deprecated | `make catalog` |
| `niflow/services/catalog.py` | the same for controller services | `make catalog` |
| `niflow/processors/compat_v1.py` | the **1.x** rulebook, harvested from a 1.x server | `make catalog-v1` |
| `niflow/version_map.py` + `docs/version-compat.md` | the 1.x↔2.x property difference map and its readable report | `make version-map` (both servers up) |
| `niflow/import_defaults.py` | what a flow **import** writes that no descriptor predicts | `make import-defaults`, `make import-defaults-v1` |

Everything degrades gracefully: a missing table means "unknown", and the code
falls back to a heuristic rather than guessing.

```python
from niflow.processors import catalog
catalog.AttributesToJSON()               # thin shell over the FQCN
catalog.RESTRICTED, catalog.DEPRECATED   # FQCN sets
```

Regenerating against **your** NiFi is the point at work: it picks up custom
NARs, which no shipped catalog can know about.

## Why two lines matter so much

You author against the 2.x catalog; work runs 1.24/1.28. Between those lines
Apache renamed most property **keys** to their display names, added properties
1.x does not have, and dropped others.

A 2.x-only key pushed at a 1.x server does **not** error. NiFi files it under
*dynamic properties*, the real property keeps its default, and the processor
quietly does the wrong thing. That silence is what the map ends.

The shape of it, 2.7.2 vs 1.24.0 on stock containers: **252 processor types on
both**, 40 only on 2.x, 102 only on 1.24, and 217 of the shared ones differ —
**1302 renamed** properties, 151 only-2.x, 241 only-1.24. Controller services
were the worst blind spot, because nothing harvested them at all: 90 shared
types with **437 renamed** properties, every one of which used to land on 1.24
as an inert dynamic property. (Numbers from `SUMMARY` in
`niflow/version_map.py`; they move when you regenerate against your own
servers.)

What niflow does with it:

* **Emission** rewrites renamed keys into the target's namespace and omits
  what cannot land there, warning per component before the first mutation.
* **The differ** judges against the target line's own descriptors and defaults.
* **`validate`** checks the compatibility baseline
  (`NIFLOW_MIN_NIFI_VERSION`, default 1.24) with no flag and exits non-zero —
  see [validate.md](validate.md).
* **`doctor`** reports catalog-vs-server skew and names the flows under
  `flows/` that would not survive the baseline.

`docs/version-compat.md` is the generated report: every renamed, added and
dropped property, ranked by how much trouble it can cause.

## Import defaults: what create doesn't tell you

Two things a descriptor cannot predict, both harvested by pushing one instance
of every type through the *real import path*:

* **Values the import writes.** Creating a `JsonRecordSetWriter` on 2.7.2
  gives `Allow Scientific Notation = 'false'`, exactly as its descriptor says.
  *Importing* a flow that never mentions the property gives `'true'` — NiFi
  preserving what older flows did. 2.7.2 does this for two types; 1.24.0 for
  none.
* **Components the import creates.** NiFi 2.x moved the AWS processors' inline
  credentials into a controller service, and its import migration *creates* an
  `AWSCredentialsProviderControllerService` and wires it into the required
  property — a component your flow never mentioned (30 types on 2.7.2, none on
  1.24.0).

Without this table the differ read both as drift and planned to unset the
property and delete the service, on every single plan. Now silence means "what
the server will really put there", and naming the service in your flow makes it
an ordinary, fully diffed component again.

## Rulebook dumps

`python -m niflow.codegen --dump-rulebook out.json` writes the *complete*
untrimmed harvest (processors and services) for one server. `make version-map`
dumps both lines and diffs them; the diff is deliberately conservative — a
property is only paired with a counterpart when the pairing is 1:1 in both
directions, and everything it refuses to guess is listed in the report rather
than silently translated. Renames confirmed by hand live in
`CURATED_TYPE_RENAMES` in `niflow/processors/rules.py`, with the evidence for
each one written next to it.
