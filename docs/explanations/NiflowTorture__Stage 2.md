<!-- niflow-explanation group="NiflowTorture/Stage 2" fingerprint="e0625e151176" generated="2026-08-15 19:25 UTC" model="gemini-flash-lite-latest" -->
# NiflowTorture/Stage 2

**Summary:** This process group receives data from the input port, tags it with an attribute indicating it has passed through the second stage, and passes it along to the output port.

## Walkthrough
Data enters the "Stage 2" process group through the input port named **in**, which feeds into the **in** processor (a UpdateAttribute processor). 

The **in** processor runs continuously on a timer-driven schedule of `0 sec`. As FlowFiles pass through it, it sets a single configuration property: it adds an attribute named `stage` with the value `second` to every FlowFile. 

From the **in** processor, the flow splits into two connections:
- A self-referencing connection back to the **in** processor with no relationships defined.
- A **success** relationship connection that routes the modified FlowFiles directly to the **out** output port, exiting this process group for downstream consumption.

## Gotchas
- The UpdateAttribute processor **in** has a self-referencing connection back to itself with no relationships selected, which is an invalid or redundant configuration that will likely cause routing errors or infinite loops in NiFi.

---
*Generated 2026-08-15 19:25 UTC by gemini-flash-lite-latest via `niflow explain` — regenerate after the flow changes.*
