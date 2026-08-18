<!-- niflow-explanation group="NiflowTorture/Stage" fingerprint="e27db96ee6d7" generated="2026-08-15 19:25 UTC" model="gemini-flash-lite-latest" -->
# NiflowTorture/Stage

**Summary:** This process group receives data through an input port, routes it to a disabled processor, and forwards any output to an output port.

## Walkthrough
Data enters the process group through the input port named `in` and flows directly into a processor named `Worker`. The `Worker` processor is of type `UpdateAttribute` and is configured to set the attribute `stage` to the literal value `first`. However, because `Worker` is currently in a **DISABLED** state, no data can be processed or move forward. 

If it were enabled, the `Worker` processor would run on a timer-driven schedule of `0 sec` (triggering as fast as possible) and route successful FlowFiles via the `success` relationship to the output port named `out`, which exits the group.

## Gotchas
- The `Worker` processor is **DISABLED**, meaning all data entering the `in` port will queue up and stall at this step until the processor is manually enabled.

---
*Generated 2026-08-15 19:25 UTC by gemini-flash-lite-latest via `niflow explain` — regenerate after the flow changes.*
