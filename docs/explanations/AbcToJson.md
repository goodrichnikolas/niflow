<!-- niflow-explanation group="AbcToJson" fingerprint="ef20f30a01df" generated="2026-08-15 19:25 UTC" model="gemini-flash-lite-latest" -->
# AbcToJson

**Summary:** This process group generates an empty FlowFile every 60 seconds, enriches it with sequential attributes `a`, `b`, and `c`, converts those attributes into a JSON payload, and writes the resulting file to a local directory.

## Walkthrough
1. **CreateFlowFile** acts as the trigger for the flow. It runs on a timer-driven schedule every 60 seconds, generating a new, empty (0 byte) FlowFile with text data format, and passes it forward via the `success` relationship.
2. The FlowFile moves to **AddA**, which runs continuously (0 sec period). It adds an attribute named `a` with the value `1` to the FlowFile's metadata and routes to **AddB** via `success`.
3. **AddB** is supposed to add an attribute `b` with the value `2`, but because it is stopped, the FlowFile halts here. Assuming it were running, it would pass the FlowFile via `success` to **AddC**.
4. **AddC** runs continuously and adds an attribute named `c` with the value `3` to the FlowFile's metadata. From here, the flow splits into three parallel branches via its `success` relationship:
   - **Branch 1 (JSON Output):** The FlowFile goes to **AbcJson**. This processor takes the attributes `a`, `b`, and `c` (along with core attributes) and writes them into the FlowFile content as a JSON object (`Destination` set to `flowfile-content`, JSON handling strategy set to `ESCAPED`). Any `failure` is auto-terminated. From `AbcJson`, the FlowFile moves to **Sink** (`PutFile`), which writes the file to the local directory `/out` (creating missing directories if needed, failing on conflicts) and auto-terminates both its `success` and `failure` relationships.
   - **Branch 2 (CSV Output):** The FlowFile goes to **CSV Creation**. This processor attempts to convert attributes into a CSV format, but its `Destination` is set to `flowfile-attribute` while both `success` and `failure` relationships are auto-terminated, meaning the transformed FlowFiles are dropped entirely.
   - **Branch 3 (Logging):** The FlowFile goes to **LogAttribute**. This logs the FlowFile's properties matching the regular expression `.*` (all attributes) to the NiFi logs at INFO level, with the `success` relationship auto-terminated.

## Gotchas
- **Processor AddB is stopped:** The processor **AddB** is in a `STOPPED` state, which creates a complete dead end for all data coming out of **AddA**. No FlowFiles will ever reach **AddC** or any downstream steps while **AddB** remains stopped.
- **Dead-end branch (CSV Creation):** The **CSV Creation** processor has both its `success` and `failure` relationships auto-terminated, meaning any data routed to it is silently discarded without producing an output.
- **Zero-period scheduling on all running processors:** Processors configured with a period of `0 sec` run as fast as possible continuously, which can consume CPU resources rapidly if data volume spikes.

---
*Generated 2026-08-15 19:25 UTC by gemini-flash-lite-latest via `niflow explain` — regenerate after the flow changes.*
