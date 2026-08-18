<!-- niflow-explanation group="NiflowTorture" fingerprint="b777fea61461" generated="2026-08-15 19:25 UTC" model="gemini-flash-lite-latest" -->
# NiflowTorture

**Summary:** This process group generates mock text data every 60 seconds, fans it out into multiple processing branches involving attribute tagging, regex text rewriting, a looping self-reference, and conditional retry routing, ultimately terminating at a logging sink or passing data into nested process groups.

## Walkthrough
1. **Source Generation:** The flow starts at **Gen 🚀 'source'**, which runs every 60 seconds (Timer-Driven with a 0-second period indicated as 60 sec in its schedule) to generate a single FlowFile containing custom CSV-like text lines with escaped expressions and backslashes.
2. **Branching Out:** The successful output of **Gen 🚀 'source'** fans out simultaneously to four paths:
   - **Path A (Ouroboros Loop):** Goes to **Ouroboros**, which sets an attribute `seen` to `true`. From there, it loops back infinitely into itself via its `success` relationship, and also routes a copy to **Sink 'log' <ω>** to log attributes at INFO level before auto-terminating the `success` relationship.
   - **Path B (Worker Processing):** Goes to **Fork**, which checks the `code` attribute for the strings "alpha" (route `hot`) or "beta" (route `cold`). Both `hot` and `cold` routes go to **Worker** (which sets attribute `lane` to `one`) and **Worker 2** (which sets attribute `lane` to `two`). **Worker** sends its output into the nested group **Stage** via its input port `Stage :: in`. **Worker 2** sends its output into the nested group **Stage 2** via its input port `Stage 2 :: in`.
   - **Path C (Regex Rewrite & Fanout):** Goes to **Rewrite \\d+ \"regex\"**, which evaluates text line-by-line using a regex search value (`(\d{2,4})-(\w+)\s*$`) and rewrites it. Successful FlowFiles then hit **Fanout**, which checks the `fan` attribute against values 0 through 23 (`r00` through `r23`). The matched routes (`r00` through `r15`) feed into internal funnels which ultimately connect to the stopped **Cron 'audit'** processor; relationships `r16` through `r23` and `unmatched` are auto-terminated.
   - **Path D (Retry Logic):** Goes to **RetryRouter**, which checks if the attribute `retry.count` is greater than or equal to 3 (`give-up`). If it gives up, it routes to **Sink 'log' <ω>** to log the attributes and terminate. If unmatched, it routes to **Bump**, which increments `retry.count` by 1 (`${retry.count:plus(1)}`) and loops back to **RetryRouter**.

## Nested groups
- Stage: This process group receives data through an input port, routes it to a disabled processor, and forwards any output to an output port.
- Stage 2: This process group receives data from the input port, tags it with an attribute indicating it has passed through the second stage, and passes it along to the output port.

## Gotchas
- **Cron 'audit' is stopped:** The **Cron 'audit'** processor is in a `STOPPED` state and has its `success` relationship auto-terminated, meaning any data reaching it via the funnels from **Fanout** and the nested groups will queue up indefinitely or backpressure unless cleared.
- **Ouroboros Infinite Loop:** **Ouroboros** routes to itself on `success`, creating an endless self-referencing loop while simultaneously spinning off copies to the sink.

---
*Generated 2026-08-15 19:25 UTC by gemini-flash-lite-latest via `niflow explain` — regenerate after the flow changes.*
