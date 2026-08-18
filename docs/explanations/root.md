<!-- niflow-explanation group="(root)" fingerprint="9bef070e33a9" generated="2026-08-15 19:25 UTC" model="gemini-flash-lite-latest" -->
# (root)

**Summary:** This process group serves as the top-level container that coordinates data generation, transformation, and routing across two specialized child pipelines.

## Walkthrough
Since this top-level group contains no processors or connections directly on its canvas, all data processing is delegated entirely to its two nested child process groups, **AbcToJson** and **NiflowTorture**. 

## Nested groups
- AbcToJson: This process group generates an empty FlowFile every 60 seconds, enriches it with sequential attributes `a`, `b`, and `c`, converts those attributes into a JSON payload, and writes the resulting file to a local directory.
- NiflowTorture: This process group generates mock text data every 60 seconds, fans it out into multiple processing branches involving attribute tagging, regex text rewriting, a looping self-reference, and conditional retry routing, ultimately terminating at a logging sink or passing data into nested process groups.

---
*Generated 2026-08-15 19:25 UTC by gemini-flash-lite-latest via `niflow explain` — regenerate after the flow changes.*
