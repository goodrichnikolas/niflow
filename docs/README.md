# NiFlow docs

Short, tutorial-style walkthroughs of each part of niflow. Every page is
written against what the code actually does on **NiFi 1.24/1.28 and 2.x**, and
says where the two lines differ — that gap is where most of the surprises live.

| Page | What it covers |
|---|---|
| [pull-and-push.md](pull-and-push.md) | Getting a live flow into Python and back again |
| [plan-and-apply.md](plan-and-apply.md) | `plan`, `push --update`, and what the differ will and won't touch |
| [validate.md](validate.md) | Catching on your laptop what would fail on the server |
| [trace-and-follow.md](trace-and-follow.md) | Replaying one FlowFile's journey, and stepping one live |
| [testing.md](testing.md) | `niflow test`: inject a FlowFile, assert what comes out |
| [backup-and-rollback.md](backup-and-rollback.md) | The safety net under every mutating push |
| [watch.md](watch.md) | "It was working, nothing changed, it broke at 14:02" |
| [explain.md](explain.md) | LLM-written walkthroughs of a live group |
| [guis.md](guis.md) | The browser helper and the desktop helper |
| [catalog-and-versions.md](catalog-and-versions.md) | The harvested rulebooks and the 1.x↔2.x map |
| [fuzz.md](fuzz.md) | Hunting niflow's own bugs in bulk |
| [version-compat.md](version-compat.md) | Generated report: every property that differs between the lines |
| [work-nifi-setup.md](work-nifi-setup.md) | Connecting to a locked-down corporate NiFi |

## The 60-second version

```bash
niflow list                                   # what's on the canvas
niflow copy "Prod Flow"                       # work on a detached copy, not prod
niflow pull "Prod Flow (copy)" -o flows/prod.py
$EDITOR flows/prod.py
niflow validate flows/prod.py                 # offline: structure + 1.24 compatibility
niflow plan flows/prod.py                     # what would change, semantically
niflow push flows/prod.py --update            # apply just that delta
```

Connection settings come from `.niflow.env` (see
[work-nifi-setup.md](work-nifi-setup.md)); `niflow doctor` tells you why a
connection isn't working, in one screen.
