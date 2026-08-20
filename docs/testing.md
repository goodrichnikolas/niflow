# Testing flows

`niflow test` answers "what does this flow actually do to a file?" as a test,
not as an afternoon of clicking.

```python
# flows/prod.py — alongside the flow itself
from niflow.testing import TestCase

tests = [
    TestCase(
        name="urgent rows reach the audit log as JSON",
        inject_at="Stamp",                       # processor or input port
        content="id,priority\n1,urgent\n",
        attributes={"source": "unit-test"},      # attributes on the injected file
        expect_at="Audit",                       # its INPUT queue is inspected
        expect_attributes={"mime.type": "application/json"},
        expect_content_contains='"priority"',
        expect_count=1,
        timeout=30.0,
    ),
]
```

```bash
niflow test flows/prod.py
niflow test flows/prod.py --keep         # leave the sandbox on the canvas
niflow test flows/prod.py --sandbox "My Sandbox"
```

What it does, per run:

1. pushes the flow to a **throwaway sandbox group** — the real group is never
   touched;
2. starts it, but stops every source processor (the test injects its own data)
   and every `expect_at` collector, so results queue up instead of vanishing;
3. injects each case's FlowFile via a temporary `GenerateFlowFile` wired to
   `inject_at` and triggered exactly once — the case's attributes ride along as
   dynamic properties;
4. waits for files to arrive in the queue feeding `expect_at` and checks the
   expectations against every one collected;
5. deletes the sandbox (`--keep` leaves it for an autopsy).

`inject_at`/`expect_at` take a bare name, or a `Group/Sub/Name` path when names
repeat across groups. Works the same on 1.x and 2.x.

## What to assert

| Field | Meaning |
|---|---|
| `expect_attributes` | every key must match on each collected file |
| `expect_content` | exact payload |
| `expect_content_contains` | substring — the forgiving one |
| `expect_count` | how many files must arrive (default 1) |
| `timeout` | seconds to wait for them (default 30) |

Failures print per case with what was actually collected, so "it produced
something, just not that" is one read rather than a hunt.

## Related

* [validate.md](validate.md) — everything checkable without a server.
* [trace-and-follow.md](trace-and-follow.md) — when a test fails and you want
  to watch one file walk through it.
