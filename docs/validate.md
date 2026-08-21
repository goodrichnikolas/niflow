# Validate

`niflow validate` is the gate: everything it can prove wrong on your laptop,
before a push turns it into an afternoon at work.

```bash
niflow validate flows/prod.py                      # offline, exits 1 on problems
niflow validate flows/prod.py --target-version 2.7.2
niflow validate flows/prod.py --live               # + NiFi's own validation
```

## What it checks offline

**Structure**

* duplicate names within a group (niflow identity is name-based);
* a controller service used as a property value but never added to the flow;
* a connection endpoint that is not part of the flow;
* a connection whose endpoints live in **different groups** — NiFi needs an
  input/output port to cross a group boundary, and without this the push fails
  with "no component could be found in the Process Group", which reads like a
  niflow bug;
* `#{param}` references when **no parameter context is bound** anywhere up the
  tree. (`##{x}` is NiFi's escape for a literal `#{x}` and is not a reference.)
* a **non-sensitive property referencing a sensitive parameter**. NiFi says it
  itself — *"Cannot add Parameter with name 'x' unless that Parameter is Not
  Sensitive because a Parameter with that name is already referenced from a
  Property that is not Sensitive"* — and a flow that reaches that state on 1.24
  can no longer be **downloaded at all** (HTTP 500), which breaks pull, plan and
  backup together.
* a **controller-service reference holding a string**: a property whose
  descriptor identifies a service must be wired to one, not set to `#{param}`
  or an expression. Same 500 on 1.24 for the parameter case.

**Relationships**

* every relationship of every processor is connected or auto-terminated,
  judged against the *target line's* relationship set — 1.24's `UpdateAttribute`
  has a `set state fail` relationship that 2.x does not, and a processor with
  an unhandled relationship will not start;
* a relationship that does not exist on the type (a typo in `auto_terminate`);
* conditional relationships — ones a property value switches on — and
  dynamic-property relationships (`RouteOnAttribute`) are both understood.

**Properties**

* required properties that are not set (processors **and** controller
  services) — except the ones the target line fills in *itself* on import, like
  the AWS credentials service NiFi 2.x creates and wires in;
* values outside a property's allowable set;
* a key that is a property of neither NiFi line but is one separator or one
  capital away from a real one — `max-bin-age` where MergeContent wants
  `Max Bin Age`. NiFi files an unrecognised key under *dynamic properties*,
  does nothing with it, and marks the component invalid on the server. This
  check is where you find out instead.

**Model-level values**: scheduling periods and durations, CRON field counts,
concurrent tasks, run duration, `PRIMARY` execution node on a processor with
an incoming connection (NiFi rejects it).

## The compatibility baseline

Every validate also checks the flow against the **oldest NiFi line you have to
support** — `NIFLOW_MIN_NIFI_VERSION` in `.niflow.env`, default `1.24`. This is
on by default on purpose: a 2.x-only property lands on a 1.24 server as an
inert dynamic property and fails *silently*, and the cheapest moment to hear
about it is at home.

```bash
niflow validate flows/prod.py --no-compat-check       # only this line matters
NIFLOW_MIN_NIFI_VERSION=none niflow validate flows/prod.py
niflow validate flows/prod.py --target-version 1.24   # ad-hoc: judge against that line
```

`--target-version` uses the generated cross-version map, so it works offline —
you can find out at home what will break at work.

## `--live`

```bash
niflow validate flows/prod.py --live
```

Pushes a throwaway sandbox group, waits for NiFi's (asynchronous) validation to
settle, prints the server's own errors for every processor **and controller
service**, and deletes the sandbox. This catches what no static rulebook can:
bad EL, unsatisfied service requirements, values this particular build rejects.
