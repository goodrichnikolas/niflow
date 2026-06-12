"""End-to-end "it creates" sweep across the entire processor + controller-service
catalog (marker: integration).

For each type listed in :mod:`niflow.processors.catalog` / :mod:`niflow.services.catalog`,
this deploys a tiny one-component flow under a shared parent process group and
asserts the component shows up on the NiFi canvas. We do *not* configure or run
anything — "good enough that it just creates them" — so the bar is exactly
"NiFi accepted the create-component call". Types flagged as RESTRICTED or
DEPRECATED in the catalog are run as ``xfail`` so the suite stays green when
they cannot be created standalone (e.g. require additional policies).

Auto-skipped when NiFi isn't reachable (see ``conftest.py``'s ``nifi`` fixture).
Opt-in via ``make test-integration`` (after ``make nifi-up && make nifi-wait``);
expect roughly 5–10 minutes for the full ~415-type sweep.
"""
from __future__ import annotations

import re

import pytest

from niflow import Flow
from niflow.core import ControllerService, Processor
from niflow.processors import catalog as proc_catalog
from niflow.services import catalog as svc_catalog

pytestmark = pytest.mark.integration

PARENT = "NiFlowCatalog"

# A component name must satisfy NiFi's identifier rules; the simple class name
# is generally fine, but we sanitise just in case.
_SAFE_NAME = re.compile(r"[^0-9A-Za-z_-]")


def _simple(fqcn: str) -> str:
    return _SAFE_NAME.sub("_", fqcn.rsplit(".", 1)[-1])


def _cleanup_parent(nipyapi, name: str = PARENT) -> None:
    """Stop and delete the shared parent PG (cascades to every catalog child)."""
    root_id = nipyapi.canvas.get_root_pg_id()
    for pg in nipyapi.canvas.list_all_process_groups(root_id):
        if pg.component.name == name and pg.component.parent_group_id == root_id:
            try:
                nipyapi.canvas.schedule_process_group(pg.id, False)
            except Exception:
                pass
            nipyapi.canvas.delete_process_group(pg, force=True, refresh=True)


@pytest.fixture(scope="module")
def catalog_pg(nifi):
    """Create the shared ``NiFlowCatalog`` parent once, bulk-delete it at teardown."""
    import nipyapi

    _cleanup_parent(nipyapi)
    # Stand up an empty parent group by deploying a no-op Flow.
    Flow(PARENT, parent_pg="root").deploy()
    yield nipyapi
    _cleanup_parent(nipyapi)


def _xfail_param(fqcn: str, restricted: set, deprecated: set):
    if fqcn in restricted:
        return pytest.param(fqcn, marks=pytest.mark.xfail(reason="restricted"))
    if fqcn in deprecated:
        return pytest.param(fqcn, marks=pytest.mark.xfail(reason="deprecated"))
    return pytest.param(fqcn)


PROC_PARAMS = [
    _xfail_param(t, proc_catalog.RESTRICTED, proc_catalog.DEPRECATED)
    for t in proc_catalog.TYPES
]
SVC_PARAMS = [
    _xfail_param(t, svc_catalog.RESTRICTED, svc_catalog.DEPRECATED)
    for t in svc_catalog.TYPES
]


@pytest.mark.parametrize("ptype", PROC_PARAMS, ids=lambda p: p.rsplit(".", 1)[-1])
def test_processor_creates(catalog_pg, ptype: str):
    nipyapi = catalog_pg
    name = _simple(ptype)
    flow = Flow(f"cat_{name}", parent_pg=PARENT)
    flow.add_processor(Processor(name=name, type=ptype))
    flow.deploy()

    procs = nipyapi.canvas.list_all_processors(flow.nifi_id)
    assert any(
        getattr(p.component, "type", None) == ptype for p in procs
    ), f"Processor {ptype!r} not found on canvas under {flow.name!r}"


@pytest.mark.parametrize("stype", SVC_PARAMS, ids=lambda p: p.rsplit(".", 1)[-1])
def test_controller_service_creates(catalog_pg, stype: str):
    nipyapi = catalog_pg
    name = _simple(stype)
    flow = Flow(f"cat_{name}", parent_pg=PARENT)
    flow.add_controller_service(
        ControllerService(name=name, type=stype, enabled=False)
    )
    flow.deploy()

    services = nipyapi.canvas.list_all_controllers(flow.nifi_id)
    assert any(
        getattr(s.component, "type", None) == stype for s in services
    ), f"Controller service {stype!r} not found on canvas under {flow.name!r}"
