"""Direct REST client for NiFi 1.x and 2.x — the pull/push engine.

Why not nipyapi: nipyapi >= 1.0 only speaks NiFi 2.x, and the pull/push
workflow needs exactly two heavyweight endpoints that exist on both lines
(1.11+/1.13+ and 2.x):

* ``GET  /process-groups/{id}/download`` — a process group as a
  ``VersionedFlowSnapshot`` (the JSON :mod:`niflow.formats.json_format` parses).
* ``POST /process-groups/{id}/process-groups`` with an inline
  ``versionedFlowSnapshot`` — create a fully-wired group in one call (with a
  multipart ``/upload`` fallback for servers that dropped the inline form).

Everything else here is small bookkeeping: token login (single-user and
LDAP both POST ``/access/token``), name→id resolution, stop/empty/delete for
replace semantics, and parameter-context updates so Python parameter values —
including sensitive ones supplied via a secrets mapping — win after a push.

Secrets: pass a dict or a path to an env-style file with lines like::

    db.password=hunter2              # applies to that parameter in any context
    etl-context::db.password=hunter2 # scoped to one context

Sensitive parameter *values* never come back from NiFi, so they live only in
that (git-ignored) file and are applied at push time.
"""
from __future__ import annotations

from niflow.rest.common import (  # noqa: F401  (re-exported public surface)
    NiFiApiError,
    _iter_contexts,
    _load_env_overlay,
    _load_secrets,
    _strip_version_control,
    logger,
)
from niflow.rest.flows import FlowsMixin
from niflow.rest.inspect import InspectMixin
from niflow.rest.ops import OpsMixin
from niflow.rest.transport import TransportMixin

__all__ = ["NiFiApiError", "NiFiClient"]


class NiFiClient(TransportMixin, InspectMixin, FlowsMixin, OpsMixin):
    """Thin, version-agnostic NiFi REST client.

    ``session`` is injectable for tests; anything with ``request(method, url,
    **kw) -> response`` (a ``requests.Session`` in production) works.
    

    Implementation lives in :mod:`niflow.rest` — one mixin per concern:
    ``transport`` (session/auth/inventory), ``inspect`` (state, queues,
    FlowFiles, provenance), ``flows`` (pull/plan/push/parameters), and
    ``ops`` (lifecycle, registry version control, backup/rollback). This
    class is the sum; import it from here, never the mixins directly.
    """
