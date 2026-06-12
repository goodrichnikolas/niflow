"""Factory functions for common NiFi 2.x controller services.

Record reader/writer services pair with record processors like
:func:`~niflow.processors.standard.ConvertRecord`. Add them to the flow with
``add_controller_service(...)`` and reference the instance from a processor's
properties (e.g. ``{"Record Reader": reader}``); NiFlow substitutes the NiFi
UUID at deploy time.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from niflow.core import ControllerService


def _service(
    type_: str,
    name: str,
    defaults: Optional[Dict[str, Any]] = None,
    properties: Optional[Dict[str, Any]] = None,
) -> ControllerService:
    merged = {**(defaults or {}), **(properties or {})}
    return ControllerService(name=name, type=type_, properties=merged)


def JsonTreeReader(name: str = "JsonTreeReader", properties: Optional[Dict[str, Any]] = None) -> ControllerService:
    """Read JSON into NiFi records."""
    return _service("org.apache.nifi.json.JsonTreeReader", name, properties=properties)


def JsonRecordSetWriter(name: str = "JsonRecordSetWriter", properties: Optional[Dict[str, Any]] = None) -> ControllerService:
    """Write NiFi records out as JSON."""
    return _service("org.apache.nifi.json.JsonRecordSetWriter", name, properties=properties)
