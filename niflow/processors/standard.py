"""Factory functions for common NiFi 2.x standard processors.

Each returns a configured :class:`~niflow.core.Processor`. Pass ``name``, an
optional ``properties`` dict (merged over sensible defaults), and any processor
settings (``scheduling_period``, ``scheduling_strategy``, ``concurrent_tasks``,
``auto_terminate``, ``position``).

Type strings are verified against the NiFi 2.x bundles. Note ``UpdateAttribute``
lives in the ``...processors.attributes`` package, not ``...standard``.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from niflow.core import Processor


def _processor(
    type_: str,
    name: str,
    defaults: Optional[Dict[str, Any]] = None,
    properties: Optional[Dict[str, Any]] = None,
    **settings: Any,
) -> Processor:
    merged = {**(defaults or {}), **(properties or {})}
    return Processor(name=name, type=type_, properties=merged, **settings)


def GetFile(name: str = "GetFile", properties: Optional[Dict[str, Any]] = None, **settings: Any) -> Processor:
    """Pull files from a local directory into the flow."""
    return _processor(
        "org.apache.nifi.processors.standard.GetFile",
        name,
        defaults={"Input Directory": "/in", "Keep Source File": "false"},
        properties=properties,
        **settings,
    )


def PutFile(name: str = "PutFile", properties: Optional[Dict[str, Any]] = None, **settings: Any) -> Processor:
    """Write FlowFiles to a local directory."""
    return _processor(
        "org.apache.nifi.processors.standard.PutFile",
        name,
        defaults={"Directory": "/out"},
        properties=properties,
        **settings,
    )


def InvokeHTTP(name: str = "InvokeHTTP", properties: Optional[Dict[str, Any]] = None, **settings: Any) -> Processor:
    """Make an HTTP request (the property is 'HTTP URL' in NiFi 2.x)."""
    return _processor(
        "org.apache.nifi.processors.standard.InvokeHTTP",
        name,
        defaults={"HTTP Method": "GET"},
        properties=properties,
        **settings,
    )


def ConvertRecord(name: str = "ConvertRecord", properties: Optional[Dict[str, Any]] = None, **settings: Any) -> Processor:
    """Convert records between formats. Requires a Record Reader and Record Writer
    controller service (pass the service instances as property values)."""
    return _processor(
        "org.apache.nifi.processors.standard.ConvertRecord",
        name,
        properties=properties,
        **settings,
    )


def SplitJson(name: str = "SplitJson", properties: Optional[Dict[str, Any]] = None, **settings: Any) -> Processor:
    """Split a JSON array into one FlowFile per element."""
    return _processor(
        "org.apache.nifi.processors.standard.SplitJson",
        name,
        defaults={"JsonPath Expression": "$.*"},
        properties=properties,
        **settings,
    )


def UpdateAttribute(name: str = "UpdateAttribute", properties: Optional[Dict[str, Any]] = None, **settings: Any) -> Processor:
    """Add or modify FlowFile attributes (dynamic properties become attributes)."""
    return _processor(
        "org.apache.nifi.processors.attributes.UpdateAttribute",
        name,
        properties=properties,
        **settings,
    )
