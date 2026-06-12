"""Factory functions for NiFi processors."""
from niflow.processors.standard import (
    ConvertRecord,
    GetFile,
    InvokeHTTP,
    PutFile,
    SplitJson,
    UpdateAttribute,
)

__all__ = [
    "GetFile",
    "PutFile",
    "InvokeHTTP",
    "ConvertRecord",
    "SplitJson",
    "UpdateAttribute",
]
