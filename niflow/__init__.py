"""NiFlow — declarative NiFi flows in Python (NiFi 1.x and 2.x)."""
from niflow.config import NiFiConfig
from niflow.core import (
    Bundle,
    Connection,
    ControllerService,
    Flow,
    Funnel,
    InputPort,
    Label,
    NiFiComponent,
    OutputPort,
    Parameter,
    ParameterContext,
    Port,
    Processor,
    ProcessGroup,
)

__version__ = "0.2.0"

__all__ = [
    "Flow",
    "ProcessGroup",
    "Processor",
    "ControllerService",
    "Connection",
    "Port",
    "InputPort",
    "OutputPort",
    "Funnel",
    "Label",
    "Parameter",
    "ParameterContext",
    "Bundle",
    "NiFiComponent",
    "NiFiConfig",
]
