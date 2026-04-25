"""Snoop-side websocket client and parser logic for HA sensor updates."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .client import OCPPSnoopClient
    from .models import OCPPSensorData
    from .parser import OCPPFilter

__all__ = ["OCPPSnoopClient", "OCPPSensorData", "OCPPFilter"]


def __getattr__(name: str):
    if name == "OCPPSnoopClient":
        from .client import OCPPSnoopClient

        return OCPPSnoopClient
    if name == "OCPPSensorData":
        from .models import OCPPSensorData

        return OCPPSensorData
    if name == "OCPPFilter":
        from .parser import OCPPFilter

        return OCPPFilter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
