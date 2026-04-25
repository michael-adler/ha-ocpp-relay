"""Data models for OCPP-derived sensor state stored in the integration runtime."""

from dataclasses import dataclass
from typing import Any


@dataclass
class OCPPSensorData:
    cp_id: str
    topic: str
    unique_id: str
    name: str
    value: Any
    timestamp: str | None = None
    manufacturer: str | None = None
    device_class: str | None = None
    state_class: str | None = None
    unit: str | None = None
