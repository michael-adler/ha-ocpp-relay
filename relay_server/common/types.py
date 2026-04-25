"""Shared dataclasses used by relay, snoop, and OCPP filtering components."""

from dataclasses import dataclass
from typing import Any, Literal

from custom_components.ha_ocpp_relay.relay.core import MessageData


@dataclass
class SensorData:
    """Normalized sensor information generated from OCPP traffic."""

    cp_id: str
    topic: str
    manufacturer: str | None
    unique_id: str
    name: str
    value: Any
    device_class: str | None = None
    state_class: str | None = None
    unit: str | None = None
    timestamp: str | None = None
