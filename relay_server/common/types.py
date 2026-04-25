"""Shared dataclasses used by relay, snoop, and OCPP filtering components."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal


@dataclass
class MessageData:
    """Message passed on the relay snoop stream."""

    event: Literal["Connection", "Disconnection", "Message"]
    sender: Literal["CP", "CSMS"]
    protocol: str | None = None
    cp_id: str | None = None
    payload: Any = field(default_factory=dict)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )


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
