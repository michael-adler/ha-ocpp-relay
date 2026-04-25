"""Shared dataclasses used across HA integration and standalone relay utilities."""

from __future__ import annotations

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
class OCPPSensorData:
    """Normalized sensor information generated from OCPP traffic."""

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
