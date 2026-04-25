"""Shared message and sensor type aliases used by relay_server modules."""

from custom_components.ha_ocpp_relay.shared.models import MessageData, OCPPSensorData

SensorData = OCPPSensorData

__all__ = ["MessageData", "SensorData"]
