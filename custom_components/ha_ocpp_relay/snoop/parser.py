"""Parser that converts snooped OCPP payloads into normalized Home Assistant sensors."""

import logging
from typing import Any

from ..shared.models import OCPPSensorData


class OCPPFilter:
    """Stateful parser for OCPP snoop messages into Home Assistant sensor data."""

    def __init__(self) -> None:
        """Initialize the instance state."""
        self._logger = logging.getLogger(__name__)
        self._manufacturer: dict[str, str | None] = {}

    @staticmethod
    def _field(msg: Any, key: str, default=None):
        """Read a message field from dict payloads or dataclass/object envelopes."""
        if isinstance(msg, dict):
            return msg.get(key, default)
        return getattr(msg, key, default)

    def filter(self, msg: Any) -> list[OCPPSensorData] | None:
        """Translate one snoop envelope into zero or more normalized sensors.

        This is the protocol boundary where raw OCPP message arrays become
        stable sensor records consumed by the Home Assistant sensor platform.
        """
        if self._field(msg, "event") != "Message":
            return None
        if self._field(msg, "sender") != "CP":
            return None

        cp_id = self._field(msg, "cp_id") or "unknown"
        protocol = self._field(msg, "protocol")
        if protocol and protocol.lower().startswith("ocpp"):
            # Normalize values like "ocpp1.6" -> "1.6" for version dispatch below.
            protocol = protocol[4:]

        ocpp = self._field(msg, "payload")
        # OCPP CALL frames are [2, unique_id, action, payload].
        if not isinstance(ocpp, list) or len(ocpp) < 4:
            return None
        if ocpp[0] != 2:
            return None

        # Cache vendor/manufacturer per charge point when it first appears.
        if cp_id not in self._manufacturer:
            self._manufacturer[cp_id] = None
        if not self._manufacturer[cp_id]:
            self._manufacturer[cp_id] = self._get_manufacturer(ocpp)

        timestamp = self._field(msg, "timestamp")
        if ocpp[2] == "Heartbeat":
            # Heartbeat does not carry measured values; use receive timestamp as sensor state.
            return [
                OCPPSensorData(
                    cp_id=cp_id,
                    topic="heartbeat",
                    unique_id=f"OCPP_{cp_id}_heartbeat",
                    name=f"Heartbeat CP {cp_id}",
                    device_class="timestamp",
                    value=timestamp,
                    manufacturer=self._manufacturer[cp_id],
                    timestamp=timestamp,
                )
            ]

        if protocol == "1.6":
            return self._filter_ocpp16(cp_id, timestamp, ocpp)
        return self._filter_ocpp20(cp_id, timestamp, ocpp)

    def _get_manufacturer(self, ocpp: list) -> str | None:
        """Extract vendor/manufacturer information from DataTransfer frames."""
        action = ocpp[2]
        payload = ocpp[3]
        if action == "DataTransfer" and isinstance(payload, dict):
            return payload.get("vendorId")
        return None

    def _new_meter_data(
        self,
        cp_id: str,
        timestamp: str,
        value_type: str,
        evse_id: str,
        location: str,
        value,
        unit: str,
    ) -> OCPPSensorData | None:
        """Build one normalized measurement sensor from a sampled value row."""
        value_type = (value_type or "Energy.Active.Import.Register").replace(".", "-")

        if value_type.startswith("Current"):
            device_class = "current"
            state_class = "measurement"
        elif value_type.startswith("Energy"):
            device_class = "energy"
            state_class = "total_increasing"
        elif value_type.startswith("Power"):
            device_class = "power"
            state_class = "measurement"
        elif value_type.startswith("Voltage"):
            device_class = "voltage"
            state_class = "measurement"
        else:
            return None

        location = location or "Outlet"
        topic = f"{evse_id}/{location}/{value_type}"
        unique_id = f"OCPP_{cp_id}_{topic}".replace("/", "_")

        # Map W/Wh to kW/kWh, convert value, update unit and name
        orig_unit = unit
        orig_value = value
        if unit == "W":
            unit = "kW"
            try:
                value = float(value) / 1000
            except (TypeError, ValueError):
                pass
        elif unit == "Wh":
            unit = "kWh"
            try:
                value = float(value) / 1000
            except (TypeError, ValueError):
                pass

        if not evse_id:
            name = f"{value_type.replace('-', ' ')} {location} CP {cp_id}"
        else:
            name = f"C{evse_id} {value_type.replace('-', ' ')} {location} CP {cp_id}"

        return OCPPSensorData(
            cp_id=cp_id,
            topic=topic,
            unique_id=unique_id,
            name=name,
            value=value,
            manufacturer=self._manufacturer[cp_id],
            device_class=device_class,
            state_class=state_class,
            unit=unit,
            timestamp=timestamp,
        )

    def _filter_ocpp16(self, cp_id: str, timestamp: str, ocpp: list) -> list[OCPPSensorData] | None:
        """Parse OCPP 1.6 CALL payloads into normalized telemetry sensors."""
        action = ocpp[2]
        payload = ocpp[3]

        if action == "StatusNotification":
            cable_id = payload.get("connectorId")
            topic = f"{cable_id}/status"
            unique_id = f"OCPP_{cp_id}_{cable_id}_status"
            name = f"Status CP {cp_id}" if not cable_id else f"C{cable_id} Status CP {cp_id}"
            return [
                OCPPSensorData(
                    cp_id=cp_id,
                    topic=topic,
                    unique_id=unique_id,
                    name=name,
                    value=payload.get("status"),
                    manufacturer=self._manufacturer[cp_id],
                    timestamp=timestamp,
                )
            ]

        if action == "MeterValues":
            messages: list[OCPPSensorData] = []
            # Flatten OCPP 1.6 meterValue/sampledValue nesting into one sensor list.
            for meter_value in payload.get("meterValue", []):
                for sampled_value in meter_value.get("sampledValue", []):
                    sensor = self._new_meter_data(
                        cp_id,
                        timestamp,
                        sampled_value.get("measurand"),
                        payload.get("connectorId"),
                        sampled_value.get("location"),
                        sampled_value.get("value"),
                        sampled_value.get("unit"),
                    )
                    if sensor:
                        messages.append(sensor)
            return messages

        return None

    def _filter_ocpp20(self, cp_id: str, timestamp: str, ocpp: list) -> list[OCPPSensorData] | None:
        """Parse OCPP 2.0.1 CALL payloads into normalized telemetry sensors."""
        action = ocpp[2]
        payload = ocpp[3]

        if action == "StatusNotification":
            cable_id = payload.get("evseId")
            topic = f"{cable_id}/status"
            unique_id = f"OCPP_{cp_id}_{cable_id}_status"
            name = f"Status CP {cp_id}" if not cable_id else f"C{cable_id} Status CP {cp_id}"
            return [
                OCPPSensorData(
                    cp_id=cp_id,
                    topic=topic,
                    unique_id=unique_id,
                    name=name,
                    value=payload.get("connectorStatus"),
                    manufacturer=self._manufacturer[cp_id],
                    timestamp=timestamp,
                )
            ]

        if action == "MeterValues":
            messages: list[OCPPSensorData] = []
            # OCPP 2.0 carries units in a nested object, defaulting to Wh when omitted.
            for meter_value in payload.get("meterValue", []):
                for sampled_value in meter_value.get("sampledValue", []):
                    unit_obj = sampled_value.get("unitOfMeasure") or {}
                    unit = unit_obj.get("unit", "Wh")
                    sensor = self._new_meter_data(
                        cp_id,
                        timestamp,
                        sampled_value.get("measurand"),
                        payload.get("evseId"),
                        sampled_value.get("location"),
                        sampled_value.get("value"),
                        unit,
                    )
                    if sensor:
                        messages.append(sensor)
            return messages

        return None
