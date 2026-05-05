"""Parser that converts snooped OCPP payloads into normalized Home Assistant sensors."""

import logging
import re
from typing import Any

from ..shared.models import OCPPSensorData


class OCPPFilter:
    """Stateful parser for OCPP snoop messages into Home Assistant sensor data."""

    _protocol_version_re = re.compile(r"([0-9]+\.[0-9]+(\.[0-9]+)?)")

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
        protocol_raw = self._field(msg, "protocol")
        protocol = None
        if protocol_raw:
            # Extract version number from protocol string, e.g. "ocpp1.6", "OCPP2.0.1"
            match = self._protocol_version_re.search(protocol_raw)
            if match:
                protocol = match.group(1)
            else:
                self._logger.warning(f"Unknown OCPP protocol format: {protocol_raw!r} (cp_id={cp_id})")
                protocol = None

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

        # Route based on normalized protocol version
        if protocol == "1.6":
            return self._filter_ocpp16(cp_id, timestamp, ocpp)
        elif protocol in ("2.0", "2.0.1"):
            return self._filter_ocpp20(cp_id, timestamp, ocpp)
        else:
            self._logger.warning(f"Unsupported or unknown OCPP protocol version: {protocol_raw!r} (parsed: {protocol!r}, cp_id={cp_id})")
            return None

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

        if evse_id is None:
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

        if not isinstance(payload, dict):
            self._logger.warning(f"OCPP 1.6 payload is not a dict: {payload!r} (cp_id={cp_id})")
            return None

        if action == "StatusNotification":
            cable_id = payload.get("connectorId")
            status = payload.get("status")
            if status is None:
                self._logger.warning(f"Missing 'status' in StatusNotification payload: {payload!r} (cp_id={cp_id})")
                return None
            topic = f"{cable_id}/status"
            unique_id = f"OCPP_{cp_id}_{cable_id}_status"
            name = f"Status CP {cp_id}" if cable_id is None else f"C{cable_id} Status CP {cp_id}"
            return [
                OCPPSensorData(
                    cp_id=cp_id,
                    topic=topic,
                    unique_id=unique_id,
                    name=name,
                    value=status,
                    manufacturer=self._manufacturer[cp_id],
                    timestamp=timestamp,
                )
            ]

        if action == "MeterValues":
            messages: list[OCPPSensorData] = []
            meter_values = payload.get("meterValue") or payload.get("meterValues")
            if not isinstance(meter_values, list):
                self._logger.warning(f"Missing or invalid 'meterValue' in MeterValues payload: {payload!r} (cp_id={cp_id})")
                return None
            connector_id = payload.get("connectorId")
            for meter_value in meter_values:
                # Prefer per-sample timestamp when present
                local_ts = meter_value.get("timestamp") if isinstance(meter_value, dict) else None
                sampled_values = (
                    meter_value.get("sampledValue")
                    or meter_value.get("sampledValues")
                    if isinstance(meter_value, dict)
                    else None
                )
                if not isinstance(sampled_values, list):
                    self._logger.warning(f"Missing or invalid 'sampledValue' in meterValue: {meter_value!r} (cp_id={cp_id})")
                    continue
                for sampled_value in sampled_values:
                    if not isinstance(sampled_value, dict):
                        self._logger.warning(f"Invalid sampledValue entry: {sampled_value!r} (cp_id={cp_id})")
                        continue
                    # Resolve unit and multiplier (some vendors include unitOfMeasure objects)
                    unit_obj = sampled_value.get("unitOfMeasure") or {}
                    unit = sampled_value.get("unit")
                    if isinstance(unit_obj, dict):
                        if "unit" in unit_obj:
                            unit = unit_obj.get("unit")
                        multiplier = unit_obj.get("multiplier")
                    else:
                        multiplier = None

                    value = sampled_value.get("value")
                    if multiplier is not None:
                        try:
                            value = float(value) * (10 ** int(multiplier))
                        except Exception:
                            pass

                    sensor = self._new_meter_data(
                        cp_id,
                        local_ts or timestamp,
                        sampled_value.get("measurand"),
                        connector_id,
                        sampled_value.get("location"),
                        value,
                        unit,
                    )
                    if sensor:
                        messages.append(sensor)
            return messages

        return None

    def _filter_ocpp20(self, cp_id: str, timestamp: str, ocpp: list) -> list[OCPPSensorData] | None:
        """Parse OCPP 2.0.1 CALL payloads into normalized telemetry sensors."""
        action = ocpp[2]
        payload = ocpp[3]

        if not isinstance(payload, dict):
            self._logger.warning(f"OCPP 2.0.1 payload is not a dict: {payload!r} (cp_id={cp_id})")
            return None

        if action == "StatusNotification":
            # Some implementations may use evseId or connectorId; likewise some
            # vendors still use 'status' field names. Accept common variants.
            cable_id = payload.get("evseId") or payload.get("connectorId")
            connector_status = payload.get("connectorStatus") or payload.get("status")
            if connector_status is None:
                self._logger.warning(f"Missing 'connectorStatus' in StatusNotification payload: {payload!r} (cp_id={cp_id})")
                return None
            topic = f"{cable_id}/status"
            unique_id = f"OCPP_{cp_id}_{cable_id}_status"
            name = f"Status CP {cp_id}" if cable_id is None else f"C{cable_id} Status CP {cp_id}"
            return [
                OCPPSensorData(
                    cp_id=cp_id,
                    topic=topic,
                    unique_id=unique_id,
                    name=name,
                    value=connector_status,
                    manufacturer=self._manufacturer[cp_id],
                    timestamp=timestamp,
                )
            ]

        if action == "MeterValues":
            messages: list[OCPPSensorData] = []
            meter_values = payload.get("meterValue") or payload.get("meterValues")
            if not isinstance(meter_values, list):
                self._logger.warning(f"Missing or invalid 'meterValue' in MeterValues payload: {payload!r} (cp_id={cp_id})")
                return None
            evse_id = payload.get("evseId")
            for meter_value in meter_values:
                # Prefer per-sample timestamp when present
                local_ts = meter_value.get("timestamp") if isinstance(meter_value, dict) else None
                sampled_values = (
                    meter_value.get("sampledValue")
                    or meter_value.get("sampledValues")
                    if isinstance(meter_value, dict)
                    else None
                )
                if not isinstance(sampled_values, list):
                    self._logger.warning(f"Missing or invalid 'sampledValue' in meterValue: {meter_value!r} (cp_id={cp_id})")
                    continue
                for sampled_value in sampled_values:
                    if not isinstance(sampled_value, dict):
                        self._logger.warning(f"Invalid sampledValue entry: {sampled_value!r} (cp_id={cp_id})")
                        continue
                    unit_obj = sampled_value.get("unitOfMeasure") or {}
                    measurand = sampled_value.get("measurand") or "Energy.Active.Import.Register"
                    if "unit" in unit_obj:
                        unit = unit_obj["unit"]
                    elif measurand.startswith("Power"):
                        unit = "W"
                    elif measurand.startswith("Current"):
                        unit = "A"
                    elif measurand.startswith("Voltage"):
                        unit = "V"
                    else:
                        unit = "Wh"

                    # Apply unitOfMeasure.multiplier when present
                    multiplier = None
                    if isinstance(unit_obj, dict):
                        multiplier = unit_obj.get("multiplier")

                    value = sampled_value.get("value")
                    if multiplier is not None:
                        try:
                            value = float(value) * (10 ** int(multiplier))
                        except Exception:
                            pass

                    sensor = self._new_meter_data(
                        cp_id,
                        local_ts or timestamp,
                        sampled_value.get("measurand"),
                        evse_id,
                        sampled_value.get("location"),
                        value,
                        unit,
                    )
                    if sensor:
                        messages.append(sensor)
            return messages

        return None
