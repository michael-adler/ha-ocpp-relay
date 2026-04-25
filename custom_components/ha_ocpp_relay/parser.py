"""Parser that converts snooped OCPP payloads into normalized Home Assistant sensors."""

import logging

from .models import OCPPSensorData


class OCPPFilter:
    """Stateful parser for OCPP snoop messages into Home Assistant sensor data."""

    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)
        self._manufacturer: dict[str, str | None] = {}

    def filter(self, msg: dict) -> list[OCPPSensorData] | None:
        if msg.get("event") != "Message":
            return None
        if msg.get("sender") != "CP":
            return None

        cp_id = msg.get("cp_id") or "unknown"
        protocol = msg.get("protocol")
        if protocol and protocol.lower().startswith("ocpp"):
            protocol = protocol[4:]

        ocpp = msg.get("payload")
        if not isinstance(ocpp, list) or len(ocpp) < 4:
            return None
        if ocpp[0] != 2:
            return None

        if cp_id not in self._manufacturer:
            self._manufacturer[cp_id] = None
        if not self._manufacturer[cp_id]:
            self._manufacturer[cp_id] = self._get_manufacturer(ocpp)

        timestamp = msg.get("timestamp")
        if ocpp[2] == "Heartbeat":
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
