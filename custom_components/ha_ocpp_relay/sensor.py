"""Dynamic sensor entity platform for values discovered from OCPP snoop traffic."""

from __future__ import annotations

import json
from datetime import datetime
import logging
from pathlib import Path
from urllib.parse import urlparse

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity, SensorStateClass
from homeassistant.helpers.restore_state import RestoreEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.entity import DeviceInfo, EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import CONF_CPMS_URL, DOMAIN, SIGNAL_NEW_SENSOR, SIGNAL_SENSOR_UPDATE
from .snoop.client import OCPPSnoopClient

_LOGGER = logging.getLogger(__name__)


def _load_manifest_version() -> str | None:
    """Read integration version from manifest.json for device metadata."""
    try:
        manifest_path = Path(__file__).with_name("manifest.json")
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
        version = data.get("version")
        return version if isinstance(version, str) and version else None
    except (OSError, json.JSONDecodeError):
        return None


_INTEGRATION_SW_VERSION = _load_manifest_version()


def _registry_sensor_entries(hass: HomeAssistant, entry: ConfigEntry) -> dict[str, str | None]:
    """Return known OCPP sensor IDs already stored in HA's entity registry.

    This allows recreating entities immediately after integration reload,
    before fresh OCPP frames arrive.
    """
    entity_registry = er.async_get(hass)
    entries: dict[str, str | None] = {}
    unique_id_prefix = f"{entry.entry_id}_"

    for registry_entry in er.async_entries_for_config_entry(entity_registry, entry.entry_id):
        if registry_entry.domain != "sensor" or registry_entry.platform != DOMAIN:
            continue
        if not registry_entry.unique_id.startswith(unique_id_prefix):
            continue

        ocpp_unique_id = registry_entry.unique_id[len(unique_id_prefix) :]
        entries[ocpp_unique_id] = registry_entry.original_name or registry_entry.name

    return entries


def _configuration_url(value: str | None) -> str | None:
    """Return a HA-valid configuration URL or None.

    Home Assistant device registry accepts http/https URLs here, but rejects
    websocket endpoints such as ws:// and wss://.
    """
    if not value:
        return None

    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        return None
    return value


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Create dynamic sensor entities backed by snoop-discovered metrics.

    Existing sensors are created immediately, then new sensors are added later
    through dispatcher events emitted by the snoop client.
    """
    client: OCPPSnoopClient = hass.data[DOMAIN][entry.entry_id]["client"]
    known_entities: dict[str, OCPPSensorEntity] = {}
    registry_entries = _registry_sensor_entries(hass, entry)

    def add_entity(unique_id: str) -> None:
        """Create and register an entity for one discovered sensor ID."""
        if unique_id in known_entities:
            return

        entity = OCPPSensorEntity(
            entry,
            client,
            unique_id,
            restored_name=registry_entries.get(unique_id),
        )
        known_entities[unique_id] = entity
        async_add_entities([entity])

    for unique_id in sorted(set(client.sensors.keys()) | set(registry_entries.keys())):
        add_entity(unique_id)

    @callback
    def _on_new_sensor(unique_id: str) -> None:
        """React to runtime sensor discovery from the snoop client."""
        add_entity(unique_id)

    entry.async_on_unload(
        async_dispatcher_connect(
            hass,
            f"{SIGNAL_NEW_SENSOR}_{entry.entry_id}",
            _on_new_sensor,
        )
    )



class OCPPSensorEntity(SensorEntity, RestoreEntity):
    _attr_has_entity_name = True
    _DIAGNOSTIC_ID_SUFFIXES: tuple[str, ...] = ("_heartbeat", "_vendor", "_firmware")
    _RESTORE_ID_SUFFIXES: tuple[str, ...] = ("_vendor", "_firmware")

    def __init__(
        self,
        entry: ConfigEntry,
        client: OCPPSnoopClient,
        unique_id: str,
        restored_name: str | None = None,
    ) -> None:
        """Initialize the instance state."""
        self._entry = entry
        self._client = client
        self._ocpp_unique_id = unique_id
        self._unsub = None
        self._restored_value = None
        self._restored_device_class = None
        self._restored_state_class = None
        self._restored_native_unit = None
        self._restored_attributes = None
        self._restore_on_unavailable = unique_id.endswith(self._RESTORE_ID_SUFFIXES)

        sensor = self._client.sensors.get(self._ocpp_unique_id)
        self._attr_unique_id = f"{self._entry.entry_id}_{self._ocpp_unique_id}"
        if sensor is not None:
            self._attr_name = sensor.name
        elif restored_name:
            self._attr_name = restored_name
        else:
            self._attr_name = self._ocpp_unique_id
        if unique_id.endswith(self._DIAGNOSTIC_ID_SUFFIXES):
            self._attr_entity_category = EntityCategory.DIAGNOSTIC
        if sensor is not None and sensor.device_class == "energy":
            self._restore_on_unavailable = True

    async def async_added_to_hass(self) -> None:
        """Subscribe to per-entry sensor update events and restore state for energy sensors."""
        @callback
        def _on_update(unique_id: str) -> None:
            """Write HA state when this entity's backing sensor changes."""
            if unique_id == self._ocpp_unique_id:
                sensor = self._sensor
                if sensor is not None:
                    self._attr_name = sensor.name
                    if sensor.device_class == "energy":
                        self._restore_on_unavailable = True
                self.async_write_ha_state()

        self._unsub = async_dispatcher_connect(
            self.hass,
            f"{SIGNAL_SENSOR_UPDATE}_{self._entry.entry_id}",
            _on_update,
        )

        # Restore last state for sensors that should survive temporary source gaps.
        last_state = await self.async_get_last_state()
        if last_state is None:
            return

        last_attrs = dict(last_state.attributes)
        self._restored_device_class = last_attrs.get("device_class")
        self._restored_state_class = last_attrs.get("state_class")
        self._restored_native_unit = last_attrs.get("unit_of_measurement")
        self._restored_attributes = {
            key: last_attrs.get(key)
            for key in ("cp_id", "topic", "timestamp", "manufacturer")
            if last_attrs.get(key) is not None
        }

        if self._restored_device_class == "energy":
            self._restore_on_unavailable = True

        if self._restore_on_unavailable and last_state.state not in (None, "unknown", "unavailable"):
            self._restored_value = last_state.state

    async def async_will_remove_from_hass(self) -> None:
        """Disconnect dispatcher subscriptions before entity removal."""
        if self._unsub:
            self._unsub()
            self._unsub = None

    @property
    def _sensor(self):
        """Return the latest parsed sensor model for this entity, if available."""
        return self._client.sensors.get(self._ocpp_unique_id)

    @property
    def native_value(self):
        """Expose a Home Assistant-friendly state value.

        Timestamp sensors are parsed to datetime; numeric measurements are
        converted to float so recorder/statistics treat them correctly.
        For energy sensors, restore last value if integration is not running.
        """
        sensor = self._sensor
        if sensor is None:
            if self._restore_on_unavailable and self._restored_value is not None:
                if self.device_class == SensorDeviceClass.ENERGY:
                    try:
                        return float(self._restored_value)
                    except (TypeError, ValueError):
                        return self._restored_value
                return self._restored_value
            return None

        if sensor.device_class == "timestamp" and sensor.value:
            try:
                return datetime.fromisoformat(str(sensor.value).replace("Z", "+00:00"))
            except ValueError:
                return None

        if sensor.device_class in {"current", "energy", "power", "voltage"}:
            try:
                return float(sensor.value)
            except (TypeError, ValueError):
                return None

        return sensor.value

    @property
    def available(self) -> bool:
        """Energy sensors are always available (show last value); others use default logic."""
        sensor = self._sensor
        if self._restore_on_unavailable:
            # Available if we have a value (live or restored)
            return sensor is not None or self._restored_value is not None
        # Default: available if sensor is present
        return sensor is not None

    @property
    def native_unit_of_measurement(self):
        """Expose the unit parsed from OCPP sampled value payloads."""
        sensor = self._sensor
        if sensor is None:
            return self._restored_native_unit
        return sensor.unit

    _DEVICE_CLASS_MAP: dict[str, SensorDeviceClass] = {
        "current": SensorDeviceClass.CURRENT,
        "energy": SensorDeviceClass.ENERGY,
        "power": SensorDeviceClass.POWER,
        "timestamp": SensorDeviceClass.TIMESTAMP,
        "voltage": SensorDeviceClass.VOLTAGE,
    }

    _STATE_CLASS_MAP: dict[str, SensorStateClass] = {
        "measurement": SensorStateClass.MEASUREMENT,
        "total": SensorStateClass.TOTAL,
        "total_increasing": SensorStateClass.TOTAL_INCREASING,
    }

    @property
    def device_class(self) -> SensorDeviceClass | None:
        """Map parser-derived semantic type to Home Assistant device class."""
        sensor = self._sensor
        if sensor is not None and sensor.device_class is not None:
            return self._DEVICE_CLASS_MAP.get(sensor.device_class)

        restored_device_class = self._restored_device_class
        if isinstance(restored_device_class, SensorDeviceClass):
            return restored_device_class
        if isinstance(restored_device_class, str):
            return self._DEVICE_CLASS_MAP.get(restored_device_class)
        return None

    @property
    def state_class(self) -> SensorStateClass | None:
        """Expose state class so long-term statistics are computed correctly."""
        sensor = self._sensor
        if sensor is not None and sensor.state_class is not None:
            return self._STATE_CLASS_MAP.get(sensor.state_class)

        restored_state_class = self._restored_state_class
        if isinstance(restored_state_class, SensorStateClass):
            return restored_state_class
        if isinstance(restored_state_class, str):
            return self._STATE_CLASS_MAP.get(restored_state_class)
        return None

    @property
    def extra_state_attributes(self):
        """Attach protocol context useful for debugging and automation rules."""
        sensor = self._sensor
        if sensor is None:
            return self._restored_attributes

        attrs = {
            "cp_id": sensor.cp_id,
            "topic": sensor.topic,
            "timestamp": sensor.timestamp,
        }
        if sensor.manufacturer:
            attrs["manufacturer"] = sensor.manufacturer
        return attrs

    @property
    def device_info(self) -> DeviceInfo | None:
        """Group sensors under one HA device per charge point ID."""
        sensor = self._sensor
        if sensor is None or not sensor.cp_id:
            return None

        return DeviceInfo(
            identifiers={(DOMAIN, f"{self._entry.entry_id}_{sensor.cp_id}")},
            name=f"Charge Point {sensor.cp_id}",
            suggested_area=self._entry.title or None,
            manufacturer=sensor.manufacturer,
            model="OCPP Charge Point",
            sw_version=_INTEGRATION_SW_VERSION,
            configuration_url=_configuration_url(self._entry.data.get(CONF_CPMS_URL)),
        )
