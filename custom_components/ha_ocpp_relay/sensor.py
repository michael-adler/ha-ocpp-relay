"""Dynamic sensor entity platform for values discovered from OCPP snoop traffic."""

from __future__ import annotations

from datetime import datetime
import logging

from homeassistant.components.sensor import SensorEntity
from homeassistant.helpers.restore_state import RestoreEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN, SIGNAL_NEW_SENSOR, SIGNAL_SENSOR_UPDATE
from .snoop.client import OCPPSnoopClient

_LOGGER = logging.getLogger(__name__)


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

    def add_entity(unique_id: str) -> None:
        """Create and register an entity for one discovered sensor ID."""
        if unique_id in known_entities:
            return
        sensor_data = client.sensors.get(unique_id)
        if sensor_data is None:
            return

        entity = OCPPSensorEntity(entry, client, unique_id)
        known_entities[unique_id] = entity
        async_add_entities([entity])

    for unique_id in client.sensors.keys():
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

    def __init__(self, entry: ConfigEntry, client: OCPPSnoopClient, unique_id: str) -> None:
        """Initialize the instance state."""
        self._entry = entry
        self._client = client
        self._ocpp_unique_id = unique_id
        self._unsub = None
        self._restored_value = None

        sensor = self._client.sensors[self._ocpp_unique_id]
        self._attr_unique_id = f"{self._entry.entry_id}_{sensor.unique_id}"
        self._attr_name = sensor.name

    async def async_added_to_hass(self) -> None:
        """Subscribe to per-entry sensor update events and restore state for energy sensors."""
        @callback
        def _on_update(unique_id: str) -> None:
            """Write HA state when this entity's backing sensor changes."""
            if unique_id == self._ocpp_unique_id:
                self.async_write_ha_state()

        self._unsub = async_dispatcher_connect(
            self.hass,
            f"{SIGNAL_SENSOR_UPDATE}_{self._entry.entry_id}",
            _on_update,
        )

        # Restore last state for energy sensors
        sensor = self._client.sensors.get(self._ocpp_unique_id)
        if sensor and sensor.device_class == "energy":
            last_state = await self.async_get_last_state()
            if last_state and last_state.state not in (None, "unknown", "unavailable"):
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
            # Only restore for energy sensors
            if self.device_class == "energy" and self._restored_value is not None:
                try:
                    return float(self._restored_value)
                except (TypeError, ValueError):
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
        if self.device_class == "energy":
            # Always available if we have a value (live or restored)
            return sensor is not None or self._restored_value is not None
        # Default: available if sensor is present
        return sensor is not None

    @property
    def native_unit_of_measurement(self):
        """Expose the unit parsed from OCPP sampled value payloads."""
        sensor = self._sensor
        return None if sensor is None else sensor.unit

    @property
    def device_class(self):
        """Map parser-derived semantic type to Home Assistant device class."""
        sensor = self._sensor
        return None if sensor is None else sensor.device_class

    @property
    def state_class(self):
        """Expose state class so long-term statistics are computed correctly."""
        sensor = self._sensor
        return None if sensor is None else sensor.state_class

    @property
    def extra_state_attributes(self):
        """Attach protocol context useful for debugging and automation rules."""
        sensor = self._sensor
        if sensor is None:
            return None

        attrs = {
            "cp_id": sensor.cp_id,
            "topic": sensor.topic,
            "timestamp": sensor.timestamp,
        }
        if sensor.manufacturer:
            attrs["manufacturer"] = sensor.manufacturer
        return attrs
