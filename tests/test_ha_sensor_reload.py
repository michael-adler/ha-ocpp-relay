"""Test sensor availability after an integration reload with no live OCPP data.

After a reload the snoop client starts with an empty sensor cache.  Sensors that
are declared to restore (energy, vendor, firmware) should remain available and
retain their previous value.  All other sensors (heartbeat, status, power,
voltage, current) should become unavailable until fresh OCPP traffic arrives.
"""

import importlib
import pathlib
import sys

import pytest

pytest.importorskip("pytest_homeassistant_custom_component")
pytest_plugins = "pytest_homeassistant_custom_component"

from pytest_homeassistant_custom_component.common import MockConfigEntry  # noqa: E402

repo_root = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from custom_components.ha_ocpp_relay.const import (  # noqa: E402
    CONF_RELAY_IS_LOCAL,
    CONF_RELAY_OCPP_HOST,
    CONF_RELAY_OCPP_PORT,
    CONF_RELAY_SNOOP_HOST,
    CONF_RELAY_SNOOP_PORT,
    CONF_SNOOP_SOCKET,
    DOMAIN,
)

_CFG = {
    CONF_RELAY_IS_LOCAL: False,
    CONF_RELAY_OCPP_HOST: "127.0.0.1",
    CONF_RELAY_OCPP_PORT: 8500,
    CONF_RELAY_SNOOP_HOST: "127.0.0.1",
    CONF_RELAY_SNOOP_PORT: 8551,
    CONF_SNOOP_SOCKET: "ws://127.0.0.1:8551/",
}

_CP_ID = "TESTCP01"

# (ocpp_unique_id, prior_state_value, device_class_attr, should_be_available_after_reload)
_SENSOR_SPECS = [
    (f"OCPP_{_CP_ID}_1_Outlet_Energy-Active-Import-Register", "440.62", "energy",    True),
    (f"OCPP_{_CP_ID}_vendor",                                  "ACME Corp", None,    True),
    (f"OCPP_{_CP_ID}_firmware",                                "1.2.3",     None,    True),
    (f"OCPP_{_CP_ID}_heartbeat",     "2026-05-08T12:00:00Z",  "timestamp",           False),
    (f"OCPP_{_CP_ID}_1_status",                                "Charging",  None,    False),
    (f"OCPP_{_CP_ID}_1_Outlet_Power-Active-Import",            "11.378",    "power", False),
    (f"OCPP_{_CP_ID}_1_Outlet_Voltage",                        "245.36",    "voltage", False),
    (f"OCPP_{_CP_ID}_1_Outlet_Current-Import",                 "46.91",     "current", False),
]


@pytest.mark.asyncio
async def test_ha_sensor_restore_after_reload(hass, monkeypatch):
    """Sensors restore or become unavailable correctly after an integration reload.

    This test simulates the "reload" portion of the lifecycle: the snoop client
    starts empty (no live data yet) but HA has stored states from the previous
    run.  It verifies that restore-eligible sensors (energy, vendor, firmware)
    stay available while all other sensors become unavailable.
    """
    from homeassistant.core import State as HAState
    from homeassistant.helpers import entity_registry as er

    sensor_mod = importlib.import_module("custom_components.ha_ocpp_relay.sensor")

    entry = MockConfigEntry(domain=DOMAIN, data=_CFG)
    entry.add_to_hass(hass)

    # Register all sensors in the entity registry so that _registry_sensor_entries
    # can discover them during setup (the normal path after a real reload).
    registry = er.async_get(hass)
    uid_to_entity_id: dict[str, str] = {}
    for uid, _val, _dc, _restore in _SENSOR_SPECS:
        reg_entry = registry.async_get_or_create(
            domain="sensor",
            platform=DOMAIN,
            unique_id=f"{entry.entry_id}_{uid}",
            config_entry=entry,
            original_name=f"Test {uid}",
        )
        uid_to_entity_id[uid] = reg_entry.entity_id

    # Build synthetic "prior run" states.  These mimic what HA's RestoreStateData
    # would return from async_get_last_state after a previous integration run.
    # The device_class attribute is critical: async_added_to_hass uses it to decide
    # whether to set _restore_on_unavailable for energy sensors.
    prior_states: dict[str, HAState] = {}
    for uid, val, dc, _ in _SENSOR_SPECS:
        attrs: dict = {
            "cp_id": _CP_ID,
            "topic": "test",
            "timestamp": "2026-05-08T12:00:00Z",
        }
        if dc:
            attrs["device_class"] = dc
        entity_id = uid_to_entity_id[uid]
        prior_states[entity_id] = HAState(entity_id, val, attrs)

    async def _mock_get_last_state(entity_self) -> HAState | None:
        return prior_states.get(entity_self.entity_id)

    monkeypatch.setattr(
        sensor_mod.OCPPSensorEntity, "async_get_last_state", _mock_get_last_state
    )

    # Empty client simulates the integration having just restarted with no live
    # OCPP traffic yet.
    class _EmptyClient:
        def __init__(self) -> None:
            self.sensors: dict = {}

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = {
        "client": _EmptyClient(),
        "relay": None,
        "config": _CFG,
    }

    # Run the sensor platform setup.  The real HA EntityPlatform calls
    # async_added_to_hass as part of entity registration; here we replicate that
    # step manually after collecting the entities.
    added_entities: list = []

    def _collect_entities(new_entities, *args, **kwargs) -> None:
        added_entities.extend(new_entities)

    await sensor_mod.async_setup_entry(hass, entry, _collect_entities)
    await hass.async_block_till_done()

    # Wire each entity to the hass instance and assign the entity_id that HA
    # already registered in the entity registry.
    for entity in added_entities:
        entity.hass = hass
        entity.entity_id = uid_to_entity_id.get(entity._ocpp_unique_id)

    for entity in added_entities:
        if entity.entity_id:
            await entity.async_added_to_hass()

    # --- Assertions ---
    entities_by_uid = {e._ocpp_unique_id: e for e in added_entities if e.entity_id}
    failures: list[str] = []

    for uid, val, _dc, should_restore in _SENSOR_SPECS:
        entity = entities_by_uid.get(uid)
        if entity is None:
            failures.append(f"Entity for {uid!r} was not created during reload setup")
            continue

        if should_restore and not entity.available:
            failures.append(
                f"{uid}: expected available (restore-eligible) but is unavailable"
            )
        elif not should_restore and entity.available:
            failures.append(
                f"{uid}: expected unavailable after reload but is available"
            )

        if should_restore and entity.available:
            native = entity.native_value
            try:
                val_ok = float(native) == pytest.approx(float(val))
            except (TypeError, ValueError):
                val_ok = str(native) == str(val)
            if not val_ok:
                failures.append(
                    f"{uid}: restored value {native!r} != expected {val!r}"
                )

    preserved = sum(1 for e in entities_by_uid.values() if e.available)
    not_preserved = len(entities_by_uid) - preserved
    print(
        f"TEST SUMMARY: {len(entities_by_uid)} sensors at reload, "
        f"{preserved} preserved, {not_preserved} unavailable",
        flush=True,
    )

    assert not failures, (
        "Sensor reload availability checks failed:\n"
        + "\n".join(f"  - {f}" for f in failures)
    )

    # Unsubscribe dispatcher connections to keep the hass fixture clean.
    for entity in added_entities:
        await entity.async_will_remove_from_hass()
