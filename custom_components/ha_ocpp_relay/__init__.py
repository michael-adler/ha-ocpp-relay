"""Home Assistant integration setup, teardown, and runtime wiring for HA OCPP Relay."""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .client import OCPPSnoopClient
from .const import (
    CONF_RELAY_IS_LOCAL,
    DOMAIN,
    PLATFORMS,
    normalize_relay_config,
)
from .local_relay import LocalRelaySupervisor


def _merged_config(entry: ConfigEntry) -> dict:
    merged = dict(entry.data)
    merged.update(entry.options)
    return normalize_relay_config(merged)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})

    config = _merged_config(entry)
    client = OCPPSnoopClient(hass, entry.entry_id, config)

    local_relay = None
    if config[CONF_RELAY_IS_LOCAL]:
        local_relay = LocalRelaySupervisor(hass, config)
        await local_relay.async_start()

    hass.data[DOMAIN][entry.entry_id] = {
        "client": client,
        "relay": local_relay,
        "config": config,
    }

    await client.async_start()
    entry.async_on_unload(entry.add_update_listener(async_reload_entry))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    runtime = hass.data[DOMAIN].pop(entry.entry_id)
    client: OCPPSnoopClient = runtime["client"]
    local_relay: LocalRelaySupervisor | None = runtime["relay"]

    if local_relay is not None:
        await local_relay.async_stop()
    await client.async_stop()

    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if not hass.data[DOMAIN]:
        hass.data.pop(DOMAIN)
    return unload_ok


async def async_reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)
