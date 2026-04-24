from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .client import OCPPSnoopClient
from .const import DOMAIN, PLATFORMS


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    hass.data.setdefault(DOMAIN, {})

    client = OCPPSnoopClient(hass, entry.entry_id, entry.data)
    hass.data[DOMAIN][entry.entry_id] = client

    await client.async_start()
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    client: OCPPSnoopClient = hass.data[DOMAIN].pop(entry.entry_id)
    await client.async_stop()

    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if not hass.data[DOMAIN]:
        hass.data.pop(DOMAIN)
    return unload_ok
