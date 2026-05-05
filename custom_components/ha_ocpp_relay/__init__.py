"""Home Assistant integration setup, teardown, and runtime wiring for HA OCPP Relay."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from homeassistant.const import EVENT_HOMEASSISTANT_STARTED

from .const import (
    CONF_RELAY_IS_LOCAL,
    DOMAIN,
    PLATFORMS,
    normalize_relay_config,
)

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from .relay.local_relay_supervisor import LocalRelaySupervisor
    from .snoop.client import OCPPSnoopClient


def _merged_config(entry: ConfigEntry) -> dict[str, Any]:
    """Combine entry data and options into the normalized runtime config.

    Home Assistant keeps initial config in entry.data and user edits in
    entry.options. This helper produces the effective configuration consumed by
    the client and optional local relay supervisor.
    """
    merged = dict(entry.data)
    merged.update(entry.options)
    return normalize_relay_config(merged)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Initialize integration runtime for one config entry.

    This wires the snoop client, optionally starts the embedded relay stack for
    local mode, stores runtime objects in hass.data, and forwards setup to
    platform entities.
    """
    from .relay.local_relay_supervisor import LocalRelaySupervisor
    from .snoop.client import OCPPSnoopClient

    hass.data.setdefault(DOMAIN, {})

    config = _merged_config(entry)
    client = OCPPSnoopClient(hass, entry.entry_id, config)

    local_relay = None
    if config[CONF_RELAY_IS_LOCAL]:
        local_relay = LocalRelaySupervisor(hass, config)

    hass.data[DOMAIN][entry.entry_id] = {
        "client": client,
        "relay": local_relay,
        "config": config,
    }

    async def _start_tasks(_event=None) -> None:
        if local_relay is not None:
            await local_relay.async_start()
        await client.async_start()

    if hass.is_running:
        # Integration loaded after boot (e.g. added via UI) — start immediately.
        await _start_tasks()
    else:
        # During bootstrap — defer until HA has fully started so these
        # long-running tasks don't trigger the bootstrap timeout warning.
        hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, _start_tasks)

    entry.async_on_unload(entry.add_update_listener(async_reload_entry))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Tear down runtime objects and unload entity platforms for an entry."""
    from .snoop.client import OCPPSnoopClient

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
    """Trigger a full unload/setup cycle after config options change."""
    await hass.config_entries.async_reload(entry.entry_id)
