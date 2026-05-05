"""Home Assistant integration setup, teardown, and runtime wiring for HA OCPP Relay."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

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


def _config_has_changed(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Return whether a config entry update changed runtime settings.

    Home Assistant invokes config entry update listeners for metadata changes
    such as title renames as well as real config edits. Only the latter should
    force a full integration reload.
    """
    runtime = hass.data.get(DOMAIN, {}).get(entry.entry_id)
    if runtime is None:
        return True
    return runtime.get("config") != _merged_config(entry)


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

    if local_relay is None:
        # External mode: start snoop client immediately to minimize missed
        # startup messages from already-running upstream snoop servers.
        await client.async_start()
    elif hass.is_running:
        # Local mode after boot (e.g. added via UI): start relay and client now.
        await _start_tasks()
    else:
        from homeassistant.const import EVENT_HOMEASSISTANT_STARTED

        # Local mode during bootstrap: defer start until HA is fully running to
        # avoid startup hangs/timeouts while binding local relay services.
        cancel_listener = hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STARTED, _start_tasks)
        entry.async_on_unload(cancel_listener)

    entry.async_on_unload(entry.add_update_listener(async_handle_entry_update))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Tear down runtime objects and unload entity platforms for an entry."""
    runtime = hass.data.get(DOMAIN, {}).get(entry.entry_id)
    client: OCPPSnoopClient | None = None
    local_relay: LocalRelaySupervisor | None = None
    if runtime is not None:
        client = runtime.get("client")
        local_relay = runtime.get("relay")

    if local_relay is not None:
        await local_relay.async_stop()
    if client is not None:
        await client.async_stop()

    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if runtime is not None:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    if DOMAIN in hass.data and not hass.data[DOMAIN]:
        hass.data.pop(DOMAIN)
    return unload_ok


async def async_handle_entry_update(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload the integration only when effective runtime config changes."""
    if not _config_has_changed(hass, entry):
        return
    await async_reload_entry(hass, entry)


async def async_reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Trigger a full unload/setup cycle after config options change."""
    await hass.config_entries.async_reload(entry.entry_id)
