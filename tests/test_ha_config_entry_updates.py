from __future__ import annotations

from types import SimpleNamespace

import pytest

from custom_components.ha_ocpp_relay import (
    DOMAIN,
    _config_has_changed,
    _consume_skip_next_update_reload,
    async_handle_entry_update,
    async_unload_entry,
)
from custom_components.ha_ocpp_relay.const import CONF_RELAY_SNOOP_PORT, normalize_relay_config


class _FakeConfigEntries:
    def __init__(self) -> None:
        self.reload_calls: list[str] = []
        self.unload_calls: list[tuple[object, tuple[str, ...]]] = []

    async def async_reload(self, entry_id: str) -> bool:
        self.reload_calls.append(entry_id)
        return True

    async def async_unload_platforms(self, entry, platforms) -> bool:
        self.unload_calls.append((entry, tuple(platforms)))
        return True


class _FakeHass:
    def __init__(self) -> None:
        self.data: dict = {}
        self.config_entries = _FakeConfigEntries()


class _FakeStoppable:
    def __init__(self) -> None:
        self.stop_calls = 0

    async def async_stop(self) -> None:
        self.stop_calls += 1


def _entry(*, data: dict | None = None, options: dict | None = None, entry_id: str = "entry-1"):
    return SimpleNamespace(
        entry_id=entry_id,
        data=data or {},
        options=options or {},
        title="OCPP Relay",
    )


def test_config_has_changed_ignores_title_only_updates() -> None:
    hass = _FakeHass()
    entry = _entry(data={CONF_RELAY_SNOOP_PORT: 8501})
    hass.data[DOMAIN] = {
        entry.entry_id: {
            "config": normalize_relay_config({CONF_RELAY_SNOOP_PORT: 8501}),
        }
    }

    assert _config_has_changed(hass, entry) is False


@pytest.mark.asyncio
async def test_async_handle_entry_update_skips_reload_for_title_only_updates() -> None:
    hass = _FakeHass()
    entry = _entry(data={CONF_RELAY_SNOOP_PORT: 8501})
    hass.data[DOMAIN] = {
        entry.entry_id: {
            "config": normalize_relay_config({CONF_RELAY_SNOOP_PORT: 8501}),
        }
    }

    await async_handle_entry_update(hass, entry)

    assert hass.config_entries.reload_calls == []


@pytest.mark.asyncio
async def test_async_handle_entry_update_reloads_on_runtime_config_change() -> None:
    hass = _FakeHass()
    entry = _entry(data={CONF_RELAY_SNOOP_PORT: 8502})
    hass.data[DOMAIN] = {
        entry.entry_id: {
            "config": normalize_relay_config({CONF_RELAY_SNOOP_PORT: 8501}),
        }
    }

    await async_handle_entry_update(hass, entry)

    assert hass.config_entries.reload_calls == [entry.entry_id]


def test_consume_skip_next_update_reload_is_one_shot() -> None:
    hass = _FakeHass()
    entry = _entry()
    hass.data[DOMAIN] = {
        entry.entry_id: {
            "skip_next_update_reload": True,
        }
    }

    assert _consume_skip_next_update_reload(hass, entry) is True
    assert _consume_skip_next_update_reload(hass, entry) is False


@pytest.mark.asyncio
async def test_async_handle_entry_update_skips_suppressed_reload() -> None:
    hass = _FakeHass()
    entry = _entry(data={CONF_RELAY_SNOOP_PORT: 8502})
    hass.data[DOMAIN] = {
        entry.entry_id: {
            "config": normalize_relay_config({CONF_RELAY_SNOOP_PORT: 8501}),
            "skip_next_update_reload": True,
        }
    }

    await async_handle_entry_update(hass, entry)

    assert hass.config_entries.reload_calls == []


@pytest.mark.asyncio
async def test_async_unload_entry_handles_missing_runtime() -> None:
    hass = _FakeHass()
    entry = _entry()

    unload_ok = await async_unload_entry(hass, entry)

    assert unload_ok is True
    assert hass.config_entries.unload_calls == [(entry, ("sensor",))]


@pytest.mark.asyncio
async def test_async_unload_entry_stops_runtime_and_cleans_domain() -> None:
    hass = _FakeHass()
    entry = _entry()
    client = _FakeStoppable()
    relay = _FakeStoppable()
    hass.data[DOMAIN] = {
        entry.entry_id: {
            "client": client,
            "relay": relay,
            "config": {},
        }
    }

    unload_ok = await async_unload_entry(hass, entry)

    assert unload_ok is True
    assert client.stop_calls == 1
    assert relay.stop_calls == 1
    assert DOMAIN not in hass.data