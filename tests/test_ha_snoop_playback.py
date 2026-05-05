import os
import sys
import time
import json
import asyncio
import logging

import pytest
# Skip this test module unless the Home Assistant pytest plugin is
# available; that keeps CLI-only test runs from failing on import.
pytest.importorskip("pytest_homeassistant_custom_component")
pytest_plugins = "pytest_homeassistant_custom_component"
import pathlib
from pytest_homeassistant_custom_component.common import MockConfigEntry

# Ensure repo root is on sys.path so `custom_components` package is importable
repo_root = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from custom_components.ha_ocpp_relay.const import (
    DOMAIN,
    CONF_SNOOP_SOCKET,
    CONF_RELAY_IS_LOCAL,
    CONF_RELAY_OCPP_HOST,
    CONF_RELAY_OCPP_PORT,
    CONF_RELAY_SNOOP_HOST,
    CONF_RELAY_SNOOP_PORT,
)
import importlib


logging.basicConfig(level=logging.DEBUG, force=True)




@pytest.mark.asyncio
async def test_ha_integration_creates_sensors_from_snoop_playback(hass, tmp_path, monkeypatch):
    """Start snoop playback and verify HA sensors are created from the stream.

    This mirrors tests/test_cli_mqtt.py but uses the Home Assistant test harness
    to load the `ha_ocpp_relay` integration in external mode and assert that
    sensors are created from the snoop websocket stream.
    """

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    logging.getLogger().setLevel(logging.DEBUG)

    # Read the recorded log file and monkeypatch websockets.connect so the
    # integration's snoop client consumes these messages without opening sockets.
    log_path = os.path.join(project_root, "tests", "data", "ocpp_log.json")
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f.readlines() if ln.strip()]
    except Exception as e:
        pytest.fail(f"Failed to read log file: {e}")
    print(f"TEST: read {len(lines)} log lines from {log_path}", flush=True)

    class FakeWebSocket:
        def __init__(self, msgs):
            self._msgs = list(msgs)
            # Pre-create iterator so async context/await variations work
            self._iter = iter(self._msgs)
            self._msg_count = 0

        async def __aenter__(self):
            # Return an async iterator over messages
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                msg = next(self._iter)
                self._msg_count += 1
                # Only print the first few messages and then one every 50
                if self._msg_count <= 5 or self._msg_count % 50 == 0:
                    print(f"TEST: yielding message #{self._msg_count}: {msg[:200]}", flush=True)
                return msg
            except StopIteration:
                raise StopAsyncIteration

    class FakeConnector:
        def __init__(self, msgs):
            self._msgs = msgs

        def __await__(self):
            async def _coro():
                return FakeWebSocket(self._msgs)

            return _coro().__await__()

        async def __aenter__(self):
            return FakeWebSocket(self._msgs)

        async def __aexit__(self, exc_type, exc, tb):
            return False

    connect_invocations = 0

    class ErrorConnector:
        async def __aenter__(self):
            # Raise CancelledError so the client's run loop exits cleanly
            raise asyncio.CancelledError()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def fake_connect(*args, **kwargs):
        nonlocal connect_invocations
        connect_invocations += 1
        if connect_invocations == 1:
            return FakeConnector(lines)
        return ErrorConnector()

    # Patch the websockets.connect used by the snoop client module
    import importlib
    client_mod = importlib.import_module("custom_components.ha_ocpp_relay.snoop.client")
    monkeypatch.setattr(client_mod.websockets, "connect", fake_connect)
    print("TEST: patched websockets.connect in snoop.client", flush=True)

    # Create the integration via a MockConfigEntry to avoid relying on
    # the config flow loader during tests. The integration consumes the
    # merged dict of `entry.data` and `entry.options` so provide the
    # external-mode configuration directly in `data`.
    cfg = {
        CONF_RELAY_IS_LOCAL: False,
        CONF_RELAY_OCPP_HOST: "127.0.0.1",
        CONF_RELAY_OCPP_PORT: 8500,
        CONF_RELAY_SNOOP_HOST: "127.0.0.1",
        CONF_RELAY_SNOOP_PORT: 8551,
        CONF_SNOOP_SOCKET: "ws://127.0.0.1:8551/",
    }

    entry = MockConfigEntry(domain=DOMAIN, data=cfg)
    entry.add_to_hass(hass)
    print("TEST: added MockConfigEntry to hass", flush=True)
    # Some test environments do not allow the HA loader to discover
    # custom_components; call the integration setup directly to avoid
    # loader-related "Integration not found" errors.
    # Some test environments try to load platforms via the HA loader which
    # may not be configured for this repo. Stub out forwarding to platforms
    # so the integration setup can proceed without loader discovery.
    async def _noop_forward(*args, **kwargs):
        return True

    monkeypatch.setattr(hass.config_entries, "async_forward_entry_setups", _noop_forward)

    integration_mod = importlib.import_module("custom_components.ha_ocpp_relay")
    await integration_mod.async_setup_entry(hass, entry)
    entry_id = entry.entry_id
    print(f"TEST: integration setup complete for entry {entry_id}", flush=True)

    # Wait until the integration is fully set up
    await hass.async_block_till_done()

    # Access the running client to observe sensors
    client = hass.data[DOMAIN][entry_id]["client"]

    # Wait for the client to populate sensors from the playback stream
    # Wait for the client to populate sensors from the playback stream
    async def _wait_for_sensors(client):
        while not client.sensors:
            await asyncio.sleep(0.1)

    try:
        print("TEST: waiting up to 5s for sensors to appear", flush=True)
        await asyncio.wait_for(_wait_for_sensors(client), timeout=5.0)
    except asyncio.TimeoutError:
        pytest.fail("No sensors discovered from snoop playback stream (timeout)")

    # Log names and values of discovered sensors
    print("--- HA OCPP sensors discovered ---")
    for uid, s in client.sensors.items():
        try:
            value = s.value
        except Exception:
            value = None
        print(json.dumps({"name": s.name, "unique_id": uid, "value": value}))

    # Assert the specific expected sensors (from CI playback) exist with correct values
    expected = {
        "OCPP_AL0123456789ABCDEF_heartbeat": {
            "name": "Heartbeat CP AL0123456789ABCDEF",
            "value": "2026-04-18T20:09:23Z",
        },
        "OCPP_AL0123456789ABCDEF_1_status": {
            "name": "C1 Status CP AL0123456789ABCDEF",
            "value": "Charging",
        },
        "OCPP_AL0123456789ABCDEF_1_Outlet_Energy-Active-Import-Register": {
            "name": "C1 Energy Active Import Register Outlet CP AL0123456789ABCDEF",
            "value": 440.6223,
        },
        "OCPP_AL0123456789ABCDEF_1_Outlet_Voltage": {
            "name": "C1 Voltage Outlet CP AL0123456789ABCDEF",
            "value": "245.360000",
        },
        "OCPP_AL0123456789ABCDEF_1_Outlet_Current-Export": {
            "name": "C1 Current Export Outlet CP AL0123456789ABCDEF",
            "value": "46.916000",
        },
        "OCPP_AL0123456789ABCDEF_1_Outlet_Current-Import": {
            "name": "C1 Current Import Outlet CP AL0123456789ABCDEF",
            "value": "46.916000",
        },
        "OCPP_AL0123456789ABCDEF_1_Outlet_Power-Offered": {
            "name": "C1 Power Offered Outlet CP AL0123456789ABCDEF",
            "value": 11.378,
        },
        "OCPP_AL0123456789ABCDEF_1_Outlet_Power-Active-Import": {
            "name": "C1 Power Active Import Outlet CP AL0123456789ABCDEF",
            "value": 11.378,
        },
    }

    passed = 0
    failed = 0
    failures: list[str] = []

    for uid, exp in expected.items():
        sensor = client.sensors.get(uid)
        if sensor is None:
            failed += 1
            failures.append(f"Missing sensor {uid}")
            continue

        name_ok = sensor.name == exp["name"]
        val = sensor.value
        if isinstance(exp["value"], float):
            try:
                val_ok = float(val) == pytest.approx(exp["value"])
            except Exception:
                val_ok = False
        else:
            val_ok = str(val) == str(exp["value"])

        if name_ok and val_ok:
            passed += 1
        else:
            failed += 1
            parts = []
            if not name_ok:
                parts.append(f"name {sensor.name!r} != {exp['name']!r}")
            if not val_ok:
                parts.append(f"value {val!r} != {exp['value']!r}")
            failures.append(f"{uid}: " + "; ".join(parts))

    # Print summary of sensor checks
    print(f"TEST SUMMARY: {passed} passed, {failed} failed sensor checks", flush=True)
    if failures:
        print("TEST SUMMARY DETAILS:", flush=True)
        for f in failures:
            print(f" - {f}", flush=True)

    # Basic assertion: at least one sensor exists
    assert len(client.sensors) > 0

    # Fail the test if any expected sensors failed
    assert failed == 0, f"{failed} sensor checks failed (see summary above)"

    # Stop the client task explicitly to avoid lingering background tasks
    await client.async_stop()

    # Cleanup: unload integration
    await hass.config_entries.async_unload(entry_id)
    await hass.async_block_till_done()
