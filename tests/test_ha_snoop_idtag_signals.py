"""Verify the idTag sensor's dispatcher signal lifecycle.

The idTag sensor must only be announced as "new" (SIGNAL_NEW_SENSOR) once, the
first time a StartTransaction is seen for a connector. Later updates -
including StopTransaction clearing the value to None/unknown - must only fire
SIGNAL_SENSOR_UPDATE. A charge point that never starts a transaction must
never get an idTag sensor at all.
"""

import json
import os

import pytest

pytest.importorskip("pytest_homeassistant_custom_component")
pytest_plugins = "pytest_homeassistant_custom_component"

from homeassistant.helpers.dispatcher import async_dispatcher_connect  # noqa: E402

from custom_components.ha_ocpp_relay.const import (  # noqa: E402
    CONF_SNOOP_SOCKET,
    SIGNAL_NEW_SENSOR,
    SIGNAL_SENSOR_UPDATE,
)
from custom_components.ha_ocpp_relay.snoop.client import OCPPSnoopClient  # noqa: E402


def _load_lines(filename: str) -> list[str]:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    log_path = os.path.join(project_root, "tests", "data", filename)
    with open(log_path, "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f.readlines() if ln.strip()]


@pytest.mark.asyncio
async def test_idtag_signal_fires_once_and_stop_only_updates(hass):
    entry_id = "test-entry-idtag-signals"
    client = OCPPSnoopClient(hass, entry_id, {CONF_SNOOP_SOCKET: "ws://127.0.0.1:0/"})

    idtag_uid = "OCPP_ChargerSerialNr_1_idtag"
    new_events: list[str] = []
    update_events: list[str] = []

    unsub_new = async_dispatcher_connect(
        hass, f"{SIGNAL_NEW_SENSOR}_{entry_id}", lambda uid: new_events.append(uid)
    )
    unsub_update = async_dispatcher_connect(
        hass, f"{SIGNAL_SENSOR_UPDATE}_{entry_id}", lambda uid: update_events.append(uid)
    )

    try:
        for line in _load_lines("ocpp_3phase_log.json"):
            await client._handle_message(line)
    finally:
        unsub_new()
        unsub_update()

    # SIGNAL_NEW_SENSOR must fire exactly once for the idTag sensor - at the
    # first (and only) StartTransaction - not again when StopTransaction
    # later updates the same unique_id.
    assert [uid for uid in new_events if uid == idtag_uid] == [idtag_uid]

    # SIGNAL_SENSOR_UPDATE must fire at least twice: once for StartTransaction
    # (setting the idTag) and once for StopTransaction (clearing it).
    idtag_updates = [uid for uid in update_events if uid == idtag_uid]
    assert len(idtag_updates) >= 2

    # StopTransaction is the last transaction-related message in the log, so
    # the sensor's final value must be cleared to None (HA shows "unknown").
    assert client.sensors[idtag_uid].value is None


@pytest.mark.asyncio
async def test_no_idtag_signal_without_start_transaction(hass):
    entry_id = "test-entry-idtag-no-start"
    client = OCPPSnoopClient(hass, entry_id, {CONF_SNOOP_SOCKET: "ws://127.0.0.1:0/"})

    new_events: list[str] = []
    unsub_new = async_dispatcher_connect(
        hass, f"{SIGNAL_NEW_SENSOR}_{entry_id}", lambda uid: new_events.append(uid)
    )

    messages = [
        {
            "event": "Message",
            "sender": "CP",
            "protocol": "ocpp1.6",
            "cp_id": "NoTxCP",
            "payload": [2, "1", "StatusNotification", {"connectorId": 1, "errorCode": "NoError", "status": "Available"}],
            "timestamp": "2026-08-01T12:00:00Z",
        },
        {
            "event": "Message",
            "sender": "CP",
            "protocol": "ocpp1.6",
            "cp_id": "NoTxCP",
            "payload": [2, "2", "Heartbeat", {}],
            "timestamp": "2026-08-01T12:00:10Z",
        },
    ]

    try:
        for msg in messages:
            await client._handle_message(json.dumps(msg))
    finally:
        unsub_new()

    assert not any(uid.endswith("_idtag") for uid in new_events)
    assert not any(uid.endswith("_idtag") for uid in client.sensors)
