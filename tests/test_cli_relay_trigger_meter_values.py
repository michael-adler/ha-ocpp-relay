"""Tests for relay-initiated TriggerMessage / MeterValues interception.

The relay sends TriggerMessage to the CP after BootNotification to initialize
sensors.  It must:
  - not forward the TriggerMessage Accepted CallResult to the CPMS
  - not forward the next MeterValues from the CP to the CPMS
  - still put that MeterValues on the snoop queue
  - clear trigger state on CP disconnect so a reconnect starts fresh
"""

import asyncio
import json
import socket

import pytest
import websockets

pytestmark = pytest.mark.cli

from custom_components.ha_ocpp_relay.relay.core import OCPPRelay, SnoopWebSocketServer


LOCALHOST = "127.0.0.1"
OCPP_SUBPROTOCOL = "ocpp1.6"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind((LOCALHOST, 0))
        return s.getsockname()[1]


async def _recv_json(ws, timeout=3.0):
    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    return json.loads(raw)


@pytest.fixture
async def relay_harness():
    """Start an OCPPRelay with a SnoopWebSocketServer and a fake CPMS."""
    cp_port = _free_port()
    snoop_port = _free_port()
    snoop_queue = asyncio.Queue()

    cpms_port = _free_port()
    cpms_received: list[list] = []
    cpms_ready = asyncio.Event()
    cpms_ws_holder: list = []

    async def cpms_handler(ws):
        cpms_ws_holder.append(ws)
        cpms_ready.set()
        async for raw in ws:
            cpms_received.append(json.loads(raw))

    cpms_server = await websockets.serve(
        cpms_handler, LOCALHOST, cpms_port, subprotocols=[OCPP_SUBPROTOCOL]
    )

    relay = OCPPRelay(f"ws://{LOCALHOST}:{cpms_port}", snoop_queue=snoop_queue)
    relay_server = await relay.start(LOCALHOST, cp_port)

    snoop = SnoopWebSocketServer(snoop_queue=snoop_queue)
    snoop_server = await snoop.start(LOCALHOST, snoop_port)

    yield {
        "cp_port": cp_port,
        "snoop_port": snoop_port,
        "cpms_received": cpms_received,
        "cpms_ready": cpms_ready,
        "cpms_ws_holder": cpms_ws_holder,
    }

    snoop_server.close()
    await snoop_server.wait_closed()
    relay_server.close()
    await relay_server.wait_closed()
    cpms_server.close()
    await cpms_server.wait_closed()


@pytest.mark.asyncio
async def test_trigger_sent_after_boot_notification(relay_harness):
    """After BootNotification the relay sends TriggerMessage to the CP."""
    h = relay_harness
    cp_port = h["cp_port"]
    cpms_ready = h["cpms_ready"]

    async with websockets.connect(
        f"ws://{LOCALHOST}:{cp_port}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(cpms_ready.wait(), timeout=3.0)

        boot = [2, "boot-1", "BootNotification", {"chargePointVendor": "Acme", "chargePointModel": "X1"}]
        await cp_ws.send(json.dumps(boot))

        # The relay should send a TriggerMessage to the CP.
        trigger = await _recv_json(cp_ws)
        assert trigger[0] == 2
        assert trigger[2] == "TriggerMessage"
        assert trigger[3].get("requestedMessage") == "MeterValues"


@pytest.mark.asyncio
async def test_trigger_accepted_not_forwarded_to_cpms(relay_harness):
    """The Accepted CallResult for the relay's TriggerMessage must not reach the CPMS."""
    h = relay_harness
    cpms_received = h["cpms_received"]
    cpms_ready = h["cpms_ready"]

    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(cpms_ready.wait(), timeout=3.0)

        boot = [2, "boot-1", "BootNotification", {"chargePointVendor": "Acme", "chargePointModel": "X1"}]
        await cp_ws.send(json.dumps(boot))

        trigger = await _recv_json(cp_ws)
        trigger_id = trigger[1]

        # Reply Accepted — the relay must swallow this.
        accepted = [3, trigger_id, {"status": "Accepted"}]
        await cp_ws.send(json.dumps(accepted))

        # Give relay a moment to process.
        await asyncio.sleep(0.1)

        # Only the BootNotification should have reached the CPMS, not the Accepted.
        assert all(frame[1] != trigger_id for frame in cpms_received), (
            f"TriggerMessage CallResult leaked to CPMS: {cpms_received}"
        )


@pytest.mark.asyncio
async def test_meter_values_snooped_not_forwarded(relay_harness):
    """The triggered MeterValues must reach a snoop subscriber but not the CPMS."""
    h = relay_harness
    cpms_received = h["cpms_received"]
    cpms_ready = h["cpms_ready"]

    meter_values = [
        2,
        "mv-1",
        "MeterValues",
        {
            "connectorId": 1,
            "meterValue": [
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "sampledValue": [{"value": "230", "measurand": "Voltage", "unit": "V"}],
                }
            ],
        },
    ]

    snoop_messages: list[dict] = []
    got_meter_values = asyncio.Event()

    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['snoop_port']}"
    ) as snoop_ws, websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        async def collect_snoop():
            async for raw in snoop_ws:
                msg = json.loads(raw)
                snoop_messages.append(msg)
                if msg.get("payload") == meter_values:
                    got_meter_values.set()

        snoop_task = asyncio.create_task(collect_snoop())

        await asyncio.wait_for(cpms_ready.wait(), timeout=3.0)

        boot = [2, "boot-1", "BootNotification", {"chargePointVendor": "Acme", "chargePointModel": "X1"}]
        await cp_ws.send(json.dumps(boot))

        trigger = await _recv_json(cp_ws)
        await cp_ws.send(json.dumps([3, trigger[1], {"status": "Accepted"}]))
        await cp_ws.send(json.dumps(meter_values))

        # Wait for the MeterValues to arrive at the snoop subscriber.
        await asyncio.wait_for(got_meter_values.wait(), timeout=3.0)

        snoop_task.cancel()
        await asyncio.gather(snoop_task, return_exceptions=True)

    # MeterValues must NOT have reached the CPMS.
    assert all(frame[1] != "mv-1" for frame in cpms_received), (
        f"Triggered MeterValues leaked to CPMS: {cpms_received}"
    )


@pytest.mark.asyncio
async def test_subsequent_meter_values_forwarded(relay_harness):
    """After the triggered MeterValues is captured, subsequent ones flow normally."""
    h = relay_harness
    cpms_received = h["cpms_received"]
    cpms_ready = h["cpms_ready"]

    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(cpms_ready.wait(), timeout=3.0)

        boot = [2, "boot-1", "BootNotification", {"chargePointVendor": "Acme", "chargePointModel": "X1"}]
        await cp_ws.send(json.dumps(boot))

        trigger = await _recv_json(cp_ws)
        trigger_id = trigger[1]

        await cp_ws.send(json.dumps([3, trigger_id, {"status": "Accepted"}]))

        # First MeterValues — captured by relay.
        mv1 = [2, "mv-1", "MeterValues", {"connectorId": 1, "meterValue": []}]
        await cp_ws.send(json.dumps(mv1))

        # Second MeterValues — must pass through to CPMS.
        mv2 = [2, "mv-2", "MeterValues", {"connectorId": 1, "meterValue": []}]
        await cp_ws.send(json.dumps(mv2))

        await asyncio.sleep(0.1)

        assert all(frame[1] != "mv-1" for frame in cpms_received), "First MeterValues should be captured"
        assert any(frame[1] == "mv-2" for frame in cpms_received), "Second MeterValues should be forwarded"


@pytest.mark.asyncio
async def test_trigger_state_cleared_on_disconnect(relay_harness):
    """After disconnect, a reconnecting CP gets a fresh TriggerMessage cycle."""
    h = relay_harness
    cpms_ready = h["cpms_ready"]

    # First connection.
    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(cpms_ready.wait(), timeout=3.0)

        boot = [2, "boot-1", "BootNotification", {"chargePointVendor": "Acme", "chargePointModel": "X1"}]
        await cp_ws.send(json.dumps(boot))

        trigger1 = await _recv_json(cp_ws)
        assert trigger1[2] == "TriggerMessage"
        # Don't reply — disconnect with awaiting state potentially set.

    # Give relay time to process the disconnect.
    await asyncio.sleep(0.1)
    cpms_ready.clear()

    # Second connection — relay must send a fresh TriggerMessage.
    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(cpms_ready.wait(), timeout=3.0)

        boot = [2, "boot-2", "BootNotification", {"chargePointVendor": "Acme", "chargePointModel": "X1"}]
        await cp_ws.send(json.dumps(boot))

        trigger2 = await _recv_json(cp_ws)
        assert trigger2[2] == "TriggerMessage"
        assert trigger2[1] != trigger1[1], "Expected a fresh TriggerMessage id on reconnect"
