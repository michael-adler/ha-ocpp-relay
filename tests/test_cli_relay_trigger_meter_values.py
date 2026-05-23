"""Tests for relay-initiated TriggerMessage / MeterValues interception.

The relay sends TriggerMessage to the CP on the first StatusNotification to
initialize sensors.  It must:
  - fire only once per connection, not on every StatusNotification
  - not forward the TriggerMessage Accepted CallResult to the CPMS
  - forward the triggered MeterValues to the CPMS so the CSMS can issue CALLRESULT
  - also deliver that MeterValues to snoop subscribers
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


async def _consume_boot_trigger(cp_ws):
    """Read and discard the relay-initiated TriggerMessage(BootNotification).

    The relay now sends this trigger immediately after the CSMS connection is
    established, before the CP sends any messages.  Tests that focus on the
    MeterValues trigger must consume it first so their own recv calls get the
    expected frames.
    """
    msg = await _recv_json(cp_ws)
    assert msg[0] == 2 and msg[2] == "TriggerMessage"
    assert msg[3].get("requestedMessage") == "BootNotification"


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

    relay = OCPPRelay(f"ws://{LOCALHOST}:{cpms_port}", snoop_queue=snoop_queue, boot_trigger_deadline=(0.0, 0.05))
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


STATUS_NOTIFICATION = [2, "sn-1", "StatusNotification", {"connectorId": 1, "status": "Available", "errorCode": "NoError"}]


@pytest.mark.asyncio
async def test_trigger_sent_after_first_status_notification(relay_harness):
    """The relay sends TriggerMessage to the CP on the first StatusNotification."""
    h = relay_harness
    cpms_ready = h["cpms_ready"]

    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(cpms_ready.wait(), timeout=3.0)
        await _consume_boot_trigger(cp_ws)
        await cp_ws.send(json.dumps(STATUS_NOTIFICATION))

        trigger = await _recv_json(cp_ws)
        assert trigger[0] == 2
        assert trigger[2] == "TriggerMessage"
        assert trigger[3].get("requestedMessage") == "MeterValues"


@pytest.mark.asyncio
async def test_trigger_sent_only_once_per_connection(relay_harness):
    """Subsequent StatusNotification frames must not cause additional TriggerMessages."""
    h = relay_harness
    cpms_ready = h["cpms_ready"]

    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(cpms_ready.wait(), timeout=3.0)
        await _consume_boot_trigger(cp_ws)

        # First StatusNotification — expect a TriggerMessage.
        await cp_ws.send(json.dumps(STATUS_NOTIFICATION))
        trigger = await _recv_json(cp_ws)
        assert trigger[2] == "TriggerMessage"

        # Second StatusNotification — must not produce another TriggerMessage.
        sn2 = [2, "sn-2", "StatusNotification", {"connectorId": 2, "status": "Available", "errorCode": "NoError"}]
        await cp_ws.send(json.dumps(sn2))

        with pytest.raises(asyncio.TimeoutError):
            await _recv_json(cp_ws, timeout=0.3)


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
        await cp_ws.send(json.dumps(STATUS_NOTIFICATION))

        trigger = await _recv_json(cp_ws)
        trigger_id = trigger[1]

        # Reply Accepted — the relay must swallow this.
        await cp_ws.send(json.dumps([3, trigger_id, {"status": "Accepted"}]))

        await asyncio.sleep(0.1)

        # The StatusNotification should have reached the CPMS, but not the Accepted.
        assert all(frame[1] != trigger_id for frame in cpms_received), (
            f"TriggerMessage CallResult leaked to CPMS: {cpms_received}"
        )


@pytest.mark.asyncio
async def test_meter_values_forwarded_and_snooped(relay_harness):
    """The triggered MeterValues must reach both the CPMS and a snoop subscriber."""
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
        await _consume_boot_trigger(cp_ws)
        await cp_ws.send(json.dumps(STATUS_NOTIFICATION))

        trigger = await _recv_json(cp_ws)
        await cp_ws.send(json.dumps([3, trigger[1], {"status": "Accepted"}]))
        await cp_ws.send(json.dumps(meter_values))

        # Wait for the MeterValues to arrive at the snoop subscriber.
        await asyncio.wait_for(got_meter_values.wait(), timeout=3.0)

        snoop_task.cancel()
        await asyncio.gather(snoop_task, return_exceptions=True)

    # MeterValues must have reached the CPMS (CSMS needs it to issue CALLRESULT).
    assert any(frame[1] == "mv-1" for frame in cpms_received), (
        f"Triggered MeterValues did not reach CPMS: {cpms_received}"
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
        await _consume_boot_trigger(cp_ws)
        await cp_ws.send(json.dumps(STATUS_NOTIFICATION))

        trigger = await _recv_json(cp_ws)
        await cp_ws.send(json.dumps([3, trigger[1], {"status": "Accepted"}]))

        # First MeterValues — triggered; forwarded to CPMS (so CSMS can CALLRESULT).
        mv1 = [2, "mv-1", "MeterValues", {"connectorId": 1, "meterValue": []}]
        await cp_ws.send(json.dumps(mv1))

        # Second MeterValues — normal forwarding continues.
        mv2 = [2, "mv-2", "MeterValues", {"connectorId": 1, "meterValue": []}]
        await cp_ws.send(json.dumps(mv2))

        await asyncio.sleep(0.1)

        assert any(frame[1] == "mv-1" for frame in cpms_received), "Triggered MeterValues should be forwarded"
        assert any(frame[1] == "mv-2" for frame in cpms_received), "Subsequent MeterValues should be forwarded"


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
        await cp_ws.send(json.dumps(STATUS_NOTIFICATION))

        trigger1 = await _recv_json(cp_ws)
        assert trigger1[2] == "TriggerMessage"
        # Don't reply — disconnect with triggered and awaiting state set.

    # Give relay time to process the disconnect.
    await asyncio.sleep(0.1)
    cpms_ready.clear()

    # Second connection — relay must send a fresh TriggerMessage.
    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(cpms_ready.wait(), timeout=3.0)
        await cp_ws.send(json.dumps(STATUS_NOTIFICATION))

        trigger2 = await _recv_json(cp_ws)
        assert trigger2[2] == "TriggerMessage"
        assert trigger2[1] != trigger1[1], "Expected a fresh TriggerMessage id on reconnect"
