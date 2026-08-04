"""Tests for relay-initiated TriggerMessage(BootNotification) on CP connect.

When a CP connects and the relay establishes the upstream CSMS connection, the relay
immediately sends TriggerMessage(BootNotification) to the CP so that the relay's
_cp_packet_cache is populated for snoop clients that connect later.  The relay must:
  - send the trigger as soon as the CSMS connection is established, before any CP message
  - not forward the CP's TriggerMessage CALLRESULT to the CPMS
  - forward the CP's subsequent BootNotification to the CPMS and cache it
  - replay the cached BootNotification to a snoop client that connects after the fact
  - clear trigger state on disconnect so a reconnecting CP gets a fresh trigger
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

BOOT_NOTIFICATION = [2, "bn-1", "BootNotification", {"chargePointVendor": "Acme", "chargePointModel": "X1"}]
BOOT_RESPONSE = [3, "bn-1", {"currentTime": "2026-01-01T00:00:00Z", "interval": 300, "status": "Accepted"}]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind((LOCALHOST, 0))
        return s.getsockname()[1]


async def _recv_json(ws, timeout=3.0):
    raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
    return json.loads(raw)


async def _make_harness(boot_trigger_deadline):
    """Shared setup for relay + snoop + fake CPMS."""
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

    relay = OCPPRelay(f"ws://{LOCALHOST}:{cpms_port}", snoop_queue=snoop_queue, boot_trigger_deadline=boot_trigger_deadline)
    relay_server = await relay.start(LOCALHOST, cp_port)

    snoop = SnoopWebSocketServer(snoop_queue=snoop_queue, cp_packet_cache=relay._cp_packet_cache)
    snoop_server = await snoop.start(LOCALHOST, snoop_port)

    servers = (snoop_server, relay_server, cpms_server)
    harness = {
        "cp_port": cp_port,
        "snoop_port": snoop_port,
        "relay": relay,
        "cpms_received": cpms_received,
        "cpms_ready": cpms_ready,
        "cpms_ws_holder": cpms_ws_holder,
    }
    return harness, servers, snoop


async def _teardown_harness(servers, snoop):
    await snoop.stop()
    for server in servers:
        server.close()
        await server.wait_closed()


@pytest.fixture
async def relay_harness():
    """Relay with near-zero deadline: trigger fires without polling the cache."""
    harness, servers, snoop = await _make_harness(boot_trigger_deadline=(0.0, 0.05))
    yield harness
    await _teardown_harness(servers, snoop)


@pytest.fixture
async def relay_harness_polling():
    """Relay with a fixed 0.1 s deadline so the polling loop actually executes."""
    harness, servers, snoop = await _make_harness(boot_trigger_deadline=(0.1, 0.1))
    yield harness
    await _teardown_harness(servers, snoop)


@pytest.mark.asyncio
async def test_boot_trigger_sent_on_cp_connect(relay_harness):
    """Relay sends TriggerMessage(BootNotification) immediately after CSMS connects."""
    h = relay_harness

    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(h["cpms_ready"].wait(), timeout=3.0)

        trigger = await _recv_json(cp_ws)
        assert trigger[0] == 2
        assert trigger[2] == "TriggerMessage"
        assert trigger[3].get("requestedMessage") == "BootNotification"


@pytest.mark.asyncio
async def test_boot_trigger_callresult_not_forwarded_to_cpms(relay_harness):
    """The CP's CALLRESULT for the boot trigger must not reach the CPMS."""
    h = relay_harness
    cpms_received = h["cpms_received"]

    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(h["cpms_ready"].wait(), timeout=3.0)

        trigger = await _recv_json(cp_ws)
        trigger_id = trigger[1]
        assert trigger_id.startswith("relay-boot-")

        await cp_ws.send(json.dumps([3, trigger_id, {"status": "Accepted"}]))
        await asyncio.sleep(0.1)

        assert all(frame[1] != trigger_id for frame in cpms_received), (
            f"Boot TriggerMessage CallResult leaked to CPMS: {cpms_received}"
        )


@pytest.mark.asyncio
async def test_boot_notification_after_trigger_forwarded_to_cpms(relay_harness):
    """BootNotification sent by the CP after the trigger is forwarded to the CPMS normally."""
    h = relay_harness
    cpms_received = h["cpms_received"]
    cpms_ws_holder = h["cpms_ws_holder"]

    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(h["cpms_ready"].wait(), timeout=3.0)

        trigger = await _recv_json(cp_ws)
        await cp_ws.send(json.dumps([3, trigger[1], {"status": "Accepted"}]))
        await cp_ws.send(json.dumps(BOOT_NOTIFICATION))

        await asyncio.sleep(0.1)

        assert any(frame[1] == "bn-1" and frame[2] == "BootNotification" for frame in cpms_received), (
            f"BootNotification did not reach CPMS: {cpms_received}"
        )


@pytest.mark.asyncio
async def test_boot_notification_cached_and_replayed_to_late_snoop_client(relay_harness):
    """A snoop client connecting after BootNotification receives it from the cache."""
    h = relay_harness

    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(h["cpms_ready"].wait(), timeout=3.0)

        trigger = await _recv_json(cp_ws)
        await cp_ws.send(json.dumps([3, trigger[1], {"status": "Accepted"}]))
        await cp_ws.send(json.dumps(BOOT_NOTIFICATION))

        # Wait for the relay to process and cache the BootNotification.
        await asyncio.sleep(0.1)

        # A snoop client connecting now should receive the cached BootNotification.
        async with websockets.connect(f"ws://{LOCALHOST}:{h['snoop_port']}") as snoop_ws:
            cached = await _recv_json(snoop_ws)
            assert cached["event"] == "Message"
            assert cached["sender"] == "CP"
            assert cached["payload"][2] == "BootNotification"
            assert cached["payload"][3] == BOOT_NOTIFICATION[3]


@pytest.mark.asyncio
async def test_boot_trigger_state_cleared_on_disconnect(relay_harness):
    """After disconnect, a reconnecting CP receives a fresh TriggerMessage(BootNotification)."""
    h = relay_harness

    # First connection.
    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(h["cpms_ready"].wait(), timeout=3.0)
        trigger1 = await _recv_json(cp_ws)
        assert trigger1[2] == "TriggerMessage"

    # Give relay time to process the disconnect and clear trigger state.
    await asyncio.sleep(0.1)
    h["cpms_ready"].clear()

    # Second connection — relay must send a fresh trigger with a new message ID.
    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(h["cpms_ready"].wait(), timeout=3.0)
        trigger2 = await _recv_json(cp_ws)
        assert trigger2[2] == "TriggerMessage"
        assert trigger2[3].get("requestedMessage") == "BootNotification"
        assert trigger2[1] != trigger1[1], "Expected a fresh trigger id on reconnect"


@pytest.mark.asyncio
async def test_boot_trigger_fires_after_deadline(relay_harness_polling):
    """Trigger fires after the deadline when no spontaneous BootNotification is cached."""
    h = relay_harness_polling

    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(h["cpms_ready"].wait(), timeout=3.0)

        # CP sends nothing; relay must fire the trigger after the 0.1 s deadline.
        trigger = await _recv_json(cp_ws, timeout=1.0)
        assert trigger[0] == 2
        assert trigger[2] == "TriggerMessage"
        assert trigger[3].get("requestedMessage") == "BootNotification"


@pytest.mark.asyncio
async def test_boot_trigger_skipped_if_spontaneous_boot_cached(relay_harness_polling):
    """Trigger is suppressed when the CP sends a spontaneous BootNotification before the deadline."""
    h = relay_harness_polling

    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01",
        subprotocols=[OCPP_SUBPROTOCOL],
    ) as cp_ws:
        await asyncio.wait_for(h["cpms_ready"].wait(), timeout=3.0)

        # CP sends BootNotification immediately; relay caches it before the deadline fires.
        await cp_ws.send(json.dumps(BOOT_NOTIFICATION))

        # Wait until the relay has forwarded the BootNotification to the CPMS.  The
        # cache write happens before target_ws.send(), so once the CPMS has the frame
        # the cache is guaranteed to be populated — regardless of system load.
        async def cpms_received_boot():
            while not any(f[2] == "BootNotification" for f in h["cpms_received"]):
                await asyncio.sleep(0.01)

        await asyncio.wait_for(cpms_received_boot(), timeout=3.0)

        # Now wait for the 0.1 s deadline to have elapsed and the boot trigger task
        # to have run its cache check.  A brief yield is enough since the cache is
        # already populated; the task will exit on its next wake-up.
        await asyncio.sleep(0.2)

        # No TriggerMessage should have been sent to the CP.
        with pytest.raises(asyncio.TimeoutError):
            await _recv_json(cp_ws, timeout=0.1)
