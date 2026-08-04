"""Tests for StatusNotification caching in OCPPRelay._cp_packet_cache.

StatusNotification frames from the CP are cached under the key
"StatusNotification:{connectorId}".  Each connector ID has its own slot;
a newer notification for the same connector overwrites the previous one.
The cache is cleared when the CP disconnects, and the snoop server replays
all cached entries to clients that connect after the fact.
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


async def _make_harness():
    """Shared setup: relay + snoop + fake CPMS with a near-zero boot trigger deadline."""
    cp_port = _free_port()
    snoop_port = _free_port()
    snoop_queue = asyncio.Queue()

    cpms_port = _free_port()
    cpms_ready = asyncio.Event()

    async def cpms_handler(ws):
        cpms_ready.set()
        async for _ in ws:
            pass

    cpms_server = await websockets.serve(
        cpms_handler, LOCALHOST, cpms_port, subprotocols=[OCPP_SUBPROTOCOL]
    )

    relay = OCPPRelay(
        f"ws://{LOCALHOST}:{cpms_port}",
        snoop_queue=snoop_queue,
        boot_trigger_deadline=(0.0, 0.05),
    )
    relay_server = await relay.start(LOCALHOST, cp_port)

    snoop = SnoopWebSocketServer(snoop_queue=snoop_queue, cp_packet_cache=relay._cp_packet_cache)
    snoop_server = await snoop.start(LOCALHOST, snoop_port)

    return {
        "cp_port": cp_port,
        "snoop_port": snoop_port,
        "relay": relay,
        "cpms_ready": cpms_ready,
        "servers": (snoop_server, relay_server, cpms_server),
        "snoop": snoop,
    }


async def _teardown(h):
    await h["snoop"].stop()
    for server in h["servers"]:
        server.close()
        await server.wait_closed()


@pytest.fixture
async def harness():
    h = await _make_harness()
    yield h
    await _teardown(h)


def _status_notification(msg_id: str, connector_id: int, status: str = "Available") -> list:
    return [2, msg_id, "StatusNotification", {
        "connectorId": connector_id, "errorCode": "NoError", "status": status,
    }]


@pytest.mark.asyncio
async def test_status_notification_cached(harness):
    """StatusNotification is stored under 'StatusNotification:{connectorId}'."""
    h = harness
    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01", subprotocols=[OCPP_SUBPROTOCOL]
    ) as cp_ws:
        await asyncio.wait_for(h["cpms_ready"].wait(), timeout=3.0)
        await cp_ws.send(json.dumps(_status_notification("sn-1", connector_id=1)))
        await asyncio.sleep(0.1)

        cache = h["relay"]._cp_packet_cache.get("CP-01", {})
        assert "StatusNotification:1" in cache
        assert cache["StatusNotification:1"].payload[2] == "StatusNotification"
        assert cache["StatusNotification:1"].payload[3]["connectorId"] == 1


@pytest.mark.asyncio
async def test_status_notification_overwrites_same_connector(harness):
    """A second StatusNotification for the same connector replaces the first."""
    h = harness
    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01", subprotocols=[OCPP_SUBPROTOCOL]
    ) as cp_ws:
        await asyncio.wait_for(h["cpms_ready"].wait(), timeout=3.0)
        await cp_ws.send(json.dumps(_status_notification("sn-1", connector_id=1, status="Available")))
        await cp_ws.send(json.dumps(_status_notification("sn-2", connector_id=1, status="Charging")))
        await asyncio.sleep(0.1)

        cache = h["relay"]._cp_packet_cache.get("CP-01", {})
        assert cache["StatusNotification:1"].payload[3]["status"] == "Charging"


@pytest.mark.asyncio
async def test_status_notifications_keyed_per_connector(harness):
    """StatusNotifications for different connectors occupy independent cache slots."""
    h = harness
    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01", subprotocols=[OCPP_SUBPROTOCOL]
    ) as cp_ws:
        await asyncio.wait_for(h["cpms_ready"].wait(), timeout=3.0)
        await cp_ws.send(json.dumps(_status_notification("sn-0", connector_id=0, status="Available")))
        await cp_ws.send(json.dumps(_status_notification("sn-1", connector_id=1, status="Charging")))
        await cp_ws.send(json.dumps(_status_notification("sn-2", connector_id=2, status="Faulted")))
        await asyncio.sleep(0.1)

        cache = h["relay"]._cp_packet_cache.get("CP-01", {})
        assert cache["StatusNotification:0"].payload[3]["status"] == "Available"
        assert cache["StatusNotification:1"].payload[3]["status"] == "Charging"
        assert cache["StatusNotification:2"].payload[3]["status"] == "Faulted"


@pytest.mark.asyncio
async def test_status_notification_replayed_to_late_snoop_client(harness):
    """A snoop client connecting after a StatusNotification receives it from the cache."""
    h = harness
    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01", subprotocols=[OCPP_SUBPROTOCOL]
    ) as cp_ws:
        await asyncio.wait_for(h["cpms_ready"].wait(), timeout=3.0)
        await cp_ws.send(json.dumps(_status_notification("sn-1", connector_id=1, status="Charging")))
        # Let the relay process and cache the frame, and _forward_messages drain the queue.
        await asyncio.sleep(0.1)

        async with websockets.connect(f"ws://{LOCALHOST}:{h['snoop_port']}") as snoop_ws:
            received = []
            while True:
                try:
                    received.append(await _recv_json(snoop_ws, timeout=0.5))
                except asyncio.TimeoutError:
                    break

        status_msgs = [
            m for m in received
            if m.get("event") == "Message" and m["payload"][2] == "StatusNotification"
        ]
        assert len(status_msgs) == 1
        assert status_msgs[0]["payload"][3]["connectorId"] == 1
        assert status_msgs[0]["payload"][3]["status"] == "Charging"


@pytest.mark.asyncio
async def test_snoop_replay_order(harness):
    """Cache replay delivers BootNotification first, then StatusNotifications sorted by connector ID."""
    h = harness
    boot = [2, "bn-1", "BootNotification", {"chargePointVendor": "Acme", "chargePointModel": "X1"}]
    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01", subprotocols=[OCPP_SUBPROTOCOL]
    ) as cp_ws:
        await asyncio.wait_for(h["cpms_ready"].wait(), timeout=3.0)
        # Send StatusNotifications out of order, then a BootNotification.
        await cp_ws.send(json.dumps(_status_notification("sn-3", connector_id=3, status="Faulted")))
        await cp_ws.send(json.dumps(_status_notification("sn-1", connector_id=1, status="Charging")))
        await cp_ws.send(json.dumps(boot))
        await cp_ws.send(json.dumps(_status_notification("sn-2", connector_id=2, status="Available")))
        # Let the relay process all frames and _forward_messages drain the queue.
        await asyncio.sleep(0.1)

        async with websockets.connect(f"ws://{LOCALHOST}:{h['snoop_port']}") as snoop_ws:
            received = []
            while True:
                try:
                    received.append(await _recv_json(snoop_ws, timeout=0.5))
                except asyncio.TimeoutError:
                    break

    # Filter to only the replayed Message frames (excludes any live queue events).
    messages = [m for m in received if m.get("event") == "Message"]
    actions = [m["payload"][2] for m in messages]
    assert actions[0] == "BootNotification"
    connector_ids = [
        m["payload"][3]["connectorId"]
        for m in messages
        if m["payload"][2] == "StatusNotification"
    ]
    assert connector_ids == [1, 2, 3]


@pytest.mark.asyncio
async def test_status_notification_cache_cleared_on_disconnect(harness):
    """All StatusNotification cache entries for a CP are removed on disconnect."""
    h = harness
    async with websockets.connect(
        f"ws://{LOCALHOST}:{h['cp_port']}/CP-01", subprotocols=[OCPP_SUBPROTOCOL]
    ) as cp_ws:
        await asyncio.wait_for(h["cpms_ready"].wait(), timeout=3.0)
        await cp_ws.send(json.dumps(_status_notification("sn-1", connector_id=1)))
        await asyncio.sleep(0.1)
        assert "StatusNotification:1" in h["relay"]._cp_packet_cache.get("CP-01", {})

    # Give the relay time to process the disconnect and purge the cache.
    await asyncio.sleep(0.1)
    assert "CP-01" not in h["relay"]._cp_packet_cache
