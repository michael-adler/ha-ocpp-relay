"""Verify the relay's WebSocket keepalive settings for charge point connections.

The relay must tolerate short network outages on the charge point link
(issue #7): it keeps pinging every CP_PING_INTERVAL seconds but waits up to
CP_PING_TIMEOUT seconds for a pong before closing the connection.
"""

import asyncio

import pytest
import websockets

from custom_components.ha_ocpp_relay.relay.core import (
    CP_PING_INTERVAL,
    CP_PING_TIMEOUT,
    OCPPRelay,
)

CP_PORT = 8570
CSMS_PORT = 8571


def test_keepalive_constants():
    assert CP_PING_INTERVAL == 20
    assert CP_PING_TIMEOUT == 120


@pytest.mark.cli
def test_cp_connection_uses_tolerant_ping_timeout():
    async def scenario():
        async def csms(ws):
            await ws.wait_closed()

        csms_server = await websockets.serve(csms, "127.0.0.1", CSMS_PORT, subprotocols=["ocpp1.6"])
        relay = OCPPRelay(f"ws://127.0.0.1:{CSMS_PORT}", boot_trigger_deadline=(60.0, 60.0))
        relay_server = await relay.start("127.0.0.1", CP_PORT)
        try:
            async with websockets.connect(
                f"ws://127.0.0.1:{CP_PORT}/CP-KEEPALIVE", subprotocols=["ocpp1.6"]
            ):
                for _ in range(50):
                    if relay_server.connections:
                        break
                    await asyncio.sleep(0.05)
                (cp_conn,) = relay_server.connections
                return cp_conn.ping_interval, cp_conn.ping_timeout
        finally:
            relay_server.close()
            await relay_server.wait_closed()
            csms_server.close()
            await csms_server.wait_closed()

    assert asyncio.run(scenario()) == (CP_PING_INTERVAL, CP_PING_TIMEOUT)
