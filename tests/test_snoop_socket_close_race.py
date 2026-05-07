import asyncio
import contextlib

import pytest

from custom_components.ha_ocpp_relay.relay.core import SnoopWebSocketServer
from custom_components.ha_ocpp_relay.shared.models import MessageData


class FakeSnoopSocket:
    def __init__(self) -> None:
        self.close_calls = 0
        self._recv_waiter = asyncio.Event()

    async def send(self, _message: str) -> None:
        raise RuntimeError("simulated send failure")

    async def recv(self) -> str:
        await self._recv_waiter.wait()
        return "noop"

    async def close(self, *args, **kwargs) -> None:  # noqa: ARG002
        self.close_calls += 1


async def _wait_for(predicate, timeout: float = 1.0) -> None:
    end = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < end:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("Timed out waiting for condition")


@pytest.mark.asyncio
async def test_snoop_socket_closed_once_on_send_failure_and_disconnect_cleanup() -> None:
    queue: asyncio.Queue = asyncio.Queue()
    server = SnoopWebSocketServer(queue)
    ws = FakeSnoopSocket()

    connect_task = asyncio.create_task(server._on_connect(ws))
    forward_task = asyncio.create_task(server._forward_messages())

    try:
        await _wait_for(lambda: ws in server._snoop_sockets)

        queue.put_nowait(
            MessageData(
                event="Message",
                sender="CP",
                protocol="ocpp1.6",
                cp_id="CP-1",
                payload={"k": "v"},
            )
        )

        await _wait_for(lambda: ws.close_calls == 1)

        connect_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await connect_task

        assert ws.close_calls == 1
    finally:
        forward_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await forward_task
