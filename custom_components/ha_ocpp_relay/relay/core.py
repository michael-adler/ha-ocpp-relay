"""Shared relay and snoop websocket server core used by HA and standalone entrypoints."""

from __future__ import annotations

import asyncio
import base64
import contextlib
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import logging
from typing import Any, Literal

import websockets


def basic_auth_header(username: str, password: str) -> tuple[str, str]:
    """Build an HTTP Basic auth header tuple for websocket upgrade requests."""
    user_pass = f"{username}:{password}"
    basic_credentials = base64.b64encode(user_pass.encode()).decode()
    return ("Authorization", f"Basic {basic_credentials}")


@dataclass
class MessageData:
    """Message passed on the relay snoop stream."""

    event: Literal["Connection", "Disconnection", "Message"]
    sender: Literal["CP", "CSMS"]
    protocol: str | None = None
    cp_id: str | None = None
    payload: Any = field(default_factory=dict)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )


class SnoopWebSocketServer:
    """Forward all snoop queue messages to each connected client."""

    def __init__(self, snoop_queue: asyncio.Queue) -> None:
        if snoop_queue is None:
            raise ValueError("snoop_queue must be set")
        self._logger = logging.getLogger(__name__)
        self._snoop_queue = snoop_queue
        self._snoop_sockets: set = set()
        self._forward_task: asyncio.Task | None = None

    async def start(self, host: str, port: int, ssl_context=None):
        self._forward_task = asyncio.create_task(self._forward_messages())
        server = await websockets.serve(self._on_connect, host, port, ssl=ssl_context)
        self._logger.info("Snoop server started on %s:%s", host, port)
        return server

    async def stop(self) -> None:
        if self._forward_task is not None:
            self._forward_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._forward_task
            self._forward_task = None

    async def _forward_messages(self) -> None:
        while True:
            msg = await self._snoop_queue.get()
            msg_json = json.dumps(asdict(msg))
            for ws in self._snoop_sockets.copy():
                try:
                    await ws.send(msg_json)
                except Exception as err:  # noqa: BLE001
                    self._logger.warning("Error sending to snoop client: %s", err)
                    self._snoop_sockets.discard(ws)

    async def _on_connect(self, ws) -> None:
        self._logger.info("Snoop client connected")
        self._snoop_sockets.add(ws)
        try:
            while True:
                await ws.recv()
        except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedOK):
            self._logger.info("Snoop connection closed")
        finally:
            self._snoop_sockets.discard(ws)


class OCPPRelay:
    """WebSocket relay that forwards messages between CP and CSMS."""

    def __init__(
        self,
        csms_url: str,
        csms_id: str | None = None,
        csms_pass: str | None = None,
        snoop_queue: asyncio.Queue | None = None,
    ) -> None:
        if not csms_url:
            raise ValueError("csms_url must not be empty")
        self._logger = logging.getLogger(__name__)
        self._csms_url = csms_url
        self._csms_id = csms_id
        self._csms_pass = csms_pass
        self._snoop_queue = snoop_queue

    async def start(self, host: str, port: int, ssl_context=None):
        server = await websockets.serve(self._on_connect, host, port, ssl=ssl_context)
        self._logger.info("Relay server started on %s:%s", host, port)
        return server

    async def _on_connect(self, cp_ws) -> None:
        charge_point_id = cp_ws.request.path.strip("/")

        try:
            ws_subprotocol = cp_ws.request.headers["Sec-WebSocket-Protocol"]
        except KeyError:
            self._logger.error("Client did not specify OCPP sub-protocol. Closing connection.")
            await cp_ws.close()
            return

        self._logger.info("Charge point connected: %s (protocol=%s)", charge_point_id, ws_subprotocol)

        extra_headers = []
        if all([self._csms_id, self._csms_pass]):
            extra_headers.append(basic_auth_header(self._csms_id, self._csms_pass))

        csms_uri = f"{self._csms_url}/{charge_point_id}"
        async with websockets.connect(
            csms_uri,
            subprotocols=[ws_subprotocol],
            additional_headers=extra_headers,
        ) as csms_ws:
            tasks = [
                asyncio.create_task(
                    self._relay(
                        cp_ws,
                        csms_ws,
                        source_name="CP",
                        target_name="CSMS",
                        cp_id=charge_point_id,
                        protocol=ws_subprotocol,
                    )
                ),
                asyncio.create_task(
                    self._relay(
                        csms_ws,
                        cp_ws,
                        source_name="CSMS",
                        target_name="CP",
                        cp_id=charge_point_id,
                        protocol=ws_subprotocol,
                    )
                ),
            ]

            try:
                await asyncio.gather(*tasks)
            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedOK):
                self._logger.info("Relay connection closed for charge point %s", charge_point_id)
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _relay(
        self,
        source_ws,
        target_ws,
        source_name: str,
        target_name: str,
        cp_id: str,
        protocol: str,
    ) -> None:
        while True:
            message = await source_ws.recv()
            json_message = json.loads(message)
            await target_ws.send(message)
            self._logger.info(
                "Relayed message from %s to %s (%s)", source_name, target_name, json_message[1]
            )
            if self._snoop_queue is not None:
                self._snoop_queue.put_nowait(
                    MessageData(
                        event="Message",
                        sender=source_name,
                        protocol=protocol,
                        cp_id=cp_id,
                        payload=json_message,
                    )
                )
