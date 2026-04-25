"""Local relay supervisor and embedded relay/snoop websocket server implementations."""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import logging
from typing import Any

import websockets

from .const import (
    CONF_CPMS_URL,
    CONF_RELAY_OCPP_HOST,
    CONF_RELAY_OCPP_PORT,
    CONF_RELAY_SNOOP_HOST,
    CONF_RELAY_SNOOP_PORT,
)


@dataclass
class MessageData:
    event: str
    sender: str
    protocol: str | None = None
    cp_id: str | None = None
    payload: Any = field(default_factory=dict)
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )


class SnoopWebSocketServer:
    def __init__(self, snoop_queue: asyncio.Queue) -> None:
        self._logger = logging.getLogger(__name__)
        self._snoop_queue = snoop_queue
        self._snoop_sockets: set = set()
        self._forward_task: asyncio.Task | None = None

    async def start(self, host: str, port: int):
        self._forward_task = asyncio.create_task(self._forward_messages())
        server = await websockets.serve(self._on_connect, host, port)
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
        except websockets.exceptions.ConnectionClosed:
            self._logger.info("Snoop connection closed")
        finally:
            self._snoop_sockets.discard(ws)


class OCPPRelay:
    def __init__(self, csms_url: str, snoop_queue: asyncio.Queue) -> None:
        self._logger = logging.getLogger(__name__)
        self._csms_url = csms_url
        self._snoop_queue = snoop_queue

    async def start(self, host: str, port: int):
        server = await websockets.serve(self._on_connect, host, port)
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

        csms_uri = f"{self._csms_url}/{charge_point_id}"
        async with websockets.connect(csms_uri, subprotocols=[ws_subprotocol]) as csms_ws:
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
            except websockets.exceptions.ConnectionClosed:
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
            self._snoop_queue.put_nowait(
                MessageData(
                    event="Message",
                    sender=source_name,
                    protocol=protocol,
                    cp_id=cp_id,
                    payload=json_message,
                )
            )


class LocalRelaySupervisor:
    def __init__(self, hass, config: dict[str, Any]) -> None:
        self._hass = hass
        self._config = config
        self._logger = logging.getLogger(__name__)
        self._task: asyncio.Task | None = None

    async def async_start(self) -> None:
        if self._task is None:
            self._task = self._hass.async_create_task(self._run())

    async def async_stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _run(self) -> None:
        while True:
            relay_server = None
            snoop_server = None
            snoop_ws_server: SnoopWebSocketServer | None = None
            try:
                cpms_url = self._config.get(CONF_CPMS_URL)
                if not cpms_url:
                    raise ValueError("Local relay mode requires cpms_url")

                msg_queue: asyncio.Queue = asyncio.Queue()
                relay = OCPPRelay(cpms_url, snoop_queue=msg_queue)
                relay_server = await relay.start(
                    self._config[CONF_RELAY_OCPP_HOST],
                    self._config[CONF_RELAY_OCPP_PORT],
                )

                snoop_ws_server = SnoopWebSocketServer(snoop_queue=msg_queue)
                snoop_server = await snoop_ws_server.start(
                    self._config[CONF_RELAY_SNOOP_HOST],
                    self._config[CONF_RELAY_SNOOP_PORT],
                )

                await asyncio.gather(relay_server.wait_closed(), snoop_server.wait_closed())
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001
                self._logger.exception("Local relay crashed: %s. Restarting in 15 seconds.", err)
                await asyncio.sleep(15)
            finally:
                if relay_server is not None:
                    relay_server.close()
                    await relay_server.wait_closed()
                if snoop_server is not None:
                    snoop_server.close()
                    await snoop_server.wait_closed()
                if snoop_ws_server is not None:
                    await snoop_ws_server.stop()
