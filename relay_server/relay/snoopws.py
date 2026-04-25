"""Snoop websocket server that forwards queued relay events to connected observers."""

import asyncio
from dataclasses import asdict
import json
import logging

import websockets


class SnoopWebSocketServer:
    """Forward all snoop queue messages to each connected client."""

    def __init__(self, snoop_queue: asyncio.Queue):
        if snoop_queue is None:
            raise ValueError("snoop_queue must be set")
        self.logger = logging.getLogger(__name__)
        self.snoop_queue = snoop_queue
        self.snoop_sockets = set()
        asyncio.create_task(self._forward_messages())

    async def _forward_messages(self):
        while True:
            msg = await self.snoop_queue.get()
            msg_json = json.dumps(asdict(msg))
            for ws in self.snoop_sockets.copy():
                try:
                    await ws.send(msg_json)
                except Exception as err:
                    self.logger.error("Error sending to snoop client: %s", err)
                    self.snoop_sockets.remove(ws)

    async def _relay(self, source_ws):
        while True:
            try:
                await source_ws.recv()
            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedOK):
                self.logger.info("Snoop connection closed.")
                break

    async def _on_connect(self, ws):
        self.logger.info("Snoop client connected.")
        self.snoop_sockets.add(ws)
        await self._relay(ws)
        self.snoop_sockets.discard(ws)

    async def start(self, host, port, ssl_context=None):
        server = await websockets.serve(self._on_connect, host, port, ssl=ssl_context)
        self.logger.info("Snoop server started on %s", port)
        return server
