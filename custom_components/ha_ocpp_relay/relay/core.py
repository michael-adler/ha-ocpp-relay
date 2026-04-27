"""Shared relay and snoop websocket server core used by HA and standalone entrypoints."""

from __future__ import annotations

import asyncio
import base64
import contextlib
from dataclasses import asdict
import json
import logging

import websockets
import websockets.exceptions

from ..shared.models import MessageData


def basic_auth_header(username: str, password: str) -> tuple[str, str]:
    """Build an HTTP Basic auth header tuple for websocket upgrade requests."""
    user_pass = f"{username}:{password}"
    basic_credentials = base64.b64encode(user_pass.encode()).decode()
    return ("Authorization", f"Basic {basic_credentials}")


async def _close_ws(ws, *, timeout: float = 1.0) -> None:
    """Close a websocket politely, suppressing all errors."""
    try:
        if not getattr(ws, "open", False):
            return
        try:
            await ws.close(code=1000, reason="server closing")
        except TypeError:
            await ws.close()
        wait_coro = getattr(ws, "wait_closed", None)
        if callable(wait_coro):
            with contextlib.suppress(Exception):
                await asyncio.wait_for(wait_coro(), timeout=timeout)
    except Exception:  # noqa: BLE001
        pass


class SnoopWebSocketServer:
    """Forward all snoop queue messages to each connected client."""

    def __init__(self, snoop_queue: asyncio.Queue) -> None:
        """Bind a message queue to a fanout websocket endpoint."""
        if snoop_queue is None:
            raise ValueError("snoop_queue must be set")
        self._logger = logging.getLogger(__name__)
        self._snoop_queue = snoop_queue
        self._snoop_sockets: set = set()
        self._forward_task: asyncio.Task | None = None

    async def start(self, host: str, port: int, ssl_context=None):
        """Start websocket serving and queue-to-client fanout task."""
        self._forward_task = asyncio.create_task(self._forward_messages())
        server = await websockets.serve(self._on_connect, host, port, ssl=ssl_context)
        self._logger.info("Snoop server started on %s:%s", host, port)
        return server

    async def stop(self) -> None:
        """Stop fanout task and release server-side resources."""
        if self._forward_task is not None:
            self._forward_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._forward_task
            self._forward_task = None
        # Politely close any remaining snoop client sockets.
        for ws in list(self._snoop_sockets):
            await _close_ws(ws)
            self._snoop_sockets.discard(ws)

    async def _forward_messages(self) -> None:
        """Consume relay events and broadcast them to all snoop clients."""
        while True:
            msg = await self._snoop_queue.get()
            msg_json = json.dumps(asdict(msg))
            # Iterate over a snapshot so disconnected sockets can be removed safely.
            for ws in list(self._snoop_sockets):
                try:
                    await ws.send(msg_json)
                except Exception as err:  # noqa: BLE001
                    self._logger.warning("Error sending to snoop client: %s", err)
                    self._snoop_sockets.discard(ws)

    async def _on_connect(self, ws) -> None:
        """Track one snoop client connection until it disconnects."""
        self._logger.info("Snoop client connected")
        self._snoop_sockets.add(ws)
        try:
            while True:
                await ws.recv()
        except websockets.exceptions.ConnectionClosed:
            # Covers ConnectionClosedOK and ConnectionClosedError — both are subclasses.
            self._logger.info("Snoop connection closed")
        finally:
            # Remove from set first, then close — prevents double-close race with stop().
            self._snoop_sockets.discard(ws)
            await _close_ws(ws)


class OCPPRelay:
    """WebSocket relay that forwards messages between CP and CSMS."""

    def __init__(
        self,
        csms_url: str,
        csms_id: str | None = None,
        csms_pass: str | None = None,
        snoop_queue: asyncio.Queue | None = None,
    ) -> None:
        """Configure CP<->CSMS relay behavior for one server instance."""
        if not csms_url:
            raise ValueError("csms_url must not be empty")
        self._logger = logging.getLogger(__name__)
        self._csms_url = csms_url
        self._csms_id = csms_id
        self._csms_pass = csms_pass
        self._snoop_queue = snoop_queue

    def _put_snoop(self, msg: MessageData) -> None:
        """Put a message on the snoop queue, logging if the queue is full."""
        if self._snoop_queue is None:
            return
        try:
            self._snoop_queue.put_nowait(msg)
        except asyncio.QueueFull:
            self._logger.warning(
                "Snoop queue is full; dropping %s event for CP %s", msg.event, msg.cp_id
            )

    async def start(self, host: str, port: int, ssl_context=None):
        """Start accepting charge point websocket connections."""
        server = await websockets.serve(self._on_connect, host, port, ssl=ssl_context)
        self._logger.info("Relay server started on %s:%s", host, port)
        return server

    async def _on_connect(self, cp_ws) -> None:
        """Bridge one charge point connection to the configured CSMS peer."""
        # The CP identifier is encoded in the websocket URL path.
        charge_point_id = cp_ws.request.path.strip("/")

        try:
            # OCPP requires an agreed subprotocol (for example ocpp1.6 or ocpp2.0.1).
            ws_subprotocol = cp_ws.request.headers["Sec-WebSocket-Protocol"]
        except KeyError:
            self._logger.error("Client did not specify OCPP sub-protocol. Closing connection.")
            await cp_ws.close()
            return

        self._logger.info(
            "Charge point connected: %s (protocol=%s)", charge_point_id, ws_subprotocol
        )
        self._put_snoop(
            MessageData(
                event="Connection",
                sender="CP",
                protocol=ws_subprotocol,
                cp_id=charge_point_id,
            )
        )

        extra_headers = []
        if all([self._csms_id, self._csms_pass]):
            # Forward optional upstream BasicAuth credentials only to the CSMS side.
            extra_headers.append(basic_auth_header(self._csms_id, self._csms_pass))

        csms_uri = f"{self._csms_url}/{charge_point_id}"
        try:
            async with websockets.connect(
                csms_uri,
                subprotocols=[ws_subprotocol],
                additional_headers=extra_headers,
            ) as csms_ws:
                # Relay in both directions until either side closes.
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
                finally:
                    # Ensure both loops terminate to avoid orphaned websocket reads.
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)

                    # Politely close both websockets if still open.
                    for sock in (cp_ws, csms_ws):
                        await _close_ws(sock)

            self._logger.info("Relay connection closed for charge point %s", charge_point_id)

        except Exception as err:  # noqa: BLE001
            self._logger.error(
                "Unexpected error in relay for charge point %s: %s", charge_point_id, err
            )
        finally:
            self._logger.info("Charge point disconnected: %s", charge_point_id)
            self._put_snoop(
                MessageData(
                    event="Disconnection",
                    sender="CP",
                    protocol=ws_subprotocol,
                    cp_id=charge_point_id,
                )
            )
            # Ensure the CP socket is closed even if the CSMS connect itself failed.
            await _close_ws(cp_ws)

    async def _relay(
        self,
        source_ws,
        target_ws,
        source_name: str,
        target_name: str,
        cp_id: str,
        protocol: str,
    ) -> None:
        """Forward frames one direction and optionally mirror them to snoop."""
        while True:
            try:
                message = await source_ws.recv()
            except websockets.exceptions.ConnectionClosed:
                self._logger.info(
                    "Connection closed on %s side for CP %s", source_name, cp_id
                )
                return

            # Guard against malformed (non-list) OCPP frames before indexing.
            try:
                json_message = json.loads(message)
            except json.JSONDecodeError as err:
                self._logger.warning(
                    "Non-JSON frame from %s for CP %s (dropping): %s", source_name, cp_id, err
                )
                continue

            try:
                await target_ws.send(message)
            except websockets.exceptions.ConnectionClosed:
                self._logger.info(
                    "Connection closed on %s side while relaying for CP %s", target_name, cp_id
                )
                return

            # Safe log: json_message may be a dict or other non-list for malformed frames.
            if isinstance(json_message, list) and len(json_message) > 1:
                msg_id = json_message[1]
            else:
                msg_id = "<non-list frame>"
            self._logger.info(
                "Relayed message from %s to %s (%s)", source_name, target_name, msg_id
            )

            # Use _put_snoop() to handle QueueFull gracefully.
            self._put_snoop(
                MessageData(
                    event="Message",
                    sender=source_name,
                    protocol=protocol,
                    cp_id=cp_id,
                    payload=json_message,
                )
            )
