"""Shared relay and snoop websocket server core used by HA and standalone entrypoints."""

from __future__ import annotations

import asyncio
import base64
import contextlib
from dataclasses import asdict
import json
import logging
import ssl
from urllib.parse import urlsplit, urlunsplit

import websockets
import websockets.exceptions

from ..shared.models import MessageData

# Maximum number of pending snoop/MQTT messages before older ones are dropped.
# Prevents unbounded memory growth when consumers are slow or disconnected.
SNOOP_QUEUE_MAXSIZE = 1000


def basic_auth_header(username: str, password: str) -> tuple[str, str]:
    """Build an HTTP Basic auth header tuple for websocket upgrade requests."""
    user_pass = f"{username}:{password}"
    basic_credentials = base64.b64encode(user_pass.encode()).decode()
    return ("Authorization", f"Basic {basic_credentials}")


def join_websocket_url(base_url: str, path_segment: str) -> str:
    """Append a websocket path segment without introducing duplicate slashes."""
    split_url = urlsplit(base_url)
    base_path = split_url.path.rstrip("/")
    joined_path = f"{base_path}/{path_segment.lstrip('/')}"
    return urlunsplit(split_url._replace(path=joined_path))


async def _close_ws(ws, *, timeout: float = 1.0) -> None:
    """Close a websocket politely, suppressing all errors."""
    try:
        # Attempt close without pre-checking .open: the websockets asyncio API
        # (v14+) dropped the .open attribute, and close() is idempotent so it
        # is safe to call on an already-closing connection.
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
        server = await websockets.serve(self._on_connect, host, port, ssl=ssl_context, reuse_address=True)
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
        csms_ssl_context: ssl.SSLContext | None = None,
    ) -> None:
        """Configure CP<->CSMS relay behavior for one server instance.

        Args:
            csms_url: WebSocket URL of the upstream CSMS.
            csms_id: Optional Basic-Auth username for the CSMS connection.
            csms_pass: Optional Basic-Auth password for the CSMS connection.
            snoop_queue: Optional bounded queue for message observation.
            csms_ssl_context: Optional SSL context for the upstream CSMS
                connection.  When *None* and the CSMS URL uses ``wss://``,
                the default system CA bundle is used (certificate verification
                enabled).  Pass a custom ``ssl.SSLContext`` to supply a
                private CA bundle or to adjust protocol/cipher settings.
        """
        if not csms_url:
            raise ValueError("csms_url must not be empty")
        self._logger = logging.getLogger(__name__)
        self._csms_url = csms_url
        self._csms_id = csms_id
        self._csms_pass = csms_pass
        self._snoop_queue = snoop_queue
        self._csms_ssl_context = csms_ssl_context

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
        # Pre-create the default SSL context off the event loop to avoid blocking
        # the loop with load_default_certs on every wss:// upstream connection.
        if self._csms_ssl_context is None and self._csms_url.startswith("wss://"):
            loop = asyncio.get_running_loop()
            self._csms_ssl_context = await loop.run_in_executor(
                None, ssl.create_default_context
            )
        server = await websockets.serve(self._on_connect, host, port, ssl=ssl_context, reuse_address=True)
        self._logger.info("Relay server started on %s:%s", host, port)
        return server

    async def _on_connect(self, cp_ws) -> None:
        """Bridge one charge point connection to the configured CSMS peer."""
        # The CP identifier is encoded in the websocket URL path.
        charge_point_id = cp_ws.request.path.strip("/")

        # Prefer the negotiated subprotocol attribute set by the websockets server.
        # Fallback to the Sec-WebSocket-Protocol header if the negotiated value
        # is not available (some transports/providers put a comma-separated
        # list in the header).
        ws_subprotocol = getattr(cp_ws, "subprotocol", None)
        if not ws_subprotocol:
            header = cp_ws.request.headers.get("Sec-WebSocket-Protocol")
            if header:
                # Use the first token when multiple subprotocols are listed.
                ws_subprotocol = header.split(",")[0].strip()

        if not ws_subprotocol:
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

        csms_uri = join_websocket_url(self._csms_url, charge_point_id)
        connect_kwargs = {
            "subprotocols": [ws_subprotocol],
            "additional_headers": extra_headers,
        }
        if self._csms_ssl_context is not None:
            connect_kwargs["ssl"] = self._csms_ssl_context

        self._logger.info(
            "Connecting charge point %s to upstream CSMS at %s",
            charge_point_id,
            csms_uri,
        )
        try:
            async with websockets.connect(
                csms_uri,
                **connect_kwargs,
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
                    # Exit as soon as either relay direction closes so the other
                    # is not left waiting on a dead connection.  Using
                    # FIRST_COMPLETED means a CP disconnect during a reload
                    # immediately cancels the CSMS-side reader instead of
                    # blocking until the CSMS heartbeat timeout fires.
                    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
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
            self._logger.debug(
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
