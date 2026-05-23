"""Shared relay and snoop websocket server core used by HA and standalone entrypoints."""

from __future__ import annotations

import asyncio
import base64
import contextlib
from dataclasses import asdict
import json
import logging
import random
import ssl
import uuid
import weakref
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

    def __init__(self, snoop_queue: asyncio.Queue, cp_packet_cache: dict | None = None) -> None:
        """Bind a message queue to a fanout websocket endpoint.

        cp_packet_cache, if supplied, should be the OCPPRelay._cp_packet_cache dict.
        It is a two-level mapping: cp_id → {action → MessageData}.  When a new snoop
        client connects, every cached packet for every active CP is replayed immediately
        so the client does not miss events that occurred before it connected.
        """
        if snoop_queue is None:
            raise ValueError("snoop_queue must be set")
        self._logger = logging.getLogger(__name__)
        self._snoop_queue = snoop_queue
        self._cp_packet_cache: dict = cp_packet_cache if cp_packet_cache is not None else {}
        self._snoop_sockets: set = set()
        self._closing_sockets: weakref.WeakSet = weakref.WeakSet()
        self._closed_sockets: weakref.WeakSet = weakref.WeakSet()
        self._snoop_sockets_lock = asyncio.Lock()
        self._forward_task: asyncio.Task | None = None

    async def _drop_socket(self, ws, *, close: bool) -> None:
        """Remove a socket from fanout and close it at most once."""
        should_close = False
        async with self._snoop_sockets_lock:
            self._snoop_sockets.discard(ws)
            if (
                close
                and ws not in self._closing_sockets
                and ws not in self._closed_sockets
            ):
                self._closing_sockets.add(ws)
                should_close = True

        if not should_close:
            return

        try:
            await _close_ws(ws)
        finally:
            async with self._snoop_sockets_lock:
                self._closing_sockets.discard(ws)
                self._closed_sockets.add(ws)

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
        async with self._snoop_sockets_lock:
            sockets = list(self._snoop_sockets)
        for ws in sockets:
            await self._drop_socket(ws, close=True)

    async def _forward_messages(self) -> None:
        """Consume relay events and broadcast them to all snoop clients."""
        while True:
            msg = await self._snoop_queue.get()
            try:
                msg_json = json.dumps(asdict(msg))
                # Iterate over a snapshot so disconnected sockets can be removed safely.
                async with self._snoop_sockets_lock:
                    sockets = list(self._snoop_sockets)
                for ws in sockets:
                    try:
                        await ws.send(msg_json)
                    except Exception as err:  # noqa: BLE001
                        self._logger.warning("Error sending to snoop client: %s", err)
                        await self._drop_socket(ws, close=True)
            finally:
                self._snoop_queue.task_done()

    async def _on_connect(self, ws) -> None:
        """Track one snoop client connection until it disconnects."""
        self._logger.info("Snoop client connected")
        async with self._snoop_sockets_lock:
            self._snoop_sockets.add(ws)
            # Defensive cleanup when object ids are re-used in tests.
            self._closed_sockets.discard(ws)
        # Replay all cached packets for active CPs so the client does not miss
        # events that occurred before it connected.
        try:
            for per_cp in list(self._cp_packet_cache.values()):
                for msg in list(per_cp.values()):
                    await ws.send(json.dumps(asdict(msg)))
        except Exception:  # noqa: BLE001
            pass
        try:
            while True:
                await ws.recv()
        except websockets.exceptions.ConnectionClosed:
            # Covers ConnectionClosedOK and ConnectionClosedError — both are subclasses.
            self._logger.info("Snoop connection closed")
        finally:
            await self._drop_socket(ws, close=True)


class OCPPRelay:
    """WebSocket relay that forwards messages between CP and CSMS."""

    def __init__(
        self,
        csms_url: str,
        csms_id: str | None = None,
        csms_pass: str | None = None,
        snoop_queue: asyncio.Queue | None = None,
        csms_ssl_context: ssl.SSLContext | None = None,
        boot_trigger_deadline: tuple[float, float] = (25.0, 35.0),
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
            boot_trigger_deadline: ``(min, max)`` seconds to wait for a
                spontaneous BootNotification before sending
                TriggerMessage(BootNotification).  A compliant CP sends one
                immediately; the deadline only matters for non-compliant CPs.
                Randomised to avoid thundering-herd bursts on reconnect.
        """
        if not csms_url:
            raise ValueError("csms_url must not be empty")
        self._logger = logging.getLogger(__name__)
        self._csms_url = csms_url
        self._csms_id = csms_id
        self._csms_pass = csms_pass
        self._snoop_queue = snoop_queue
        self._csms_ssl_context = csms_ssl_context
        self._boot_trigger_deadline = boot_trigger_deadline
        # Per-CP state for TriggerMessage-based sensor initialization.
        # pending_ids: TriggerMessage unique-ids sent by relay, not yet acknowledged.
        # awaiting: True after Accepted, until the next MeterValues is captured.
        self._trigger_state: dict[str, dict] = {}
        self._background_tasks: set[asyncio.Task] = set()
        # Per-CP packet cache: cp_id → {action → MessageData snoop event}.
        # Cleared per-CP on disconnect; replayed to snoop clients that connect mid-session.
        self._cp_packet_cache: dict[str, dict[str, MessageData]] = {}

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

    async def _send_boot_notification_trigger(self, cp_ws, cp_id: str) -> None:
        """Send TriggerMessage(BootNotification) if the CP does not send one spontaneously.

        A compliant CP sends BootNotification immediately after connecting; the relay
        caches it as it passes through and no trigger is needed.  Non-compliant CPs that
        skip the spontaneous BootNotification are handled by waiting up to a randomised
        deadline, then triggering one explicitly.
        """
        # Ensure the state entry exists so _process_cp_to_cpms_frame can handle
        # the eventual CALLRESULT even before any other trigger fires.
        self._trigger_state.setdefault(
            cp_id, {"pending_ids": set(), "awaiting": False, "triggered": False, "boot_trigger_ids": set()}
        )

        # Poll until a spontaneous BootNotification is cached, the CP disconnects,
        # or the deadline expires.  Sleep in ≤0.5 s slices so the loop wakes promptly
        # at the deadline regardless of its value.
        deadline = random.uniform(*self._boot_trigger_deadline)
        elapsed = 0.0
        while elapsed < deadline:
            sleep_for = min(0.5, deadline - elapsed)
            await asyncio.sleep(sleep_for)
            elapsed += sleep_for
            if cp_id not in self._trigger_state:
                return  # CP disconnected while waiting
            if self._cp_packet_cache.get(cp_id, {}).get("BootNotification") is not None:
                self._logger.debug(
                    "BootNotification already cached for CP %s; skipping trigger", cp_id
                )
                return

        # Deadline expired with no cached BootNotification — send the trigger.
        if cp_id not in self._trigger_state:
            return  # CP disconnected during final check
        state = self._trigger_state[cp_id]
        msg_id = f"relay-boot-{uuid.uuid4().hex}"
        state["boot_trigger_ids"].add(msg_id)
        payload = json.dumps([2, msg_id, "TriggerMessage", {"requestedMessage": "BootNotification"}])
        try:
            await cp_ws.send(payload)
            self._logger.debug("Sent TriggerMessage(BootNotification) to CP %s (id=%s)", cp_id, msg_id)
        except Exception as err:  # noqa: BLE001
            state["boot_trigger_ids"].discard(msg_id)
            self._logger.warning("Failed to send TriggerMessage(BootNotification) to CP %s: %s", cp_id, err)

    async def _send_trigger_message(self, cp_ws, cp_id: str) -> None:
        """Send a TriggerMessage for MeterValues to the CP to initialize sensors."""
        msg_id = f"relay-trigger-{uuid.uuid4().hex}"
        state = self._trigger_state.setdefault(cp_id, {"pending_ids": set(), "awaiting": False, "triggered": False, "boot_trigger_ids": set()})
        state["pending_ids"].add(msg_id)
        payload = json.dumps([2, msg_id, "TriggerMessage", {"requestedMessage": "MeterValues"}])
        try:
            await cp_ws.send(payload)
            self._logger.debug("Sent TriggerMessage to CP %s (id=%s)", cp_id, msg_id)
        except Exception as err:  # noqa: BLE001
            state["pending_ids"].discard(msg_id)
            self._logger.warning("Failed to send TriggerMessage to CP %s: %s", cp_id, err)

    def _process_cp_to_cpms_frame(self, json_message: list, cp_id: str, cp_ws) -> tuple[bool, bool]:
        """Observe every CP→CSMS frame to drive sensor initialization via TriggerMessage.

        The relay reads this stream (rather than injecting a separate listener) because
        it is the only place where all CP-originated frames pass in order, making it safe
        to correlate requests and responses without races or extra buffering.

        Returns (should_forward, should_snoop).
        """
        msg_type = json_message[0]
        msg_id = json_message[1]
        state = self._trigger_state.get(cp_id)

        # StatusNotification is a more reliable trigger point than BootNotification:
        # the CP has completed its boot sequence and reported connector state, so it
        # is ready to respond to TriggerMessage.  We fire only on the first
        # StatusNotification per connection; subsequent ones (for other connectors or
        # state changes) are ignored so we don't stack up redundant triggers.
        if msg_type == 2 and len(json_message) >= 3 and json_message[2] == "StatusNotification":
            state = self._trigger_state.setdefault(
                cp_id, {"pending_ids": set(), "awaiting": False, "triggered": False, "boot_trigger_ids": set()}
            )
            if not state["triggered"]:
                state["triggered"] = True
                task = asyncio.create_task(self._send_trigger_message(cp_ws, cp_id))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)

        # Drop CALLRESULT/CALLERROR for the relay-initiated TriggerMessage(BootNotification).
        # The CPMS never issued this request so it must not see the response.
        if msg_type in (3, 4) and state is not None and msg_id in state.get("boot_trigger_ids", set()):
            state["boot_trigger_ids"].discard(msg_id)
            self._logger.debug("Dropped boot TriggerMessage response from CP %s", cp_id)
            return False, False

        # The relay sends TriggerMessage under its own message IDs, which the CPMS has
        # never seen.  When the CP replies with a CallResult/CallError [3/4, id, …] for
        # one of those IDs, forwarding it to the CPMS would confuse it (a response to a
        # request it never issued).  Drop unconditionally; if CALLRESULT status is
        # Accepted we arm the awaiting flag so the next MeterValues can be captured.
        if msg_type in (3, 4) and state is not None and msg_id in state["pending_ids"]:
            state["pending_ids"].discard(msg_id)
            result = json_message[2] if len(json_message) >= 3 else {}
            if isinstance(result, dict) and result.get("status") == "Accepted":
                state["awaiting"] = True
                self._logger.debug(
                    "TriggerMessage accepted by CP %s; will capture next MeterValues", cp_id
                )
            else:
                self._logger.debug(
                    "TriggerMessage not accepted by CP %s (result=%r)", cp_id, result
                )
            return False, False

        # The CP sends MeterValues in response to TriggerMessage without any link back
        # to the trigger's message ID, so we can only identify it by position: it is the
        # next MeterValues after an Accepted response.  Forward it normally so the CSMS
        # can issue the required CALLRESULT; dropping it would stall the CP (per OCPP
        # §3.1, a CP must not send further messages until it receives a CALLRESULT).
        if (
            msg_type == 2
            and len(json_message) >= 3
            and state is not None
            and state.get("awaiting")
            and json_message[2] == "MeterValues"
        ):
            state["awaiting"] = False
            self._logger.debug(
                "Captured triggered MeterValues from CP %s; forwarding and snooping", cp_id
            )
            return True, True

        return True, True

    async def _on_connect(self, cp_ws) -> None:
        """Bridge one charge point connection to the configured CSMS peer."""
        # The CP identifier is encoded in the websocket URL path.
        charge_point_id = cp_ws.request.path.strip("/")
        if not charge_point_id:
            self._logger.error(
                "Charge point connected with empty path; closing connection (code=4001)."
            )
            await cp_ws.close(code=4001, reason="missing charge point id")
            return

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
            await cp_ws.close(code=4001, reason="missing OCPP sub-protocol")
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
                # Trigger BootNotification immediately so _cp_packet_cache is populated
                # for any snoop client that connects after the CP.  The CALLRESULT will
                # be intercepted in _process_cp_to_cpms_frame and not forwarded to CSMS.
                boot_task = asyncio.create_task(
                    self._send_boot_notification_trigger(cp_ws, charge_point_id)
                )
                self._background_tasks.add(boot_task)
                boot_task.add_done_callback(self._background_tasks.discard)

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
                            trigger_cp_ws=cp_ws,
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
                    # Cancel the boot trigger task so it does not outlive this
                    # connection and cannot pollute a reconnecting CP's state.
                    if not boot_task.done():
                        boot_task.cancel()
                    # Ensure both relay loops and the boot task terminate.
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, boot_task, return_exceptions=True)

                    # Politely close both websockets if still open.
                    for sock in (cp_ws, csms_ws):
                        await _close_ws(sock)

            self._logger.info("Relay connection closed for charge point %s", charge_point_id)

        except Exception as err:  # noqa: BLE001
            self._logger.error(
                "Unexpected error in relay for charge point %s: %s", charge_point_id, err
            )
        finally:
            self._trigger_state.pop(charge_point_id, None)
            self._cp_packet_cache.pop(charge_point_id, None)
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
        trigger_cp_ws=None,
    ) -> None:
        """Forward frames one direction and optionally mirror them to snoop.

        When trigger_cp_ws is set (CP→CSMS direction), TriggerMessage injection
        and MeterValues interception are active for sensor initialization.
        """
        _warn_count = 0
        _WARN_LIMIT = 100
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
                if _warn_count < _WARN_LIMIT:
                    self._logger.warning(
                        "Non-JSON frame from %s for CP %s (dropping): %s", source_name, cp_id, err
                    )
                    _warn_count += 1
                continue

            if not isinstance(json_message, list) or len(json_message) < 3:
                if _warn_count < _WARN_LIMIT:
                    self._logger.warning(
                        "Malformed OCPP frame from %s for CP %s (dropping): %r",
                        source_name,
                        cp_id,
                        json_message,
                    )
                    _warn_count += 1
                continue

            should_forward = True
            should_snoop = True
            if trigger_cp_ws is not None:
                if (
                    json_message[0] == 2
                    and len(json_message) >= 3
                    and json_message[2] == "BootNotification"
                ):
                    self._cp_packet_cache.setdefault(cp_id, {})["BootNotification"] = MessageData(
                        event="Message",
                        sender="CP",
                        protocol=protocol,
                        cp_id=cp_id,
                        payload=json_message,
                    )
                should_forward, should_snoop = self._process_cp_to_cpms_frame(
                    json_message, cp_id, trigger_cp_ws
                )

            if should_forward:
                try:
                    await target_ws.send(message)
                except websockets.exceptions.ConnectionClosed:
                    self._logger.info(
                        "Connection closed on %s side while relaying for CP %s", target_name, cp_id
                    )
                    return

            msg_id = json_message[1]
            if should_forward:
                self._logger.debug(
                    "Relayed message from %s to %s (%s)", source_name, target_name, msg_id
                )

            if should_snoop:
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
