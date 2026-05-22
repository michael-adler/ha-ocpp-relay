#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Forward OCPP traffic observed on the relay snoop port to a secondary CSMS.

Overview
--------
This tool connects to the relay snoop port and mirrors each active charge
point's (CP) OCPP traffic to an independent secondary CSMS.  It is read-only
on the snoop side — it never injects traffic into the primary relay — and opens
its own WebSocket connection to the secondary CSMS for each tracked CP.

The secondary CSMS URL is given as a base, and the CP ID is appended as a path
component (e.g. ``wss://secondary.example.com/ocpp/CP-001``), matching the
convention used by most OCPP servers.

Protocol management
-------------------
The tool waits until the OCPP subprotocol negotiated between the CP and the
primary relay is known before opening a connection to the secondary CSMS.
Normally this comes from the ``Connection`` event on the snoop stream.  If the
tool starts while a CP is already connected (and the ``Connection`` event has
therefore been missed), the protocol is learned lazily from the ``protocol``
field carried by the first ``Message`` event for that CP.

The same subprotocol string negotiated by the CP (e.g. ``ocpp1.6`` or
``OCPP2.0.1``) is offered to the secondary CSMS.  If the CP later reconnects
with a different protocol, the secondary CSMS connection is dropped and
re-established with the new protocol.

Traffic forwarding
------------------
Only CALL frames (message type 2) originating from the CP are forwarded.
CALLRESULT and CALLERROR frames from the primary CSMS are not forwarded; the
secondary CSMS is expected to generate its own responses to the CALLs it
receives.

Heartbeat CALLs from the CP are dropped.  Instead, synthetic Heartbeat CALLs
are generated locally at the interval specified by the secondary CSMS in its
``BootNotificationResponse``.  This is necessary because the heartbeat interval
is set by the CSMS, not the CP — different CSMSs return different values, and
the secondary CSMS's preferred interval may differ from the one the primary CSMS
told the CP to use.  Before the first ``BootNotificationResponse`` is seen, a
default interval of 5 minutes is used.  The secondary CSMS's CALLRESULT
responses to these synthetic heartbeats are discarded.

Responses to secondary CSMS CALLs
----------------------------------
The secondary CSMS may send CALL frames of its own (e.g. ``GetConfiguration``,
``TriggerMessage``).  Because this tool is not a real CP, it cannot honor most
of them.  The strategy is to leave most CALLs unanswered rather than return an
error: the CP will eventually send the requested information to the primary CSMS
as part of normal operation, and the secondary CSMS should receive it at that
point via the forwarded CP traffic.

The one exception is ``Heartbeat``: if the secondary CSMS sends a Heartbeat
CALL, a synthetic CALLRESULT with the current UTC time is returned so that the
CSMS does not time out waiting for a response.

CALLRESULT and CALLERROR frames sent by the secondary CSMS are discarded,
except for the ``BootNotificationResponse`` which is inspected to extract the
``heartbeatInterval`` field.

Connection lifecycle
--------------------
A CSMS connection is opened for a CP when:

- a ``Connection`` event arrives with a known protocol, or
- the protocol is learned lazily from the first CP message.

A CSMS connection is closed when:

- the CP sends a ``Disconnection`` event,
- the snoop WebSocket itself disconnects (all CSMS connections are dropped and
  rebuilt from scratch once the snoop reconnects and new ``Connection`` or
  ``Message`` events arrive),
- the CP reconnects with a changed protocol (the old connection is replaced).

If the secondary CSMS drops the connection unexpectedly, the tool reconnects
with exponential backoff starting at 1 s and capped at 30 s.
"""

import argparse
import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field, fields as dc_fields
from datetime import datetime, timezone
from typing import Optional

import websockets
import websockets.exceptions

from relay_server.cli.common import apply_yaml_section_defaults, configure_logging
from relay_server.common.types import MessageData


_MSG_CALL = 2
_MSG_CALLRESULT = 3
_MSG_CALLERROR = 4

_CSMS_RECONNECT_MIN = 1.0     # initial reconnect delay in seconds
_CSMS_RECONNECT_MAX = 30.0    # maximum reconnect delay in seconds
_HEARTBEAT_DEFAULT = 300.0    # heartbeat interval until BootNotificationResponse arrives


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class _CPState:
    cp_id: str
    protocol: Optional[str] = None
    send_queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=200))
    csms_task: Optional[asyncio.Task] = None
    heartbeat_interval: float = _HEARTBEAT_DEFAULT
    boot_msg_id: Optional[str] = None  # msg_id of the forwarded BootNotification CALL


class _Forwarder:
    """Connects to the relay snoop port and mirrors CP traffic to a secondary CSMS."""

    def __init__(self, csms_url: str) -> None:
        self._csms_base = csms_url.rstrip("/")
        self._cp: dict[str, _CPState] = {}
        self._log = logging.getLogger(__name__)

    async def run(self, snoop_uri: str, exit_on_disconnect: bool = False) -> None:
        self._log.info("Connecting to snoop at %s", snoop_uri)
        async for ws in websockets.connect(snoop_uri):
            self._log.info("Snoop connected")
            try:
                async for raw in ws:
                    await self._on_raw(raw)
            except websockets.exceptions.ConnectionClosed as err:
                code = err.rcvd.code if err.rcvd else None
                reason = err.rcvd.reason if err.rcvd else ""
                self._log.warning("Snoop closed (%s: %s)", code, reason)
            self._log.info("Dropping all CSMS connections")
            await self._drop_all()
            if exit_on_disconnect:
                return

    # -- snoop ingestion -----------------------------------------------

    async def _on_raw(self, raw: str | bytes) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as err:
            self._log.error("JSON decode error: %s", err)
            return
        known = {f.name for f in dc_fields(MessageData)}
        filtered = {k: v for k, v in data.items() if k in known}
        try:
            msg = MessageData(**filtered)
        except TypeError as err:
            self._log.error("MessageData error: %s", err)
            return
        await self._dispatch(msg)

    async def _dispatch(self, msg: MessageData) -> None:
        if msg.cp_id is None:
            return
        cp_id = msg.cp_id
        if msg.event == "Connection":
            await self._on_cp_connect(cp_id, msg.protocol)
        elif msg.event == "Disconnection":
            await self._on_cp_disconnect(cp_id)
        elif msg.event == "Message" and msg.sender == "CP":
            await self._on_cp_message(cp_id, msg)

    # -- CP lifecycle --------------------------------------------------

    async def _on_cp_connect(self, cp_id: str, protocol: str | None) -> None:
        state = self._cp.get(cp_id)
        if state is None:
            state = _CPState(cp_id=cp_id)
            self._cp[cp_id] = state

        if protocol and protocol != state.protocol:
            if state.protocol is not None:
                self._log.info(
                    "CP %s protocol changed %s → %s, reconnecting CSMS",
                    cp_id, state.protocol, protocol,
                )
            state.protocol = protocol
            await self._restart_csms(cp_id)
        elif protocol and (state.csms_task is None or state.csms_task.done()):
            await self._restart_csms(cp_id)

    async def _on_cp_disconnect(self, cp_id: str) -> None:
        self._log.info("CP %s disconnected", cp_id)
        state = self._cp.pop(cp_id, None)
        if state:
            await self._cancel_task(state.csms_task)

    async def _on_cp_message(self, cp_id: str, msg: MessageData) -> None:
        payload = msg.payload
        if not isinstance(payload, list) or len(payload) < 3 or payload[0] != _MSG_CALL:
            return

        action = payload[2]

        # If the Connection event was missed, learn the protocol from the message.
        state = self._cp.get(cp_id)
        if state is None:
            state = _CPState(cp_id=cp_id)
            self._cp[cp_id] = state
        if msg.protocol and not state.protocol:
            self._log.info("CP %s: learned protocol %s from message", cp_id, msg.protocol)
            state.protocol = msg.protocol
            await self._restart_csms(cp_id)

        # Heartbeats requested by the CSMS will be synthesized below, so the CP's
        # own heartbeats are dropped.
        if action == "Heartbeat":
            self._log.debug("Dropping Heartbeat from %s", cp_id)
            return

        if state.csms_task and not state.csms_task.done():
            try:
                state.send_queue.put_nowait(json.dumps(payload))
                self._log.debug("Queued %s from %s", action, cp_id)
                if action == "BootNotification":
                    state.boot_msg_id = payload[1]
            except asyncio.QueueFull:
                self._log.warning("Queue full for %s, dropping %s", cp_id, action)

    # -- CSMS connection management ------------------------------------

    async def _restart_csms(self, cp_id: str) -> None:
        state = self._cp.get(cp_id)
        if state is None:
            return
        await self._cancel_task(state.csms_task)
        while not state.send_queue.empty():
            state.send_queue.get_nowait()
        state.boot_msg_id = None
        state.heartbeat_interval = _HEARTBEAT_DEFAULT
        state.csms_task = asyncio.create_task(
            self._csms_loop(cp_id), name=f"csms-{cp_id}"
        )

    async def _drop_all(self) -> None:
        states = list(self._cp.values())
        self._cp.clear()
        for state in states:
            await self._cancel_task(state.csms_task)

    @staticmethod
    async def _cancel_task(task: Optional[asyncio.Task]) -> None:
        if task and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    # -- CSMS I/O ------------------------------------------------------

    async def _csms_loop(self, cp_id: str) -> None:
        """Maintain a CSMS connection for cp_id, reconnecting with exponential backoff."""
        delay = _CSMS_RECONNECT_MIN
        while self._cp.get(cp_id) is not None:
            state = self._cp.get(cp_id)
            if state is None or not state.protocol:
                return
            url = f"{self._csms_base}/{cp_id}"
            proto = state.protocol
            self._log.info("CSMS connect: %s → %s (%s)", cp_id, url, proto)
            try:
                t0 = asyncio.get_running_loop().time()
                await self._csms_connect_once(cp_id, url, proto)
                if asyncio.get_running_loop().time() - t0 >= _CSMS_RECONNECT_MIN:
                    delay = _CSMS_RECONNECT_MIN  # reset only after a lasting connection
            except asyncio.CancelledError:
                raise
            except Exception as err:
                self._log.error("CSMS error for %s: %s", cp_id, err)

            if self._cp.get(cp_id) is None:
                return
            self._log.info(
                "CSMS disconnected for %s, reconnecting in %.0fs",
                cp_id, delay,
            )
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise
            delay = min(delay * 2, _CSMS_RECONNECT_MAX)

    async def _csms_connect_once(self, cp_id: str, url: str, proto: str) -> None:
        """Run one CSMS session: send CP traffic, synthesize heartbeats, respond to CSMS CALLs."""
        async with websockets.connect(url, subprotocols=[proto]) as ws:
            self._log.info("CSMS connected for %s", cp_id)
            send_task = asyncio.create_task(
                self._csms_send_loop(cp_id, ws), name=f"send-{cp_id}"
            )
            recv_task = asyncio.create_task(
                self._csms_recv_loop(cp_id, ws), name=f"recv-{cp_id}"
            )
            hb_task = asyncio.create_task(
                self._heartbeat_loop(cp_id), name=f"hb-{cp_id}"
            )
            try:
                await asyncio.wait(
                    {send_task, recv_task, hb_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for t in (send_task, recv_task, hb_task):
                    if not t.done():
                        t.cancel()
                        try:
                            await t
                        except (asyncio.CancelledError, Exception):
                            pass
        self._log.info("CSMS session ended for %s", cp_id)

    async def _heartbeat_loop(self, cp_id: str) -> None:
        """Send synthetic Heartbeat CALLs at the interval set by the secondary CSMS."""
        while True:
            state = self._cp.get(cp_id)
            if state is None:
                return
            await asyncio.sleep(state.heartbeat_interval)
            state = self._cp.get(cp_id)
            if state is None:
                return
            hb = [_MSG_CALL, str(uuid.uuid4()), "Heartbeat", {}]
            try:
                state.send_queue.put_nowait(json.dumps(hb))
                self._log.debug("Synthesized Heartbeat for %s", cp_id)
            except asyncio.QueueFull:
                self._log.warning("Queue full for %s, dropping synthetic Heartbeat", cp_id)

    async def _csms_send_loop(self, cp_id: str, ws) -> None:
        """Drain the per-CP queue and send each frame to the CSMS."""
        state = self._cp.get(cp_id)
        if state is None:
            return
        while True:
            raw = await state.send_queue.get()
            await ws.send(raw)

    async def _csms_recv_loop(self, cp_id: str, ws) -> None:
        """Read frames from the CSMS, responding to CALLs and discarding the rest."""
        async for raw in ws:
            try:
                frame = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(frame, list) or len(frame) < 2:
                continue
            msg_type = frame[0]
            msg_id = frame[1]
            if msg_type == _MSG_CALL:
                action = frame[2] if len(frame) > 2 else ""
                reply = self._synthesize_response(action, msg_id)
                if reply is not None:
                    self._log.debug("CSMS CALL %s for %s, synthesizing response", action, cp_id)
                    try:
                        await ws.send(json.dumps(reply))
                    except Exception as err:
                        self._log.error("CSMS reply error for %s: %s", cp_id, err)
                        return
                else:
                    self._log.debug("CSMS CALL %s for %s, no response (waiting for CP)", action, cp_id)
            elif msg_type == _MSG_CALLRESULT:
                state = self._cp.get(cp_id)
                if state and state.boot_msg_id == msg_id and len(frame) >= 3 and isinstance(frame[2], dict):
                    interval = frame[2].get("interval")
                    if interval is not None:
                        state.heartbeat_interval = float(interval)
                        self._log.info(
                            "CP %s heartbeat interval set to %.0fs from BootNotificationResponse",
                            cp_id, state.heartbeat_interval,
                        )
                    state.boot_msg_id = None
                else:
                    self._log.debug("CSMS CALLRESULT for %s (discarded)", cp_id)
            elif msg_type == _MSG_CALLERROR:
                self._log.debug("CSMS CALLERROR for %s (discarded)", cp_id)

    @staticmethod
    def _synthesize_response(action: str, msg_id: str) -> list | None:
        """Build a CALLRESULT for Heartbeat; return None to leave other CALLs unanswered."""
        if action == "Heartbeat":
            return [_MSG_CALLRESULT, msg_id, {"currentTime": _utcnow()}]
        return None


# -- CLI -------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Forward OCPP traffic from the relay snoop port to a secondary CSMS.",
        epilog="""
The charge point ID is appended to CSMS_URL as a path component, matching
the convention used by most OCPP servers.  Example:

  ocpp-snoop2another-csms wss://csms.example.com/ocpp

will open wss://csms.example.com/ocpp/<cp_id> for each active charge point.
""",
    )

    parser.add_argument(
        "csms_url",
        type=str,
        help="Base WebSocket URL of the secondary CSMS.",
    )

    parser.add_argument(
        "--snoop-socket",
        type=str,
        default="ws://localhost:8501/",
        help="URL of the OCPP relay's snoop port (default: %(default)s).",
    )

    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to YAML config file. Values in a 'snoop2another-csms' section will be used as defaults.",
    )

    parser.add_argument(
        "--syslog",
        action="store_true",
        help="Write logs to syslog instead of stdout.",
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true", help="Verbose output.")
    group.add_argument("-q", "--quiet", action="store_true", help="Reduce output.")

    parser.add_argument(
        "--exit-on-snoop-disconnect",
        action="store_true",
        help="Exit if snoop connection closes (mostly for test scripts).",
    )

    apply_yaml_section_defaults(parser, section="snoop2another-csms")

    return parser.parse_args()


async def core(args):
    forwarder = _Forwarder(csms_url=args.csms_url)
    await forwarder.run(
        snoop_uri=args.snoop_socket,
        exit_on_disconnect=args.exit_on_snoop_disconnect,
    )


def main():
    args = parse_args()
    configure_logging(
        app_name="ocpp-snoop2another-csms",
        verbose=args.verbose,
        quiet=args.quiet,
        use_syslog=args.syslog,
    )
    try:
        asyncio.run(core(args))
    except KeyboardInterrupt:
        print("Exiting...")


if __name__ == "__main__":
    main()
