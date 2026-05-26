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

Each ``BootNotification`` CALL forwarded from the CP is also cached locally.
The cache is cleared when the CP disconnects or the secondary CSMS connection
is restarted (e.g. on a protocol change).

Responses to secondary CSMS CALLs
----------------------------------
The secondary CSMS may send CALL frames of its own (e.g. ``GetConfiguration``,
``TriggerMessage``).  Because this tool is not a real CP, it cannot honor most
of them.  The strategy is to leave most CALLs unanswered rather than return an
error: the CP will eventually send the requested information to the primary CSMS
as part of normal operation, and the secondary CSMS should receive it at that
point via the forwarded CP traffic.

There are two exceptions:

``Heartbeat``: a synthetic CALLRESULT with the current UTC time is returned
immediately so that the CSMS does not time out waiting for a response.

``TriggerMessage(requestedMessage=BootNotification)``: if a cached
``BootNotification`` is available, the request is accepted (``CALLRESULT``
with ``status=Accepted``) and the cached frame is immediately re-sent as a
new CALL with a fresh message ID.  This allows a secondary CSMS that joined
mid-session to learn the CP's identity without waiting for the CP to reboot.
If no cached ``BootNotification`` is available the request is left unanswered.

``TriggerMessage(requestedMessage=StatusNotification)``: the most recent
``StatusNotification`` seen from the CP is cached per connector ID.  If the
request includes ``connectorId=0`` (or omits ``connectorId``), all cached
connector statuses are re-sent as individual CALLs with fresh message IDs and
the request is accepted.  If ``connectorId`` is greater than 0, only the
cached status for that connector is re-sent.  If no cached status is available
the request is left unanswered.  ``StatusNotification`` CALLs from the CP
continue to be forwarded normally in addition to being cached.

CALLRESULT and CALLERROR frames sent by the secondary CSMS are discarded,
except for the ``BootNotificationResponse`` (identified by message ID) which
is inspected to extract the ``heartbeatInterval`` field.  This also covers
the response to a triggered ``BootNotification``.

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


def _describe_frame(frame) -> str:
    """Return a short human-readable label for an OCPP JSON frame."""
    if not isinstance(frame, list) or len(frame) < 2:
        return "(malformed)"
    msg_type = frame[0]
    raw_id = str(frame[1])
    short_id = raw_id[:8] if len(raw_id) > 8 else raw_id
    if msg_type == _MSG_CALL and len(frame) >= 3:
        return f"CALL {frame[2]} id={short_id}"
    if msg_type == _MSG_CALLRESULT:
        return f"CALLRESULT id={short_id}"
    if msg_type == _MSG_CALLERROR:
        code = frame[2] if len(frame) > 2 else "?"
        return f"CALLERROR {code} id={short_id}"
    return f"type={msg_type} id={short_id}"

def _call_detail(action: str, payload) -> str:
    """Return a short summary of key CALL payload fields for verbose logging."""
    if not isinstance(payload, dict):
        return ""

    if action == "GetConfiguration":  # OCPP 1.6
        keys = payload.get("key")
        return f"key={keys}" if keys else "key=* (all)"

    if action == "ChangeConfiguration":  # OCPP 1.6
        return f"key={payload.get('key')!r} value={payload.get('value')!r}"

    if action == "GetVariables":  # OCPP 2.0.1
        data = payload.get("getVariableData", [])
        items = [
            f"{d.get('component', {}).get('name', '?')}/{d.get('variable', {}).get('name', '?')}"
            for d in data[:5]
        ]
        suffix = f" (+{len(data) - 5} more)" if len(data) > 5 else ""
        return f"variables=[{', '.join(items)}{suffix}]"

    if action == "SetVariables":  # OCPP 2.0.1
        data = payload.get("setVariableData", [])
        items = [
            f"{d.get('component', {}).get('name', '?')}/{d.get('variable', {}).get('name', '?')}"
            f"={d.get('attributeValue', '?')!r}"
            for d in data[:5]
        ]
        suffix = f" (+{len(data) - 5} more)" if len(data) > 5 else ""
        return f"variables=[{', '.join(items)}{suffix}]"

    if action == "GetBaseReport":  # OCPP 2.0.1
        return f"reportBase={payload.get('reportBase')!r} requestId={payload.get('requestId')}"

    if action == "TriggerMessage":
        parts = [f"requestedMessage={payload.get('requestedMessage')!r}"]
        if "connectorId" in payload:
            parts.append(f"connector={payload['connectorId']}")
        if "evse" in payload:
            parts.append(f"evse={payload['evse']}")
        return " ".join(parts)

    if action == "Reset":
        parts = [f"type={payload.get('type')!r}"]
        if "evseId" in payload:
            parts.append(f"evse={payload['evseId']}")
        return " ".join(parts)

    if action == "RemoteStartTransaction":  # OCPP 1.6
        parts = []
        if "connectorId" in payload:
            parts.append(f"connector={payload['connectorId']}")
        if "idTag" in payload:
            parts.append(f"idTag={payload['idTag']!r}")
        return " ".join(parts)

    if action == "RequestStartTransaction":  # OCPP 2.0.1
        parts = []
        if "evseId" in payload:
            parts.append(f"evse={payload['evseId']}")
        tok = payload.get("idToken", {})
        if isinstance(tok, dict) and tok.get("idToken"):
            parts.append(f"idToken={tok['idToken']!r} ({tok.get('type')})")
        return " ".join(parts)

    if action in ("RemoteStopTransaction", "RequestStopTransaction"):
        return f"transactionId={payload.get('transactionId')}"

    if action == "UnlockConnector":  # OCPP 1.6
        return f"connector={payload.get('connectorId')}"

    if action == "ChangeAvailability":
        parts = []
        if "connectorId" in payload:  # OCPP 1.6
            parts.append(f"connector={payload['connectorId']}")
            parts.append(f"type={payload.get('type')!r}")
        else:  # OCPP 2.0.1
            if "evse" in payload:
                parts.append(f"evse={payload['evse']}")
            if "operationalStatus" in payload:
                parts.append(f"status={payload['operationalStatus']!r}")
        return " ".join(parts)

    if action == "SetChargingProfile":
        parts = []
        if "connectorId" in payload:
            parts.append(f"connector={payload['connectorId']}")
        if "evseId" in payload:
            parts.append(f"evse={payload['evseId']}")
        profile = payload.get("csChargingProfiles") or payload.get("chargingProfile") or {}
        if isinstance(profile, dict) and "chargingProfilePurpose" in profile:
            parts.append(f"purpose={profile['chargingProfilePurpose']!r}")
        return " ".join(parts)

    if action == "ReserveNow":
        parts = []
        if "connectorId" in payload:
            parts.append(f"connector={payload['connectorId']}")
        if "idTag" in payload:
            parts.append(f"idTag={payload['idTag']!r}")
        if "reservationId" in payload:
            parts.append(f"reservationId={payload['reservationId']}")
        return " ".join(parts)

    if action == "CancelReservation":
        return f"reservationId={payload.get('reservationId')}"

    if action == "SendLocalList":
        lst = payload.get("localAuthorizationList", [])
        return (
            f"version={payload.get('listVersion')} "
            f"type={payload.get('updateType')!r} "
            f"entries={len(lst)}"
        )

    if action == "DataTransfer":
        parts = [f"vendor={payload.get('vendorId')!r}"]
        if "messageId" in payload:
            parts.append(f"messageId={payload['messageId']!r}")
        return " ".join(parts)

    if action == "GetDiagnostics":
        return f"location={payload.get('location')!r}"

    if action == "UpdateFirmware":
        loc = payload.get("location") or (payload.get("firmware") or {}).get("location")
        if loc:
            return f"location={loc!r}"
        if "requestId" in payload:
            return f"requestId={payload['requestId']}"
        return ""

    if action == "GetLog":
        return f"logType={payload.get('logType')!r} requestId={payload.get('requestId')}"

    return ""


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
    boot_msg_id: Optional[str] = None
    packet_cache: dict[str, list] = field(default_factory=dict)  # "BootNotification" or "StatusNotification:{id}" -> CALL frame


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
            self._log.debug("Snoop Connection cp=%s proto=%s", cp_id, msg.protocol)
            await self._on_cp_connect(cp_id, msg.protocol)
        elif msg.event == "Disconnection":
            self._log.debug("Snoop Disconnection cp=%s", cp_id)
            await self._on_cp_disconnect(cp_id)
        elif msg.event == "Message" and msg.sender == "CP":
            self._log.debug("Snoop Message cp=%s: %s", cp_id, _describe_frame(msg.payload))
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
                self._log.debug("Queued %s id=%s from %s", action, payload[1], cp_id)
                if action == "BootNotification":
                    state.boot_msg_id = payload[1]
                    state.packet_cache["BootNotification"] = payload
                elif action == "StatusNotification" and len(payload) > 3 and isinstance(payload[3], dict):
                    sn = payload[3]
                    if state.protocol and state.protocol.startswith("OCPP2"):
                        connector_id = sn.get("evseId", 0)
                    else:
                        connector_id = sn.get("connectorId", 0)
                    state.packet_cache[f"StatusNotification:{connector_id}"] = payload
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
        state.packet_cache = {}
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
            try:
                self._log.debug("→ secondary [%s]: %s", cp_id, _describe_frame(json.loads(raw)))
            except Exception:
                pass
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
            if msg_type == _MSG_CALL and len(frame) > 2:
                detail = _call_detail(frame[2], frame[3] if len(frame) > 3 else {})
                suffix = f" — {detail}" if detail else ""
                self._log.debug("← secondary [%s]: %s%s", cp_id, _describe_frame(frame), suffix)
            else:
                self._log.debug("← secondary [%s]: %s", cp_id, _describe_frame(frame))
            if msg_type == _MSG_CALL:
                action = frame[2] if len(frame) > 2 else ""
                if action == "TriggerMessage":
                    await self._handle_trigger_message(cp_id, msg_id, frame, ws)
                else:
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

    async def _handle_trigger_message(self, cp_id: str, msg_id: str, frame: list, ws) -> None:
        """Handle a TriggerMessage CALL from the secondary CSMS.

        TriggerMessage(BootNotification): if a cached BootNotification is available,
        accept the request and re-send it with a fresh message ID.

        TriggerMessage(StatusNotification): if cached statuses are available, accept
        the request and re-send each matching status with a fresh message ID.
        connectorId=0 (or absent) sends all known connector statuses.

        All other requestedMessage values are left unanswered.
        """
        payload = frame[3] if len(frame) > 3 else {}
        requested = payload.get("requestedMessage") if isinstance(payload, dict) else None

        if requested == "BootNotification":
            state = self._cp.get(cp_id)
            if state is None or state.packet_cache.get("BootNotification") is None:
                self._log.debug(
                    "CSMS TriggerMessage(BootNotification) for %s: no cached notification, leaving unanswered", cp_id
                )
                return
            try:
                await ws.send(json.dumps([_MSG_CALLRESULT, msg_id, {"status": "Accepted"}]))
            except Exception as err:
                self._log.error("CSMS reply error for %s: %s", cp_id, err)
                return
            new_id = str(uuid.uuid4())
            boot_frame = list(state.packet_cache["BootNotification"])
            boot_frame[1] = new_id
            try:
                state.send_queue.put_nowait(json.dumps(boot_frame))
                state.boot_msg_id = new_id
                self._log.debug("Queued triggered BootNotification for %s id=%s", cp_id, new_id)
            except asyncio.QueueFull:
                self._log.warning("Queue full for %s, dropping triggered BootNotification", cp_id)

        elif requested == "StatusNotification":
            state = self._cp.get(cp_id)
            if state is None:
                return
            if state.protocol and state.protocol.startswith("OCPP2"):
                evse = payload.get("evse") if isinstance(payload, dict) else None
                connector_id = evse.get("id", 0) if isinstance(evse, dict) else 0
            else:
                connector_id = payload.get("connectorId", 0) if isinstance(payload, dict) else 0
            if connector_id == 0:
                frames_to_send = [v for k, v in state.packet_cache.items() if k.startswith("StatusNotification:")]
            else:
                cached = state.packet_cache.get(f"StatusNotification:{connector_id}")
                frames_to_send = [cached] if cached is not None else []
            if not frames_to_send:
                self._log.debug(
                    "CSMS TriggerMessage(StatusNotification, connectorId=%s) for %s: no cached status, leaving unanswered",
                    connector_id, cp_id,
                )
                return
            try:
                await ws.send(json.dumps([_MSG_CALLRESULT, msg_id, {"status": "Accepted"}]))
            except Exception as err:
                self._log.error("CSMS reply error for %s: %s", cp_id, err)
                return
            for status_frame in frames_to_send:
                new_id = str(uuid.uuid4())
                new_frame = list(status_frame)
                new_frame[1] = new_id
                try:
                    state.send_queue.put_nowait(json.dumps(new_frame))
                    self._log.debug("Queued triggered StatusNotification for %s id=%s", cp_id, new_id)
                except asyncio.QueueFull:
                    self._log.warning("Queue full for %s, dropping triggered StatusNotification", cp_id)

        else:
            self._log.debug("CSMS CALL TriggerMessage(%s) for %s, no response (waiting for CP)", requested, cp_id)

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
