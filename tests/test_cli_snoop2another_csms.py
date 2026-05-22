"""Unit tests for relay_server.ocpp_snoop2another_csms._Forwarder."""

import asyncio
import json

import pytest

from relay_server.common.types import MessageData
from relay_server.ocpp_snoop2another_csms import _CPState, _Forwarder

pytestmark = pytest.mark.cli


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeWebSocket:
    """Async-iterable fake WebSocket that delivers pre-loaded frames and records sends."""

    def __init__(self, frames: list[list]):
        self._frames = [json.dumps(f) for f in frames]
        self.sent: list[list] = []

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._frames:
            return self._frames.pop(0)
        raise StopAsyncIteration

    async def send(self, data: str) -> None:
        self.sent.append(json.loads(data))


def _forwarder() -> _Forwarder:
    return _Forwarder("ws://csms.example.com/ocpp")


def _state_with_task(cp_id: str, protocol: str = "ocpp1.6") -> _CPState:
    """Return a _CPState whose csms_task is a live never-ending task."""
    state = _CPState(cp_id=cp_id, protocol=protocol)
    state.csms_task = asyncio.create_task(asyncio.sleep(9999))
    return state


async def _cancel(state: _CPState) -> None:
    if state.csms_task and not state.csms_task.done():
        state.csms_task.cancel()
        await asyncio.gather(state.csms_task, return_exceptions=True)


# ---------------------------------------------------------------------------
# _synthesize_response (static, no I/O)
# ---------------------------------------------------------------------------

def test_synthesize_response_heartbeat_returns_callresult():
    reply = _Forwarder._synthesize_response("Heartbeat", "hb-1")
    assert reply is not None
    assert reply[0] == 3           # CALLRESULT
    assert reply[1] == "hb-1"
    assert "currentTime" in reply[2]


def test_synthesize_response_other_action_returns_none():
    assert _Forwarder._synthesize_response("GetConfiguration", "gc-1") is None
    assert _Forwarder._synthesize_response("TriggerMessage", "tm-1") is None
    assert _Forwarder._synthesize_response("RemoteStartTransaction", "rst-1") is None


# ---------------------------------------------------------------------------
# CP message filtering
# ---------------------------------------------------------------------------

async def test_heartbeat_from_cp_is_not_queued():
    fw = _forwarder()
    state = _state_with_task("CP-01")
    fw._cp["CP-01"] = state

    msg = MessageData(
        event="Message", sender="CP", protocol="ocpp1.6", cp_id="CP-01",
        payload=[2, "hb-1", "Heartbeat", {}],
    )
    await fw._dispatch(msg)

    assert state.send_queue.empty(), "Heartbeat should be dropped, not queued"
    await _cancel(state)


async def test_status_notification_from_cp_is_queued():
    fw = _forwarder()
    payload = [2, "sn-1", "StatusNotification", {"connectorId": 1, "status": "Available"}]
    state = _state_with_task("CP-01")
    fw._cp["CP-01"] = state

    msg = MessageData(
        event="Message", sender="CP", protocol="ocpp1.6", cp_id="CP-01",
        payload=payload,
    )
    await fw._dispatch(msg)

    assert not state.send_queue.empty()
    assert json.loads(state.send_queue.get_nowait()) == payload
    await _cancel(state)


async def test_csms_message_from_primary_is_ignored():
    """Messages from the primary CSMS on the snoop stream must not be forwarded."""
    fw = _forwarder()
    state = _state_with_task("CP-01")
    fw._cp["CP-01"] = state

    msg = MessageData(
        event="Message", sender="CSMS", protocol="ocpp1.6", cp_id="CP-01",
        payload=[3, "hb-1", {"currentTime": "2026-01-01T00:00:00Z"}],
    )
    await fw._dispatch(msg)

    assert state.send_queue.empty()
    await _cancel(state)


# ---------------------------------------------------------------------------
# Lazy protocol learning
# ---------------------------------------------------------------------------

async def test_protocol_learned_from_first_cp_message():
    """When the Connection event is missed, protocol is inferred from the message field."""
    fw = _forwarder()

    restarted: list[str] = []

    async def fake_restart(cp_id: str) -> None:
        restarted.append(cp_id)

    fw._restart_csms = fake_restart  # type: ignore[method-assign]

    msg = MessageData(
        event="Message", sender="CP", protocol="ocpp1.6", cp_id="CP-01",
        payload=[2, "bn-1", "BootNotification", {"chargePointVendor": "Acme"}],
    )
    await fw._dispatch(msg)

    assert fw._cp["CP-01"].protocol == "ocpp1.6"
    assert "CP-01" in restarted


# ---------------------------------------------------------------------------
# CSMS receive loop: synthesized responses
# ---------------------------------------------------------------------------

async def test_csms_recv_loop_replies_to_heartbeat():
    fw = _forwarder()
    fw._cp["CP-01"] = _CPState(cp_id="CP-01", protocol="ocpp1.6")

    ws = _FakeWebSocket([[2, "hb-csms-1", "Heartbeat", {}]])
    await fw._csms_recv_loop("CP-01", ws)

    assert len(ws.sent) == 1
    reply = ws.sent[0]
    assert reply[0] == 3           # CALLRESULT
    assert reply[1] == "hb-csms-1"
    assert "currentTime" in reply[2]


async def test_csms_recv_loop_does_not_reply_to_get_configuration():
    fw = _forwarder()
    fw._cp["CP-01"] = _CPState(cp_id="CP-01", protocol="ocpp1.6")

    ws = _FakeWebSocket([[2, "gc-1", "GetConfiguration", {"key": []}]])
    await fw._csms_recv_loop("CP-01", ws)

    assert ws.sent == [], "GetConfiguration should be left unanswered"


async def test_csms_recv_loop_discards_callresult_silently():
    fw = _forwarder()
    fw._cp["CP-01"] = _CPState(cp_id="CP-01", protocol="ocpp1.6")

    ws = _FakeWebSocket([[3, "sn-1", {}]])
    await fw._csms_recv_loop("CP-01", ws)   # must not raise

    assert ws.sent == []


async def test_csms_recv_loop_extracts_heartbeat_interval_from_boot_response():
    fw = _forwarder()
    state = _CPState(cp_id="CP-01", protocol="ocpp1.6")
    state.boot_msg_id = "bn-1"
    fw._cp["CP-01"] = state

    boot_response = [3, "bn-1", {"currentTime": "2026-01-01T00:00:00Z", "interval": 1800, "status": "Accepted"}]
    ws = _FakeWebSocket([boot_response])
    await fw._csms_recv_loop("CP-01", ws)

    assert state.heartbeat_interval == 1800.0
    assert state.boot_msg_id is None


# ---------------------------------------------------------------------------
# BootNotification caching
# ---------------------------------------------------------------------------

async def test_boot_notification_from_cp_is_cached():
    """BootNotification payload is stored in state.boot_notification and boot_msg_id is set."""
    fw = _forwarder()
    state = _state_with_task("CP-01")
    fw._cp["CP-01"] = state

    boot_payload = [2, "bn-1", "BootNotification", {"chargePointVendor": "Acme", "chargePointModel": "X1"}]
    msg = MessageData(
        event="Message", sender="CP", protocol="ocpp1.6", cp_id="CP-01",
        payload=boot_payload,
    )
    await fw._dispatch(msg)

    assert state.boot_notification == boot_payload
    assert state.boot_msg_id == "bn-1"
    await _cancel(state)


async def test_restart_csms_clears_boot_notification():
    """_restart_csms clears boot_notification and boot_msg_id alongside other per-session state."""
    fw = _forwarder()
    state = _CPState(cp_id="CP-01", protocol="ocpp1.6")
    state.boot_notification = [2, "bn-1", "BootNotification", {"chargePointVendor": "Acme"}]
    state.boot_msg_id = "bn-1"
    fw._cp["CP-01"] = state

    await fw._restart_csms("CP-01")

    assert state.boot_notification is None
    assert state.boot_msg_id is None
    await _cancel(state)


# ---------------------------------------------------------------------------
# TriggerMessage handling
# ---------------------------------------------------------------------------

async def test_trigger_boot_notification_with_cache_accepts_and_queues():
    """TriggerMessage(BootNotification) with a cached entry is accepted and the
    cached frame is queued with a fresh message ID."""
    fw = _forwarder()
    original_boot = [2, "bn-orig", "BootNotification", {"chargePointVendor": "Acme"}]
    state = _state_with_task("CP-01")
    state.boot_notification = original_boot
    fw._cp["CP-01"] = state

    ws = _FakeWebSocket([[2, "tm-1", "TriggerMessage", {"requestedMessage": "BootNotification"}]])
    await fw._csms_recv_loop("CP-01", ws)

    # CALLRESULT(Accepted) sent for the TriggerMessage
    assert len(ws.sent) == 1
    accept = ws.sent[0]
    assert accept[0] == 3           # CALLRESULT
    assert accept[1] == "tm-1"
    assert accept[2] == {"status": "Accepted"}

    # BootNotification queued with a new (non-original) msg_id, same body
    assert not state.send_queue.empty()
    queued = json.loads(state.send_queue.get_nowait())
    assert queued[0] == 2           # CALL
    assert queued[2] == "BootNotification"
    assert queued[1] != "bn-orig"   # fresh msg_id
    assert queued[3] == original_boot[3]

    # boot_msg_id updated so the CSMS response updates heartbeat_interval
    assert state.boot_msg_id == queued[1]
    await _cancel(state)


async def test_trigger_boot_notification_without_cache_is_unanswered():
    """TriggerMessage(BootNotification) with no cached notification is left unanswered."""
    fw = _forwarder()
    state = _state_with_task("CP-01")
    state.boot_notification = None
    fw._cp["CP-01"] = state

    ws = _FakeWebSocket([[2, "tm-1", "TriggerMessage", {"requestedMessage": "BootNotification"}]])
    await fw._csms_recv_loop("CP-01", ws)

    assert ws.sent == []
    await _cancel(state)


async def test_trigger_other_message_type_is_unanswered():
    """TriggerMessage for any requestedMessage other than BootNotification is left unanswered."""
    fw = _forwarder()
    state = _state_with_task("CP-01")
    fw._cp["CP-01"] = state

    ws = _FakeWebSocket([
        [2, "tm-2", "TriggerMessage", {"requestedMessage": "StatusNotification", "connectorId": 1}]
    ])
    await fw._csms_recv_loop("CP-01", ws)

    assert ws.sent == []
    await _cancel(state)


async def test_triggered_boot_notification_response_updates_heartbeat_interval():
    """The CSMS response to a triggered BootNotification is recognised by boot_msg_id
    and updates heartbeat_interval exactly as a naturally forwarded one would."""
    fw = _forwarder()
    original_boot = [2, "bn-orig", "BootNotification", {"chargePointVendor": "Acme"}]
    state = _state_with_task("CP-01")
    state.boot_notification = original_boot
    fw._cp["CP-01"] = state

    # Step 1: handle TriggerMessage → queues BootNotification with new id
    trigger_ws = _FakeWebSocket([[2, "tm-1", "TriggerMessage", {"requestedMessage": "BootNotification"}]])
    await fw._csms_recv_loop("CP-01", trigger_ws)
    queued = json.loads(state.send_queue.get_nowait())
    new_id = queued[1]

    # Step 2: CSMS responds to the triggered BootNotification
    response_ws = _FakeWebSocket([
        [3, new_id, {"currentTime": "2026-01-01T00:00:00Z", "interval": 600, "status": "Accepted"}]
    ])
    await fw._csms_recv_loop("CP-01", response_ws)

    assert state.heartbeat_interval == 600.0
    assert state.boot_msg_id is None
    await _cancel(state)
