"""
CLI relay service integration test — no Home Assistant dependency.

Launches the relay server as a subprocess, then drives synthetic traffic
through it from multiple simulated charge points (CPs) and a simulated
central-system emulator (CPMS).  A snoop client independently receives
every message forwarded by the relay and verifies that the full traffic
reaches it.

What is tested
--------------
* CP → CPMS routing: each CP sends MESSAGES_PER_DIRECTION OCPP messages
  to its paired CPMS connection; the CPMS side verifies payload and CP-ID.
* CPMS → CP routing: the CPMS side sends the same volume back; CP side
  verifies round-trip integrity.
* Snoop completeness: every message sent in both directions must appear on
  the snoop WebSocket port.
* All SIMULATED_CP_COUNT CP channels run concurrently (asyncio tasks gated
  on a shared barrier event) so the relay is exercised under parallel load.

Key tunables
------------
SIMULATED_CP_COUNT         — number of simulated CPs (default 6)
MESSAGES_PER_DIRECTION     — target messages per CP per direction; capped
                             automatically by SNOOP_QUEUE_MAXSIZE so the
                             snoop assertion remains valid
VERBOSE_PACKET_LOGGING     — None: follow pytest -v/-vv flag
                             True/False: force on or off

Running
-------
  pytest tests/test_cli_relay_service.py          # summary only
  pytest tests/test_cli_relay_service.py -v       # enable packet logs
  pytest tests/test_cli_relay_service.py -m cli   # by marker
"""

import asyncio
import json
import logging
import os
import socket
import subprocess
import sys
import time

import pytest
import websockets

from custom_components.ha_ocpp_relay.relay.core import SNOOP_QUEUE_MAXSIZE


CP_RELAY_WS_PORT = 8560
SNOOP_WS_PORT = 8561
CPMS_EMULATOR_WS_PORT = 8562
SIMULATED_CP_COUNT = 6
LOCALHOST = "localhost"
OCPP_SUBPROTOCOL = "ocpp1.6"
MESSAGES_PER_DIRECTION = 1000
# Set to True/False to force packet logging on/off.
# Set to None to auto-follow pytest verbosity (-v / -vv enables packet logs).
VERBOSE_PACKET_LOGGING = None

# The relay snoop queue is bounded. Keep test traffic within that bound so
# "all messages reach snoop" remains a valid assertion.
EFFECTIVE_MESSAGES_PER_DIRECTION = max(
    1,
    min(MESSAGES_PER_DIRECTION, SNOOP_QUEUE_MAXSIZE // (2 * SIMULATED_CP_COUNT)),
)


async def _wait_until(predicate, timeout=10.0, interval=0.05, desc="condition"):
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"Timed out waiting for {desc}")


def _wait_for_port(host: str, port: int, timeout: float = 10.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def _ocpp_frame(sender: str, cp_id: str, idx: int) -> list:
    return [
        2,
        f"{sender}-{cp_id}-{idx}",
        "Heartbeat",
        {
            "sender": sender,
            "cpId": cp_id,
            "sequence": idx,
            "payload": f"synthetic-{sender.lower()}-{cp_id}-{idx}",
        },
    ]


@pytest.mark.cli
@pytest.mark.asyncio
async def test_cli_relay_routes_cp_and_cpms_messages_and_snoops_all(tmp_path, request):
    """Validate relay routing across CP, CPMS, and snoop channels without Home Assistant."""

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    python = sys.executable
    logger = logging.getLogger(__name__)
    cli_args = [str(arg) for arg in request.config.invocation_params.args]
    cli_v_count = 0
    cli_q_count = 0
    for arg in cli_args:
        if arg.startswith("--"):
            if arg in ("--verbose",):
                cli_v_count += 1
            if arg in ("--quiet",):
                cli_q_count += 1
            continue
        if arg.startswith("-"):
            flags = arg[1:]
            cli_v_count += flags.count("v")
            cli_q_count += flags.count("q")
    verbose_packet_logging = (
        VERBOSE_PACKET_LOGGING
        if VERBOSE_PACKET_LOGGING is not None
        else (cli_v_count > cli_q_count)
    )

    def _verbose_log(line: str) -> None:
        if verbose_packet_logging:
            logger.info(line)

    cp_ids = [f"CP-{i:02d}" for i in range(1, SIMULATED_CP_COUNT + 1)]
    cpms_url = f"ws://{LOCALHOST}:{CPMS_EMULATOR_WS_PORT}"

    cpms_connections: dict[str, websockets.WebSocketServerProtocol] = {}
    cpms_received: dict[str, list[list]] = {cp_id: [] for cp_id in cp_ids}
    snoop_events: list[dict] = []

    async def cpms_handler(websocket):
        cp_id = websocket.request.path.strip("/")
        cpms_connections[cp_id] = websocket
        _verbose_log(f"[CPMS recv-open] cp_id={cp_id}")
        try:
            async for raw in websocket:
                packet = json.loads(raw)
                cpms_received.setdefault(cp_id, []).append(packet)
                _verbose_log(f"[CPMS recv] cp_id={cp_id} packet={packet!r}")
        finally:
            if cpms_connections.get(cp_id) is websocket:
                cpms_connections.pop(cp_id, None)
            _verbose_log(f"[CPMS recv-close] cp_id={cp_id}")

    cpms_server = await websockets.serve(
        cpms_handler,
        LOCALHOST,
        CPMS_EMULATOR_WS_PORT,
        subprotocols=[OCPP_SUBPROTOCOL],
    )

    relay_cmd = [
        python,
        "-m",
        "relay_server.ocpp_relay_server",
        "--cpms",
        cpms_url,
        "--ocpp-host",
        LOCALHOST,
        "--ocpp-port",
        str(CP_RELAY_WS_PORT),
        "--snoop-host",
        LOCALHOST,
        "--snoop-port",
        str(SNOOP_WS_PORT),
    ]

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    relay_proc = subprocess.Popen(
        relay_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=project_root,
        env=env,
    )

    cp_connections = []
    cp_received: dict[str, list[list]] = {cp_id: [] for cp_id in cp_ids}
    snoop_ws = None
    snoop_task = None
    summary_lines: list[str] = []

    def _build_summary_lines() -> list[str]:
        lines = ["--- relay traffic summary ---"]
        lines.append(f"cp relay port {CP_RELAY_WS_PORT} (CP<-CPMS) counts by cp_id:")
        for cp_id in cp_ids:
            lines.append(f"  {cp_id}: {len(cp_received.get(cp_id, []))}")

        lines.append(f"cpms emulator port {CPMS_EMULATOR_WS_PORT} (CP->CPMS) counts by cp_id:")
        for cp_id in cp_ids:
            lines.append(f"  {cp_id}: {len(cpms_received.get(cp_id, []))}")

        total_snoop_messages = len([e for e in snoop_events if e.get("event") == "Message"])
        lines.append(f"snoop port {SNOOP_WS_PORT} total Message events: {total_snoop_messages}")

        return lines

    try:
        await _wait_until(
            lambda: _wait_for_port(LOCALHOST, CP_RELAY_WS_PORT, timeout=0.2),
            timeout=10.0,
            desc="relay CP websocket port",
        )
        await _wait_until(
            lambda: _wait_for_port(LOCALHOST, SNOOP_WS_PORT, timeout=0.2),
            timeout=10.0,
            desc="relay snoop websocket port",
        )

        snoop_ws = await websockets.connect(f"ws://{LOCALHOST}:{SNOOP_WS_PORT}")

        async def collect_snoop() -> None:
            assert snoop_ws is not None
            async for raw in snoop_ws:
                event = json.loads(raw)
                snoop_events.append(event)
                _verbose_log(
                    "[SNOOP recv] "
                    f"sender={event.get('sender')} cp_id={event.get('cp_id')} "
                    f"event={event.get('event')} payload={event.get('payload')!r}"
                )

        snoop_task = asyncio.create_task(collect_snoop())

        for cp_id in cp_ids:
            ws = await websockets.connect(
                f"ws://{LOCALHOST}:{CP_RELAY_WS_PORT}/{cp_id}",
                subprotocols=[OCPP_SUBPROTOCOL],
            )
            cp_connections.append((cp_id, ws))

        cp_ws_by_id = dict(cp_connections)

        await _wait_until(
            lambda: all(cp_id in cpms_connections for cp_id in cp_ids),
            timeout=10.0,
            desc="all CPMS upstream connections",
        )

        expected_cp_to_cpms: dict[str, list[list]] = {
            cp_id: [
                _ocpp_frame("CP", cp_id, i)
                for i in range(1, EFFECTIVE_MESSAGES_PER_DIRECTION + 1)
            ]
            for cp_id in cp_ids
        }
        expected_cpms_to_cp: dict[str, list[list]] = {
            cp_id: [
                _ocpp_frame("CSMS", cp_id, i)
                for i in range(1, EFFECTIVE_MESSAGES_PER_DIRECTION + 1)
            ]
            for cp_id in cp_ids
        }

        # Start all writers at once so traffic is interleaved across every CP/CPMS pair.
        traffic_start = asyncio.Event()

        async def send_cp_traffic(cp_id: str) -> None:
            await traffic_start.wait()
            cp_ws = cp_ws_by_id[cp_id]
            for frame in expected_cp_to_cpms[cp_id]:
                await cp_ws.send(json.dumps(frame))
                _verbose_log(f"[CP send] cp_id={cp_id} packet={frame!r}")
                await asyncio.sleep(0)

        async def send_cpms_traffic(cp_id: str) -> None:
            await traffic_start.wait()
            cpms_ws = cpms_connections[cp_id]
            for frame in expected_cpms_to_cp[cp_id]:
                await cpms_ws.send(json.dumps(frame))
                _verbose_log(f"[CPMS send] cp_id={cp_id} packet={frame!r}")
                await asyncio.sleep(0)

        async def recv_cp_traffic(cp_id: str) -> None:
            await traffic_start.wait()
            cp_ws = cp_ws_by_id[cp_id]
            for _ in range(EFFECTIVE_MESSAGES_PER_DIRECTION):
                packet = json.loads(await asyncio.wait_for(cp_ws.recv(), timeout=5.0))
                cp_received[cp_id].append(packet)
                _verbose_log(f"[CP recv] cp_id={cp_id} packet={packet!r}")

        send_cp_tasks = [asyncio.create_task(send_cp_traffic(cp_id)) for cp_id in cp_ids]
        send_cpms_tasks = [asyncio.create_task(send_cpms_traffic(cp_id)) for cp_id in cp_ids]
        recv_cp_tasks = [asyncio.create_task(recv_cp_traffic(cp_id)) for cp_id in cp_ids]

        traffic_start.set()
        await asyncio.gather(*send_cp_tasks, *send_cpms_tasks, *recv_cp_tasks)

        await _wait_until(
            lambda: all(
                len(cpms_received.get(cp_id, [])) >= EFFECTIVE_MESSAGES_PER_DIRECTION
                for cp_id in cp_ids
            ),
            timeout=10.0,
            desc="relay forwarding CP->CPMS messages",
        )

        await _wait_until(
            lambda: len([e for e in snoop_events if e.get("event") == "Message"])
            >= SIMULATED_CP_COUNT * EFFECTIVE_MESSAGES_PER_DIRECTION * 2,
            timeout=10.0,
            desc="relay forwarding all events to snoop",
        )

        for cp_id in cp_ids:
            observed = cpms_received[cp_id][:EFFECTIVE_MESSAGES_PER_DIRECTION]
            assert observed == expected_cp_to_cpms[cp_id], (
                f"CP->CPMS payload mismatch for {cp_id}: {observed!r} "
                f"!= {expected_cp_to_cpms[cp_id]!r}"
            )

        for cp_id in cp_ids:
            observed = cp_received[cp_id]
            assert observed == expected_cpms_to_cp[cp_id], (
                f"CPMS->CP payload mismatch for {cp_id}: {observed!r} "
                f"!= {expected_cpms_to_cp[cp_id]!r}"
            )

        expected_snoop = set()
        for cp_id in cp_ids:
            for frame in expected_cp_to_cpms[cp_id]:
                expected_snoop.add(("CP", cp_id, json.dumps(frame, sort_keys=True)))
            for frame in expected_cpms_to_cp[cp_id]:
                expected_snoop.add(("CSMS", cp_id, json.dumps(frame, sort_keys=True)))

        observed_snoop = {
            (
                event.get("sender"),
                event.get("cp_id"),
                json.dumps(event.get("payload"), sort_keys=True),
            )
            for event in snoop_events
            if event.get("event") == "Message"
        }

        missing = expected_snoop - observed_snoop
        assert not missing, f"Missing snoop messages: {sorted(missing)!r}"

        summary_lines = _build_summary_lines()

    finally:
        if not summary_lines:
            summary_lines = _build_summary_lines()
        for line in summary_lines:
            logger.info(line)

        for _, ws in cp_connections:
            try:
                await ws.close()
            except Exception:
                pass

        if snoop_ws is not None:
            try:
                await snoop_ws.close()
            except Exception:
                pass

        if snoop_task is not None:
            snoop_task.cancel()
            await asyncio.gather(snoop_task, return_exceptions=True)

        cpms_server.close()
        await cpms_server.wait_closed()

        relay_proc.terminate()
        try:
            relay_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            relay_proc.kill()
            relay_proc.wait(timeout=5)

        if relay_proc.stdout is not None:
            relay_proc.stdout.close()
        if relay_proc.stderr is not None:
            relay_proc.stderr.close()
