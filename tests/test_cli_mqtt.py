import os
import sys
import time
import json
import threading
import subprocess
import socket
import logging

import pytest
# Skip the whole module during collection if paho-mqtt isn't installed in the environment
mqtt = pytest.importorskip("paho.mqtt.client")
from paho.mqtt.client import CallbackAPIVersion


logging.basicConfig(level=logging.DEBUG, force=True)

def wait_until(predicate, timeout=10.0, interval=0.05, desc="condition"):
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(interval)
    raise AssertionError(f"Timed out waiting for {desc}")

def wait_for_process_start(proc: subprocess.Popen, timeout=5.0):
    def alive():
        return proc.poll() is None
    wait_until(alive, timeout=timeout, desc="process to stay alive after start")

def wait_for_port(host: str, port: int, timeout: float = 5.0) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except Exception:
            time.sleep(0.1)
    return False

@pytest.mark.cli
def test_playback_and_snoop2mqtt_collects_messages(tmp_path):
    """Start a playback server and ocpp-snoop2mqtt and collect MQTT messages.

    Requires an MQTT broker on localhost:1883 (CI workflow will install mosquitto).
    """

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    python = sys.executable
    logging.getLogger().setLevel(logging.DEBUG)

    # MQTT subscriber to collect published messages
    messages = []

    def on_message(client, userdata, msg):
        try:
            payload = msg.payload.decode("utf-8")
            messages.append({"topic": msg.topic, "payload": payload})
        except Exception:
            messages.append({"topic": msg.topic, "payload": None})

    # Prefer the new callback API when available; fall back for older paho versions.
    assert wait_for_port("localhost", 8583, timeout=10.0), "MQTT broker not reachable"
    try:
        client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    except Exception:
        client = mqtt.Client()
    # Use a blocking connect so we can be sure subscription happens after connection.
    client.connect("localhost", 8583)
    client.loop_start()

    # Wait for the client to be connected before subscribing.
    wait_until(lambda: client.is_connected(), timeout=5.0, desc="MQTT connect")

    # subscribe to all ocpp-related topics and register per-topic callbacks
    client.message_callback_add("ocpp/#", on_message)
    client.message_callback_add("homeassistant/#", on_message)
    client.subscribe([("ocpp/#", 1), ("homeassistant/#", 1)])

    assert wait_for_port("localhost", 8583, timeout=10.0), "MQTT broker not reachable"

    # Start playback server
    playback_cmd = [
        python,
        "-m",
        "relay_server.debug.snoop_playback",
        "tests/data/ocpp_log.json",
        "--snoop-port",
        "8551",
        "-v",
    ]

    # Start snoop2mqtt
    snoop_cmd = [
        python,
        "-m",
        "relay_server.ocpp_snoop2mqtt",
        "--snoop-socket",
        "ws://127.0.0.1:8551/",
        "--mqtt-broker-host",
        "localhost",
        "--mqtt-broker-port",
        "8583",
        "--exit-on-snoop-disconnect",
        "-v",
    ]

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    # Helper to continuously read a binary stream into a list to avoid pipe blocking.
    def _start_stream_reader(stream):
        buf: list[bytes] = []

        def _reader():
            try:
                while True:
                    chunk = stream.readline()
                    if not chunk:
                        break
                    buf.append(chunk)
            except Exception:
                pass

        th = threading.Thread(target=_reader, daemon=True)
        th.start()
        return buf, th

    p_playback = subprocess.Popen(
        playback_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=project_root, env=env
    )
    wait_for_process_start(p_playback)
    playback_out_buf, playback_out_thread = _start_stream_reader(p_playback.stdout)
    playback_err_buf, playback_err_thread = _start_stream_reader(p_playback.stderr)

    # Give playback server a moment to start (longer timeout for CI/slow hosts).
    if not wait_for_port("127.0.0.1", 8551, timeout=15.0):
        # If playback exited early, surface its logs to aid debugging.
        if p_playback.poll() is not None:
            try:
                out, err = p_playback.communicate(timeout=1)
            except Exception:
                out, err = b"", b""
            print("--- playback stdout ---")
            print(out.decode("utf-8", errors="replace"))
            print("--- playback stderr ---")
            print(err.decode("utf-8", errors="replace"))
            pytest.fail("Playback server exited before listening on 8551")
        else:
            pytest.fail("Playback server port not open after timeout")

    p_snoop = subprocess.Popen(
        snoop_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=project_root, env=env
    )
    wait_for_process_start(p_snoop)
    snoop_out_buf, snoop_out_thread = _start_stream_reader(p_snoop.stdout)
    snoop_err_buf, snoop_err_thread = _start_stream_reader(p_snoop.stderr)

    # Wait for snoop2mqtt to exit or timeout after 60s
    try:
        ret = p_snoop.wait(timeout=60)
    except subprocess.TimeoutExpired:
        # Timeout: kill processes and fail the test
        p_snoop.kill()
        p_playback.kill()
        client.loop_stop()
        # Join reader threads briefly and dump their buffers for debugging
        snoop_out_thread.join(timeout=0.1)
        snoop_err_thread.join(timeout=0.1)
        playback_out_thread.join(timeout=0.1)
        playback_err_thread.join(timeout=0.1)

        print("--- snoop2mqtt stdout ---")
        print(b"".join(snoop_out_buf).decode("utf-8", errors="replace"))
        print("--- snoop2mqtt stderr ---")
        print(b"".join(snoop_err_buf).decode("utf-8", errors="replace"))

        print("--- playback stdout (on timeout) ---")
        print(b"".join(playback_out_buf).decode("utf-8", errors="replace"))
        print("--- playback stderr (on timeout) ---")
        print(b"".join(playback_err_buf).decode("utf-8", errors="replace"))

        pytest.fail("snoop2mqtt did not exit within 60 seconds (timeout)")

    # Join reader threads briefly and print captured DUT logs so -s shows them
    snoop_out_thread.join(timeout=0.1)
    snoop_err_thread.join(timeout=0.1)
    playback_out_thread.join(timeout=0.1)
    playback_err_thread.join(timeout=0.1)

    print("--- snoop2mqtt stdout (after exit) ---")
    print(b"".join(snoop_out_buf).decode("utf-8", errors="replace"))
    print("--- snoop2mqtt stderr (after exit) ---")
    print(b"".join(snoop_err_buf).decode("utf-8", errors="replace"))

    print("--- playback stdout (after exit) ---")
    print(b"".join(playback_out_buf).decode("utf-8", errors="replace"))
    print("--- playback stderr (after exit) ---")
    print(b"".join(playback_err_buf).decode("utf-8", errors="replace"))

    # Give a little time for MQTT deliveries to arrive
    time.sleep(2.0)

    # Stop playback server
    p_playback.kill()

    client.loop_stop()

    # Dump all collected messages via logging (visible with pytest log_cli)
    logger = logging.getLogger(__name__)
    logger.info("--- MQTT messages captured ---")
    for m in messages:
        logger.info(json.dumps(m))

    # Also log topics and parsed values for easier human inspection
    logger.info("--- MQTT topics and values ---")
    for m in messages:
        topic = m.get("topic")
        payload = m.get("payload")
        value = None
        if payload:
            try:
                parsed = json.loads(payload)
                if isinstance(parsed, dict) and "value" in parsed:
                    value = parsed["value"]
                else:
                    value = parsed
            except Exception:
                value = payload
        logger.info(f"{topic} -> {value}")

    # Validate specific expected topics and payloads were published
    expected = [
        {"topic": "ocpp/AL0123456789ABCDEF/1/Outlet/Energy-Active-Import-Register/state", "payload": '{"value": 440.6223}'},
        {"topic": "ocpp/AL0123456789ABCDEF/1/Outlet/Voltage/state", "payload": '{"value": "245.360000"}'},
        {"topic": "ocpp/AL0123456789ABCDEF/1/Outlet/Current-Export/state", "payload": '{"value": "46.916000"}'},
        {"topic": "ocpp/AL0123456789ABCDEF/1/Outlet/Current-Import/state", "payload": '{"value": "46.916000"}'},
        {"topic": "ocpp/AL0123456789ABCDEF/1/Outlet/Power-Offered/state", "payload": '{"value": 11.378}'},
        {"topic": "ocpp/AL0123456789ABCDEF/1/Outlet/Power-Active-Import/state", "payload": '{"value": 11.378}'},
    ]

    matched = []
    missing = []
    for exp in expected:
        found = any(m.get("topic") == exp["topic"] and m.get("payload") == exp["payload"] for m in messages)
        if found:
            matched.append(exp)
        else:
            missing.append(exp)

    # Log summary of results (visible during test run due to log_cli)
    total = len(expected)
    matched_count = len(matched)
    missing_count = len(missing)
    logger.info(f"--- Expected topics tested: {total}; matched: {matched_count}; missing: {missing_count} ---")

    if missing_count > 0:
        logger.info("--- Missing expected messages ---")
        for m in missing:
            logger.info(json.dumps(m))
        pytest.fail(f"{missing_count} expected MQTT messages not found out of {total}. See above for details.")
    else:
        logger.info("All expected MQTT messages found.")

    # This CP only ever reports "phase": "L1" (single phase), so sensor names
    # and state topics must not carry a phase suffix.
    discovery_names = [
        json.loads(m["payload"])["components"][uid]["name"]
        for m in messages
        if m.get("topic", "").startswith("homeassistant/device/ocpp/") and m["topic"].endswith("/config")
        for uid in json.loads(m["payload"])["components"]
    ]
    logger.info("--- Discovered sensor names (single-phase) ---")
    for name in discovery_names:
        logger.info(name)
    assert discovery_names, "No discovery messages captured"
    for name in discovery_names:
        tokens = set(name.split())
        assert not (tokens & {"L1", "L2", "L3"}), f"Single-phase sensor name unexpectedly contains phase suffix: {name!r}"
    for m in messages:
        topic = m.get("topic", "")
        assert not any(f"/{p}/" in topic for p in ("L1", "L2", "L3")), (
            f"Single-phase state topic unexpectedly contains phase segment: {topic!r}"
        )


@pytest.mark.cli
def test_playback_and_snoop2mqtt_collects_3phase_messages(tmp_path):
    """Same as test_playback_and_snoop2mqtt_collects_messages but for a 3-phase CP.

    Verifies that per-phase measurands (Current.Import, Voltage,
    Power.Active.Import) reach MQTT as separate L1/L2/L3 sensors -- both in
    the state topic and in the Home Assistant discovery "name" field -- while
    non-phase measurands (Energy.Active.Import.Register) keep their ordinary
    name.
    """

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    python = sys.executable
    logging.getLogger().setLevel(logging.DEBUG)

    messages = []

    def on_message(client, userdata, msg):
        try:
            payload = msg.payload.decode("utf-8")
            messages.append({"topic": msg.topic, "payload": payload})
        except Exception:
            messages.append({"topic": msg.topic, "payload": None})

    assert wait_for_port("localhost", 8583, timeout=10.0), "MQTT broker not reachable"
    try:
        client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    except Exception:
        client = mqtt.Client()
    client.connect("localhost", 8583)
    client.loop_start()

    wait_until(lambda: client.is_connected(), timeout=5.0, desc="MQTT connect")

    client.message_callback_add("ocpp/#", on_message)
    client.message_callback_add("homeassistant/#", on_message)
    client.subscribe([("ocpp/#", 1), ("homeassistant/#", 1)])

    assert wait_for_port("localhost", 8583, timeout=10.0), "MQTT broker not reachable"

    # Use a distinct snoop port from the single-phase test so the two playback
    # servers can never collide, even if a prior run's process lingers.
    playback_cmd = [
        python,
        "-m",
        "relay_server.debug.snoop_playback",
        "tests/data/ocpp_3phase_log.json",
        "--snoop-port",
        "8552",
        "-v",
    ]

    snoop_cmd = [
        python,
        "-m",
        "relay_server.ocpp_snoop2mqtt",
        "--snoop-socket",
        "ws://127.0.0.1:8552/",
        "--mqtt-broker-host",
        "localhost",
        "--mqtt-broker-port",
        "8583",
        "--exit-on-snoop-disconnect",
        "-v",
    ]

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"

    def _start_stream_reader(stream):
        buf: list[bytes] = []

        def _reader():
            try:
                while True:
                    chunk = stream.readline()
                    if not chunk:
                        break
                    buf.append(chunk)
            except Exception:
                pass

        th = threading.Thread(target=_reader, daemon=True)
        th.start()
        return buf, th

    p_playback = subprocess.Popen(
        playback_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=project_root, env=env
    )
    wait_for_process_start(p_playback)
    playback_out_buf, playback_out_thread = _start_stream_reader(p_playback.stdout)
    playback_err_buf, playback_err_thread = _start_stream_reader(p_playback.stderr)

    if not wait_for_port("127.0.0.1", 8552, timeout=15.0):
        if p_playback.poll() is not None:
            try:
                out, err = p_playback.communicate(timeout=1)
            except Exception:
                out, err = b"", b""
            print("--- playback stdout ---")
            print(out.decode("utf-8", errors="replace"))
            print("--- playback stderr ---")
            print(err.decode("utf-8", errors="replace"))
            pytest.fail("Playback server exited before listening on 8552")
        else:
            pytest.fail("Playback server port not open after timeout")

    p_snoop = subprocess.Popen(
        snoop_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=project_root, env=env
    )
    wait_for_process_start(p_snoop)
    snoop_out_buf, snoop_out_thread = _start_stream_reader(p_snoop.stdout)
    snoop_err_buf, snoop_err_thread = _start_stream_reader(p_snoop.stderr)

    try:
        ret = p_snoop.wait(timeout=60)
    except subprocess.TimeoutExpired:
        p_snoop.kill()
        p_playback.kill()
        client.loop_stop()
        snoop_out_thread.join(timeout=0.1)
        snoop_err_thread.join(timeout=0.1)
        playback_out_thread.join(timeout=0.1)
        playback_err_thread.join(timeout=0.1)

        print("--- snoop2mqtt stdout ---")
        print(b"".join(snoop_out_buf).decode("utf-8", errors="replace"))
        print("--- snoop2mqtt stderr ---")
        print(b"".join(snoop_err_buf).decode("utf-8", errors="replace"))

        print("--- playback stdout (on timeout) ---")
        print(b"".join(playback_out_buf).decode("utf-8", errors="replace"))
        print("--- playback stderr (on timeout) ---")
        print(b"".join(playback_err_buf).decode("utf-8", errors="replace"))

        pytest.fail("snoop2mqtt did not exit within 60 seconds (timeout)")

    snoop_out_thread.join(timeout=0.1)
    snoop_err_thread.join(timeout=0.1)
    playback_out_thread.join(timeout=0.1)
    playback_err_thread.join(timeout=0.1)

    print("--- snoop2mqtt stdout (after exit) ---")
    print(b"".join(snoop_out_buf).decode("utf-8", errors="replace"))
    print("--- snoop2mqtt stderr (after exit) ---")
    print(b"".join(snoop_err_buf).decode("utf-8", errors="replace"))

    print("--- playback stdout (after exit) ---")
    print(b"".join(playback_out_buf).decode("utf-8", errors="replace"))
    print("--- playback stderr (after exit) ---")
    print(b"".join(playback_err_buf).decode("utf-8", errors="replace"))

    time.sleep(2.0)

    p_playback.kill()
    client.loop_stop()

    logger = logging.getLogger(__name__)
    logger.info("--- MQTT messages captured (3-phase) ---")
    for m in messages:
        logger.info(json.dumps(m))

    # Expected values come from the very first MeterValues sample in
    # tests/data/ocpp_3phase_log.json, which reports all-zero readings.
    expected_state = [
        {"topic": "ocpp/ChargerSerialNr/1/Outlet/Energy-Active-Import-Register/state", "payload": '{"value": 0.0}'},
        {"topic": "ocpp/ChargerSerialNr/1/Outlet/Current-Import/L1/state", "payload": '{"value": "0.00"}'},
        {"topic": "ocpp/ChargerSerialNr/1/Outlet/Current-Import/L2/state", "payload": '{"value": "0.00"}'},
        {"topic": "ocpp/ChargerSerialNr/1/Outlet/Current-Import/L3/state", "payload": '{"value": "0.00"}'},
        {"topic": "ocpp/ChargerSerialNr/1/Outlet/Voltage/L1/state", "payload": '{"value": "230.6"}'},
        {"topic": "ocpp/ChargerSerialNr/1/Outlet/Voltage/L2/state", "payload": '{"value": "237.7"}'},
        {"topic": "ocpp/ChargerSerialNr/1/Outlet/Voltage/L3/state", "payload": '{"value": "233.1"}'},
        {"topic": "ocpp/ChargerSerialNr/1/Outlet/Power-Active-Import/L1/state", "payload": '{"value": 0.0}'},
        {"topic": "ocpp/ChargerSerialNr/1/Outlet/Power-Active-Import/L2/state", "payload": '{"value": 0.0}'},
        {"topic": "ocpp/ChargerSerialNr/1/Outlet/Power-Active-Import/L3/state", "payload": '{"value": 0.0}'},
    ]

    matched = []
    missing = []
    for exp in expected_state:
        found = any(m.get("topic") == exp["topic"] and m.get("payload") == exp["payload"] for m in messages)
        if found:
            matched.append(exp)
        else:
            missing.append(exp)

    total = len(expected_state)
    matched_count = len(matched)
    missing_count = len(missing)
    logger.info(f"--- Expected 3-phase state topics tested: {total}; matched: {matched_count}; missing: {missing_count} ---")

    if missing_count > 0:
        logger.info("--- Missing expected state messages ---")
        for m in missing:
            logger.info(json.dumps(m))
        pytest.fail(f"{missing_count} expected MQTT state messages not found out of {total}. See above for details.")

    # Also confirm the Home Assistant discovery "name" field carries the phase,
    # inserted right before "CP <cp_id>", for per-phase sensors.
    discovery_by_uid: dict[str, str] = {}
    for m in messages:
        topic = m.get("topic", "")
        if not (topic.startswith("homeassistant/device/ocpp/") and topic.endswith("/config")):
            continue
        parsed = json.loads(m["payload"])
        for uid, component in parsed["components"].items():
            discovery_by_uid[uid] = component["name"]

    logger.info("--- Discovered sensor names (3-phase) ---")
    for uid, name in sorted(discovery_by_uid.items()):
        logger.info(f"{uid} -> {name}")

    expected_names = {
        "OCPP_ChargerSerialNr_1_Outlet_Current-Import_L1_value": "C1 Current Import Outlet L1 CP ChargerSerialNr",
        "OCPP_ChargerSerialNr_1_Outlet_Current-Import_L2_value": "C1 Current Import Outlet L2 CP ChargerSerialNr",
        "OCPP_ChargerSerialNr_1_Outlet_Current-Import_L3_value": "C1 Current Import Outlet L3 CP ChargerSerialNr",
        "OCPP_ChargerSerialNr_1_Outlet_Voltage_L1_value": "C1 Voltage Outlet L1 CP ChargerSerialNr",
        "OCPP_ChargerSerialNr_1_Outlet_Voltage_L2_value": "C1 Voltage Outlet L2 CP ChargerSerialNr",
        "OCPP_ChargerSerialNr_1_Outlet_Voltage_L3_value": "C1 Voltage Outlet L3 CP ChargerSerialNr",
        "OCPP_ChargerSerialNr_1_Outlet_Power-Active-Import_L1_value": "C1 Power Active Import Outlet L1 CP ChargerSerialNr",
        "OCPP_ChargerSerialNr_1_Outlet_Power-Active-Import_L2_value": "C1 Power Active Import Outlet L2 CP ChargerSerialNr",
        "OCPP_ChargerSerialNr_1_Outlet_Power-Active-Import_L3_value": "C1 Power Active Import Outlet L3 CP ChargerSerialNr",
        # Non-phase measurand: no L<phase> suffix even on this multi-phase CP.
        "OCPP_ChargerSerialNr_1_Outlet_Energy-Active-Import-Register_value": "C1 Energy Active Import Register Outlet CP ChargerSerialNr",
    }

    name_missing = []
    name_mismatch = []
    for uid, expected_name in expected_names.items():
        actual = discovery_by_uid.get(uid)
        if actual is None:
            name_missing.append(uid)
        elif actual != expected_name:
            name_mismatch.append((uid, actual, expected_name))

    if name_missing:
        logger.info(f"Missing discovery messages for: {name_missing}")
    if name_mismatch:
        logger.info(f"Discovery name mismatches: {name_mismatch}")

    assert not name_missing, f"Missing discovery messages for unique_ids: {name_missing}"
    assert not name_mismatch, f"Discovery name mismatches: {name_mismatch}"

    # Collapsed, phase-less unique_ids must not appear for per-phase measurands.
    for measurand_topic in ("Current-Import", "Voltage", "Power-Active-Import"):
        collapsed_uid = f"OCPP_ChargerSerialNr_1_Outlet_{measurand_topic}_value"
        assert collapsed_uid not in discovery_by_uid, f"Unexpected collapsed sensor: {collapsed_uid}"
