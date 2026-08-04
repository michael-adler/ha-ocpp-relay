import json
import os

from custom_components.ha_ocpp_relay.snoop.parser import OCPPFilter


def _load_sensors():
    """Replay the recorded 3-phase log through the parser and collect sensors by unique_id."""
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    log_path = os.path.join(project_root, "tests", "data", "ocpp_3phase_log.json")
    with open(log_path, "r", encoding="utf-8") as f:
        lines = [ln.strip() for ln in f.readlines() if ln.strip()]

    parser = OCPPFilter()
    sensors: dict[str, object] = {}
    for line in lines:
        msg = json.loads(line)
        result = parser.filter(msg)
        if not result:
            continue
        for sensor in result:
            sensors[sensor.unique_id] = sensor
    return sensors


def test_3phase_meter_values_get_separate_sensors_per_phase():
    sensors = _load_sensors()

    # Per-phase measurands (Current.Import, Voltage, Power.Active.Import) must be
    # split into one sensor per phase, with the phase inserted before "CP <id>".
    for measurand_topic, measurand_name in (
        ("Current-Import", "Current Import"),
        ("Voltage", "Voltage"),
        ("Power-Active-Import", "Power Active Import"),
    ):
        for phase in ("L1", "L2", "L3"):
            unique_id = f"OCPP_ChargerSerialNr_1_Outlet_{measurand_topic}_{phase}"
            assert unique_id in sensors, f"Missing per-phase sensor {unique_id}"
            sensor = sensors[unique_id]
            assert sensor.name == f"C1 {measurand_name} Outlet {phase} CP ChargerSerialNr"
            assert sensor.topic == f"1/Outlet/{measurand_topic}/{phase}"

        # No collapsed, phase-less sensor should exist for a per-phase measurand.
        collapsed_id = f"OCPP_ChargerSerialNr_1_Outlet_{measurand_topic}"
        assert collapsed_id not in sensors


def test_3phase_meter_values_leaves_non_phase_measurand_name_unchanged():
    sensors = _load_sensors()

    # Energy.Active.Import.Register never carries a "phase" field, so it must
    # keep its ordinary (no-phase) name even though the CP is multi-phase.
    unique_id = "OCPP_ChargerSerialNr_1_Outlet_Energy-Active-Import-Register"
    assert unique_id in sensors
    sensor = sensors[unique_id]
    assert sensor.name == "C1 Energy Active Import Register Outlet CP ChargerSerialNr"
    assert sensor.topic == "1/Outlet/Energy-Active-Import-Register"


def test_single_phase_meter_values_keep_current_naming():
    """A single-phase sample (only one distinct phase value) must not get an L<phase> suffix."""
    parser = OCPPFilter()
    msg = {
        "event": "Message",
        "sender": "CP",
        "protocol": "ocpp1.6",
        "cp_id": "SinglePhaseCP",
        "payload": [
            2,
            "1",
            "MeterValues",
            {
                "connectorId": 1,
                "meterValue": [
                    {
                        "timestamp": "2026-08-01T14:42:16",
                        "sampledValue": [
                            {
                                "value": "16.10",
                                "measurand": "Current.Import",
                                "phase": "L1",
                                "unit": "A",
                            },
                            {
                                "value": "223.2",
                                "measurand": "Voltage",
                                "phase": "L1",
                                "unit": "V",
                            },
                        ],
                    }
                ],
            },
        ],
        "timestamp": "2026-08-01T12:42:17Z",
    }

    result = parser.filter(msg)
    sensors = {s.unique_id: s for s in result}

    assert "OCPP_SinglePhaseCP_1_Outlet_Current-Import" in sensors
    assert sensors["OCPP_SinglePhaseCP_1_Outlet_Current-Import"].name == "C1 Current Import Outlet CP SinglePhaseCP"
    assert "OCPP_SinglePhaseCP_1_Outlet_Voltage" in sensors
    assert sensors["OCPP_SinglePhaseCP_1_Outlet_Voltage"].name == "C1 Voltage Outlet CP SinglePhaseCP"
