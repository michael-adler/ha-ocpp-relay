"""Verify idTag masking in payloads, free text and log records."""

import json
import logging

import pytest

from custom_components.ha_ocpp_relay.shared import anonymize
from custom_components.ha_ocpp_relay.shared.anonymize import (
    IdentifierMaskingFilter,
    anonymize_payload,
    anonymize_text,
    clear_registered_identifiers,
    install_masking_on_loggers,
    package_logger_names,
    mask_id_tag,
    mask_identifier,
    register_identifier,
)
from custom_components.ha_ocpp_relay.shared.models import MessageData, OCPPSensorData

TAG = "050CCD9E810000"
OTHER_TAG = "050CCD9E820000"
CP_ID = "AE0012345678901234"


@pytest.fixture(autouse=True)
def _fresh_registry():
    clear_registered_identifiers()
    yield
    clear_registered_identifiers()


def test_mask_is_stable_distinct_and_idempotent():
    masked = mask_id_tag(TAG)
    assert masked == mask_id_tag(TAG)
    assert masked != mask_id_tag(OTHER_TAG)
    assert TAG not in masked
    assert mask_id_tag(masked) == masked
    assert mask_id_tag("") == ""


def test_payload_ocpp16_and_201():
    frame16 = [2, "1", "StartTransaction", {"connectorId": 1, "idTag": TAG, "meterStart": 0}]
    result16 = [3, "1", {"idTagInfo": {"status": "Accepted", "parentIdTag": OTHER_TAG}}]
    frame201 = [2, "2", "TransactionEvent", {
        "idToken": {"idToken": TAG, "type": "ISO14443"},
        "groupIdToken": {"idToken": OTHER_TAG, "type": "Central"},
    }]
    masked = json.dumps([anonymize_payload(f) for f in (frame16, result16, frame201)])
    assert TAG not in masked and OTHER_TAG not in masked
    assert mask_id_tag(TAG) in masked and mask_id_tag(OTHER_TAG) in masked
    # Non-tag fields survive and the input is not modified.
    assert anonymize_payload(frame16)[3]["meterStart"] == 0
    assert frame16[3]["idTag"] == TAG


def test_text_forms():
    raw_json = json.dumps([2, "1", "Authorize", {"idTag": TAG}])
    raw_201 = json.dumps({"idToken": {"idToken": TAG, "type": "ISO14443"}})
    py_repr = repr({"connectorId": 1, "idTag": TAG})
    summary = f"connector=1 idTag={TAG!r}"
    sensor = repr(OCPPSensorData(
        cp_id="CP1", topic="1/idtag", unique_id="OCPP_CP1_1_idtag",
        name="C1 Id Tag CP CP1", value=TAG,
    ))
    for text in (raw_json, raw_201, py_repr, summary, sensor):
        masked = anonymize_text(text)
        assert TAG not in masked, text
        assert mask_id_tag(TAG) in masked, text
    assert anonymize_text("no tags here") == "no tags here"


def _record(msg, *args):
    return logging.LogRecord("test", logging.DEBUG, __file__, 1, msg, args, None)


def test_filter_masks_formatted_and_structured_args():
    flt = IdentifierMaskingFilter()

    rec = _record("Relayed CP -> CSMS: %s", json.dumps([2, "1", "Authorize", {"idTag": TAG}]))
    assert flt.filter(rec)
    assert TAG not in rec.getMessage()

    sensor = OCPPSensorData(
        cp_id="CP1", topic="1/idtag", unique_id="OCPP_CP1_1_idtag", name="n", value=TAG
    )
    rec = _record("Publishing %s", sensor)
    flt.filter(rec)
    assert TAG not in rec.getMessage()
    assert sensor.value == TAG  # the caller's object is not mutated

    msg = MessageData(event="Message", sender="CP", payload=[2, "1", "Authorize", {"idTag": TAG}])
    rec = _record("Handle %s and %d", msg, 7)
    flt.filter(rec)
    assert TAG not in rec.getMessage()
    assert rec.getMessage().endswith(" and 7")


def _integration_logger_names():
    return package_logger_names(
        "custom_components.ha_ocpp_relay", [str(anonymize.__file__).rsplit("/shared/", 1)[0]]
    )


def test_truncated_websockets_frame_log():
    # websockets shortens long frames in its debug log, cutting keys mid-name.
    text = '''< TEXT '[3, "a1", {"idTagInfo": {"status": "Accepted", ...tIdTag": "PARENT123"}}]' [76 bytes]'''
    masked = anonymize_text(text)
    assert "PARENT123" not in masked
    assert masked == '''< TEXT '[3, "a1", ...' [76 bytes]'''


def test_package_loggers_get_filter(caplog):
    names = _integration_logger_names()
    assert "custom_components.ha_ocpp_relay.relay.core" in names
    assert "custom_components.ha_ocpp_relay.snoop.parser" in names
    install_masking_on_loggers(names)
    core_logger = logging.getLogger("custom_components.ha_ocpp_relay.relay.core")
    assert any(isinstance(f, IdentifierMaskingFilter) for f in core_logger.filters)

    # Installing twice must not stack filters.
    count = len(core_logger.filters)
    install_masking_on_loggers(names)
    assert len(core_logger.filters) == count

    with caplog.at_level(logging.DEBUG, logger=core_logger.name):
        core_logger.debug("frame %s", json.dumps({"idTag": TAG}))
    assert TAG not in caplog.text
    assert mask_id_tag(TAG) in caplog.text


def test_identifier_mask_is_distinct_from_tag_mask():
    assert mask_identifier(CP_ID).startswith("anon-id-")
    assert mask_id_tag(TAG).startswith("anon-tag-")
    assert mask_identifier(CP_ID) == mask_identifier(CP_ID)
    assert mask_identifier(mask_identifier(CP_ID)) == mask_identifier(CP_ID)


def test_boot_notification_identifiers_masked():
    boot16 = [2, "1", "BootNotification", {
        "chargePointVendor": "Autel", "chargePointModel": "MaxiChargerAC",
        "chargePointSerialNumber": CP_ID, "chargeBoxSerialNumber": "BOX123456",
        "meterSerialNumber": "MTR123456", "iccid": "8931440400012345678", "imsi": "204080123456789",
        "firmwareVersion": "2.4.3.0",
    }]
    boot201 = [2, "2", "BootNotification", {
        "reason": "PowerUp",
        "chargingStation": {"model": "M", "vendorName": "V", "serialNumber": CP_ID,
                            "modem": {"iccid": "8931440400012345678", "imsi": "204080123456789"}},
    }]
    for frame in (boot16, boot201):
        for text in (json.dumps(anonymize_payload(frame)), anonymize_text(json.dumps(frame))):
            for secret in (CP_ID, "BOX123456", "MTR123456", "8931440400012345678", "204080123456789"):
                assert secret not in text, (secret, text)
            assert mask_identifier(CP_ID) in text
            # Non-identifying fields are kept.
            assert ("2.4.3.0" in text) or ("PowerUp" in text)


def test_registered_cp_id_masked_everywhere():
    register_identifier(CP_ID)
    lines = [
        f"Charge point connected: {CP_ID} (protocol=ocpp1.6)",
        f"Connecting charge point {CP_ID} to upstream CSMS at ws://ocpp.example.nl/1.6/vattenfall/{CP_ID}",
        f"Publishing data to topic ocpp/{CP_ID}/1/power",
        f"unique_id='OCPP_{CP_ID}_1_idtag'",
    ]
    for line in lines:
        masked = anonymize_text(line)
        assert CP_ID not in masked, line
        assert mask_identifier(CP_ID) in masked, line
    # A longer word that merely contains the ID is left alone.
    assert anonymize_text(f"X{CP_ID}9") == f"X{CP_ID}9"


def test_short_ids_not_registered_but_keyed_values_masked():
    register_identifier("CP1")
    assert anonymize_text("connector 1 on CP1") == "connector 1 on CP1"
    assert "CP1" not in anonymize_text('{"cp_id": "CP1"}')


def test_url_credentials_and_handshake_request_line():
    masked = anonymize_text("Connecting to wss://user:secret@csms.example.com/ocpp/X")
    assert "user" not in masked and "secret" not in masked
    assert "wss://***@csms.example.com" in masked

    # The websockets library logs the handshake before the relay sees the CP.
    masked = anonymize_text(f"< GET /{CP_ID} HTTP/1.1")
    assert CP_ID not in masked
    # ...and the ID is then known for later lines.
    assert CP_ID not in anonymize_text(f"Charge point connected: {CP_ID}")


def test_snoop_record_masked_consistently():
    record = {
        "event": "Message", "sender": "CP", "protocol": "ocpp1.6", "cp_id": CP_ID,
        "payload": [2, "1", "StartTransaction", {"connectorId": 1, "idTag": TAG}],
        "timestamp": "2026-11-15T09:52:47Z",
    }
    masked = anonymize_payload(record)
    assert masked["cp_id"] == mask_identifier(CP_ID)
    assert masked["payload"][3]["idTag"] == mask_id_tag(TAG)
    assert record["cp_id"] == CP_ID  # input untouched
    # Registered by the record, so later free text is masked too.
    assert CP_ID not in anonymize_text(f"for CP {CP_ID}")


def test_filter_masks_cp_id_in_structured_args():
    flt = IdentifierMaskingFilter()
    sensor = OCPPSensorData(
        cp_id=CP_ID, topic="1/power", unique_id=f"OCPP_{CP_ID}_1_power",
        name=f"C1 Power CP {CP_ID}", value=7.2,
    )
    rec = _record("Publishing data %s to topic %s", sensor, f"ocpp/{CP_ID}/1/power")
    flt.filter(rec)
    assert CP_ID not in rec.getMessage()
    assert "value=7.2" in rec.getMessage()


def test_truncated_frame_preview_keeps_only_frame_start():
    # The cut leaves a key fragment ('i"') that cannot be recognized.
    text = '''< TEXT '[2, "b1", "BootNotification", {"chargePointVend...i": "204080123456789"}]' [240 bytes]'''
    masked = anonymize_text(text)
    assert "204080123456789" not in masked
    assert masked == '''< TEXT '[2, "b1", "BootNotification", ...' [240 bytes]'''
    # Complete previews are masked normally, not shortened.
    full = '''> TEXT '[2, "a1", "Authorize", {"idTag": "050CCD9E810000"}]' [51 bytes]'''
    assert anonymize_text(full) == full.replace("050CCD9E810000", mask_id_tag("050CCD9E810000"))
