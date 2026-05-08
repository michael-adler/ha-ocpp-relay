from custom_components.ha_ocpp_relay.snoop.parser import OCPPFilter


def _msg(cp_id: str, action: str, payload: dict) -> dict:
    return {
        "event": "Message",
        "sender": "CP",
        "cp_id": cp_id,
        "protocol": "ocpp1.6",
        "timestamp": "2026-05-07T10:00:00Z",
        "payload": [2, "uid-1", action, payload],
    }


def test_manufacturer_cache_stays_unset_on_non_matching_frames_and_updates_later():
    class TrackingFilter(OCPPFilter):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def _get_manufacturer(self, ocpp):
            self.calls += 1
            return super()._get_manufacturer(ocpp)

    parser = TrackingFilter()
    cp_id = "CP-1"

    hb1 = parser.filter(_msg(cp_id, "Heartbeat", {}))
    hb2 = parser.filter(_msg(cp_id, "Heartbeat", {}))

    assert hb1 is not None
    assert hb2 is not None
    assert hb1[0].manufacturer is None
    assert hb2[0].manufacturer is None
    # While unset, each frame re-checks manufacturer; _get_manufacturer decides if it matches.
    assert parser.calls == 2

    parser.filter(_msg(cp_id, "DataTransfer", {"vendorId": "ACME"}))
    hb3 = parser.filter(_msg(cp_id, "Heartbeat", {}))

    assert parser.calls == 3
    assert hb3 is not None
    assert hb3[0].manufacturer == "ACME"

    parser.filter(_msg(cp_id, "Heartbeat", {}))
    # Once cached, no further manufacturer extraction should occur.
    assert parser.calls == 3
