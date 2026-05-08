from types import SimpleNamespace

import pytest

from custom_components.ha_ocpp_relay.relay import core as relay_core


class FakeCPWebSocket:
    def __init__(self, path: str) -> None:
        self.request = SimpleNamespace(path=path, headers={})
        self.subprotocol = "ocpp1.6"
        self.close_calls: list[dict] = []

    async def close(self, *args, **kwargs) -> None:  # noqa: ARG002
        self.close_calls.append(kwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_path", ["/", ""])
async def test_on_connect_rejects_empty_charge_point_id(
    monkeypatch, caplog, invalid_path: str
) -> None:
    async def _unexpected_connect(*args, **kwargs):  # noqa: ARG001
        raise AssertionError("websockets.connect must not be called for empty cp id")

    monkeypatch.setattr(relay_core.websockets, "connect", _unexpected_connect)

    relay = relay_core.OCPPRelay("ws://csms.example")
    cp_ws = FakeCPWebSocket(invalid_path)

    await relay._on_connect(cp_ws)

    assert cp_ws.close_calls == [{"code": 4001, "reason": "missing charge point id"}]
    assert "empty path" in caplog.text
