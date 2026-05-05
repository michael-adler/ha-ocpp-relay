import logging

from relay_server.cli.common import WebSocketKeepaliveFilter


def make_record(name: str, message: str) -> logging.LogRecord:
    """Build a log record for filter testing."""
    return logging.LogRecord(name, logging.DEBUG, "test_cli_logging.py", 1, message, (), None)


def test_websocket_keepalive_filter_drops_ping_pong_noise():
    """Suppress websocket keepalive chatter without muting the logger entirely."""
    log_filter = WebSocketKeepaliveFilter()

    assert not log_filter.filter(make_record("websockets.client", "% sent keepalive ping"))
    assert not log_filter.filter(make_record("websockets.server", "% received keepalive pong"))
    assert not log_filter.filter(make_record("websockets.client", "> PING 12 34"))
    assert not log_filter.filter(make_record("websockets.server", "< PONG 12 34"))


def test_websocket_keepalive_filter_keeps_other_debug_logs():
    """Keep non-keepalive websocket and application debug records visible."""
    log_filter = WebSocketKeepaliveFilter()

    assert log_filter.filter(make_record("websockets.client", "connected to upstream"))
    assert log_filter.filter(make_record("custom_components.ha_ocpp_relay", "> PING from app log"))