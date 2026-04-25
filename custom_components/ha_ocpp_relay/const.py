"""Constants and default values used by the HA OCPP Relay integration."""

DOMAIN = "ha_ocpp_relay"
PLATFORMS = ["sensor"]

CONF_SNOOP_SOCKET = "snoop_socket"
CONF_RELAY_IS_LOCAL = "relay_is_local"
CONF_CPMS_URL = "cpms_url"
CONF_RELAY_OCPP_HOST = "relay_ocpp_host"
CONF_RELAY_OCPP_PORT = "relay_ocpp_port"
CONF_RELAY_SNOOP_HOST = "relay_snoop_host"
CONF_RELAY_SNOOP_PORT = "relay_snoop_port"

DEFAULT_RELAY_IS_LOCAL = True
DEFAULT_RELAY_OCPP_HOST = "0.0.0.0"
DEFAULT_RELAY_OCPP_PORT = 8500
DEFAULT_RELAY_SNOOP_HOST = "127.0.0.1"
DEFAULT_RELAY_SNOOP_PORT = 8501
DEFAULT_SNOOP_SOCKET = "ws://127.0.0.1:8501/"

SIGNAL_NEW_SENSOR = "ha_ocpp_relay_new_sensor"
SIGNAL_SENSOR_UPDATE = "ha_ocpp_relay_sensor_update"


def default_snoop_socket_for_container(port: int) -> str:
    """Build the default local snoop websocket URL from a configured port."""
    return f"ws://127.0.0.1:{port}/"


def normalize_relay_config(config: dict) -> dict:
    """Return config with defaults applied and local-mode invariants enforced."""
    normalized = dict(config)

    normalized.setdefault(CONF_RELAY_IS_LOCAL, DEFAULT_RELAY_IS_LOCAL)
    normalized.setdefault(CONF_RELAY_OCPP_HOST, DEFAULT_RELAY_OCPP_HOST)
    normalized.setdefault(CONF_RELAY_OCPP_PORT, DEFAULT_RELAY_OCPP_PORT)
    normalized.setdefault(CONF_RELAY_SNOOP_HOST, DEFAULT_RELAY_SNOOP_HOST)
    normalized.setdefault(CONF_RELAY_SNOOP_PORT, DEFAULT_RELAY_SNOOP_PORT)
    normalized.setdefault(CONF_CPMS_URL, "")

    if normalized[CONF_RELAY_IS_LOCAL]:
        normalized[CONF_RELAY_SNOOP_HOST] = DEFAULT_RELAY_SNOOP_HOST
        normalized[CONF_SNOOP_SOCKET] = default_snoop_socket_for_container(
            normalized[CONF_RELAY_SNOOP_PORT]
        )
    elif not normalized.get(CONF_SNOOP_SOCKET):
        normalized[CONF_SNOOP_SOCKET] = default_snoop_socket_for_container(
            normalized[CONF_RELAY_SNOOP_PORT]
        )

    return normalized
