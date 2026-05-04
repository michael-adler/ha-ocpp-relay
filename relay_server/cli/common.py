"""Shared helpers for relay_server CLI argument handling and logging setup."""

from __future__ import annotations

import argparse
import logging
from logging.handlers import SysLogHandler
import os
import sys
from typing import Any

import yaml


class WebSocketKeepaliveFilter(logging.Filter):
    """Drop noisy websocket ping/pong debug records while keeping other debug logs."""

    _suppressed_messages = (
        "% sent keepalive ping",
        "% received keepalive pong",
        "- timed out waiting for keepalive pong",
    )
    _suppressed_frame_prefixes = (
        "> PING",
        "< PING",
        "> PONG",
        "< PONG",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        """Return False for websocket keepalive and ping/pong frame debug logs."""
        if not record.name.startswith("websockets"):
            return True

        message = record.getMessage()
        if message in self._suppressed_messages:
            return False
        return not message.startswith(self._suppressed_frame_prefixes)


def install_websocket_keepalive_filter() -> None:
    """Attach the websocket keepalive filter to all configured root handlers."""
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if any(isinstance(existing_filter, WebSocketKeepaliveFilter) for existing_filter in handler.filters):
            continue
        handler.addFilter(WebSocketKeepaliveFilter())


def apply_yaml_section_defaults(
    parser: argparse.ArgumentParser,
    section: str,
    argv: list[str] | None = None,
) -> None:
    """Apply YAML section keys as parser defaults unless overridden on argv.

    This preserves CLI flag precedence while allowing a config file to populate
    omitted options.
    """
    argv = list(sys.argv[1:] if argv is None else argv)

    preliminary = parser.parse_known_args(argv)[0]
    config_path = getattr(preliminary, "config", None)
    if not config_path:
        return

    try:
        with open(config_path, "r", encoding="utf-8") as file_obj:
            cfg = yaml.safe_load(file_obj) or {}
    except FileNotFoundError:
        print(f"Config file not found: {config_path}", file=sys.stderr)
        raise SystemExit(1)
    except Exception as err:  # noqa: BLE001
        print(f"Error loading config file {config_path}: {err}", file=sys.stderr)
        raise SystemExit(1)

    defaults = cfg.get(section, {}) if isinstance(cfg, dict) else {}
    if not isinstance(defaults, dict):
        print(f"Invalid '{section}' section in config file: expected mapping", file=sys.stderr)
        raise SystemExit(1)

    # Determine which dest names were explicitly provided on the command line so
    # that YAML values do not override them.  A raw substring search on argv
    # tokens is unreliable for boolean store_true/store_false flags: e.g. if the
    # user passes --quiet, "--verbose" is simply absent from argv so a YAML
    # verbose:true default would still be applied.  Instead, match each action's
    # option strings directly against the argv list.
    cli_supplied = {
        action.dest
        for action in parser._actions
        if any(opt in argv for opt in action.option_strings)
    }

    for key, value in defaults.items():
        if key not in cli_supplied:
            parser.set_defaults(**{key: value})


def configure_logging(*, app_name: str, verbose: bool, quiet: bool, use_syslog: bool) -> None:
    """Configure consistent logging for relay_server command entrypoints."""
    level = logging.DEBUG if verbose else logging.INFO
    if quiet:
        level = logging.WARNING

    default_format = (
        "%(asctime)s - [%(levelname)-4.4s] - [%(threadName)-7.7s] - "
        "[%(name)-20.20s] - %(message)s"
    )

    if not use_syslog:
        logging.basicConfig(level=level, format=default_format)
        install_websocket_keepalive_filter()
        return

    address: str | tuple[str, int]
    address = "/dev/log" if os.path.exists("/dev/log") else ("localhost", 514)

    try:
        handler = SysLogHandler(address=address, facility=SysLogHandler.LOG_LOCAL0)
        logging.basicConfig(
            level=level,
            handlers=[handler],
            format=f"{app_name}: %(levelname)s - %(threadName)s - %(name)s - %(message)s",
        )
        install_websocket_keepalive_filter()
    except Exception:  # noqa: BLE001
        logging.basicConfig(level=level, format=default_format)
        install_websocket_keepalive_filter()
