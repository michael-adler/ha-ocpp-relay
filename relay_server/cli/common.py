"""Shared helpers for relay_server CLI argument handling and logging setup."""

from __future__ import annotations

import argparse
import logging
from logging.handlers import SysLogHandler
import os
import sys
from typing import Any

import yaml


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

    for key, value in defaults.items():
        argname = f"--{key.replace('_', '-')}"
        if not any(argname in arg for arg in argv):
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
    except Exception:  # noqa: BLE001
        logging.basicConfig(level=level, format=default_format)
