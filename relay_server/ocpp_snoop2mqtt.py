#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import asyncio
import logging
from logging.handlers import SysLogHandler
import os
import sys

import yaml

from relay_server.common.ocppfilter import OCPPFilter
from relay_server.common.ocppsnoop import receive_ocpp_snoop
from relay_server.mqtt.mqttpublish import MQTTPublisher


def parse_args():
    """Parse command line arguments."""
    msg = """
Map a stream of OCPP messages to MQTT topics.
"""

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Map OCPP metering data from a charge point to MQTT topics.",
        epilog=msg,
    )

    parser.add_argument(
        "--snoop-socket",
        type=str,
        default="ws://localhost:8501/",
        help="""URL of the OCPP relay's snoop port (default: %(default)s).""",
    )

    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="""Path to YAML config file. Values in a 'snoop2mqtt' section will be used as defaults.""",
    )

    parser.add_argument(
        "--mqtt-broker-host",
        type=str,
        default="localhost",
        help="""Hostname or IP address of the MQTT broker (default: %(default)s).""",
    )
    parser.add_argument(
        "--mqtt-broker-port",
        type=int,
        default=1883,
        help="""Port of the MQTT broker (default: %(default)s).""",
    )
    parser.add_argument(
        "--mqtt-broker-username",
        type=str,
        default=None,
        help="""Username for MQTT broker authentication (default: %(default)s).""",
    )
    parser.add_argument(
        "--mqtt-broker-password",
        type=str,
        default=None,
        help="""Password for MQTT broker authentication.""",
    )

    parser.add_argument("--syslog", action="store_true", help="Write logs to syslog instead of stdout")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true", help="""Verbose output.""")
    group.add_argument("-q", "--quiet", action="store_true", help="""Reduce output.""")

    preliminary = parser.parse_known_args()[0]

    yaml_defaults = {}
    if preliminary and preliminary.config:
        try:
            with open(preliminary.config, "r", encoding="utf-8") as file_obj:
                cfg = yaml.safe_load(file_obj) or {}
                yaml_defaults = cfg.get("snoop2mqtt", {}) if isinstance(cfg, dict) else {}
        except FileNotFoundError:
            print(f"Config file not found: {preliminary.config}", file=sys.stderr)
            sys.exit(1)
        except Exception as err:  # noqa: BLE001
            print(f"Error loading config file {preliminary.config}: {err}", file=sys.stderr)
            sys.exit(1)

    for key, val in yaml_defaults.items():
        argname = f"--{key.replace('_', '-')}"
        if not any(argname in arg for arg in sys.argv[1:]):
            parser.set_defaults(**{key: val})

    return parser.parse_args()


async def process_messages(publisher: MQTTPublisher, snoop_socket: str):
    """Consume snoop stream, map OCPP frames, and enqueue MQTT publications."""
    logger = logging.getLogger(__name__)
    ocpp_filter = OCPPFilter()

    async for msg in receive_ocpp_snoop(ws_uri=snoop_socket):
        filtered = ocpp_filter.filter(msg)
        if filtered:
            for message in filtered:
                logger.info("Handle message: %s", message)
                await publisher.publish_data(message)

    logger.info("Message source closed. Stopping publisher...")
    publisher.stop()


async def core(args):
    """Run publisher loop and snoop-consumer loop as one pipeline."""
    publisher = MQTTPublisher(
        broker_host=args.mqtt_broker_host,
        broker_port=args.mqtt_broker_port,
        broker_username=args.mqtt_broker_username,
        broker_password=args.mqtt_broker_password,
    )

    await asyncio.gather(
        publisher.start(),
        process_messages(publisher, args.snoop_socket),
    )


def main():
    """Parse arguments, initialize logging, and run snoop-to-MQTT bridge."""
    args = parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    if args.quiet:
        level = logging.WARNING

    if args.syslog:
        address = "/dev/log" if os.path.exists("/dev/log") else ("localhost", 514)
        try:
            handler = SysLogHandler(address=address, facility=SysLogHandler.LOG_LOCAL0)
            logging.basicConfig(
                level=level,
                handlers=[handler],
                format="ocpp-snoop2mqtt: %(levelname)s - %(threadName)s - %(name)s - %(message)s",
            )
        except Exception:  # noqa: BLE001
            logging.basicConfig(
                level=level,
                format="%(asctime)s - [%(levelname)-4.4s] - [%(threadName)-7.7s] - [%(name)-20.20s] - %(message)s",
            )
    else:
        logging.basicConfig(
            level=level,
            format="%(asctime)s - [%(levelname)-4.4s] - [%(threadName)-7.7s] - [%(name)-20.20s] - %(message)s",
        )

    try:
        asyncio.run(core(args))
    except KeyboardInterrupt:
        print("Exiting...")
        sys.exit(1)


if __name__ == "__main__":
    main()
