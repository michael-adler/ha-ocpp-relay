#!/usr/bin/env python3

import argparse
import asyncio
import logging
from logging.handlers import SysLogHandler
import os
import ssl
import sys

import yaml

from relay_server.relay.ocpprelay import OCPPRelay
from relay_server.relay.snoopws import SnoopWebSocketServer


def parse_args():
    parser = argparse.ArgumentParser(description="Relay OCPP traffic between a charge point and a CSMS.")
    parser.add_argument("--config", type=str, default=None)
    parser.add_argument("--cpms", type=str, default=None)
    parser.add_argument("--ocpp-host", type=str, default=None)
    parser.add_argument("--ocpp-port", type=int, default=8500)
    parser.add_argument("--snoop-host", type=str, default="localhost")
    parser.add_argument("--snoop-port", type=int, default=8501)
    parser.add_argument("--ssl-cert", default=None)
    parser.add_argument("--ssl-key", default=None)
    parser.add_argument("--syslog", action="store_true")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true")
    group.add_argument("-q", "--quiet", action="store_true")

    preliminary = parser.parse_known_args()[0]
    yaml_defaults = {}
    if preliminary.config:
        with open(preliminary.config, "r", encoding="utf-8") as file_obj:
            cfg = yaml.safe_load(file_obj) or {}
            yaml_defaults = cfg.get("relay", {}) if isinstance(cfg, dict) else {}

    for key, value in yaml_defaults.items():
        argname = f"--{key.replace('_', '-')}"
        if not any(argname in arg for arg in sys.argv[1:]):
            parser.set_defaults(**{key: value})

    args = parser.parse_args()
    if not args.cpms:
        print("CPMS URL must be set by --cpms or config file.", file=sys.stderr)
        sys.exit(1)
    return args


def get_ssl_context(ssl_cert, ssl_key):
    if not ssl_cert or not ssl_key:
        return None

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(certfile=ssl_cert, keyfile=ssl_key)
    return ssl_context


async def core(args):
    ssl_context = get_ssl_context(args.ssl_cert, args.ssl_key)
    msg_queue = asyncio.Queue()

    relay = OCPPRelay(args.cpms, snoop_queue=msg_queue)
    relay_server = await relay.start(args.ocpp_host, args.ocpp_port, ssl_context=ssl_context)

    snoop = SnoopWebSocketServer(snoop_queue=msg_queue)
    snoop_server = await snoop.start(
        args.snoop_host,
        args.snoop_port,
        ssl_context=(None if args.snoop_host == "localhost" else ssl_context),
    )

    await asyncio.gather(relay_server.wait_closed(), snoop_server.wait_closed())


def main():
    args = parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    if args.quiet:
        level = logging.WARNING

    if args.syslog:
        address = "/dev/log" if os.path.exists("/dev/log") else ("localhost", 514)
        handler = SysLogHandler(address=address, facility=SysLogHandler.LOG_LOCAL0)
        logging.basicConfig(
            level=level,
            handlers=[handler],
            format="ocpp-relay-server: %(levelname)s - %(threadName)s - %(name)s - %(message)s",
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


if __name__ == "__main__":
    main()
