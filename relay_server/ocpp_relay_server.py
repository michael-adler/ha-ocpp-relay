#!/usr/bin/env python3
"""CLI entrypoint that starts the standalone OCPP relay and snoop websocket server."""

import argparse
import asyncio
import ssl
import sys

from custom_components.ha_ocpp_relay.relay.core import OCPPRelay, SnoopWebSocketServer
from relay_server.cli.common import apply_yaml_section_defaults, configure_logging


def parse_args():
    """Parse CLI arguments and optional YAML defaults for relay startup."""
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

    # Parse once to discover config path, then apply YAML values as parser defaults.
    apply_yaml_section_defaults(parser, section="relay")

    args = parser.parse_args()
    if not args.cpms:
        print("CPMS URL must be set by --cpms or config file.", file=sys.stderr)
        sys.exit(1)
    return args


def get_ssl_context(ssl_cert, ssl_key):
    """Create TLS server context when both certificate and key are provided."""
    if not ssl_cert or not ssl_key:
        return None

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(certfile=ssl_cert, keyfile=ssl_key)
    return ssl_context


async def core(args):
    """Start relay and snoop servers and keep both running.

    The relay produces a shared message queue, and the snoop websocket server
    publishes that queue to external observers.
    """
    ssl_context = get_ssl_context(args.ssl_cert, args.ssl_key)
    msg_queue = asyncio.Queue()

    relay = OCPPRelay(args.cpms, snoop_queue=msg_queue)
    relay_server = await relay.start(args.ocpp_host, args.ocpp_port, ssl_context=ssl_context)

    snoop = SnoopWebSocketServer(snoop_queue=msg_queue)
    snoop_server = await snoop.start(
        args.snoop_host,
        args.snoop_port,
        # Keep localhost development simple; use TLS only for externally reachable snoop endpoint.
        ssl_context=(None if args.snoop_host == "localhost" else ssl_context),
    )

    await asyncio.gather(relay_server.wait_closed(), snoop_server.wait_closed())


def main():
    """Configure logging and run the relay server CLI entrypoint."""
    args = parse_args()

    configure_logging(
        app_name="ocpp-relay-server",
        verbose=args.verbose,
        quiet=args.quiet,
        use_syslog=args.syslog,
    )

    try:
        asyncio.run(core(args))
    except KeyboardInterrupt:
        print("Exiting...")


if __name__ == "__main__":
    main()
