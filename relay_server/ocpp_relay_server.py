#!/usr/bin/env python3
"""CLI entrypoint that starts the standalone OCPP relay and snoop websocket server."""

import argparse
import asyncio
import ssl
import sys

from custom_components.ha_ocpp_relay.relay.core import OCPPRelay, SnoopWebSocketServer, SNOOP_QUEUE_MAXSIZE
from relay_server.cli.common import apply_yaml_section_defaults, configure_logging


def parse_args():
    """Parse CLI arguments and optional YAML defaults for relay startup."""
    parser = argparse.ArgumentParser(description="Relay OCPP traffic between a charge point and a CSMS.")
    parser.add_argument(
        "--config", type=str, default=None,
        help="""Path to YAML config file. Values in a 'relay' section will be used as defaults."""
    )
    parser.add_argument(
        "--cpms", type=str, default=None,
        help="""URL of the real CPMS."""
    )
    parser.add_argument(
        "--ocpp-host", type=str, default=None,
        help="""OCPP relay server interface address (default: all interfaces)."""
    )
    parser.add_argument(
        "--ocpp-port", type=int, default=8500,
        help="""OCPP relay server port for charge point connections (default: %(default)d)."""
    )
    parser.add_argument(
        "--snoop-host", type=str, default="localhost",
        help="""Snoop server interface address (default: %(default)s)."""
    )
    parser.add_argument(
        "--snoop-port", type=int, default=8501,
        help="""Snoop server port for clients that monitor OCPP traffic (default: %(default)d)."""
    )
    parser.add_argument(
        "--ssl-cert", default=None,
        help="""Path to SSL certificate file (default: None). Some chargers don't store trust chains and require that certificates are loaded onto the charger explicitly."""
    )
    parser.add_argument(
        "--ssl-key", default=None,
        help="""Path to SSL private key file (default: None)."""
    )
    parser.add_argument(
        "--csms-ca-cert", default=None,
        help="""Path to a CA certificate (or bundle) file used to verify the upstream CSMS
TLS certificate (default: None, uses the system CA bundle). Set this when your CSMS
uses a private or self-signed certificate authority."""
    )
    parser.add_argument(
        "--syslog", action="store_true",
        help="""Write logs to syslog instead of stdout."""
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-v", "--verbose", action="store_true",
        help="""Verbose output."""
    )
    group.add_argument(
        "-q", "--quiet", action="store_true",
        help="""Reduce output."""
    )

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
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
    ssl_context.load_cert_chain(certfile=ssl_cert, keyfile=ssl_key)
    return ssl_context


def get_csms_ssl_context(csms_ca_cert: str | None) -> ssl.SSLContext | None:
    """Create a TLS client context for the upstream CSMS connection.

    When *csms_ca_cert* is provided the given CA file is loaded instead of
    the system bundle.  When it is *None* and the CSMS URL starts with
    ``wss://``, ``websockets`` will use the default context (system CA bundle,
    verification enabled), so returning *None* is safe for the common case.
    We only build an explicit context when a custom CA is supplied so we can
    also enforce a TLS 1.2 minimum without touching the default context.
    """
    if not csms_ca_cert:
        return None

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.load_verify_locations(cafile=csms_ca_cert)
    return ctx


async def core(args):
    """Start relay and snoop servers and keep both running.

    The relay produces a shared message queue, and the snoop websocket server
    publishes that queue to external observers.
    """
    ssl_context = get_ssl_context(args.ssl_cert, args.ssl_key)
    csms_ssl_context = get_csms_ssl_context(args.csms_ca_cert)
    msg_queue = asyncio.Queue(maxsize=SNOOP_QUEUE_MAXSIZE)

    relay = OCPPRelay(args.cpms, snoop_queue=msg_queue, csms_ssl_context=csms_ssl_context)
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
