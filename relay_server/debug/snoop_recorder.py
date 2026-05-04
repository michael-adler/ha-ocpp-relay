#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import asyncio
import json
import logging

import websockets

from relay_server.cli.common import configure_logging


def parse_args():
    """Parse command line arguments."""
    msg = """
Record a stream of OCPP snoop messages to a file.
"""

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Record the OCPP snoop stream for playback later.",
        epilog=msg,
    )

    parser.add_argument(
        "--snoop-socket",
        type=str,
        default="ws://localhost:8501/",
        help="""URL of the OCPP relay's snoop port (default: %(default)s).""",
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default="output.json",
        help="""Output file (default: %(default)s).""",
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true", help="Verbose output.")
    group.add_argument("-q", "--quiet", action="store_true", help="Reduce output.")

    return parser.parse_args()


async def receive_forever(args):
    """Subscribe to snoop stream and append each message as JSONL."""
    uri = args.snoop_socket
    print(f"Connecting to {uri}...")
    try:
        with open(args.output, "w", encoding="utf-8") as outfile:
            async with websockets.connect(uri) as websocket:
                print("Connection established. Waiting for messages...")
                async for message in websocket:
                    json_msg = json.loads(message)
                    print(f"Client receives: < {json.dumps(json_msg, indent=2)}")
                    outfile.write(json.dumps(json_msg) + "\n")
                    outfile.flush()
    except websockets.exceptions.ConnectionClosed as err:
        # err.rcvd is None when the connection dropped without a close frame.
        code = err.rcvd.code if err.rcvd else "no close frame"
        reason = err.rcvd.reason if err.rcvd else ""
        print(f"Connection closed: {code} ({reason})")
    except ConnectionRefusedError:
        print("Connection refused. Is the server running?")
    except Exception as err:  # noqa: BLE001
        print(f"An unexpected error occurred: {err}")


def main():
    """Parse CLI settings and run the snoop recorder utility."""
    args = parse_args()

    configure_logging(
        app_name="ocpp-snoop-recorder",
        verbose=args.verbose,
        quiet=args.quiet,
        use_syslog=False,
    )

    try:
        asyncio.run(receive_forever(args))
    except KeyboardInterrupt:
        print("Exiting...")


if __name__ == "__main__":
    main()
