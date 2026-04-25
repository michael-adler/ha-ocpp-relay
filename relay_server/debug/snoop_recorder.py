#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import asyncio
import json
import logging

import websockets


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
        print(f"Connection closed: {err.code} ({err.reason})")
    except ConnectionRefusedError:
        print("Connection refused. Is the server running?")
    except Exception as err:  # noqa: BLE001
        print(f"An unexpected error occurred: {err}")


def main():
    """Parse CLI settings and run the snoop recorder utility."""
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - [%(levelname)-4.4s] - [%(threadName)-7.7s] - [%(name)-20.20s] - %(message)s",
    )

    try:
        asyncio.run(receive_forever(args))
    except KeyboardInterrupt:
        print("Exiting...")


if __name__ == "__main__":
    main()
