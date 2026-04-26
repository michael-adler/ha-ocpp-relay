#!/usr/bin/env python3
"""
CLI tool to playback OCPP snoop logs to a snoop WebSocket server for testing/CI.

Streams the contents of a JSON log file (as written by snoop-recorder) to any client that connects to the snoop port.
"""
import argparse
import asyncio
import json
import sys
from custom_components.ha_ocpp_relay.relay.core import SnoopWebSocketServer
from custom_components.ha_ocpp_relay.shared.models import MessageData
from relay_server.cli.common import configure_logging


def parse_args():
    parser = argparse.ArgumentParser(description="""
        Debugging script that takes the place of the relay server. It implements only the snoop
        port. When a client connects, the script streams the contents of an OCPP transaction log file
        (as written by snoop-recorder). No relay functionality is implemented.""")
    parser.add_argument("ocpp_log_file", type=str, help="Path to OCPP transaction log JSON file")
    parser.add_argument("--snoop-host", type=str, default="localhost", help="Snoop server interface address (default: %(default)s)")
    parser.add_argument("--snoop-port", type=int, default=8501, help="Snoop server port for clients (default: %(default)d)")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-v", "--verbose", action="store_true", help="Verbose output.")
    group.add_argument("-q", "--quiet", action="store_true", help="Reduce output.")
    return parser.parse_args()


async def stream_log_to_clients(snoop_server, log_lines):
    """Whenever a client connects, stream the log lines to it."""
    import logging
    logger = logging.getLogger("ocpp-playback-server")
    # Push each recorded MessageData into the server queue so it is broadcast
    logger.info("Starting playback to snoop server (%d lines)", len(log_lines))

    # Wait for at least one client to connect so they receive the playback stream.
    # This avoids the playback completing before a client has a chance to connect.
    wait_start = asyncio.get_running_loop().time()
    wait_timeout = 10.0
    while not getattr(snoop_server, "_snoop_sockets", set()):
        if asyncio.get_running_loop().time() - wait_start > wait_timeout:
            logger.warning("No snoop clients connected after %.1fs; proceeding anyway", wait_timeout)
            break
        await asyncio.sleep(0.05)

    for line in log_lines:
        try:
            msg = json.loads(line)
            # Convert to MessageData dataclass used by the server
            try:
                md = MessageData(**msg)
            except Exception:
                # If the record already matches the expected structure loosely,
                # fall back to using raw dict payload inside a MessageData wrapper.
                md = MessageData(event="Message", sender="CP", payload=msg)

            await snoop_server._snoop_queue.put(md)
            await asyncio.sleep(0)  # yield to allow broadcast task to run
        except Exception as e:
            logger.error(f"Error enqueuing playback message: {e}")

    # Allow some time for broadcasts to be delivered, then politely close client sockets.
    # Increase grace period so clients can complete any pending reads/handshakes.
    await asyncio.sleep(1.0)
    logger.info("Playback finished, closing snoop client sockets")
    for ws in list(getattr(snoop_server, "_snoop_sockets", [])):
        try:
            # Only attempt an orderly close for sockets that reached OPEN state.
            if getattr(ws, "open", False):
                # Request a polite close (1000 = normal) and wait briefly for acknowledgement.
                try:
                    await ws.close(code=1000, reason="playback finished")
                except TypeError:
                    # Older websockets versions may not accept code/reason params.
                    await ws.close()

                # Wait for the TCP/websocket close to complete, but don't block indefinitely.
                wait_coro = getattr(ws, "wait_closed", None)
                if callable(wait_coro):
                    try:
                        await asyncio.wait_for(wait_coro(), timeout=1.0)
                    except Exception:
                        # Fall through and discard socket below
                        pass
            else:
                logger.debug("Snoop socket not open, skipping polite close")
        except Exception:
            pass


async def main_async(args):
    # Read all log lines into memory (for simplicity)
    try:
        with open(args.ocpp_log_file, "r", encoding="utf-8") as f:
            log_lines = f.readlines()
    except Exception as e:
        print(f"Failed to read log file: {e}", file=sys.stderr)
        sys.exit(1)

    snoop_server = SnoopWebSocketServer(snoop_queue=asyncio.Queue())  # Provide a queue for the server
    await snoop_server.start(args.snoop_host, args.snoop_port, ssl_context=None)
    await stream_log_to_clients(snoop_server, log_lines)


def main():
    args = parse_args()
    configure_logging(
        app_name="ocpp-playback-server",
        verbose=args.verbose,
        quiet=args.quiet,
        use_syslog=False,
    )
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("Exiting...")


if __name__ == "__main__":
    main()
