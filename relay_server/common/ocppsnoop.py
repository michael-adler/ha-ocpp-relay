"""Websocket snoop broadcaster that publishes captured relay message events to clients."""

import json
import logging

import websockets

from relay_server.common.types import MessageData


async def receive_ocpp_snoop(ws_uri: str, exit_on_disconnect: bool = False):
    """Yield MessageData objects from the relay snoop websocket."""

    logger = logging.getLogger(__name__)
    logger.info("Connecting to %s", ws_uri)

    async for websocket in websockets.connect(ws_uri):
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    yield MessageData(**data)
                except json.JSONDecodeError as err:
                    logger.error("Error decoding JSON: %s", err)
        except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedOK) as err:
            logger.warning("Snoop connection closed (%s: %s). Retrying...", getattr(err, 'code', None), getattr(err, 'reason', None))
            if exit_on_disconnect:
                logger.info("Exiting receive_ocpp_snoop due to exit_on_disconnect flag")
                return

        # If the inner read loop ended without an exception it means the connection
        # closed cleanly; respect exit_on_disconnect in that case too.
        if exit_on_disconnect:
            logger.info("Snoop connection closed (normal); exiting due to exit_on_disconnect flag")
            return


def receive_ocpp_from_file(file_path: str):
    """Yield MessageData objects from a JSON-lines file."""

    logger = logging.getLogger(__name__)
    logger.info("Reading OCPP messages from %s", file_path)
    try:
        with open(file_path, "r", encoding="utf-8") as infile:
            for line in infile:
                try:
                    data = json.loads(line)
                    yield MessageData(**data)
                except json.JSONDecodeError as err:
                    logger.error("Error decoding JSON: %s", err)
    except FileNotFoundError:
        logger.error("File not found: %s", file_path)
    except Exception as err:  # noqa: BLE001
        logger.error("Unexpected error reading %s: %s", file_path, err)
