"""Websocket snoop broadcaster that publishes captured relay message events to clients."""

import json
import logging

import websockets

from relay_server.common.types import MessageData


async def receive_ocpp_snoop(ws_uri: str):
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
        except websockets.exceptions.ConnectionClosed as err:
            logger.warning("Snoop connection closed (%s: %s). Retrying...", err.code, err.reason)
