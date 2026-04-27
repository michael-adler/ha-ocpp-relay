"""Local relay supervisor and embedded relay/snoop websocket server implementations."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

from ..const import (
    CONF_CPMS_URL,
    CONF_RELAY_OCPP_HOST,
    CONF_RELAY_OCPP_PORT,
    CONF_RELAY_SNOOP_HOST,
    CONF_RELAY_SNOOP_PORT,
)
from .core import OCPPRelay, SnoopWebSocketServer

# Configuration errors should not trigger an infinite restart loop.
_CONFIG_ERRORS = (ValueError, KeyError, TypeError)


class LocalRelaySupervisor:
    def __init__(self, hass, config: dict[str, Any]) -> None:
        """Store HA context and configuration for embedded relay management."""
        self._hass = hass
        self._config = config
        self._logger = logging.getLogger(__name__)
        self._task: asyncio.Task | None = None

    async def async_start(self) -> None:
        """Start the supervisor loop that owns relay and snoop servers."""
        if self._task is None:
            self._task = self._hass.async_create_task(self._run())

    async def async_stop(self) -> None:
        """Stop the supervisor loop and wait for clean task shutdown."""
        if self._task is None:
            return
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def _run(self) -> None:
        """Run relay+snoop servers and restart them after unexpected crashes."""
        while True:
            relay_server = None
            snoop_server = None
            snoop_ws_server: SnoopWebSocketServer | None = None
            try:
                cpms_url = self._config.get(CONF_CPMS_URL)
                if not cpms_url:
                    raise ValueError("Local relay mode requires cpms_url")

                msg_queue: asyncio.Queue = asyncio.Queue()
                relay = OCPPRelay(cpms_url, snoop_queue=msg_queue)
                relay_server = await relay.start(
                    self._config[CONF_RELAY_OCPP_HOST],
                    self._config[CONF_RELAY_OCPP_PORT],
                )

                snoop_ws_server = SnoopWebSocketServer(snoop_queue=msg_queue)
                snoop_server = await snoop_ws_server.start(
                    self._config[CONF_RELAY_SNOOP_HOST],
                    self._config[CONF_RELAY_SNOOP_PORT],
                )

                await asyncio.gather(relay_server.wait_closed(), snoop_server.wait_closed())

            except asyncio.CancelledError:
                raise

            except _CONFIG_ERRORS as err:
                # Configuration errors are permanent — stop the loop instead of
                # retrying indefinitely and spamming the log every 15 seconds.
                self._logger.error(
                    "Local relay configuration error (will not retry): %s", err
                )
                return

            except Exception as err:  # noqa: BLE001
                self._logger.exception("Local relay crashed: %s. Restarting in 15 seconds.", err)
                await asyncio.sleep(15)

            finally:
                # Stop the fanout task BEFORE closing the underlying server sockets
                # so the forward loop cannot attempt sends after the server is gone.
                if snoop_ws_server is not None:
                    await snoop_ws_server.stop()

                # Only call close()+wait_closed() here; do NOT call wait_closed()
                # again when gather() already returned (i.e. the server closed on its own).
                # websockets Server.close() is idempotent, and wait_closed() after an already-
                # closed server returns immediately, so this is safe in all paths.
                if relay_server is not None:
                    relay_server.close()
                    with contextlib.suppress(Exception):
                        await relay_server.wait_closed()
                if snoop_server is not None:
                    snoop_server.close()
                    with contextlib.suppress(Exception):
                        await snoop_server.wait_closed()
