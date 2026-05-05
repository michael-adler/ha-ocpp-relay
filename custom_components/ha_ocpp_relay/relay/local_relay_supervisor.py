"""Local relay supervisor and embedded relay/snoop websocket server implementations."""

from __future__ import annotations

import asyncio
import contextlib
import errno
import logging
from typing import Any

from ..const import (
    CONF_CPMS_URL,
    CONF_RELAY_OCPP_HOST,
    CONF_RELAY_OCPP_PORT,
    CONF_RELAY_SNOOP_HOST,
    CONF_RELAY_SNOOP_PORT,
)
from .core import OCPPRelay, SnoopWebSocketServer, SNOOP_QUEUE_MAXSIZE

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
            self._task = self._hass.async_create_background_task(self._run(), name="ocpp_relay_supervisor")

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
        consecutive_eaddrinuse = 0
        while True:
            restart_delay = 0
            relay_server = None
            snoop_server = None
            snoop_ws_server: SnoopWebSocketServer | None = None
            try:
                cpms_url = self._config.get(CONF_CPMS_URL)
                if not cpms_url:
                    raise ValueError("Local relay mode requires cpms_url")

                msg_queue: asyncio.Queue = asyncio.Queue(maxsize=SNOOP_QUEUE_MAXSIZE)
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
                consecutive_eaddrinuse = 0

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

            except OSError as err:
                if err.errno == errno.EADDRINUSE:
                    consecutive_eaddrinuse += 1
                    relay_host = self._config.get(CONF_RELAY_OCPP_HOST)
                    relay_port = self._config.get(CONF_RELAY_OCPP_PORT)
                    snoop_host = self._config.get(CONF_RELAY_SNOOP_HOST)
                    snoop_port = self._config.get(CONF_RELAY_SNOOP_PORT)
                    if consecutive_eaddrinuse > 3:
                        self._logger.error(
                            "Local relay port conflict (giving up after %d attempts): %s. "
                            "Check for another process using %s:%s or %s:%s, "
                            "or change relay ports in integration options.",
                            consecutive_eaddrinuse,
                            err,
                            relay_host,
                            relay_port,
                            snoop_host,
                            snoop_port,
                        )
                        return
                    self._logger.warning(
                        "Local relay port in use (attempt %d/3): %s. "
                        "Retrying in 5 seconds (ports %s:%s, %s:%s).",
                        consecutive_eaddrinuse,
                        err,
                        relay_host,
                        relay_port,
                        snoop_host,
                        snoop_port,
                    )
                    restart_delay = 5
                else:
                    self._logger.exception("Local relay crashed: %s. Restarting in 15 seconds.", err)
                    restart_delay = 15

            except Exception as err:  # noqa: BLE001
                self._logger.exception("Local relay crashed: %s. Restarting in 15 seconds.", err)
                restart_delay = 15

            finally:
                # Initiate server closes first so the underlying asyncio server
                # sockets are released even if subsequent async cleanup steps are
                # interrupted.  In websockets >= 14, Server.close() schedules an
                # async _close() task; calling close() here ensures that task is
                # queued before any await that might raise.
                if relay_server is not None:
                    relay_server.close()
                if snoop_server is not None:
                    snoop_server.close()

                # Stop the snoop fanout task and drain connected snoop clients.
                if snoop_ws_server is not None:
                    with contextlib.suppress(Exception):
                        await snoop_ws_server.stop()

                # Wait for servers to fully close (ports released).  Use
                # suppress(BaseException) so a re-delivered CancelledError in the
                # finally block does not skip the snoop server wait.
                if relay_server is not None:
                    with contextlib.suppress(BaseException):
                        await relay_server.wait_closed()
                if snoop_server is not None:
                    with contextlib.suppress(BaseException):
                        await snoop_server.wait_closed()

            if restart_delay > 0:
                await asyncio.sleep(restart_delay)
