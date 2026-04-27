"""Websocket client that consumes relay snoop events and emits HA sensor updates."""

import asyncio
import json
import logging
import random
from typing import Any

import websockets

from homeassistant.core import HomeAssistant
from homeassistant.helpers.dispatcher import async_dispatcher_send

from ..const import CONF_SNOOP_SOCKET, DOMAIN, SIGNAL_NEW_SENSOR, SIGNAL_SENSOR_UPDATE
from .models import OCPPSensorData
from .parser import OCPPFilter

# Reconnect base delay and jitter range (seconds).
_RECONNECT_BASE = 15
_RECONNECT_JITTER = 5


class OCPPSnoopClient:
    """Background client consuming OCPP snoop websocket and pushing sensor updates."""

    def __init__(self, hass: HomeAssistant, entry_id: str, config: dict[str, Any]) -> None:
        """Initialize client state for one integration entry.

        Each entry owns one snoop stream consumer so sensors remain isolated by
        config entry ID.
        """
        self._hass = hass
        self._entry_id = entry_id
        self._logger = logging.getLogger(__name__)
        self._snoop_socket = config[CONF_SNOOP_SOCKET]
        self._filter = OCPPFilter()
        self._task: asyncio.Task | None = None

        self._sensors: dict[str, OCPPSensorData] = {}

    @property
    def sensors(self) -> dict[str, OCPPSensorData]:
        """Expose the latest sensor snapshot indexed by unique ID."""
        return self._sensors

    async def async_start(self) -> None:
        """Start the long-running websocket consumer task."""
        self._task = self._hass.async_create_task(self._run(), name=f"{DOMAIN}_{self._entry_id}")

    async def async_stop(self) -> None:
        """Cancel and await the websocket consumer task."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        """Maintain a resilient connection to the relay snoop websocket.

        The loop reconnects with backoff on transient network/server failures.
        Add random jitter to the retry delay so this loop does not
        lockstep with the relay supervisor's own 15-second restart cycle.
        """
        self._logger.info("Connecting to relay snoop websocket at %s", self._snoop_socket)
        while True:
            try:
                async with websockets.connect(self._snoop_socket) as websocket:
                    async for message in websocket:
                        await self._handle_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                delay = _RECONNECT_BASE + random.uniform(0, _RECONNECT_JITTER)
                self._logger.warning(
                    "Snoop websocket error: %s. Reconnecting in %.1f seconds.", err, delay
                )
                await asyncio.sleep(delay)

    async def _handle_message(self, message: str) -> None:
        """Decode one snoop event and fan out resulting sensor updates."""
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            self._logger.debug("Skipping invalid JSON message")
            return

        sensors = self._filter.filter(data)
        if not sensors:
            return

        for sensor in sensors:
            self._update_sensor(sensor)

    def _update_sensor(self, sensor: OCPPSensorData) -> None:
        """Store sensor state and notify entity creation/update listeners."""
        is_new = sensor.unique_id not in self._sensors
        self._sensors[sensor.unique_id] = sensor

        if is_new:
            async_dispatcher_send(
                self._hass,
                f"{SIGNAL_NEW_SENSOR}_{self._entry_id}",
                sensor.unique_id,
            )

        async_dispatcher_send(
            self._hass,
            f"{SIGNAL_SENSOR_UPDATE}_{self._entry_id}",
            sensor.unique_id,
        )
