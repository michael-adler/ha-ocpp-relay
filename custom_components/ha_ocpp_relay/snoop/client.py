"""Websocket client that consumes relay snoop events and emits HA sensor updates."""

import asyncio
import json
import logging
from typing import Any

import websockets

from homeassistant.core import HomeAssistant
from homeassistant.helpers.dispatcher import async_dispatcher_send

from ..const import CONF_SNOOP_SOCKET, DOMAIN, SIGNAL_NEW_SENSOR, SIGNAL_SENSOR_UPDATE
from .models import OCPPSensorData
from .parser import OCPPFilter


class OCPPSnoopClient:
    """Background client consuming OCPP snoop websocket and pushing sensor updates."""

    def __init__(self, hass: HomeAssistant, entry_id: str, config: dict[str, Any]) -> None:
        self._hass = hass
        self._entry_id = entry_id
        self._logger = logging.getLogger(__name__)
        self._snoop_socket = config[CONF_SNOOP_SOCKET]
        self._filter = OCPPFilter()
        self._task: asyncio.Task | None = None

        self._sensors: dict[str, OCPPSensorData] = {}

    @property
    def sensors(self) -> dict[str, OCPPSensorData]:
        return self._sensors

    async def async_start(self) -> None:
        self._task = self._hass.async_create_task(self._run(), name=f"{DOMAIN}_{self._entry_id}")

    async def async_stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        self._logger.info("Connecting to relay snoop websocket at %s", self._snoop_socket)
        while True:
            try:
                async with websockets.connect(self._snoop_socket) as websocket:
                    async for message in websocket:
                        await self._handle_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                self._logger.warning("Snoop websocket error: %s. Reconnecting in 15 seconds.", err)
                await asyncio.sleep(15)

    async def _handle_message(self, message: str) -> None:
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
