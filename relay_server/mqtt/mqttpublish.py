# -*- coding: utf-8 -*-

import asyncio
import json
import logging

import paho.mqtt.client as mqtt_client

from relay_server.common.types import SensorData

# Maximum number of sensor readings buffered before the oldest are dropped.
# Prevents unbounded memory growth when the MQTT broker is slow or unreachable.
_MQTT_QUEUE_MAXSIZE = 1000


class MQTTPublisher:
    """Publish filtered relay snoop data to MQTT topics and HA discovery."""

    def __init__(
        self,
        broker_host: str,
        broker_port: int = 1883,
        broker_username: str | None = None,
        broker_password: str | None = None,
        topic_prefix: str = "homeassistant",
        origin_name: str = "ocpp2mqtt",
    ):
        self._logger = logging.getLogger(__name__)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=_MQTT_QUEUE_MAXSIZE)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._connected_event = asyncio.Event()
        self._broker_host = broker_host
        self._broker_port = broker_port
        self._broker_username = broker_username
        self._broker_password = broker_password
        self._topic_prefix = topic_prefix
        self._origin_name = origin_name
        self._exit_task = False
        self._connected = False

        self._mqtt = mqtt_client.Client()
        self._mqtt.connect_timeout = 60.0
        self._mqtt.on_connect = self._mqtt_on_connect
        self._mqtt.on_connect_fail = self._mqtt_on_connect_fail
        self._mqtt.on_disconnect = self._mqtt_on_disconnect
        self._mqtt.on_message = self._mqtt_on_message

        self._published_discoveries: dict[str, dict] = {}

    async def publish_data(self, data: SensorData):
        """Queue one message for publication to MQTT.

        If the queue is full the message is dropped and a warning is logged
        rather than blocking the caller indefinitely.
        """
        try:
            self._queue.put_nowait(data)
        except asyncio.QueueFull:
            self._logger.warning(
                "MQTT publish queue full (%d items); dropping sensor data for %s",
                self._queue.maxsize,
                getattr(data, "cp_id", "unknown"),
            )

    async def run(self):
        """Consume queue items and publish to MQTT until stopped."""
        last_msg_info = None

        await self._connected_event.wait()

        loop = asyncio.get_running_loop()

        while True:
            try:
                data: SensorData = await asyncio.wait_for(self._queue.get(), timeout=5.0)
                self._logger.debug("Publishing %s", data)

                if not self._connected:
                    await self._connected_event.wait()

                disc_info = self._mqtt_discover(data)
                if disc_info:
                    # wait_for_publish() is a blocking paho-mqtt call; run it in
                    # a thread-pool executor so it does not stall the event loop.
                    await loop.run_in_executor(None, disc_info.wait_for_publish)
                    await asyncio.sleep(2)

                last_msg_info = self._mqtt_publish_data(data)
                self._queue.task_done()
            except asyncio.TimeoutError:
                pass

            if self._exit_task and self._queue.empty():
                break

        if last_msg_info:
            await loop.run_in_executor(None, last_msg_info.wait_for_publish)
        self._mqtt.disconnect()
        self._mqtt.loop_stop()
        self._logger.info("MQTTPublisher run() task exiting.")
        self._exit_task = False

    async def start(self):
        """Connect to MQTT and start async publication loop."""
        self._logger.info("Connecting to MQTT broker at %s:%s...", self._broker_host, self._broker_port)
        self._loop = asyncio.get_running_loop()
        self._connected_event.clear()

        self._mqtt.username_pw_set(username=self._broker_username, password=self._broker_password)
        self._mqtt.loop_start()
        self._mqtt.connect_async(self._broker_host, self._broker_port)
        await self.run()

    def stop(self):
        """Request graceful shutdown once pending publishes are flushed."""
        self._logger.info("Stopping MQTTPublisher...")
        self._exit_task = True

    def _mqtt_rediscover(self):
        """Re-publish all retained discovery payloads after reconnect."""
        for topic, discover in self._published_discoveries.items():
            self._logger.info("Re-publishing discovery message for topic %s", topic)
            self._mqtt.publish(topic, json.dumps(discover), qos=1, retain=False)

    def _mqtt_on_connect(self, client, userdata, flags, rc, properties=None):
        """Handle broker connect callback."""
        if rc != 0:
            self._logger.error(
                "Error connecting to %s:%s, return code %s",
                self._broker_host,
                self._broker_port,
                rc,
            )
            return

        self._logger.info("Connection successful to %s:%s", self._broker_host, self._broker_port)
        self._mqtt_rediscover()

        status_topic = f"{self._topic_prefix}/status"
        try:
            self._mqtt.subscribe(status_topic, qos=1)
        except Exception:  # noqa: BLE001
            self._logger.exception("Failed to subscribe to %s", status_topic)

        self._connected = True
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._connected_event.set)

    def _mqtt_on_connect_fail(self, client, userdata):
        """Handle broker connect-failed callback."""
        self._logger.error("Failed to connect to MQTT broker %s:%s", self._broker_host, self._broker_port)

    def _mqtt_on_disconnect(self, client, userdata, rc, properties=None):
        """Handle broker disconnect; clear connection state so run() waits before publishing again."""
        self._logger.warning(
            "Disconnected from MQTT broker %s:%s (rc=%s)", self._broker_host, self._broker_port, rc
        )
        self._connected = False
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._connected_event.clear)

    def _mqtt_on_message(self, client, userdata, msg):
        """React to broker status messages and republish discovery when needed."""
        try:
            topic = msg.topic
            payload = msg.payload.decode("utf-8", errors="replace")
            self._logger.debug("Received MQTT message on %s: %s", topic, payload)
            if topic == f"{self._topic_prefix}/status":
                status = payload.strip().lower()
                self._logger.info("Primary client is %s", status)
                if status == "online":
                    self._mqtt_rediscover()
        except Exception:  # noqa: BLE001
            self._logger.exception("Unhandled error in _mqtt_on_message")

    def _mqtt_state_topic(self, data: SensorData) -> str:
        """Build state topic for a sensor payload."""
        return f"ocpp/{data.cp_id}/{data.topic}/state"

    @staticmethod
    def _strip_ocpp_prefix(name: str) -> str:
        """Strip the leading 'OCPP ' prefix from a sensor name.

        The parser adds this prefix so that HA native sensors are clearly
        labelled within the Home Assistant device registry.  For MQTT
        discovery the device grouping already provides that context, so the
        prefix is redundant.
        """
        return name.removeprefix("OCPP ")

    def _mqtt_discover(self, data: SensorData):
        """Publish Home Assistant discovery message for one sensor."""
        state_topic = self._mqtt_state_topic(data)
        topic = f"{self._topic_prefix}/device/ocpp/{data.unique_id}/config"

        if topic in self._published_discoveries:
            return None

        discover = {
            "device": {
                "identifiers": [f"cp_{data.cp_id}"],
                "name": "EV Charge Point",
                "serial_number": data.cp_id,
            },
            "origin": {
                "name": self._origin_name,
                "sw_version": "0.1",
                "support_url": "https://github.com/michael-adler/ha-ocpp-relay",
            },
            "components": {
                f"{data.unique_id}_value": {
                    "platform": "sensor",
                    "unique_id": f"{data.unique_id}_value",
                    "name": self._strip_ocpp_prefix(data.name),
                    "state_topic": state_topic,
                    "qos": 1,
                    "expire_after": 0,
                    "force_update": True,
                }
            },
        }

        if data.manufacturer:
            discover["device"]["manufacturer"] = data.manufacturer

        component = discover["components"][f"{data.unique_id}_value"]
        if not data.device_class:
            component["value_template"] = "{{ value_json.value }}"
        elif data.topic == "heartbeat":
            component["device_class"] = data.device_class
            component["value_template"] = "{{ value_json.value }}"
            component["expire_after"] = 3600
            component["force_update"] = False
        else:
            component["value_template"] = "{{ value_json.value|float }}"
            component["unit_of_measurement"] = data.unit
            component["icon"] = "mdi:meter-electric-outline"
            component["device_class"] = data.device_class
            if data.state_class:
                component["state_class"] = data.state_class

        self._logger.debug("Publishing discovery message for %s to topic %s", data, topic)
        info = self._mqtt.publish(topic, json.dumps(discover), qos=1, retain=False)
        if info.rc != mqtt_client.MQTT_ERR_SUCCESS:
            self._logger.error("Error publishing to topic %s: %s", topic, info.rc)
        self._published_discoveries[topic] = discover
        return info

    def _mqtt_publish_data(self, data: SensorData):
        """Publish sensor state payload."""
        topic = self._mqtt_state_topic(data)
        payload = {"value": data.value}

        self._logger.debug("Publishing data %s to topic %s", data, topic)
        info = self._mqtt.publish(topic, json.dumps(payload), qos=1, retain=False)
        if info.rc != mqtt_client.MQTT_ERR_SUCCESS:
            self._logger.error("Error publishing to topic %s: %s", topic, info.rc)
        return info
