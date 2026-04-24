import asyncio
import base64
import json
import logging

import websockets

from relay_server.common.types import MessageData


def basic_auth_header(username: str, password: str) -> tuple[str, str]:
    user_pass = f"{username}:{password}"
    basic_credentials = base64.b64encode(user_pass.encode()).decode()
    return ("Authorization", f"Basic {basic_credentials}")


class OCPPRelay:
    """WebSocket relay that forwards messages between CP and CSMS."""

    def __init__(self, csms_url, csms_id=None, csms_pass=None, snoop_queue=None):
        if csms_url is None:
            raise ValueError("csms_url must not be None")
        self.logger = logging.getLogger(__name__)
        self.csms_url, self.csms_id, self.csms_pass = csms_url, csms_id, csms_pass
        self.snoop_queue = snoop_queue

    async def _relay(self, source_ws, target_ws, source_name, target_name, cp_id, protocol):
        while True:
            message = await source_ws.recv()
            json_message = json.loads(message)
            await target_ws.send(message)
            self.logger.info(
                "Relayed message from %s to %s (%s)", source_name, target_name, json_message[1]
            )

            if self.snoop_queue:
                msg_data = MessageData(
                    event="Message",
                    sender=source_name,
                    protocol=protocol,
                    cp_id=cp_id,
                    payload=json_message,
                )
                self.snoop_queue.put_nowait(msg_data)

    async def _on_connect(self, ws):
        charge_point_id = ws.request.path.strip("/")
        cp_ws = ws

        try:
            ws_subprotocol = cp_ws.request.headers["Sec-WebSocket-Protocol"]
        except KeyError:
            self.logger.error("Client did not specify OCPP sub-protocol. Closing connection.")
            await cp_ws.close()
            return

        self.logger.info("Charge point connected: %s (protocol=%s)", charge_point_id, ws_subprotocol)

        extra_headers = []
        if all([self.csms_id, self.csms_pass]):
            extra_headers.append(basic_auth_header(self.csms_id, self.csms_pass))

        csms_uri = f"{self.csms_url}/{charge_point_id}"
        async with websockets.connect(
            csms_uri, subprotocols=[ws_subprotocol], additional_headers=extra_headers
        ) as csms_ws:
            tasks = [
                asyncio.create_task(
                    self._relay(
                        cp_ws,
                        csms_ws,
                        source_name="CP",
                        target_name="CSMS",
                        cp_id=charge_point_id,
                        protocol=ws_subprotocol,
                    )
                ),
                asyncio.create_task(
                    self._relay(
                        csms_ws,
                        cp_ws,
                        source_name="CSMS",
                        target_name="CP",
                        cp_id=charge_point_id,
                        protocol=ws_subprotocol,
                    )
                ),
            ]

            try:
                await asyncio.gather(*tasks)
            except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedOK):
                self.logger.info("Relay connection closed for charge point %s", charge_point_id)
            finally:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def start(self, host, port, ssl_context=None):
        server = await websockets.serve(self._on_connect, host, port, ssl=ssl_context)
        self.logger.info("Relay server started on %s", port)
        return server
