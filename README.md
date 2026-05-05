# ha-ocpp-relay

EV chargers are typically controlled by the [Open Charge Point Protocol](https://openchargealliance.org/protocols/open-charge-point-protocol/) \(OCPP\). The protocol includes metering. For some users, connecting to a particular remote OCPP server is essential for electrical billing discounts or access control. The code here monitors OCPP traffic to track energy use but acts only as a relay between an EV charger and the remote Charge Point Management System \(CPMS\). The relay exposes a second service where OCPP traffic can be monitored. No commands are accepted on this snoop port. Multiple clients may connect to the snoop service and each one receives the same OCPP JSON stream.

The relay should work with any charge point. Data is forwarded blindly. It is derived from https://github.com/saisasidhar/ocpp-relay.

The provided HACS integration monitors OCPP messages and maps them to Home Assistant sensors. The repository also contains functionally equivalent command-line tools that share the same underlying libraries and implement the relay and MQTT sensor streams. Most Home Assistant users should choose the HACS integration. The alternate command-line tools can be installed on any host using pip install, without depending on Home Assistant libraries. 

## HACS installation

**This integration is not in the official HACS list. You must add it as a custom repository before installation:**

1. In Home Assistant, open HACS → Integrations.
2. Click the three dots (⋮) in the top right and select **Custom repositories**.
3. Add this repository URL: `https://github.com/michael-adler/ha-ocpp-relay` as type **Integration**.
4. After adding, search for **OCPP Relay** in HACS and download it.
5. Restart Home Assistant.

**Quick install (after adding as custom repo):**

[![Add to HACS](https://img.shields.io/badge/HACS-Add%20Integration-blue?logo=home-assistant&style=for-the-badge)](https://my.home-assistant.io/redirect/integration/?domain=ha_ocpp_relay)

1. Click the badge above to open the add integration dialog directly in your Home Assistant instance or, in Home Assistant, go to [Settings → Devices & Services → Add Integration](https://my.home-assistant.io/redirect/integration/?domain=ha_ocpp_relay) and search for **HA OCPP Relay**.
2. Follow the prompts to configure the integration.
3. Configure relay settings in the integration UI:
	- `relay_is_local`: whether Home Assistant should run the relay process. Most users should leave this enabled.
	   When enabled, the relay is run automatically in the background.
	- `cpms_url`: remote CPMS URL used by the relay process (required when local relay is enabled).
	   Relays are typically web sockets, e.g. wss://gateway-eneprodus.autel.com/ws/webSocket.
	   The serial numbers of EV chargers connecting through the relay will automatically be
	   appended to the URL.
	- `relay_ocpp_host` and `relay_ocpp_port`: relay bind address/port for charge points. When the relay is local the host is the Home Assistant server.
	- `relay_snoop_host` and `relay_snoop_port`: relay snoop bind address/port. When the relay is local the host is the Home Assistant server.
	- `snoop_socket`: URL used by the HA snoop client. When the relay is local this can not be edited and is set to the local snoop port.

All settings are editable later from integration options.

## Repository layout

- `custom_components/ha_ocpp_relay/`: HACS custom integration.
- `relay_server/`: Standalone OCPP relay server and snoop websocket service.

## Architecture

The relay runs as a standalone process between the charge point and CPMS.
Home Assistant (installed via HACS) connects to the relay snoop websocket and
creates native sensor entities directly in Home Assistant. The stand-alone
variant is similar, but connects to an MQTT broker instead of directly to
the Home Assistant sensor API.

Arrows describe the direction in which websockets are opened, toward the listener,
and not the direction in which data flows. OCPP relay traffic is bidirectional and
snoop data flows from the relay toward the snoop client.

```mermaid
flowchart
  subgraph Snoop to Sensor Data
    Snoop[HA local or ocpp-snoop2mqtt] --> Sensor[HA sensors or MQTT broker]
  end
  subgraph OCPP Connection
    CP[EV Charger] -- port 8500 --> Relay[ocpp-relay-server]
    Relay --> CPMS[Remote OCPP Server]
  end

  Snoop -- port 8501 --> Relay
```

The relay supports multiple simultaneous EV charger connections and relays them to
individual, private OCPP server connections. Traffic from all relayed connections
is merged and forwarded to the snoop port. OCPP already tags each message with the
unique ID of each charger. Generated Home Assistant sensor names and MQTT topics
will include the ID.

## Deployment modes

### Single host with local relay (default)

- Enable local relay mode in integration options.
- The integration starts and supervises the relay process automatically.
- Default snoop socket is `ws://127.0.0.1:8501` (container-local relay snoop endpoint).
- If the relay process crashes, it is restarted automatically after 15 seconds.
- Direct your EV charger to the relay port on your Home Assistant instance, e.g. `ws://homeassistant.local:8500`.

### Separate relay host (alternate configuration)

If desired, the relay can run on a different machine than Home Assistant.

- Install the CLI tools on the relay host and start the relay with a reachable snoop bind address, for example:

```bash
ocpp-relay-server \
	--cpms wss://<actual-ocpp-server>/ws/webSocket \
	--snoop-host 0.0.0.0 \
	--snoop-port 8501
```

- Disable local relay mode in integration options.
- In the Home Assistant integration, set the snoop socket to the relay host, for example:
	`ws://relay-host-or-ip:8501/`.
- Ensure firewall/network rules allow Home Assistant to reach the relay snoop port.
- The snoop websocket has no application-layer authentication; run it on a trusted network,
	or protect it with host firewall, VPN, and/or reverse proxy controls.
- Direct your EV charger to the websocket relay port on the relay host (default 8500).

## External MQTT helper scripts

The package also includes two external CLI scripts:

- `ocpp-snoop2mqtt`: subscribes to the relay snoop websocket and publishes mapped telemetry to MQTT.
- `ocpp-snoop-recorder`: records the raw snoop websocket JSON stream to a file for later replay or analysis.

`ocpp-snoop2mqtt` is not required when you use the `ha_ocpp_relay` Home Assistant integration, because that integration already consumes the snoop stream directly and creates entities natively. It is included for deployments where both the relay and MQTT client are run outside Home Assistant.

`ocpp-snoop-recorder` can be useful for debugging. It records a log of OCPP transactions that can be replayed later.

Example usage:

```bash
ocpp-snoop2mqtt --snoop-socket ws://127.0.0.1:8501/ --mqtt-broker-host localhost
ocpp-snoop-recorder --snoop-socket ws://127.0.0.1:8501/ --output output.json
```

## Configuration example

See `configs/configuration_example.yaml`. This configuration is used only by the command-line tools. It is not loaded by the HACS integration.
