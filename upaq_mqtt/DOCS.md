# UniFi Protect Air Quality (MQTT) add-on

Bridges one or more Ubiquiti **UP-AirQuality** (Vape Detection & Air Quality)
sensors from UniFi Protect to MQTT using Home Assistant MQTT Discovery. Each
sensor shows up automatically as a device with:

- **Sensors:** CO₂, AQI, Vape Index, VOC/TVOC Index, PM1.0/PM2.5/PM4.0/PM10,
  Temperature, Humidity (each with a `status` attribute such as `neutral`)
- **Controls:** LED brightness & metric, status light, night mode (+ brightness),
  per-metric low/high alert thresholds
- **Diagnostics:** firmware version, firmware update available

It exists because the native `unifiprotect` integration doesn't surface this
device's data yet. Once native support lands you can uninstall this add-on and
switch over.

## Prerequisites

1. A UniFi Protect controller with at least one adopted UP-AirQuality sensor.
2. A **local account** on the controller (Owner/local user — not a UI
   Cloud-only login).
3. The **MQTT integration** set up in Home Assistant, connected to a broker —
   the Mosquitto broker add-on is the easy path.

## Configuration

| Option | Required | Default | Notes |
|--------|----------|---------|-------|
| `protect_host` | yes | — | Controller IP/hostname (no `https://`) |
| `protect_user` | yes | — | Local Protect username |
| `protect_pass` | yes | — | Local Protect password |
| `discovery_prefix` | no | `homeassistant` | HA MQTT discovery prefix |
| `mqtt_host` | no | *(auto)* | Leave empty to auto-detect the Mosquitto add-on |
| `mqtt_port` | no | `1883` | Only used when `mqtt_host` is set |
| `mqtt_user` | no | — | Only used when `mqtt_host` is set |
| `mqtt_pass` | no | — | Only used when `mqtt_host` is set |

If you run the official **Mosquitto broker add-on**, leave all `mqtt_*` fields
empty — the add-on picks up the broker host, port, and credentials from Home
Assistant's service discovery automatically. Fill them in only when using an
external broker.

## Usage

1. Configure and start the add-on.
2. Watch the log: it should report the MQTT connection, then
   `Discovered N UP-AirQuality sensor(s)` and the first published readings.
3. Entities appear under **Settings → Devices & Services → MQTT** within a few
   seconds. If Home Assistant already knows a UniFi Protect device with the
   same MAC, the entities attach to that existing device.

## Notes & caveats

- Updates are event-driven over Protect's WebSocket — near-instant, no polling.
- `ringLedMetric` mapping (`0 = CO2`, `1 = Air Quality`) was verified on
  firmware 1.0.12.
- Firmware 1.0.12 does not expose NOx despite the spec sheet; it will appear
  automatically if a future firmware adds it.
- Alert thresholds default to `null` (device defaults). Home Assistant can set
  a value but cannot clear it back to `null`; reset those in the UniFi app.
- The bridge uses exponential backoff on failures, so a bad password won't
  hammer the controller (UniFi rate-limits logins).

## Development

The `bridge.py` in this folder is a copy of the one at the repository root
(Supervisor builds only see files inside the add-on folder). The root file is
the source of truth: after changing it, run `cp bridge.py upaq_mqtt/bridge.py`
and bump `version` in `config.yaml` so installed add-ons pick up the update.
CI fails if the two files drift.
