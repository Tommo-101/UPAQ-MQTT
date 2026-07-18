# UniFi Protect Air Quality → MQTT

[![CI](https://github.com/Tommo-101/UPAQ-MQTT/actions/workflows/ci.yml/badge.svg)](https://github.com/Tommo-101/UPAQ-MQTT/actions/workflows/ci.yml)

A tiny bridge that pulls readings from one or more **Ubiquiti UP-AirQuality**
(Vape Detection & Air Quality Sensor) devices out of UniFi Protect and
publishes them to MQTT using **Home Assistant MQTT Discovery** — so each sensor
and its LED controls show up in Home Assistant automatically.

It exists because, as of mid-2026, the native Home Assistant `unifiprotect`
integration doesn't surface this device's data yet (the values stream over
Protect's WebSocket, but the `uiprotect` library doesn't model them). See
[home-assistant/core discussion #4047](https://github.com/orgs/home-assistant/discussions/4047).
Use this in the meantime; once native support lands you can switch over.

## What you get in Home Assistant

One Home Assistant device per adopted UP-AirQuality sensor, each with:

**Sensors** — CO₂, AQI, Vape Index (%), VOC Index (idx), TVOC Index (idx),
PM1.0, PM2.5, PM4.0, PM10, Temperature, Humidity (each with a `status`
attribute, e.g. `neutral`).

**Controls** (config entities):
- LED Brightness (0–100) and LED Metric (Air Quality / CO₂)
- Status Light on/off
- Night Mode on/off and Night Mode Brightness
- Per-metric low/high alert thresholds

**Diagnostics** — Firmware Version, Firmware Update Available.

## Requirements

- Docker + Docker Compose
- A UniFi Protect controller with one or more UP-AirQuality sensors adopted, and a **local
  account** on it (Owner/local user — not a UI Cloud-only login)
- An MQTT broker that Home Assistant uses (e.g. the Mosquitto add-on)

## Quick start

```bash
git clone https://github.com/Tommo-101/UPAQ-MQTT.git
cd UPAQ-MQTT
cp .env.example .env
$EDITOR .env            # fill in your Protect + MQTT details
docker compose up -d --build
docker compose logs -f  # watch it connect and publish
```

The entities appear in Home Assistant under **Settings → Devices & Services →
MQTT** within a few seconds.

Discovery publishes the sensor's MAC address as a device-registry connection, so
if Home Assistant already has a UniFi Protect device with the same MAC, the MQTT
entities attach to that existing device instead of creating a separate one. A
stable MQTT identifier is also published as a fallback anchor, so the device is
always valid even if a MAC isn't available. Entity `unique_id`s and MQTT topics
remain stable and MAC/id based.

## Configuration

All config is via environment variables (`.env`):

| Variable | Required | Default | Notes |
|----------|----------|---------|-------|
| `PROTECT_HOST` | yes | — | Controller IP/hostname |
| `PROTECT_USER` | yes | — | Local Protect username |
| `PROTECT_PASS` | yes | — | Local Protect password |
| `MQTT_HOST` | yes | — | Broker IP/hostname |
| `MQTT_PORT` | no | `1883` | Broker port |
| `MQTT_USER` | no | — | Broker username (omit for anonymous) |
| `MQTT_PASS` | no | — | Broker password |
| `DISCOVERY_PREFIX` | no | `homeassistant` | HA discovery prefix |

## Run as a Home Assistant add-on

On Home Assistant OS / Supervised you can run the bridge as a native add-on
instead of Docker Compose (this repository doubles as an add-on repository):

[![Add repository to my Home Assistant](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2FTommo-101%2FUPAQ-MQTT)

1. **Settings → Add-ons → Add-on store → ⋮ → Repositories**, add
   `https://github.com/Tommo-101/UPAQ-MQTT` (or click the badge above).
2. Install **UniFi Protect Air Quality (MQTT)** and fill in the Protect
   details on the Configuration tab — the add-on exposes the same settings as
   the environment variables above.
3. Start the add-on and check its log. Full details in
   [upaq_mqtt/DOCS.md](upaq_mqtt/DOCS.md).

Add-on notes:

- With the Mosquitto broker add-on, leave all `mqtt_*` options empty: host,
  port, and credentials are picked up from Home Assistant service discovery.
- Maintainer note: `upaq_mqtt/bridge.py` is a copy of the root `bridge.py`
  (add-on builds can only see files inside the add-on folder). After changing
  the root file, run `cp bridge.py upaq_mqtt/bridge.py` and bump `version` in
  `upaq_mqtt/config.yaml` so installed add-ons are offered the update. CI
  fails if the two copies drift.

## How it works

`bridge.py` logs in, reads `/proxy/protect/api/bootstrap` for initial state,
then connects to Protect's `/proxy/protect/ws/updates` WebSocket and
republishes to MQTT on every change (event-driven, ~instant). Each discovered
UP-AirQuality sensor gets its own state and control topics based on its device
MAC, while all entities share one bridge availability topic for MQTT Last Will
handling. Control changes from Home Assistant are `PATCH`ed back to the
matching sensor.

## Notes & caveats

- **`ringLedMetric` mapping** is `0 = CO2`, `1 = Air Quality` (verified on fw
  1.0.12). If a future firmware differs, flip `LED_METRIC_OPTIONS` in
  `bridge.py`.
- **No NOx** is published — fw 1.0.12 doesn't expose a `nox` field despite the
  spec sheet. It's already mapped, so it'll appear automatically if added.
- **Alert thresholds** default to `null` (device defaults). HA can set a value
  but can't clear it back to `null`; reset those in the UniFi app if needed.
- If multiple sensors have the same Protect name, Home Assistant discovery
  appends a short MAC/id suffix to the duplicate device names. MQTT topics,
  object IDs, and unique IDs are always based on the device MAC/id, not the
  display name.
- **Security:** use a **dedicated** Protect local user and a dedicated MQTT
  user for this bridge, scoped minimally. Keep your real `.env` out of git
  (it's already in `.gitignore`).
- UniFi rate-limits logins; the bridge uses exponential backoff so a failed
  login can't storm the controller.

## Credits

Built by [Tommo-101](https://github.com/Tommo-101), with the API
reverse-engineering and implementation done together with
[Claude Code](https://claude.com/claude-code) (Anthropic).
