#!/usr/bin/env python3
"""
UP-AirQuality -> MQTT bridge for Home Assistant, with LED control.

  1. logs in (cookie auth + CSRF) and reads /bootstrap for the full initial
     state + the current lastUpdateId,
  2. opens the Protect updates WebSocket and applies the binary-framed deltas
     as they arrive, republishing readings to MQTT on every airQuality change,
  3. exposes the LED ring as controllable Home Assistant entities:
       - number  "LED Brightness"  (0-100)  -> airQualitySettings.ringLedBrightness
       - select  "LED Metric"      (AQI/CO2) -> airQualitySettings.ringLedMetric
     Commands from HA are PATCHed back to the sensor.

Protect's WS "updates" packet = two frames, each: 8-byte header + payload.
Header: [type, format, deflated, _, size:uint32-be]. format 1 = JSON, deflated
means zlib. Frame 0 is the action ({action, modelKey, id, ...}), frame 1 is the
delta of changed fields.

Env vars: PROTECT_HOST / PROTECT_USER / PROTECT_PASS, MQTT_HOST / MQTT_PORT /
MQTT_USER / MQTT_PASS, DISCOVERY_PREFIX. Event-driven — no polling interval.
"""

import base64
import http.cookiejar
import json
import os
import ssl
import struct
import sys
import time
import urllib.request
import zlib

import paho.mqtt.client as mqtt
import websocket  # websocket-client

# metric key -> (HA name, device_class | None, unit | None). Unknown future
# keys (e.g. a firmware that adds "nox") still get published as raw state.
METRICS = {
    "aqi":         ("AQI",         "aqi",            None),
    "vape":        ("Vape Index",  None,             "%"),
    "co2":         ("CO2",         "carbon_dioxide", "ppm"),
    "tvoc":        ("TVOC Index",  None,             "idx"),
    "voc":         ("VOC Index",   None,             "idx"),
    "nox":         ("NOx Index",   None,             None),
    "pm1p0":       ("PM1.0",       "pm1",            "µg/m³"),
    "pm2p5":       ("PM2.5",       "pm25",           "µg/m³"),
    "pm4p0":       ("PM4.0",       "pm4",            "µg/m³"),
    "pm10p0":      ("PM10",        "pm10",           "µg/m³"),
    "temperature": ("Temperature", "temperature",    "°C"),
    "humidity":    ("Humidity",    "humidity",       "%"),
}


def slug(value):
    return "".join(ch for ch in str(value).lower() if ch.isalnum())


def sensor_slug(device):
    for key in ("mac", "id", "name"):
        value = device.get(key)
        if value:
            value_slug = slug(value)
            if value_slug:
                return value_slug
    return None


def require_sensor_slug(device):
    device_slug = sensor_slug(device)
    if not device_slug:
        raise ValueError("Cannot publish discovery without a usable mac, id, or name")
    return device_slug


def normalized_mac(device):
    value = device.get("mac")
    if not value:
        return None
    mac = str(value).strip().lower().replace(":", "").replace("-", "").replace(".", "")
    if len(mac) != 12 or any(ch not in "0123456789abcdef" for ch in mac):
        return None
    return ":".join(mac[i:i + 2] for i in range(0, 12, 2))


def discovery_device_block(device, display_name=None):
    dev_block = {
        "name": display_name or device_name(device),
        "manufacturer": "Ubiquiti",
        "model": device.get("type", "UP-AirQuality"),
        "sw_version": device.get("firmwareVersion"),
    }
    # A stable identifier guarantees the device block always has an anchor (HA
    # requires identifiers or connections). The mac connection is kept too, so
    # entities still attach to a matching native UniFi Protect device.
    if device_slug := sensor_slug(device):
        dev_block["identifiers"] = [f"up_airquality_{device_slug}"]
    if mac_connection := normalized_mac(device):
        dev_block["connections"] = [["mac", mac_connection]]
    return dev_block


def airquality_sensors(sensors):
    """Return [(device, airQuality dict), ...] for all UP-AirQuality sensors."""
    if not isinstance(sensors, list):
        sensors = sensors.get("data") or sensors.get("sensors") or [sensors]
    found = []
    for s in sensors:
        if s.get("type") != "UP-AirQuality":
            continue
        aq = s.get("airQuality")
        if isinstance(aq, dict) and aq:
            found.append((s, aq))
    return found


def device_name(device):
    return device.get("name") or "Protect Air Quality"


def discovery_device_name(device, duplicate_names):
    name = device_name(device)
    if name not in duplicate_names:
        return name
    suffix = (sensor_slug(device) or "")[-6:]
    return f"{name} ({suffix})" if suffix else name


# ringLedMetric integer <-> HA select option.
# Verified against the UniFi app: 0 = CO2, 1 = Air Quality.
LED_METRIC_OPTIONS = {"Air Quality": 1, "CO2": 0}
LED_METRIC_REVERSE = {v: k for k, v in LED_METRIC_OPTIONS.items()}

# Per-metric alert thresholds live at
# airQualitySettings.<metric>Settings.{low,high}Threshold.
# (label, unit, min, max, step) drives the HA number entities.
THRESHOLD_METRICS = {
    "aqi":         ("AQI",         None,    0,   500,  1),
    "vape":        ("Vape",        None,    0,   100,  1),
    "co2":         ("CO2",         "ppm",   0, 40000, 10),
    "tvoc":        ("TVOC",        None,    0,  1000,  1),
    "voc":         ("VOC Index",   None,    0,   500,  1),
    "pm1p0":       ("PM1.0",       "µg/m³", 0,  1000,  1),
    "pm2p5":       ("PM2.5",       "µg/m³", 0,  1000,  1),
    "pm4p0":       ("PM4.0",       "µg/m³", 0,  1000,  1),
    "pm10p0":      ("PM10",        "µg/m³", 0,  1000,  1),
    "humidity":    ("Humidity",    "%",     0,   100,  1),
    "temperature": ("Temperature", "°C",  -20,    60,  1),
}
THRESHOLD_BOUNDS = {"low": "lowThreshold", "high": "highThreshold"}
BRIDGE_AVAIL_TOPIC = "up_airquality/bridge/availability"


def env(name, default=None, required=False):
    val = os.environ.get(name, default)
    if required and not val:
        sys.exit(f"Missing required env var: {name}")
    return val


def _ctx():
    c = ssl.create_default_context()
    c.check_hostname = False
    c.verify_mode = ssl.CERT_NONE
    return c


def _csrf_from_token(token):
    """UniFi OS TOKEN is a JWT whose payload carries csrfToken."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)  # pad base64
        return json.loads(base64.urlsafe_b64decode(payload)).get("csrfToken")
    except Exception:
        return None


def login_and_bootstrap(host, user, pw):
    """Return (token_cookie_value, csrf_token, bootstrap_dict)."""
    jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=_ctx()),
        urllib.request.HTTPCookieProcessor(jar),
    )
    login = json.dumps({"username": user, "password": pw}).encode()
    req = urllib.request.Request(
        f"https://{host}/api/auth/login",
        data=login,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = opener.open(req, timeout=20)
    csrf = resp.headers.get("X-CSRF-Token") or resp.headers.get("X-Csrf-Token")
    resp.read()

    token = next((c.value for c in jar if c.name == "TOKEN"), None)
    if not token:
        sys.exit("Login succeeded but no TOKEN cookie returned")
    if not csrf:
        csrf = _csrf_from_token(token)  # fall back to the JWT payload

    req = urllib.request.Request(
        f"https://{host}/proxy/protect/api/bootstrap",
        headers={"Accept": "application/json"},
    )
    bootstrap = json.loads(opener.open(req, timeout=20).read())
    return token, csrf, bootstrap


def patch_sensor(host, token, csrf, sensor_id, body):
    """PATCH a partial settings object onto the sensor."""
    req = urllib.request.Request(
        f"https://{host}/proxy/protect/api/sensors/{sensor_id}",
        data=json.dumps(body).encode(),
        method="PATCH",
        headers={
            "Content-Type": "application/json",
            "Cookie": f"TOKEN={token}",
            "X-CSRF-Token": csrf or "",
        },
    )
    urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=_ctx())
    ).open(req, timeout=20).read()


def decode_packet(data):
    """Decode a Protect WS binary packet into a list of JSON frames."""
    frames = []
    offset = 0
    while offset + 8 <= len(data):
        pformat = data[offset + 1]
        deflated = data[offset + 2]
        size = struct.unpack(">I", data[offset + 4:offset + 8])[0]
        payload = data[offset + 8:offset + 8 + size]
        offset += 8 + size
        if deflated:
            payload = zlib.decompress(payload)
        if pformat == 1:
            frames.append(json.loads(payload))
        else:
            frames.append(payload)
    return frames


def deep_merge(base, delta):
    for k, v in delta.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            deep_merge(base[k], v)
        else:
            base[k] = v


def publish_discovery(client, prefix, device, aq, display_name=None):
    """Publish one HA discovery config per present air-quality metric.
    Returns (state_topic, avail_topic)."""
    mac = require_sensor_slug(device)
    node = f"protect_air_quality_{mac}"
    state_topic = f"up_airquality/{mac}/state"
    avail_topic = BRIDGE_AVAIL_TOPIC

    dev_block = discovery_device_block(device, display_name)

    for key in aq:
        meta = METRICS.get(key)
        if not meta:
            continue  # unknown key we don't model; skip discovery (still in state)
        name, device_class, unit = meta
        cfg = {
            "name": name,
            "unique_id": f"up_aq_{mac}_{key}",
            "object_id": f"{node}_{key}",
            "state_topic": state_topic,
            "value_template": f"{{{{ value_json.{key} }}}}",
            # Expose the per-metric band (e.g. "neutral") as a 'status' attribute.
            "json_attributes_topic": state_topic,
            "json_attributes_template":
                f"{{{{ {{'status': value_json.{key}_status}} | tojson }}}}",
            "availability_topic": avail_topic,
            "state_class": "measurement",
            "device": dev_block,
        }
        if device_class:
            cfg["device_class"] = device_class
        if unit:
            cfg["unit_of_measurement"] = unit
        client.publish(f"{prefix}/sensor/{node}/{key}/config",
                       json.dumps(cfg), qos=1, retain=True)
    return state_topic, avail_topic


def publish_control_discovery(client, prefix, device, avail_topic, display_name=None):
    """Publish HA discovery for the LED control entities. Returns topic dict."""
    mac = require_sensor_slug(device)
    node = f"protect_air_quality_{mac}"
    base = f"up_airquality/{mac}"
    dev_block = discovery_device_block(device, display_name)
    t = {
        "brightness_cmd":   f"{base}/led_brightness/set",
        "brightness_state": f"{base}/led_brightness/state",
        "metric_cmd":       f"{base}/led_metric/set",
        "metric_state":     f"{base}/led_metric/state",
        "status_cmd":       f"{base}/status_light/set",
        "status_state":     f"{base}/status_light/state",
        "night_cmd":        f"{base}/night_mode/set",
        "night_state":      f"{base}/night_mode/state",
        "night_bri_cmd":    f"{base}/night_brightness/set",
        "night_bri_state":  f"{base}/night_brightness/state",
        "fw_version_state": f"{base}/firmware_version/state",
        "fw_update_state":  f"{base}/firmware_update/state",
        "base":             base,
    }

    brightness = {
        "name": "LED Brightness",
        "unique_id": f"up_aq_{mac}_led_brightness",
        "object_id": f"{node}_led_brightness",
        "command_topic": t["brightness_cmd"],
        "state_topic": t["brightness_state"],
        "availability_topic": avail_topic,
        "min": 0, "max": 100, "step": 1, "mode": "slider",
        "icon": "mdi:brightness-6",
        "entity_category": "config",
        "device": dev_block,
    }
    metric = {
        "name": "LED Metric",
        "unique_id": f"up_aq_{mac}_led_metric",
        "object_id": f"{node}_led_metric",
        "command_topic": t["metric_cmd"],
        "state_topic": t["metric_state"],
        "availability_topic": avail_topic,
        "options": list(LED_METRIC_OPTIONS),
        "icon": "mdi:led-on",
        "entity_category": "config",
        "device": dev_block,
    }
    status_light = {
        "name": "Status Light",
        "unique_id": f"up_aq_{mac}_status_light",
        "object_id": f"{node}_status_light",
        "command_topic": t["status_cmd"],
        "state_topic": t["status_state"],
        "availability_topic": avail_topic,
        "payload_on": "ON", "payload_off": "OFF",
        "icon": "mdi:led-on",
        "entity_category": "config",
        "device": dev_block,
    }
    night_mode = {
        "name": "Night Mode",
        "unique_id": f"up_aq_{mac}_night_mode",
        "object_id": f"{node}_night_mode",
        "command_topic": t["night_cmd"],
        "state_topic": t["night_state"],
        "availability_topic": avail_topic,
        "payload_on": "ON", "payload_off": "OFF",
        "icon": "mdi:weather-night",
        "entity_category": "config",
        "device": dev_block,
    }
    night_brightness = {
        "name": "Night Mode Brightness",
        "unique_id": f"up_aq_{mac}_night_brightness",
        "object_id": f"{node}_night_brightness",
        "command_topic": t["night_bri_cmd"],
        "state_topic": t["night_bri_state"],
        "availability_topic": avail_topic,
        "min": 0, "max": 100, "step": 1, "mode": "slider",
        "icon": "mdi:brightness-3",
        "entity_category": "config",
        "device": dev_block,
    }
    client.publish(f"{prefix}/number/{node}/led_brightness/config",
                   json.dumps(brightness), qos=1, retain=True)
    client.publish(f"{prefix}/select/{node}/led_metric/config",
                   json.dumps(metric), qos=1, retain=True)
    client.publish(f"{prefix}/switch/{node}/status_light/config",
                   json.dumps(status_light), qos=1, retain=True)
    client.publish(f"{prefix}/switch/{node}/night_mode/config",
                   json.dumps(night_mode), qos=1, retain=True)
    fw_version = {
        "name": "Firmware Version",
        "unique_id": f"up_aq_{mac}_fw_version",
        "object_id": f"{node}_firmware_version",
        "state_topic": t["fw_version_state"],
        "availability_topic": avail_topic,
        "icon": "mdi:chip",
        "entity_category": "diagnostic",
        "device": dev_block,
    }
    fw_update = {
        "name": "Firmware Update Available",
        "unique_id": f"up_aq_{mac}_fw_update",
        "object_id": f"{node}_firmware_update",
        "state_topic": t["fw_update_state"],
        "availability_topic": avail_topic,
        "payload_on": "ON", "payload_off": "OFF",
        "device_class": "update",
        "entity_category": "diagnostic",
        "device": dev_block,
    }
    client.publish(f"{prefix}/number/{node}/night_brightness/config",
                   json.dumps(night_brightness), qos=1, retain=True)
    client.publish(f"{prefix}/sensor/{node}/firmware_version/config",
                   json.dumps(fw_version), qos=1, retain=True)
    client.publish(f"{prefix}/binary_sensor/{node}/firmware_update/config",
                   json.dumps(fw_update), qos=1, retain=True)

    for metric, (label, unit, mn, mx, step) in THRESHOLD_METRICS.items():
        for bound in ("low", "high"):
            cfg = {
                "name": f"{label} {bound.capitalize()} Threshold",
                "unique_id": f"up_aq_{mac}_{metric}_{bound}_thresh",
                "object_id": f"{node}_{metric}_{bound}_threshold",
                "command_topic": f"{base}/thresh/{metric}/{bound}/set",
                "state_topic": f"{base}/thresh/{metric}/{bound}/state",
                "availability_topic": avail_topic,
                "min": mn, "max": mx, "step": step, "mode": "box",
                "icon": "mdi:tune-variant",
                "entity_category": "config",
                "device": dev_block,
            }
            if unit:
                cfg["unit_of_measurement"] = unit
            client.publish(
                f"{prefix}/number/{node}/{metric}_{bound}_threshold/config",
                json.dumps(cfg), qos=1, retain=True)
    return t


def publish_control_state(client, topics, device):
    """Mirror the sensor's current LED settings to the control state topics."""
    aqs = device.get("airQualitySettings") or {}
    led = device.get("ledSettings") or {}
    b = aqs.get("ringLedBrightness")
    m = aqs.get("ringLedMetric")
    if b is not None:
        client.publish(topics["brightness_state"], b, qos=1, retain=True)
    if m is not None:
        client.publish(topics["metric_state"],
                       LED_METRIC_REVERSE.get(m, "Air Quality"), qos=1, retain=True)
    if led.get("isEnabled") is not None:
        client.publish(topics["status_state"],
                       "ON" if led["isEnabled"] else "OFF", qos=1, retain=True)
    if aqs.get("nightModeEnabled") is not None:
        client.publish(topics["night_state"],
                       "ON" if aqs["nightModeEnabled"] else "OFF", qos=1, retain=True)
    if aqs.get("nightModeBrightness") is not None:
        client.publish(topics["night_bri_state"],
                       aqs["nightModeBrightness"], qos=1, retain=True)
    base = topics.get("base")
    if base:  # per-metric thresholds (publish only the ones that are set)
        for metric in THRESHOLD_METRICS:
            ms = aqs.get(f"{metric}Settings") or {}
            for bound, key in THRESHOLD_BOUNDS.items():
                v = ms.get(key)
                if v is not None:
                    client.publish(f"{base}/thresh/{metric}/{bound}/state",
                                   v, qos=1, retain=True)


def publish_diag_state(client, topics, device):
    """Mirror firmware version + update-available to their state topics."""
    fw = device.get("firmwareVersion")
    latest = device.get("latestFirmwareVersion")
    fw_state = device.get("fwUpdateState")
    if fw:
        client.publish(topics["fw_version_state"], fw, qos=1, retain=True)
    update_avail = (fw_state not in (None, "upToDate")) or \
                   (bool(latest) and bool(fw) and latest != fw)
    client.publish(topics["fw_update_state"],
                   "ON" if update_avail else "OFF", qos=1, retain=True)


def publish_state(client, sensor_ctx):
    device = sensor_ctx["device"]
    payload = {}
    for k, v in device["airQuality"].items():
        if isinstance(v, dict):
            payload[k] = v.get("value")
            payload[f"{k}_status"] = v.get("status")
    client.publish(sensor_ctx["state_topic"], json.dumps(payload), qos=0, retain=False)
    client.publish(sensor_ctx["avail_topic"], "online", qos=1, retain=True)
    print(f"Published {sensor_ctx['mac']}: {payload}", flush=True)


def command_subscriptions(sensor_contexts):
    subs = []
    for sensor_ctx in sensor_contexts:
        topics = sensor_ctx["topics"]
        subs.extend((topics[k], 0) for k in
                    ("brightness_cmd", "metric_cmd", "status_cmd",
                     "night_cmd", "night_bri_cmd"))
        subs.append((f"{topics['base']}/thresh/+/+/set", 0))
    return subs


def main():
    host = env("PROTECT_HOST", required=True)
    user = env("PROTECT_USER", required=True)
    pw = env("PROTECT_PASS", required=True)
    mqtt_host = env("MQTT_HOST", required=True)
    mqtt_port = int(env("MQTT_PORT", "1883"))
    mqtt_user = env("MQTT_USER")
    mqtt_pass = env("MQTT_PASS")
    prefix = env("DISCOVERY_PREFIX", "homeassistant")

    # Callback threads grab one complete snapshot so reconnects cannot expose
    # mixed old/new token and sensor maps.
    ctx = {"snapshot": {"host": host, "token": None, "csrf": None,
                        "sensors_by_id": {}, "sensors_by_mac": {}}}

    def on_mqtt_message(client, userdata, msg):
        parts = msg.topic.split("/")
        if len(parts) < 3 or parts[0] != "up_airquality":
            return
        snapshot = ctx["snapshot"]
        sensor_ctx = snapshot["sensors_by_mac"].get(parts[1])
        if not sensor_ctx:
            return
        topics = sensor_ctx["topics"]
        device = sensor_ctx["device"]
        sensor_id = sensor_ctx["id"]
        payload = msg.payload.decode(errors="replace").strip()
        try:
            if msg.topic == topics["brightness_cmd"]:
                val = max(0, min(100, int(float(payload))))
                patch_sensor(snapshot["host"], snapshot["token"], snapshot["csrf"],
                             sensor_id, {"airQualitySettings":
                                         {"ringLedBrightness": val}})
                device.setdefault("airQualitySettings", {})["ringLedBrightness"] = val
                client.publish(topics["brightness_state"], val, qos=1, retain=True)
                print(f"{sensor_ctx['mac']} LED brightness -> {val}", flush=True)
            elif msg.topic == topics["metric_cmd"]:
                if payload not in LED_METRIC_OPTIONS:
                    print(f"Ignoring unknown metric option: {payload}", flush=True)
                    return
                val = LED_METRIC_OPTIONS[payload]
                patch_sensor(snapshot["host"], snapshot["token"], snapshot["csrf"],
                             sensor_id, {"airQualitySettings":
                                         {"ringLedMetric": val}})
                device.setdefault("airQualitySettings", {})["ringLedMetric"] = val
                client.publish(topics["metric_state"], payload, qos=1, retain=True)
                print(f"{sensor_ctx['mac']} LED metric -> {payload} ({val})", flush=True)
            elif msg.topic == topics["status_cmd"]:
                on = payload.upper() == "ON"
                patch_sensor(snapshot["host"], snapshot["token"], snapshot["csrf"],
                             sensor_id, {"ledSettings": {"isEnabled": on}})
                device.setdefault("ledSettings", {})["isEnabled"] = on
                client.publish(topics["status_state"], payload.upper(),
                               qos=1, retain=True)
                print(f"{sensor_ctx['mac']} Status light -> {payload.upper()}", flush=True)
            elif msg.topic == topics["night_cmd"]:
                on = payload.upper() == "ON"
                patch_sensor(snapshot["host"], snapshot["token"], snapshot["csrf"],
                             sensor_id,
                             {"airQualitySettings": {"nightModeEnabled": on}})
                device.setdefault("airQualitySettings", {})["nightModeEnabled"] = on
                client.publish(topics["night_state"], payload.upper(),
                               qos=1, retain=True)
                print(f"{sensor_ctx['mac']} Night mode -> {payload.upper()}", flush=True)
            elif msg.topic == topics["night_bri_cmd"]:
                val = max(0, min(100, int(float(payload))))
                patch_sensor(snapshot["host"], snapshot["token"], snapshot["csrf"],
                             sensor_id,
                             {"airQualitySettings": {"nightModeBrightness": val}})
                device.setdefault("airQualitySettings", {})["nightModeBrightness"] = val
                client.publish(topics["night_bri_state"], val, qos=1, retain=True)
                print(f"{sensor_ctx['mac']} Night mode brightness -> {val}", flush=True)
            elif len(parts) == 6 and parts[2] == "thresh" and parts[5] == "set":
                # up_airquality/<mac>/thresh/<metric>/<bound>/set
                metric, bound = parts[3], parts[4]
                if metric not in THRESHOLD_METRICS or bound not in THRESHOLD_BOUNDS:
                    return
                fval = float(payload)
                val = int(fval) if fval.is_integer() else fval
                key = THRESHOLD_BOUNDS[bound]
                patch_sensor(snapshot["host"], snapshot["token"], snapshot["csrf"],
                             sensor_id,
                             {"airQualitySettings": {f"{metric}Settings": {key: val}}})
                aqs = device.setdefault("airQualitySettings", {})
                aqs.setdefault(f"{metric}Settings", {})[key] = val
                client.publish(f"{topics['base']}/thresh/{metric}/{bound}/state",
                               val, qos=1, retain=True)
                print(f"{sensor_ctx['mac']} Threshold {metric}.{bound} -> {val}", flush=True)
        except Exception as e:
            print(f"command error on {msg.topic}: {e}", flush=True)

    def on_connect(client, userdata, flags, reason_code, properties):
        # Re-subscribe on every (re)connect once we know the topics.
        subs = command_subscriptions(ctx["snapshot"]["sensors_by_id"].values())
        if subs:
            client.subscribe(subs)
            print(f"Subscribed to {len(subs)} command topics", flush=True)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_mqtt_message
    client.on_connect = on_connect
    if mqtt_user:
        client.username_pw_set(mqtt_user, mqtt_pass)
    client.will_set(BRIDGE_AVAIL_TOPIC, "offline", qos=1, retain=True)
    client.connect(mqtt_host, mqtt_port, keepalive=60)
    client.loop_start()
    print(f"Connected to MQTT {mqtt_host}:{mqtt_port}", flush=True)

    sensor_contexts = []
    failures = 0  # consecutive failures, drives exponential backoff
    while True:
        connected_at = None
        try:
            token, csrf, bootstrap = login_and_bootstrap(host, user, pw)
            discovered = airquality_sensors(bootstrap.get("sensors", []))
            if not discovered:
                print("No UP-AirQuality sensor in bootstrap; retrying", flush=True)
                failures += 1
                time.sleep(min(5 * 2 ** failures, 300))
                continue
            last_update_id = bootstrap.get("lastUpdateId", "")
            print(f"Discovered {len(discovered)} UP-AirQuality sensor(s)  "
                  f"lastUpdateId={last_update_id}", flush=True)

            name_counts = {}
            for device, _ in discovered:
                name = device_name(device)
                name_counts[name] = name_counts.get(name, 0) + 1
            duplicate_names = {name for name, count in name_counts.items() if count > 1}

            sensor_contexts = []
            sensors_by_id = {}
            sensors_by_mac = {}
            for device, aq in discovered:
                sensor_id = device.get("id")
                if not sensor_id:
                    print("Skipping UP-AirQuality sensor without id", flush=True)
                    continue
                mac = sensor_slug(device)
                if not mac:
                    print("Skipping UP-AirQuality sensor without usable mac/id/name",
                          flush=True)
                    continue
                display_name = discovery_device_name(device, duplicate_names)
                state_topic, avail_topic = publish_discovery(
                    client, prefix, device, aq, display_name)
                sensor_ctx = {
                    "id": sensor_id,
                    "mac": mac,
                    "device": device,
                    "state_topic": state_topic,
                    "avail_topic": avail_topic,
                    "topics": publish_control_discovery(
                        client, prefix, device, avail_topic, display_name),
                }
                sensor_contexts.append(sensor_ctx)
                sensors_by_id[sensor_id] = sensor_ctx
                sensors_by_mac[mac] = sensor_ctx
                print(f"Sensor {mac} id={sensor_id}", flush=True)
            if not sensor_contexts:
                print("No usable UP-AirQuality sensors in bootstrap; retrying", flush=True)
                failures += 1
                time.sleep(min(5 * 2 ** failures, 300))
                continue

            ctx["snapshot"] = {
                "host": host,
                "token": token,
                "csrf": csrf,
                "sensors_by_id": sensors_by_id,
                "sensors_by_mac": sensors_by_mac,
            }
            # subscribe now (on_connect only fires on MQTT (re)connect)
            client.subscribe(command_subscriptions(sensor_contexts))

            for sensor_ctx in sensor_contexts:
                publish_state(client, sensor_ctx)  # seed readings
                publish_control_state(client, sensor_ctx["topics"],
                                      sensor_ctx["device"])  # seed LED state
                publish_diag_state(client, sensor_ctx["topics"],
                                   sensor_ctx["device"])  # seed firmware

            def on_message(ws, message):
                if not isinstance(message, (bytes, bytearray)):
                    return
                try:
                    frames = decode_packet(message)
                except Exception as e:
                    print(f"decode error: {e}", flush=True)
                    return
                if len(frames) < 2:
                    return
                action, delta = frames[0], frames[1]
                if action.get("modelKey") != "sensor":
                    return
                sensor_ctx = ctx["snapshot"]["sensors_by_id"].get(action.get("id"))
                if not sensor_ctx:
                    return
                device = sensor_ctx["device"]
                if "airQuality" in delta:
                    deep_merge(device, delta)
                    publish_state(client, sensor_ctx)
                if "airQualitySettings" in delta or "ledSettings" in delta:
                    deep_merge(device, delta)  # changed elsewhere (app)
                    publish_control_state(client, sensor_ctx["topics"], device)
                if any(k in delta for k in ("firmwareVersion", "fwUpdateState",
                                            "latestFirmwareVersion")):
                    deep_merge(device, delta)
                    publish_diag_state(client, sensor_ctx["topics"], device)

            def on_open(ws):
                nonlocal connected_at
                connected_at = time.time()

            def on_error(ws, err):
                print(f"WS error: {err}", flush=True)

            def on_close(ws, code, msg):
                print(f"WS closed: {code} {msg}", flush=True)

            ws_url = (f"wss://{host}/proxy/protect/ws/updates"
                      f"?lastUpdateId={last_update_id}")
            print(f"Connecting WS: {ws_url}", flush=True)
            ws = websocket.WebSocketApp(
                ws_url, header=[f"Cookie: TOKEN={token}"],
                on_open=on_open, on_message=on_message,
                on_error=on_error, on_close=on_close,
            )
            ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=30)
        except Exception as e:
            print(f"loop error: {e}", flush=True)
        finally:
            try:
                client.publish(BRIDGE_AVAIL_TOPIC, "offline", qos=1, retain=True)
            except Exception as e:
                print(f"availability publish error: {e}", flush=True)

        # Exponential backoff. Reset only if the WS stayed up a healthy while,
        # so a failing login can't turn into a 5s reconnect/login storm.
        if connected_at and (time.time() - connected_at) > 60:
            failures = 0
        else:
            failures += 1
        delay = min(5 * 2 ** failures, 300)
        print(f"Reconnecting in {delay}s (failures={failures})", flush=True)
        time.sleep(delay)


if __name__ == "__main__":
    main()
