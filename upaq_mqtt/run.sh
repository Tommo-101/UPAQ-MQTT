#!/usr/bin/with-contenv bashio
# Map add-on options to the env vars bridge.py expects, then exec it.

bashio::config.require 'protect_host'
bashio::config.require 'protect_user'
bashio::config.require 'protect_pass'

export PROTECT_HOST="$(bashio::config 'protect_host')"
export PROTECT_USER="$(bashio::config 'protect_user')"
export PROTECT_PASS="$(bashio::config 'protect_pass')"
export DISCOVERY_PREFIX="$(bashio::config 'discovery_prefix' 'homeassistant')"

if bashio::config.has_value 'mqtt_host'; then
    # Explicit broker settings win.
    export MQTT_HOST="$(bashio::config 'mqtt_host')"
    export MQTT_PORT="$(bashio::config 'mqtt_port' '1883')"
    if bashio::config.has_value 'mqtt_user'; then
        export MQTT_USER="$(bashio::config 'mqtt_user')"
        export MQTT_PASS="$(bashio::config 'mqtt_pass')"
    fi
    bashio::log.info "Using MQTT broker from add-on configuration: ${MQTT_HOST}:${MQTT_PORT}"
elif bashio::services.available 'mqtt'; then
    # Fall back to the broker Home Assistant provides (e.g. Mosquitto add-on).
    export MQTT_HOST="$(bashio::services 'mqtt' 'host')"
    export MQTT_PORT="$(bashio::services 'mqtt' 'port')"
    export MQTT_USER="$(bashio::services 'mqtt' 'username')"
    export MQTT_PASS="$(bashio::services 'mqtt' 'password')"
    bashio::log.info "Using MQTT broker from Home Assistant service discovery: ${MQTT_HOST}:${MQTT_PORT}"
else
    bashio::exit.nok "No MQTT broker found. Install the Mosquitto add-on or set mqtt_host in the configuration."
fi

bashio::log.info "Starting UP-AirQuality -> MQTT bridge (Protect host: ${PROTECT_HOST})"
exec python3 /app/bridge.py
