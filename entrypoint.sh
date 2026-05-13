#!/bin/sh
# Translate the MQTT_* env vars into the right CLI flags. Mirrors the flag
# layout of mqtt_logger.py and query_events.py — set MQTT_MARIADB=1 to enable
# the MariaDB backend, leave MQTT_DB unset to disable SQLite.
set -eu

args="--broker ${MQTT_BROKER} --port ${MQTT_PORT}"

if [ -n "${MQTT_DB:-}" ]; then
    args="${args} --db ${MQTT_DB}"
fi

if [ -n "${MQTT_MARIADB:-}" ]; then
    args="${args} --mariadb \
        --mariadb-host ${MQTT_MARIADB_HOST} \
        --mariadb-port ${MQTT_MARIADB_PORT} \
        --mariadb-db ${MQTT_MARIADB_DB}"
fi

exec python mqtt_logger.py ${args}
