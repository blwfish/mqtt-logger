FROM python:3.12-slim

WORKDIR /app

# Pin all runtime deps. keyring is included so the MariaDB backend can fetch
# its password from a Keychain-compatible store when run on a host that
# provides one; on Linux containers without one it is simply unused unless
# `keyrings.alt` is also installed alongside.
RUN pip install --no-cache-dir \
        paho-mqtt==2.1.0 \
        PyMySQL==1.1.1 \
        keyring==25.5.0

# Non-root user. UID 1000 matches typical host users for bind-mount
# friendliness; override with --user at run time if needed.
RUN useradd --uid 1000 --create-home --shell /usr/sbin/nologin mqtt \
    && mkdir -p /data \
    && chown mqtt:mqtt /data

COPY --chown=mqtt:mqtt mqtt_logger.py query_events.py entrypoint.sh ./
RUN chmod +x ./entrypoint.sh

USER mqtt

# Common
ENV MQTT_BROKER=localhost
ENV MQTT_PORT=1883

# SQLite (default). Empty MQTT_DB disables the SQLite backend.
ENV MQTT_DB=/data/mqtt_events.db

# MariaDB (off by default — set MQTT_MARIADB=1 to enable). When enabled,
# at least MQTT_MARIADB_HOST must point at a reachable instance. Credentials
# come from the container's keyring backend at startup.
ENV MQTT_MARIADB=
ENV MQTT_MARIADB_HOST=localhost
ENV MQTT_MARIADB_PORT=3306
ENV MQTT_MARIADB_DB=mqtt_log

ENTRYPOINT ["./entrypoint.sh"]
