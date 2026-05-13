FROM python:3.12-slim

WORKDIR /app

# Pin all runtime deps. keyring is included so the MariaDB backend can fetch
# its password from a Keychain-compatible store when run on a host that
# provides one; on Linux containers without one it is simply unused.
RUN pip install --no-cache-dir \
        paho-mqtt==2.1.0 \
        PyMySQL==1.1.1 \
        keyring==25.5.0

# Run as a non-root user. UID 1000 matches typical host users for bind-mount
# friendliness; override with --user at run time if needed.
RUN useradd --uid 1000 --create-home --shell /usr/sbin/nologin mqtt \
    && mkdir -p /data \
    && chown mqtt:mqtt /data

COPY --chown=mqtt:mqtt mqtt_logger.py query_events.py ./

USER mqtt

ENV MQTT_BROKER=localhost
ENV MQTT_PORT=1883
ENV MQTT_DB=/data/mqtt_events.db

CMD ["sh", "-c", "python mqtt_logger.py --broker ${MQTT_BROKER} --port ${MQTT_PORT} --db ${MQTT_DB}"]
