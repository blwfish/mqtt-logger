FROM python:3.12-slim

WORKDIR /app

# Install paho-mqtt
RUN pip install --no-cache-dir paho-mqtt==2.1.0

# Copy application files
COPY mqtt_logger.py query_events.py ./

# Create data directory for database and logs
RUN mkdir -p /data

# Default environment variables
ENV MQTT_BROKER=localhost
ENV MQTT_PORT=1883
ENV MQTT_DB=/data/mqtt_events.db

# Run the logger
CMD ["sh", "-c", "python mqtt_logger.py --broker ${MQTT_BROKER} --port ${MQTT_PORT} --db ${MQTT_DB}"]
