#!/usr/bin/env python3
"""
MQTT Event Logger
Subscribes to all topics (#) and logs events to SQLite database.

Usage:
    python mqtt_logger.py [--broker HOST] [--port PORT] [--db PATH]
"""

import argparse
import json
import logging
import logging.handlers
import signal
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


def init_database(db_path: str) -> sqlite3.Connection:
    """Initialize SQLite database with mqtt_events table."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS mqtt_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            topic TEXT NOT NULL,
            sender TEXT,
            payload TEXT,
            qos INTEGER NOT NULL,
            retained INTEGER NOT NULL
        )
    ''')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON mqtt_events(timestamp)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_topic ON mqtt_events(topic)')
    conn.commit()
    logger.info(f"Database initialized: {db_path}")
    return conn


def extract_sender(topic: str, payload: str) -> str | None:
    """
    Attempt to extract sender from topic or payload.

    Strategies (customize based on your topic structure):
    1. Parse from topic like 'cova/turnout/sender_id/state'
    2. Parse from JSON payload if it contains sender/client_id field
    3. Return None if sender cannot be determined
    """
    # Strategy 1: Try to extract from topic structure
    # Example: cova/device_type/device_id/... -> device_id might be sender
    parts = topic.split('/')
    if len(parts) >= 3:
        # This is a guess - adjust based on your actual topic structure
        # For now, return None and let the data reveal the pattern
        pass

    # Strategy 2: Try to parse JSON payload for sender field
    if payload:
        try:
            data = json.loads(payload)
            if isinstance(data, dict):
                # Look for common sender field names
                for key in ['sender', 'client_id', 'clientId', 'source', 'from', 'device_id']:
                    if key in data:
                        return str(data[key])
        except (json.JSONDecodeError, TypeError):
            pass

    return None


class MQTTLogger:
    def __init__(self, broker: str, port: int, db_path: str):
        self.broker = broker
        self.port = port
        self.db_conn = init_database(db_path)
        self.running = True

        # Create MQTT client (paho-mqtt 2.x API)
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info(f"Connected to broker {self.broker}:{self.port}")
            # Subscribe to all topics
            client.subscribe('#', qos=0)
            logger.info("Subscribed to # (all topics)")
        else:
            logger.error(f"Connection failed with code: {reason_code}")

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            logger.warning(f"Unexpected disconnect (code: {reason_code}), will reconnect...")

    def on_message(self, client, userdata, msg):
        try:
            # Decode payload (handle binary data gracefully)
            try:
                payload = msg.payload.decode('utf-8')
            except UnicodeDecodeError:
                payload = msg.payload.hex()  # Store as hex if not UTF-8

            timestamp = datetime.now().isoformat()
            sender = extract_sender(msg.topic, payload)
            retained = 1 if msg.retain else 0

            # Insert into database
            self.db_conn.execute(
                'INSERT INTO mqtt_events (timestamp, topic, sender, payload, qos, retained) VALUES (?, ?, ?, ?, ?, ?)',
                (timestamp, msg.topic, sender, payload, msg.qos, retained)
            )
            self.db_conn.commit()

            # Log to console (truncate long payloads)
            display_payload = payload[:100] + '...' if len(payload) > 100 else payload
            logger.debug(f"[{msg.topic}] {display_payload}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def run(self):
        """Main run loop with reconnection logic."""
        logger.info(f"Connecting to MQTT broker at {self.broker}:{self.port}")

        try:
            self.client.connect(self.broker, self.port, keepalive=60)
            self.client.loop_forever()
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        except Exception as e:
            logger.error(f"Connection error: {e}")
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean shutdown."""
        self.client.disconnect()
        self.db_conn.close()
        logger.info("MQTT Logger stopped")

    def stop(self):
        """Signal handler for graceful shutdown."""
        self.running = False
        self.client.disconnect()


def setup_logging(log_path: Path, verbose: bool):
    """Configure logging to file in script directory."""
    log_file = log_path / 'mqtt_logger.log'

    handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=5*1024*1024,  # 5MB
        backupCount=3
    )
    handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.DEBUG if verbose else logging.INFO)


def main():
    parser = argparse.ArgumentParser(description='MQTT Event Logger')
    parser.add_argument('--broker', '-b', default='localhost',
                        help='MQTT broker hostname (default: localhost)')
    parser.add_argument('--port', '-p', type=int, default=1883,
                        help='MQTT broker port (default: 1883)')
    parser.add_argument('--db', '-d', default='mqtt_events.db',
                        help='SQLite database path (default: mqtt_events.db)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')

    args = parser.parse_args()

    # Resolve paths relative to script location
    script_dir = Path(__file__).parent

    db_path = Path(args.db)
    if not db_path.is_absolute():
        db_path = script_dir / db_path

    setup_logging(script_dir, args.verbose)

    mqtt_logger = MQTTLogger(args.broker, args.port, str(db_path))

    # Handle signals for graceful shutdown
    signal.signal(signal.SIGTERM, lambda s, f: mqtt_logger.stop())
    signal.signal(signal.SIGINT, lambda s, f: mqtt_logger.stop())

    mqtt_logger.run()


if __name__ == '__main__':
    main()
