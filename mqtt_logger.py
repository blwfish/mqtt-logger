#!/usr/bin/env python3
"""
MQTT Event Logger
Subscribes to all topics (#) and logs events to one or more database backends.

Supports SQLite (legacy) and MariaDB/MySQL.  Both can run simultaneously
during a migration to validate parity before cutting over.

MariaDB credentials are read from the macOS Keychain (service "mariadb-mqtt",
accounts "logger" / "root").  Never stored in config files or command-line
arguments.

Usage:
    # SQLite only (legacy)
    python mqtt_logger.py --broker localhost

    # MariaDB only
    python mqtt_logger.py --broker localhost --mariadb

    # Both simultaneously (migration / validation period)
    python mqtt_logger.py --broker localhost --db data/mqtt_events.db --mariadb
"""

import argparse
import json
import logging
import logging.handlers
import platform
import signal
import sqlite3
import subprocess
import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


# ─── Database backends ────────────────────────────────────────────────────────

class DatabaseBackend(ABC):
    """Common interface for all storage backends."""

    @abstractmethod
    def insert(self, timestamp: datetime, topic: str, sender: str | None,
               payload: str | None, qos: int, retained: int) -> None: ...

    @abstractmethod
    def close(self) -> None: ...

    @property
    @abstractmethod
    def name(self) -> str: ...


class SQLiteBackend(DatabaseBackend):
    """SQLite backend — legacy, single-file, local only."""

    def __init__(self, db_path: str):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute('PRAGMA journal_mode=WAL')
        self._conn.execute('''
            CREATE TABLE IF NOT EXISTS mqtt_events (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT    NOT NULL,
                topic     TEXT    NOT NULL,
                sender    TEXT,
                payload   TEXT,
                qos       INTEGER NOT NULL,
                retained  INTEGER NOT NULL
            )
        ''')
        self._conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_timestamp ON mqtt_events(timestamp)')
        self._conn.execute(
            'CREATE INDEX IF NOT EXISTS idx_topic ON mqtt_events(topic)')
        self._conn.commit()
        logger.info(f"SQLite backend ready: {db_path}")

    @property
    def name(self) -> str:
        return "sqlite"

    def insert(self, timestamp, topic, sender, payload, qos, retained):
        self._conn.execute(
            'INSERT INTO mqtt_events '
            '(timestamp, topic, sender, payload, qos, retained) '
            'VALUES (?, ?, ?, ?, ?, ?)',
            (timestamp.isoformat(), topic, sender, payload, qos, retained)
        )
        self._conn.commit()

    def close(self):
        self._conn.close()


class MariaDBBackend(DatabaseBackend):
    """MariaDB/MySQL backend — network-accessible, least-privilege account.

    Credentials are fetched from the macOS Keychain at startup:
        service = "mariadb-mqtt"
        account = "logger"   (INSERT + SELECT only — cannot DELETE or DROP)

    To store / update the password:
        security add-generic-password -a logger -s mariadb-mqtt -w <pw> -U
    """

    # Reconnect on transient errors rather than crashing the process.
    _RETRYABLE = {2006, 2013, 2055}   # CR_SERVER_GONE, CR_SERVER_LOST, etc.

    def __init__(self, host: str = 'localhost', port: int = 3306,
                 database: str = 'mqtt_log', user: str = 'logger'):
        import pymysql
        import keyring

        password = keyring.get_password("mariadb-mqtt", user)
        if not password:
            raise RuntimeError(
                f"No password found in Keychain for mariadb-mqtt/{user}. "
                f"Run: security add-generic-password -a {user} "
                f"-s mariadb-mqtt -w <password> -U"
            )

        self._connect_args = dict(
            host=host, port=port, user=user, password=password,
            database=database, charset='utf8mb4',
            autocommit=True,          # each INSERT commits immediately
        )
        self._pymysql = pymysql
        self._conn = self._connect()
        logger.info(f"MariaDB backend ready: {user}@{host}:{port}/{database}")

    @property
    def name(self) -> str:
        return "mariadb"

    def _connect(self):
        conn = self._pymysql.connect(**self._connect_args)
        return conn

    def insert(self, timestamp, topic, sender, payload, qos, retained):
        try:
            self._do_insert(timestamp, topic, sender, payload, qos, retained)
        except self._pymysql.OperationalError as exc:
            if exc.args[0] in self._RETRYABLE:
                logger.warning(f"MariaDB connection lost ({exc.args[0]}), reconnecting...")
                try:
                    self._conn = self._connect()
                    self._do_insert(timestamp, topic, sender, payload, qos, retained)
                except Exception as retry_exc:
                    logger.error(f"MariaDB retry failed: {retry_exc}")
            else:
                raise

    def _do_insert(self, timestamp, topic, sender, payload, qos, retained):
        with self._conn.cursor() as cur:
            cur.execute(
                'INSERT INTO mqtt_events '
                '(timestamp, topic, sender, payload, qos, retained) '
                'VALUES (%s, %s, %s, %s, %s, %s)',
                (timestamp, topic, sender, payload, qos, retained)
                # pymysql accepts datetime objects directly — no ISO formatting needed
            )

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass


# ─── Sender extraction ────────────────────────────────────────────────────────

def extract_sender(topic: str, payload: str) -> str | None:
    """
    Attempt to extract sender from topic or payload.

    Known topic patterns:
    - /log/{board}                        board-config log messages
    - {prefix}/config/status/{board}      board-config config status (retained)
    - {prefix}/config/backup/{board}      board-config config backup (retained)

    Known payload patterns (JSON):
    - {"board": "name", ...}              board-config config request/status
    """
    parts = topic.strip('/').split('/')

    # /log/{board}
    if len(parts) == 2 and parts[0] == 'log':
        return parts[1]

    # {prefix}/config/status/{board} or {prefix}/config/backup/{board}
    if len(parts) >= 3 and parts[-2] in ('status', 'backup') and parts[-3] == 'config':
        return parts[-1]

    # JSON payload field lookup
    if payload:
        try:
            data = json.loads(payload)
            if isinstance(data, dict):
                for key in ['board', 'sender', 'client_id', 'clientId',
                            'source', 'from', 'device_id']:
                    if key in data:
                        return str(data[key])
        except (json.JSONDecodeError, TypeError):
            pass

    return None


# ─── Loop / flood detection ───────────────────────────────────────────────────

class LoopDetector:
    """Detect MQTT message floods — topics with abnormally high publish rates.

    Tracks per-topic message counts in a sliding window. When a topic exceeds
    the threshold, logs a warning and writes to an alert file. A host-side
    watcher (alert_watcher.sh) can tail this file and fire macOS notifications.
    """

    WINDOW_SEC = 5        # Sliding window length
    THRESHOLD = 10        # Messages per window to trigger alert
    COOLDOWN_SEC = 60     # Suppress repeat alerts per topic

    def __init__(self, alert_file: str | None = None):
        self._counts: dict[str, list[float]] = {}
        self._last_alert: dict[str, float] = {}
        self._alert_file = alert_file

    def record(self, topic: str) -> None:
        now = time.monotonic()
        timestamps = self._counts.setdefault(topic, [])
        timestamps.append(now)

        cutoff = now - self.WINDOW_SEC
        while timestamps and timestamps[0] < cutoff:
            timestamps.pop(0)

        if len(timestamps) >= self.THRESHOLD:
            last = self._last_alert.get(topic, 0)
            if now - last >= self.COOLDOWN_SEC:
                self._last_alert[topic] = now
                self._alert(topic, len(timestamps))

    def _alert(self, topic: str, count: int) -> None:
        msg = f"MQTT flood: {count} msgs in {self.WINDOW_SEC}s on {topic}"
        logger.warning(msg)

        if self._alert_file:
            try:
                with open(self._alert_file, 'a') as f:
                    f.write(f"{datetime.now().isoformat()} {msg}\n")
            except Exception:
                pass

        if platform.system() == 'Darwin':
            try:
                subprocess.Popen([
                    'osascript', '-e',
                    f'display notification "{msg}" '
                    f'with title "MQTT Loop Detected" sound name "Sosumi"'
                ])
            except Exception:
                pass


# ─── MQTT logger ──────────────────────────────────────────────────────────────

class MQTTLogger:
    def __init__(self, broker: str, port: int,
                 backends: list[DatabaseBackend],
                 alert_file: str | None = None):
        self.broker = broker
        self.port = port
        self.backends = backends
        self.running = True
        self.loop_detector = LoopDetector(alert_file=alert_file)

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info(f"Connected to broker {self.broker}:{self.port}")
            client.subscribe('#', qos=0)
            logger.info("Subscribed to # (all application topics, excluding $SYS)")
        else:
            logger.error(f"Connection failed with code: {reason_code}")

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        if reason_code != 0:
            logger.warning(f"Unexpected disconnect (code: {reason_code}), will reconnect...")

    def on_message(self, client, userdata, msg):
        try:
            try:
                payload = msg.payload.decode('utf-8')
            except UnicodeDecodeError:
                payload = msg.payload.hex()

            timestamp = datetime.now()
            sender    = extract_sender(msg.topic, payload)
            retained  = 1 if msg.retain else 0

            for backend in self.backends:
                try:
                    backend.insert(timestamp, msg.topic, sender,
                                   payload, msg.qos, retained)
                except Exception as exc:
                    logger.error(f"[{backend.name}] insert failed: {exc}")

            self.loop_detector.record(msg.topic)

            display_payload = payload[:100] + '...' if len(payload) > 100 else payload
            logger.debug(f"[{msg.topic}] {display_payload}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def run(self):
        logger.info(f"Connecting to MQTT broker at {self.broker}:{self.port}")
        backend_names = ', '.join(b.name for b in self.backends)
        logger.info(f"Active backends: {backend_names}")
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
        self.client.disconnect()
        for backend in self.backends:
            backend.close()
        logger.info("MQTT Logger stopped")

    def stop(self):
        self.running = False
        self.client.disconnect()


# ─── Logging setup ────────────────────────────────────────────────────────────

def setup_logging(log_path: Path, verbose: bool):
    log_file = log_path / 'mqtt_logger.log'
    handler = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=50 * 1024 * 1024, backupCount=3
    )
    handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.DEBUG if verbose else logging.INFO)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='MQTT Event Logger')
    parser.add_argument('--broker', '-b', default='localhost',
                        help='MQTT broker hostname (default: localhost)')
    parser.add_argument('--port', '-p', type=int, default=1883,
                        help='MQTT broker port (default: 1883)')

    # SQLite options
    parser.add_argument('--db', '-d', default=None,
                        help='SQLite database path. Omit to disable SQLite backend.')

    # MariaDB options
    parser.add_argument('--mariadb', action='store_true',
                        help='Enable MariaDB backend (credentials from Keychain)')
    parser.add_argument('--mariadb-host', default='localhost',
                        help='MariaDB host (default: localhost)')
    parser.add_argument('--mariadb-port', type=int, default=3306,
                        help='MariaDB port (default: 3306)')
    parser.add_argument('--mariadb-db', default='mqtt_log',
                        help='MariaDB database name (default: mqtt_log)')

    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    setup_logging(script_dir, args.verbose)

    # Build backend list — at least one must be enabled
    backends: list[DatabaseBackend] = []

    if args.db:
        db_path = Path(args.db)
        if not db_path.is_absolute():
            db_path = script_dir / db_path
        backends.append(SQLiteBackend(str(db_path)))

    if args.mariadb:
        backends.append(MariaDBBackend(
            host=args.mariadb_host,
            port=args.mariadb_port,
            database=args.mariadb_db,
        ))

    if not backends:
        # Default to SQLite for backwards compatibility
        db_path = script_dir / 'mqtt_events.db'
        logger.warning(f"No backend specified — defaulting to SQLite: {db_path}")
        backends.append(SQLiteBackend(str(db_path)))

    # Alert file next to SQLite DB (or script dir if MariaDB-only)
    sqlite_backends = [b for b in backends if isinstance(b, SQLiteBackend)]
    if sqlite_backends:
        alert_file = str(Path(sqlite_backends[0]._conn.execute(
            "PRAGMA database_list").fetchone()[2]).parent / 'alerts.log')
    else:
        alert_file = str(script_dir / 'alerts.log')

    mqtt_logger = MQTTLogger(args.broker, args.port, backends,
                             alert_file=alert_file)

    signal.signal(signal.SIGTERM, lambda s, f: mqtt_logger.stop())
    signal.signal(signal.SIGINT,  lambda s, f: mqtt_logger.stop())

    mqtt_logger.run()


if __name__ == '__main__':
    main()
