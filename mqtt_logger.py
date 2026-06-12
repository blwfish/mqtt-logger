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
import os
import platform
import signal
import sqlite3
import subprocess
import time
from abc import ABC, abstractmethod
from collections import deque
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
    """SQLite backend — legacy, single-file, local only.

    Inserts are grouped: a commit fires every COMMIT_EVERY rows or when
    COMMIT_INTERVAL_SEC has elapsed since the previous commit, whichever
    comes first. Avoids the fsync-per-row cost without dropping more than
    one batch on a crash.
    """

    COMMIT_EVERY = 25
    COMMIT_INTERVAL_SEC = 1.0

    def __init__(self, db_path: str):
        self.db_path = db_path
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
        self._pending = 0
        self._last_commit = time.monotonic()
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
        self._pending += 1
        now = time.monotonic()
        if (self._pending >= self.COMMIT_EVERY
                or now - self._last_commit >= self.COMMIT_INTERVAL_SEC):
            self._conn.commit()
            self._pending = 0
            self._last_commit = now

    def close(self):
        try:
            if self._pending:
                self._conn.commit()
        finally:
            self._conn.close()


_MARIADB_PASSWORD_ENV = "MQTT_LOGGER_MARIADB_PASSWORD"


def _resolve_mariadb_password(user: str) -> str:
    """Resolve the MariaDB password from (in order): environment, then keyring.

    The env var is documented as a fallback for environments without a
    Keychain-compatible store (e.g. Linux containers without dbus, or
    integration tests) — production hosts should use the Keychain.
    """
    env_pw = os.environ.get(_MARIADB_PASSWORD_ENV)
    if env_pw:
        return env_pw
    import keyring
    pw = keyring.get_password("mariadb-mqtt", user)
    if pw:
        return pw
    raise RuntimeError(
        f"No MariaDB password found for user {user!r}. "
        f"Either set ${_MARIADB_PASSWORD_ENV}, or store the password in "
        f"the Keychain: "
        f"security add-generic-password -a {user} -s mariadb-mqtt -w <pw> -U"
    )


class MariaDBBackend(DatabaseBackend):
    """MariaDB/MySQL backend — network-accessible, least-privilege account.

    Credentials are resolved in order:
        1. environment variable MQTT_LOGGER_MARIADB_PASSWORD (fallback)
        2. macOS Keychain (preferred)
               service = "mariadb-mqtt"
               account = "logger"   (INSERT + SELECT only)

    To store / update the password in the Keychain:
        security add-generic-password -a logger -s mariadb-mqtt -w <pw> -U
    """

    # Reconnect on transient errors rather than crashing the process.
    _RETRYABLE = {2006, 2013, 2055}   # CR_SERVER_GONE, CR_SERVER_LOST, etc.

    # Mirror of the SQLite schema. Mechanism is `CREATE TABLE IF NOT EXISTS`
    # so re-running against the existing table is a no-op. If the configured
    # user lacks CREATE privilege (the usual production setup — the `logger`
    # account is INSERT+SELECT only), the failure is logged at debug and
    # ignored; insertion will surface a real error if the table is missing.
    _DDL = (
        '''CREATE TABLE IF NOT EXISTS mqtt_events (
            id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            timestamp DATETIME(6)     NOT NULL,
            topic     TEXT            NOT NULL,
            sender    TEXT            DEFAULT NULL,
            payload   LONGTEXT        DEFAULT NULL,
            qos       TINYINT         NOT NULL,
            retained  TINYINT         NOT NULL,
            KEY idx_timestamp (timestamp),
            KEY idx_topic (topic(64))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci''',
    )

    def __init__(self, host: str = 'localhost', port: int = 3306,
                 database: str = 'mqtt_log', user: str = 'logger'):
        import pymysql

        password = _resolve_mariadb_password(user)

        self._connect_args = dict(
            host=host, port=port, user=user, password=password,
            database=database, charset='utf8mb4',
            autocommit=True,          # each INSERT commits immediately
        )
        self._pymysql = pymysql
        self._conn = self._connect()
        self._ensure_schema()
        logger.info(f"MariaDB backend ready: {user}@{host}:{port}/{database}")

    def _ensure_schema(self) -> None:
        """Best-effort schema creation. Mirrors SQLiteBackend.__init__, but
        gracefully skips when the configured user has no CREATE privilege —
        which is the expected case for the production `logger` account."""
        try:
            with self._conn.cursor() as cur:
                for stmt in self._DDL:
                    cur.execute(stmt)
        except self._pymysql.OperationalError as exc:
            # 1142 = ER_TABLEACCESS_DENIED_ERROR — fine, schema was provisioned
            # out-of-band. Any other operational error is real and worth raising.
            if exc.args[0] == 1142:
                logger.debug(
                    "MariaDB user lacks CREATE on mqtt_events; "
                    "assuming schema is provisioned out-of-band"
                )
            else:
                raise

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
            if exc.args[0] not in self._RETRYABLE:
                raise
            self._reconnect_and_retry("OperationalError", exc.args[0],
                                      timestamp, topic, sender, payload,
                                      qos, retained)
        except self._pymysql.InterfaceError as exc:
            # Raised when the underlying socket is already closed — usually
            # the server's wait_timeout fired during a quiet period. pymysql
            # surfaces this as InterfaceError(0, "") rather than an
            # OperationalError, so we have to handle it separately.
            self._reconnect_and_retry("InterfaceError", exc.args[0] if exc.args else None,
                                      timestamp, topic, sender, payload,
                                      qos, retained)

    def _reconnect_and_retry(self, exc_kind, code, timestamp, topic, sender,
                             payload, qos, retained):
        logger.warning(f"MariaDB connection lost ({exc_kind} {code}), reconnecting...")
        # If reconnect or the retried insert fails, the exception propagates
        # so the outer handler logs the dropped row instead of silently
        # swallowing it.
        self._conn = self._connect()
        self._do_insert(timestamp, topic, sender, payload, qos, retained)

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


# ─── Query backends ───────────────────────────────────────────────────────────
#
# Parallel hierarchy to the writer backends above, used by query_events.py.
# The CLI tool needs to read from whichever store the daemon is writing to,
# so the two backends must support the same set of read operations.
# Differences are limited to: SQL dialect (placeholders, regex), how
# timestamps are stored, and how the wildcard filter is implemented.


class QueryBackend(ABC):
    """Common interface for read-side backends used by query_events.py."""

    @abstractmethod
    def query_events(self, topic_pattern: str | None, since: datetime | None,
                     limit: int): ...

    @abstractmethod
    def list_topics(self): ...

    @abstractmethod
    def stats(self) -> dict: ...

    @abstractmethod
    def close(self) -> None: ...


def _mqtt_pattern_to_regex(pattern: str) -> str:
    """Translate an MQTT topic filter to an anchored regular expression.

    `+` matches exactly one level (no `/`); `#` matches the parent topic and
    any number of sub-levels (MQTT spec §4.7.1.2 — `sport/#` matches `sport`
    itself as well as `sport/tennis`, `sport/tennis/player1`, etc.).
    Both backends use the same translation — SQLite via a Python UDF, MariaDB
    via REGEXP.
    """
    import re as _re
    segments = pattern.split('/')
    parts = []
    for i, seg in enumerate(segments):
        if seg == '+':
            parts.append(r'[^/]+')
        elif seg == '#':
            if i != len(segments) - 1:
                raise ValueError(
                    f"'#' is only allowed as the final segment of an MQTT filter "
                    f"(got: {pattern!r})"
                )
            if parts:
                # e.g. "cova/#" → matches "cova", "cova/a", "cova/a/b/c"
                return '^' + '/'.join(parts) + r'(?:/.+)?$'
            else:
                # bare "#" → matches any non-empty topic
                return r'^.+$'
        else:
            parts.append(_re.escape(seg))
    return '^' + '/'.join(parts) + '$'


class SQLiteQueryBackend(QueryBackend):
    """Read-only view over the SQLite event store."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path)

    def _compile_topic_filter(self, topic_pattern: str | None):
        """Returns (sql_fragment, params). For wildcard patterns, registers a
        per-call UDF on the connection rather than translating to LIKE — `%`
        crosses `/` boundaries, which violates MQTT level semantics."""
        if not topic_pattern:
            return "", []
        if '+' in topic_pattern or '#' in topic_pattern:
            import re as _re
            compiled = _re.compile(_mqtt_pattern_to_regex(topic_pattern))
            self._conn.create_function(
                'mqtt_match', 1, lambda t: bool(compiled.match(t or ''))
            )
            return " AND mqtt_match(topic)", []
        return " AND topic = ?", [topic_pattern]

    def query_events(self, topic_pattern, since, limit):
        sql = ('SELECT timestamp, topic, sender, payload, qos, retained '
               'FROM mqtt_events WHERE 1=1')
        params = []
        frag, p = self._compile_topic_filter(topic_pattern)
        sql += frag
        params.extend(p)
        if since is not None:
            sql += ' AND timestamp >= ?'
            params.append(since.isoformat())
        sql += ' ORDER BY timestamp DESC LIMIT ?'
        params.append(limit)
        for ts_str, topic, sender, payload, qos, retained in self._conn.execute(sql, params):
            yield (datetime.fromisoformat(ts_str), topic, sender, payload, qos, retained)

    def list_topics(self):
        return self._conn.execute(
            'SELECT topic, COUNT(*) AS c FROM mqtt_events '
            'GROUP BY topic ORDER BY c DESC'
        )

    def stats(self) -> dict:
        total, unique, retained, first, last = self._conn.execute(
            'SELECT COUNT(*), COUNT(DISTINCT topic), SUM(retained), '
            'MIN(timestamp), MAX(timestamp) FROM mqtt_events'
        ).fetchone()

        def _ts(s):
            return datetime.fromisoformat(s) if s else None

        return {
            'total_events':   total or 0,
            'unique_topics':  unique or 0,
            'retained_count': retained or 0,
            'first_event':    _ts(first),
            'last_event':     _ts(last),
        }

    def close(self):
        self._conn.close()


class MariaDBQueryBackend(QueryBackend):
    """Read-only view over the MariaDB event store. Mirrors the SQLite
    backend's surface — only the dialect differs."""

    def __init__(self, host: str = 'localhost', port: int = 3306,
                 database: str = 'mqtt_log', user: str = 'logger'):
        import pymysql
        password = _resolve_mariadb_password(user)
        self._conn = pymysql.connect(
            host=host, port=port, user=user, password=password,
            database=database, charset='utf8mb4',
        )

    def _topic_filter(self, topic_pattern: str | None):
        if not topic_pattern:
            return "", []
        if '+' in topic_pattern or '#' in topic_pattern:
            return " AND topic REGEXP %s", [_mqtt_pattern_to_regex(topic_pattern)]
        return " AND topic = %s", [topic_pattern]

    def query_events(self, topic_pattern, since, limit):
        sql = ('SELECT timestamp, topic, sender, payload, qos, retained '
               'FROM mqtt_events WHERE 1=1')
        params = []
        frag, p = self._topic_filter(topic_pattern)
        sql += frag
        params.extend(p)
        if since is not None:
            sql += ' AND timestamp >= %s'
            params.append(since)   # pymysql formats datetime correctly
        sql += ' ORDER BY timestamp DESC LIMIT %s'
        params.append(limit)
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            yield from cur.fetchall()

    def list_topics(self):
        with self._conn.cursor() as cur:
            cur.execute(
                'SELECT topic, COUNT(*) AS c FROM mqtt_events '
                'GROUP BY topic ORDER BY c DESC'
            )
            yield from cur.fetchall()

    def stats(self) -> dict:
        with self._conn.cursor() as cur:
            cur.execute(
                'SELECT COUNT(*), COUNT(DISTINCT topic), '
                'SUM(retained=1), MIN(timestamp), MAX(timestamp) '
                'FROM mqtt_events'
            )
            total, unique, retained, first, last = cur.fetchone()
            return {
                'total_events': total or 0,
                'unique_topics': unique or 0,
                'retained_count': int(retained or 0),
                'first_event': first,
                'last_event': last,
            }

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

    # Periodically evict topics that haven't been seen for a long time so the
    # internal dicts can't grow without bound. The interval is wall-clockless —
    # eviction runs every Nth call to record().
    _EVICT_EVERY = 1024
    _EVICT_IDLE_SEC = 300

    def __init__(self, alert_file: str | None = None):
        self._counts: dict[str, deque[float]] = {}
        self._last_seen: dict[str, float] = {}
        self._last_alert: dict[str, float] = {}
        self._alert_file = alert_file
        self._records_since_evict = 0

    def record(self, topic: str) -> None:
        now = time.monotonic()
        timestamps = self._counts.setdefault(topic, deque())
        timestamps.append(now)
        self._last_seen[topic] = now

        cutoff = now - self.WINDOW_SEC
        while timestamps and timestamps[0] < cutoff:
            timestamps.popleft()

        if len(timestamps) >= self.THRESHOLD:
            last = self._last_alert.get(topic, 0)
            if now - last >= self.COOLDOWN_SEC:
                self._last_alert[topic] = now
                self._alert(topic, len(timestamps))

        self._records_since_evict += 1
        if self._records_since_evict >= self._EVICT_EVERY:
            self._evict_idle(now)
            self._records_since_evict = 0

    def _evict_idle(self, now: float) -> None:
        idle_cutoff = now - self._EVICT_IDLE_SEC
        idle = [t for t, seen in self._last_seen.items() if seen < idle_cutoff]
        for t in idle:
            self._counts.pop(t, None)
            self._last_seen.pop(t, None)
            self._last_alert.pop(t, None)

    # AppleScript template — receives the message as argv[1], title as argv[2],
    # so the (attacker-controlled) topic can never break out of the string literal.
    _OSASCRIPT_TEMPLATE = (
        'on run argv\n'
        '  display notification (item 1 of argv) '
        'with title (item 2 of argv) sound name "Sosumi"\n'
        'end run'
    )

    def _alert(self, topic: str, count: int) -> None:
        msg = f"MQTT flood: {count} msgs in {self.WINDOW_SEC}s on {topic}"
        logger.warning(msg)

        if self._alert_file:
            try:
                with open(self._alert_file, 'a') as f:
                    f.write(f"{datetime.now().isoformat()} {msg}\n")
            except OSError as exc:
                logger.warning(f"Could not write alert file {self._alert_file!r}: {exc}")

        if (platform.system() == 'Darwin'
                and not os.environ.get('MQTT_LOGGER_DISABLE_OSASCRIPT')):
            try:
                subprocess.Popen([
                    'osascript', '-e', self._OSASCRIPT_TEMPLATE,
                    '--', msg, 'MQTT Loop Detected',
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

            # Use wall-clock time rather than msg.timestamp (paho float) for consistency.
            # msg.mid / msg.dup are always 0/False at QoS=0 (our subscription level); not stored.
            # msg.properties (MQTT v5) — deployment is v3.1.1; not stored.
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
        # paho.loop_forever() exits cleanly when disconnect() is called from
        # any thread, including a signal handler.
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
    parser.add_argument('--mariadb-user', default='logger',
                        help='MariaDB user (default: logger)')

    parser.add_argument('--alert-file', default=None,
                        help='Override path for flood alerts '
                             '(default: <script_dir>/data/alerts.log)')

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
            user=args.mariadb_user,
        ))

    if not backends:
        # Default to SQLite for backwards compatibility
        db_path = script_dir / 'mqtt_events.db'
        logger.warning(f"No backend specified — defaulting to SQLite: {db_path}")
        backends.append(SQLiteBackend(str(db_path)))

    # Alert file location defaults to <script_dir>/data/alerts.log regardless
    # of which backend(s) are active — this matches alert_watcher.sh and the
    # Docker bind-mount. The directory is created on demand so a fresh
    # checkout works without manual setup. Tests override via --alert-file.
    if args.alert_file:
        alert_file = args.alert_file
        Path(alert_file).parent.mkdir(parents=True, exist_ok=True)
    else:
        data_dir = script_dir / 'data'
        data_dir.mkdir(parents=True, exist_ok=True)
        alert_file = str(data_dir / 'alerts.log')

    mqtt_logger = MQTTLogger(args.broker, args.port, backends,
                             alert_file=alert_file)

    signal.signal(signal.SIGTERM, lambda s, f: mqtt_logger.stop())
    signal.signal(signal.SIGINT,  lambda s, f: mqtt_logger.stop())

    mqtt_logger.run()


if __name__ == '__main__':
    main()
