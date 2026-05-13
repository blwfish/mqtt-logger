# MQTT Event Logger

Captures all MQTT messages from a broker and persists them to one or more
database backends for later analysis.

Two parallel backends are supported:

- **SQLite** — single-file, no setup, ideal for embedded / "foreign"
  deployments (e.g. running alongside audio-node or layout-coordinator on
  hosts where running a database server isn't feasible).
- **MariaDB / MySQL** — network-accessible, multi-writer friendly,
  intended for the central logging host. Credentials are fetched from the
  macOS Keychain at startup.

Both backends can run simultaneously (useful during migration validation):
every message is written to every configured backend.

## Features

- Subscribes to `#` (all application topics; `$SYS` is excluded by brokers).
- Stores timestamp, topic, payload, QoS, retained flag, and an extracted
  sender (from common JSON keys like `board`, `sender`, `client_id`).
- Binary payloads stored as hex.
- Flood / loop detector: per-topic sliding-window rate limit; alerts go to
  `data/alerts.log` and (on macOS) fire a native notification.
- Query CLI with MQTT-aware wildcards (`+` single-level, `#` multi-level).
- Runs as a Docker container, a macOS launchd service, or directly.

## Requirements

- Python 3.10+
- paho-mqtt 2.x
- PyMySQL + keyring (only when using the MariaDB backend)
- An MQTT broker (e.g. Mosquitto)

## Installation (local venv)

```bash
python3 -m venv venv
source venv/bin/activate
pip install paho-mqtt PyMySQL keyring
```

## Backend selection

The logger and the query tool share the same flag layout. Backend selection
is symmetric across both:

| Flag(s)                   | Result                                   |
|---------------------------|------------------------------------------|
| `--db PATH`               | SQLite at PATH                           |
| `--mariadb`               | MariaDB (credentials from Keychain)      |
| `--db PATH --mariadb`     | Both — dual-write / dual-query           |
| _none_                    | SQLite at the default location           |

MariaDB connection details (only meaningful when `--mariadb` is passed):

```
--mariadb-host HOST     default: localhost
--mariadb-port PORT     default: 3306
--mariadb-db   NAME     default: mqtt_log
```

### MariaDB credentials

Stored in the macOS Keychain — never in config files or command-line args.

```bash
# logger account (INSERT + SELECT only — used by the daemon)
security add-generic-password -a logger -s mariadb-mqtt -w '<password>' -U

# root or admin (only used for schema bootstrap / migrations)
security add-generic-password -a root   -s mariadb-mqtt -w '<password>' -U
```

The MariaDB backend will attempt `CREATE TABLE IF NOT EXISTS` on startup;
this is a no-op against an existing table and is silently skipped when the
configured user lacks the `CREATE` privilege.

## Running the logger

```bash
# SQLite (default)
python mqtt_logger.py --broker localhost

# MariaDB only
python mqtt_logger.py --broker localhost --mariadb

# Dual-write
python mqtt_logger.py --broker localhost --db data/mqtt_events.db --mariadb
```

## Querying

`query_events.py` mirrors the same flags:

```bash
# Recent events from SQLite
python query_events.py

# Recent events from MariaDB
python query_events.py --mariadb

# MQTT topic filter — wildcards respect level boundaries
python query_events.py --topic 'cova/+/status'     # one segment between
python query_events.py --topic 'cova/#'            # any depth below

# Time window
python query_events.py --since 1h
python query_events.py --since 7d --limit 100

# Aggregate views
python query_events.py --topics
python query_events.py --stats
```

## Docker

`docker-compose.yml` exposes both backends via env vars. SQLite is on by
default; set `MQTT_MARIADB=1` to enable MariaDB alongside or instead.

```yaml
environment:
  - MQTT_BROKER=192.168.68.250
  - MQTT_DB=/data/mqtt_events.db        # leave unset to disable SQLite
  - MQTT_MARIADB=1                      # enable MariaDB
  - MQTT_MARIADB_HOST=host.docker.internal
  - MQTT_MARIADB_DB=mqtt_log
```

```bash
docker compose up -d
docker compose logs -f
docker compose exec mqtt-logger python query_events.py --stats
```

## macOS launchd service

```bash
ln -s "$(pwd)/com.blw.mqtt-logger.plist" ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.blw.mqtt-logger.plist
launchctl list | grep mqtt
```

The bundled plist runs the daemon with `--mariadb` (current production
configuration). Adjust `ProgramArguments` to add `--db <path>` for
dual-write or to switch to SQLite-only.

## Schema

```sql
-- SQLite (auto-created by SQLiteBackend on first connect)
CREATE TABLE mqtt_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,      -- ISO 8601
    topic TEXT NOT NULL,
    sender TEXT,
    payload TEXT,                 -- text or hex of binary
    qos INTEGER NOT NULL,
    retained INTEGER NOT NULL
);
CREATE INDEX idx_timestamp ON mqtt_events(timestamp);
CREATE INDEX idx_topic     ON mqtt_events(topic);

-- MariaDB (best-effort auto-created; identical column semantics)
CREATE TABLE mqtt_events (
    id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME(6)     NOT NULL,
    topic     VARCHAR(512)    NOT NULL,
    sender    VARCHAR(255)    DEFAULT NULL,
    payload   LONGTEXT        DEFAULT NULL,
    qos       TINYINT         NOT NULL,
    retained  TINYINT         NOT NULL,
    KEY idx_timestamp (timestamp),
    KEY idx_topic (topic(64))     -- prefix index on a long VARCHAR
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
```

## Sender extraction

The `sender` field is populated from one of two sources, in this order:

1. **Topic pattern** — `log/{board}` or `{prefix}/config/(status|backup)/{board}`.
2. **JSON payload key** — first match from: `board`, `sender`, `client_id`,
   `clientId`, `source`, `from`, `device_id`.

Customise [extract_sender()](mqtt_logger.py) for project-specific patterns.

## Files

| File                                   | Description                              |
|----------------------------------------|------------------------------------------|
| `mqtt_logger.py`                       | MQTT listener + writer backends          |
| `query_events.py`                      | CLI query tool (same backend selection)  |
| `Dockerfile` / `entrypoint.sh`         | Container build + env-var → CLI mapping  |
| `docker-compose.yml`                   | Compose recipe                           |
| `com.blw.mqtt-logger.plist`            | macOS launchd config                     |
| `alert_watcher.sh` / `com.blw.mqtt-alert-watcher.plist` | Flood-alert tailer + launchd config |
| `data/mqtt_events.db` (if SQLite)      | Event store                              |
| `data/alerts.log`                      | Flood-alert log (tailed by alert_watcher)|
| `mqtt_logger.log`                      | Application log (rotating)               |
| `tests/`                               | pytest suite (76 unit tests, no I/O)     |

## Tests

```bash
pip install pytest pytest-mock freezegun
pytest tests/
```

Pure unit tests; no broker / DB / network required.

## License

MIT
