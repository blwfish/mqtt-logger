# MQTT Logger — Project Status

## Overview

| Item        | Status                                             |
|-------------|----------------------------------------------------|
| Project     | MQTT Event Logger                                  |
| Version     | 1.1.0                                              |
| Status      | Operational (MariaDB backend live)                 |
| Platform    | macOS (launchd) + Docker                           |

## Backends

Two parallel storage backends. Both are first-class — every code path that
exists for one exists for the other, with differences limited to dialect
(SQL placeholders, regex operators) and credentials.

| Backend  | Use case                              | Current host       |
|----------|---------------------------------------|--------------------|
| SQLite   | Embedded / foreign deployments        | Available, dormant |
| MariaDB  | Central logging host                  | **Active**         |

The live launchd plist runs `--mariadb` only. SQLite is enabled by adding
`--db <path>` and is the default when no backend flag is passed.

## Components

| Component           | File                              | Status   |
|---------------------|-----------------------------------|----------|
| Listener / writer   | `mqtt_logger.py`                  | Complete |
| Query CLI           | `query_events.py`                 | Complete |
| Flood-alert tailer  | `alert_watcher.sh`                | Complete |
| launchd (logger)    | `com.blw.mqtt-logger.plist`       | Complete |
| launchd (watcher)   | `com.blw.mqtt-alert-watcher.plist`| Complete |
| Docker              | `Dockerfile` + `entrypoint.sh`    | Complete |
| Unit tests          | `tests/*.py`                      | 78 tests |
| Integration tests   | `tests/integration/`              | 44 tests, 1 skip |

## Features

| Feature                                             | Status |
|-----------------------------------------------------|--------|
| Subscribe to `#`                                    | Done   |
| SQLite persistence + auto-schema                    | Done   |
| MariaDB persistence + best-effort auto-schema       | Done   |
| Dual-write (SQLite + MariaDB simultaneously)        | Done   |
| Query CLI against both backends                     | Done   |
| MQTT-aware wildcards in queries (`+` / `#`)         | Done   |
| Sender extraction (topic patterns + JSON keys)      | Done   |
| Binary payload handling (hex)                       | Done   |
| Time-window query filter                            | Done   |
| Per-topic flood detector with cooldown + eviction   | Done   |
| Batched SQLite commits (25 rows / 1 s)              | Done   |
| MariaDB reconnect on transient errors               | Done   |
| AppleScript-safe macOS notifications                | Done   |
| Rotating application log                            | Done   |
| Graceful shutdown (SIGTERM / SIGINT)                | Done   |

## Dependencies

| Package     | Version     | Required for          |
|-------------|-------------|-----------------------|
| Python      | 3.10+       | Runtime               |
| paho-mqtt   | 2.x         | All deployments       |
| PyMySQL     | 1.1.x       | MariaDB backend       |
| keyring     | 25.x        | MariaDB backend       |
| sqlite3     | (stdlib)    | SQLite backend        |

## Configuration

| Setting                  | Value / Location                       |
|--------------------------|----------------------------------------|
| Broker                   | `--broker` (default `localhost`)       |
| SQLite path              | `--db` (default `./mqtt_events.db`)    |
| MariaDB host/port/db     | `--mariadb-host` / `-port` / `-db`     |
| MariaDB credentials      | macOS Keychain (`mariadb-mqtt` service)|
| Alert file               | `./data/alerts.log` (fixed)            |
| Log rotation             | 50 MB × 3 backups                      |
| Flood threshold          | 10 msgs / 5 s window, 60 s cooldown    |
| SQLite commit batch      | 25 rows or 1 s, whichever first        |

## Known Limitations

- Sender extraction only works for JSON payloads with known field names.
- No MQTT auth / TLS — trust boundary is the LAN.
- Single-threaded callback (paho `loop_forever`); high-volume floods are
  serialised through the same thread that writes them.
- Query CLI fetches results into memory; no streaming for very large
  result sets.

## File Structure

```
mqtt-logger/
├── mqtt_logger.py           # Writer + reader backends, listener, main()
├── query_events.py          # CLI query tool
├── Dockerfile               # Container image
├── entrypoint.sh            # Env-var → CLI flag translation for Docker
├── docker-compose.yml       # Compose recipe
├── alert_watcher.sh         # Tail data/alerts.log → macOS notifications
├── com.blw.mqtt-logger.plist        # launchd config (daemon)
├── com.blw.mqtt-alert-watcher.plist # launchd config (alert watcher)
├── pyproject.toml           # pytest config
├── tests/                   # 76 unit tests, no I/O
├── data/                    # SQLite db + alerts.log (created on demand)
├── mosquitto/               # Optional local broker config
├── README.md
└── STATUS.md                # This file
```

## Changelog

### 1.2.0 — integration test tier

- New tier of 44 integration tests under `tests/integration/`, gated by
  the `integration` pytest marker. Disposable Mosquitto + MariaDB
  containers via testcontainers-python. Default `pytest` invocation
  still runs only the 78-test unit tier (~0.1 s); `pytest -m integration`
  runs the integration tier (~30 s).
- Parameterized dialect-parity tests in `test_dialect_parity.py`: the
  same test body runs against both SQLite and MariaDB.
- Daemon-as-subprocess tests covering SIGTERM/SIGINT graceful shutdown
  and the batched-commit flush-on-close guarantee.
- Reconnect tests for both MariaDB (server-side KILL) and MQTT broker
  (container stop/start).
- Real-flood test verifies alerts.log is written through the full
  pipeline, and that cooldown suppresses duplicate alerts.
- Bug fixed (surfaced by integration test): the reconnect path now also
  catches `pymysql.InterfaceError`, not just `OperationalError`.
  pymysql raises `InterfaceError(0, "")` when the socket has already
  closed (typical of `wait_timeout`-dropped idle connections); previous
  code would let this propagate and drop the row.
- New CLI flag `--alert-file` overrides the flood-alert path (used by
  integration tests; defaults match production).
- New CLI flag `--mariadb-user` on both `mqtt_logger.py` and
  `query_events.py` (default still `logger`).
- New `MQTT_LOGGER_MARIADB_PASSWORD` env-var fallback for MariaDB
  credentials — used by integration tests and Linux deployments
  without a Keychain-compatible store; Keychain remains the preferred
  primary source.

### 1.1.0 — backend parity

- MariaDB backend reaches feature-parity with SQLite: auto-schema, query
  CLI, Docker support, unified alert-file location.
- Query CLI (`query_events.py`) gained `--mariadb` flag mirroring the
  daemon's flag layout. Wildcard `+` now respects MQTT level boundaries
  in both backends (previously matched across `/`).
- Flood-alert injection vulnerability closed: attacker-controlled MQTT
  topics can no longer escape into AppleScript / shell context.
- SQLite commits batched (25 rows / 1 s) — removes per-row fsync cost.
- MariaDB reconnect-retry no longer silently drops the failing row.
- LoopDetector switched to bounded `deque` + periodic idle eviction.
- Dockerfile runs as non-root, installs PyMySQL + keyring.
- 76 unit tests added (pure unit, no broker / DB / network).

### 1.0.0

- Initial release with SQLite-only storage.
