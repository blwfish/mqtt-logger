# MQTT Logger - Project Status

## Overview

| Item | Status |
|------|--------|
| **Project** | MQTT Event Logger |
| **Version** | 1.0.0 |
| **Status** | Operational |
| **Platform** | macOS (launchd service) |

## Components

| Component | File | Status | Description |
|-----------|------|--------|-------------|
| Logger | `mqtt_logger.py` | Complete | MQTT listener with SQLite storage |
| Query Tool | `query_events.py` | Complete | CLI for querying events |
| Service | `com.blw.mqtt-logger.plist` | Complete | macOS launchd configuration |
| Database | `mqtt_events.db` | Active | SQLite event storage |

## Features

| Feature | Status | Notes |
|---------|--------|-------|
| Subscribe to all topics (#) | Done | Captures all MQTT traffic |
| SQLite persistence | Done | Indexed on timestamp and topic |
| Sender extraction | Done | Extracts from JSON payload fields |
| Binary payload handling | Done | Stores as hex string |
| Topic filtering | Done | Supports # and + wildcards |
| Time-based queries | Done | Supports m/h/d duration format |
| Statistics display | Done | Total events, topics, date range |
| Rotating logs | Done | 5MB max, 3 backups |
| launchd service | Done | Auto-start, auto-restart |
| Graceful shutdown | Done | SIGTERM/SIGINT handling |

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| Python | 3.10+ | Runtime |
| paho-mqtt | 2.x | MQTT client |
| sqlite3 | built-in | Database |

## Configuration

| Setting | Value | Location |
|---------|-------|----------|
| Default broker | localhost:1883 | CLI args |
| Database path | mqtt_events.db | CLI args |
| Log rotation | 5MB / 3 backups | mqtt_logger.py:160 |
| Service throttle | 10 seconds | plist |

## Known Limitations

- Single-threaded database writes (adequate for typical MQTT traffic)
- No authentication support (add `client.username_pw_set()` if needed)
- No TLS support (add `client.tls_set()` if needed)
- Sender extraction requires JSON payloads with known field names

## Future Enhancements

- [ ] MQTT authentication support
- [ ] TLS/SSL connections
- [ ] Web UI for browsing events
- [ ] Export to CSV/JSON
- [ ] Configurable topic filters (not just #)
- [ ] Message payload search
- [ ] Retention policy / auto-cleanup

## File Structure

```
mqtt-logger/
├── mqtt_logger.py          # Main application
├── query_events.py         # Query utility
├── mqtt_events.db          # SQLite database
├── mqtt_logger.log         # Application log
├── com.blw.mqtt-logger.plist  # launchd service
├── README.md               # Documentation
├── STATUS.md               # This file
├── venv/                   # Python virtual environment
└── mosquitto/              # Local broker config (optional)
```

## Changelog

### 1.0.0
- Initial release
- Full MQTT traffic capture
- SQLite storage with indexes
- Query tool with filtering
- macOS launchd service support
