# MQTT Event Logger

A Python-based MQTT event logging and monitoring system that captures all MQTT messages from a broker and stores them in a SQLite database for analysis.

## Features

- Subscribes to all topics (`#`) for complete traffic capture
- Stores messages with timestamp, topic, payload, QoS, and retained flag
- Automatic sender extraction from JSON payloads
- Handles binary payloads (stores as hex)
- Query tool with topic filtering and time-based queries
- Runs as macOS launchd background service
- Rotating log files (5MB max, 3 backups)

## Requirements

- Python 3.10+
- paho-mqtt 2.x
- SQLite (built-in)
- MQTT broker (e.g., Mosquitto)

## Installation

```bash
# Clone or copy to desired location
cd /path/to/mqtt-logger

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install paho-mqtt
```

## Quick Start

```bash
source venv/bin/activate

# Run manually (broker must be running)
python mqtt_logger.py --broker localhost --port 1883

# With verbose output
python mqtt_logger.py -v
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--broker`, `-b` | localhost | MQTT broker hostname |
| `--port`, `-p` | 1883 | MQTT broker port |
| `--db`, `-d` | mqtt_events.db | SQLite database path |
| `--verbose`, `-v` | off | Enable debug logging |

## Query Events

```bash
source venv/bin/activate

# Recent events (default: 50)
python query_events.py

# List all topics with counts
python query_events.py --topics

# Filter by topic (supports # and + wildcards)
python query_events.py --topic "cova/#"

# Events from last hour
python query_events.py --since 1h

# Events from last 7 days, limit 100
python query_events.py --since 7d --limit 100

# Database stats
python query_events.py --stats
```

### Query Options

| Option | Description |
|--------|-------------|
| `--topics` | List unique topics with message counts |
| `--topic`, `-t` | Filter by topic pattern (`#` = multi-level, `+` = single-level) |
| `--since`, `-s` | Time filter: `30m`, `1h`, `7d` |
| `--limit`, `-n` | Max events to display (default: 50) |
| `--stats` | Show database statistics |
| `--db` | Custom database path |

## Docker (Recommended)

### Quick Start

```bash
# If broker is running on the host machine
docker compose up -d

# Check logs
docker compose logs -f

# Stop
docker compose down
```

### Configuration

Set environment variables in `.env` or pass them directly:

```bash
# .env file
MQTT_BROKER=192.168.1.100
MQTT_PORT=1883
```

Or override on command line:
```bash
MQTT_BROKER=mybroker.local docker compose up -d
```

### Data Persistence

The SQLite database is stored in a Docker volume (`mqtt-data`). To access it:

```bash
# Query events from host
docker compose exec mqtt-logger python query_events.py --stats
docker compose exec mqtt-logger python query_events.py --topics
docker compose exec mqtt-logger python query_events.py --since 1h

# Or copy the database out
docker compose cp mqtt-logger:/data/mqtt_events.db ./mqtt_events.db
```

### Build Only

```bash
docker build -t mqtt-logger .
docker run -d --name mqtt-logger \
  -e MQTT_BROKER=host.docker.internal \
  -v mqtt-data:/data \
  --add-host=host.docker.internal:host-gateway \
  mqtt-logger
```

## macOS launchd Service (Alternative)

If you prefer running without Docker:

### Install

```bash
# Symlink plist to LaunchAgents
ln -s "$(pwd)/com.blw.mqtt-logger.plist" ~/Library/LaunchAgents/

# Load the service
launchctl load ~/Library/LaunchAgents/com.blw.mqtt-logger.plist

# Check status
launchctl list | grep mqtt
```

### Manage

```bash
# View logs
tail -f mqtt_logger.log

# Unload service
launchctl unload ~/Library/LaunchAgents/com.blw.mqtt-logger.plist

# Reload after changes
launchctl unload ~/Library/LaunchAgents/com.blw.mqtt-logger.plist
launchctl load ~/Library/LaunchAgents/com.blw.mqtt-logger.plist
```

## Database Schema

```sql
CREATE TABLE mqtt_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,      -- ISO format
    topic TEXT NOT NULL,          -- MQTT topic
    sender TEXT,                  -- Extracted from payload
    payload TEXT,                 -- Message content (or hex for binary)
    qos INTEGER NOT NULL,         -- 0, 1, or 2
    retained INTEGER NOT NULL     -- 0 or 1
);

-- Indexes
CREATE INDEX idx_timestamp ON mqtt_events(timestamp);
CREATE INDEX idx_topic ON mqtt_events(topic);
```

## Sender Extraction

The `sender` field is automatically extracted from JSON payloads by checking these fields in order:
- `sender`
- `client_id`
- `clientId`
- `source`
- `from`
- `device_id`

Customize `mqtt_logger.py:extract_sender()` for your specific topic structure.

## Files

| File | Description |
|------|-------------|
| `mqtt_logger.py` | Main MQTT listener and database writer |
| `query_events.py` | CLI query utility |
| `mqtt_events.db` | SQLite database (created on first run) |
| `mqtt_logger.log` | Application log (rotating) |
| `com.blw.mqtt-logger.plist` | macOS launchd configuration |

## License

MIT
