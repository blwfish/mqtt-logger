#!/usr/bin/env python3
"""
Query utility for MQTT events.

Selects a backend exactly the way mqtt_logger.py does:
    --db PATH    → SQLite
    --mariadb    → MariaDB (credentials from Keychain)
    neither      → auto-detect SQLite at /data/mqtt_events.db
                   (Docker volume) or ./mqtt_events.db (local).

Examples:
    python query_events.py                       # recent events from SQLite
    python query_events.py --mariadb             # recent events from MariaDB
    python query_events.py --topics              # list unique topics
    python query_events.py --topic 'cova/+/status'   # MQTT wildcard filter
    python query_events.py --since 1h --limit 100
    python query_events.py --stats
"""

import argparse
from datetime import datetime, timedelta
from pathlib import Path

from mqtt_logger import (
    MariaDBQueryBackend,
    QueryBackend,
    SQLiteQueryBackend,
)

# Re-export the regex helper so existing tests (and any external callers)
# don't break.
from mqtt_logger import _mqtt_pattern_to_regex as mqtt_pattern_to_regex  # noqa: F401


def parse_duration(duration_str: str) -> timedelta:
    """Parse duration like '1h', '30m', '7d' into timedelta."""
    unit = duration_str[-1].lower()
    value = int(duration_str[:-1])
    if unit == 'm':
        return timedelta(minutes=value)
    if unit == 'h':
        return timedelta(hours=value)
    if unit == 'd':
        return timedelta(days=value)
    raise ValueError(f"Unknown duration unit: {unit}")


def list_topics(backend: QueryBackend) -> None:
    print(f"{'Topic':<60} {'Count':>8}")
    print("-" * 70)
    for topic, count in backend.list_topics():
        print(f"{topic:<60} {count:>8}")


def show_stats(backend: QueryBackend) -> None:
    s = backend.stats()
    print("MQTT Events Database Statistics")
    print("=" * 40)
    print(f"Total events:    {s['total_events']:,}")
    print(f"Unique topics:   {s['unique_topics']:,}")
    print(f"Retained msgs:   {s['retained_count']:,}")
    print(f"First event:     {s['first_event'] or 'N/A'}")
    print(f"Last event:      {s['last_event'] or 'N/A'}")


def query_events(backend: QueryBackend, topic_pattern: str | None = None,
                 since: str | None = None, limit: int = 50) -> None:
    cutoff = None
    if since:
        try:
            cutoff = datetime.now() - parse_duration(since)
        except ValueError as e:
            print(f"Invalid duration: {e}")
            return

    for row in backend.query_events(topic_pattern, cutoff, limit):
        timestamp, topic, sender, payload, qos, retained = row
        display_payload = (payload[:80] + '...'
                           if payload and len(payload) > 80 else payload)
        ret_flag = 'R' if retained else ' '
        sender_str = f" [{sender}]" if sender else ""
        print(f"{timestamp} Q{qos}{ret_flag} {topic}{sender_str}")
        if display_payload:
            print(f"    {display_payload}")
        print()


def default_sqlite_path() -> Path:
    """Check for Docker volume path first, then fall back to local file."""
    docker_path = Path('/data/mqtt_events.db')
    if docker_path.exists():
        return docker_path
    return Path(__file__).parent / 'mqtt_events.db'


def build_backend(args) -> QueryBackend:
    if args.mariadb:
        return MariaDBQueryBackend(
            host=args.mariadb_host,
            port=args.mariadb_port,
            database=args.mariadb_db,
            user=args.mariadb_user,
        )

    db_path = Path(args.db) if args.db else default_sqlite_path()
    if not db_path.exists():
        raise SystemExit(
            f"SQLite database not found: {db_path}\n"
            "Run mqtt_logger.py first, or pass --mariadb."
        )
    return SQLiteQueryBackend(str(db_path))


def main():
    parser = argparse.ArgumentParser(description='Query MQTT events database')

    parser.add_argument('--db', default=None,
                        help='SQLite database path')

    parser.add_argument('--mariadb', action='store_true',
                        help='Query MariaDB instead of SQLite '
                             '(credentials from Keychain)')
    parser.add_argument('--mariadb-host', default='localhost',
                        help='MariaDB host (default: localhost)')
    parser.add_argument('--mariadb-port', type=int, default=3306,
                        help='MariaDB port (default: 3306)')
    parser.add_argument('--mariadb-db', default='mqtt_log',
                        help='MariaDB database name (default: mqtt_log)')
    parser.add_argument('--mariadb-user', default='logger',
                        help='MariaDB user (default: logger)')

    parser.add_argument('--topics', action='store_true',
                        help='List unique topics with message counts')
    parser.add_argument('--topic', '-t',
                        help='Filter by MQTT topic pattern (supports # and +)')
    parser.add_argument('--since', '-s',
                        help='Show events since duration (e.g., 1h, 30m, 7d)')
    parser.add_argument('--limit', '-n', type=int, default=50,
                        help='Max events to show')
    parser.add_argument('--stats', action='store_true',
                        help='Show database statistics')

    args = parser.parse_args()
    backend = build_backend(args)
    try:
        if args.topics:
            list_topics(backend)
        elif args.stats:
            show_stats(backend)
        else:
            query_events(backend, args.topic, args.since, args.limit)
    finally:
        backend.close()


if __name__ == '__main__':
    main()
