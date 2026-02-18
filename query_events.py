#!/usr/bin/env python3
"""
Query utility for MQTT events database.

Usage:
    python query_events.py                    # Show recent events
    python query_events.py --topics           # List unique topics
    python query_events.py --topic cova/#     # Filter by topic pattern
    python query_events.py --since 1h         # Events from last hour
    python query_events.py --stats            # Show statistics
"""

import argparse
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path


def get_db_path():
    # Check for Docker volume path first, then fall back to local
    docker_path = Path('/data/mqtt_events.db')
    if docker_path.exists():
        return docker_path
    return Path(__file__).parent / 'mqtt_events.db'


def parse_duration(duration_str: str) -> timedelta:
    """Parse duration like '1h', '30m', '7d' into timedelta."""
    unit = duration_str[-1].lower()
    value = int(duration_str[:-1])

    if unit == 'm':
        return timedelta(minutes=value)
    elif unit == 'h':
        return timedelta(hours=value)
    elif unit == 'd':
        return timedelta(days=value)
    else:
        raise ValueError(f"Unknown duration unit: {unit}")


def list_topics(conn: sqlite3.Connection):
    """List all unique topics with message counts."""
    cursor = conn.execute('''
        SELECT topic, COUNT(*) as count
        FROM mqtt_events
        GROUP BY topic
        ORDER BY count DESC
    ''')

    print(f"{'Topic':<60} {'Count':>8}")
    print("-" * 70)
    for row in cursor:
        print(f"{row[0]:<60} {row[1]:>8}")


def show_stats(conn: sqlite3.Connection):
    """Show database statistics."""
    stats = {}

    stats['total_events'] = conn.execute('SELECT COUNT(*) FROM mqtt_events').fetchone()[0]
    stats['unique_topics'] = conn.execute('SELECT COUNT(DISTINCT topic) FROM mqtt_events').fetchone()[0]
    stats['retained_count'] = conn.execute('SELECT COUNT(*) FROM mqtt_events WHERE retained=1').fetchone()[0]

    first = conn.execute('SELECT MIN(timestamp) FROM mqtt_events').fetchone()[0]
    last = conn.execute('SELECT MAX(timestamp) FROM mqtt_events').fetchone()[0]

    print("MQTT Events Database Statistics")
    print("=" * 40)
    print(f"Total events:    {stats['total_events']:,}")
    print(f"Unique topics:   {stats['unique_topics']:,}")
    print(f"Retained msgs:   {stats['retained_count']:,}")
    print(f"First event:     {first or 'N/A'}")
    print(f"Last event:      {last or 'N/A'}")


def query_events(conn: sqlite3.Connection, topic_pattern: str = None,
                 since: str = None, limit: int = 50):
    """Query and display events."""
    query = 'SELECT timestamp, topic, sender, payload, qos, retained FROM mqtt_events WHERE 1=1'
    params = []

    if topic_pattern:
        # Convert MQTT wildcard to SQL LIKE pattern
        sql_pattern = topic_pattern.replace('#', '%').replace('+', '%')
        query += ' AND topic LIKE ?'
        params.append(sql_pattern)

    if since:
        try:
            delta = parse_duration(since)
            cutoff = (datetime.now() - delta).isoformat()
            query += ' AND timestamp >= ?'
            params.append(cutoff)
        except ValueError as e:
            print(f"Invalid duration: {e}")
            return

    query += ' ORDER BY timestamp DESC LIMIT ?'
    params.append(limit)

    cursor = conn.execute(query, params)

    for row in cursor:
        timestamp, topic, sender, payload, qos, retained = row

        # Truncate payload for display
        display_payload = payload[:80] + '...' if payload and len(payload) > 80 else payload

        ret_flag = 'R' if retained else ' '
        sender_str = f" [{sender}]" if sender else ""

        print(f"{timestamp} Q{qos}{ret_flag} {topic}{sender_str}")
        if display_payload:
            print(f"    {display_payload}")
        print()


def main():
    parser = argparse.ArgumentParser(description='Query MQTT events database')
    parser.add_argument('--db', default=None, help='Database path')
    parser.add_argument('--topics', action='store_true', help='List unique topics')
    parser.add_argument('--topic', '-t', help='Filter by topic pattern (supports # and +)')
    parser.add_argument('--since', '-s', help='Show events since duration (e.g., 1h, 30m, 7d)')
    parser.add_argument('--limit', '-n', type=int, default=50, help='Max events to show')
    parser.add_argument('--stats', action='store_true', help='Show statistics')

    args = parser.parse_args()

    db_path = Path(args.db) if args.db else get_db_path()

    if not db_path.exists():
        print(f"Database not found: {db_path}")
        print("Run mqtt_logger.py first to create the database.")
        return

    conn = sqlite3.connect(str(db_path))

    try:
        if args.topics:
            list_topics(conn)
        elif args.stats:
            show_stats(conn)
        else:
            query_events(conn, args.topic, args.since, args.limit)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
