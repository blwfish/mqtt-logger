"""Unit tests for query_events — wildcard semantics, parsing, output."""
from datetime import datetime, timedelta

import pytest

from mqtt_logger import SQLiteBackend, SQLiteQueryBackend
from query_events import (
    list_topics,
    mqtt_pattern_to_regex,
    parse_duration,
    query_events,
    show_stats,
)


@pytest.fixture
def seeded_backend(tmp_path):
    """Build a SQLite event store via the real writer, then hand back a
    read-side backend pointed at the same file. Verifies that the writer
    and reader stay schema-compatible."""
    db_path = tmp_path / "events.db"
    writer = SQLiteBackend(str(db_path))
    now = datetime(2026, 5, 12, 10, 0, 0)
    rows = [
        ("cova/foo/status", None, "1", 0, 0),
        ("cova/bar/status", None, "2", 0, 0),
        ("cova/foo/bar/status", None, "3", 0, 0),
        ("log/board1", "board1", "4", 1, 1),
        ("other/topic", None, "5", 0, 0),
    ]
    for topic, sender, payload, qos, retained in rows:
        writer.insert(now, topic, sender, payload, qos, retained)
    writer.close()
    backend = SQLiteQueryBackend(str(db_path))
    yield backend
    backend.close()


class TestParseDuration:
    @pytest.mark.parametrize("text, delta", [
        ("30m", timedelta(minutes=30)),
        ("2h", timedelta(hours=2)),
        ("7d", timedelta(days=7)),
        ("1H", timedelta(hours=1)),  # uppercase unit accepted
    ])
    def test_units(self, text, delta):
        assert parse_duration(text) == delta

    def test_unknown_unit_raises(self):
        with pytest.raises(ValueError):
            parse_duration("5x")

    def test_non_numeric_raises(self):
        with pytest.raises(ValueError):
            parse_duration("abm")


class TestMqttPatternToRegex:
    """Regression: the previous translator replaced both `+` and `#` with SQL
    `%`, so `cova/+/status` would have matched `cova/a/b/status`. The new
    translator must respect MQTT level boundaries."""

    def test_plus_is_single_level(self):
        import re
        rx = re.compile(mqtt_pattern_to_regex("cova/+/status"))
        assert rx.match("cova/foo/status")
        assert not rx.match("cova/foo/bar/status")
        assert not rx.match("cova/status")  # `+` requires at least one segment

    def test_hash_is_multi_level(self):
        import re
        rx = re.compile(mqtt_pattern_to_regex("cova/#"))
        assert rx.match("cova/foo")
        assert rx.match("cova/foo/bar/baz")
        assert not rx.match("cova")  # `#` requires at least one segment below

    def test_literal_topic(self):
        import re
        rx = re.compile(mqtt_pattern_to_regex("log/board1"))
        assert rx.match("log/board1")
        assert not rx.match("log/board2")

    def test_special_chars_in_segment_escaped(self):
        import re
        rx = re.compile(mqtt_pattern_to_regex("a.b/c"))
        assert rx.match("a.b/c")
        assert not rx.match("aXb/c")

    def test_hash_must_be_terminal(self):
        with pytest.raises(ValueError):
            mqtt_pattern_to_regex("cova/#/foo")


class TestQueryEvents:
    def test_no_filter_returns_recent(self, seeded_backend, capsys):
        query_events(seeded_backend)
        out = capsys.readouterr().out
        # All 5 seeded topics should appear.
        assert "cova/foo/status" in out
        assert "log/board1" in out

    def test_single_level_filter_excludes_deeper(self, seeded_backend, capsys):
        query_events(seeded_backend, topic_pattern="cova/+/status")
        out = capsys.readouterr().out
        assert "cova/foo/status" in out
        assert "cova/bar/status" in out
        assert "cova/foo/bar/status" not in out  # regression assertion

    def test_multi_level_filter(self, seeded_backend, capsys):
        query_events(seeded_backend, topic_pattern="cova/#")
        out = capsys.readouterr().out
        assert "cova/foo/status" in out
        assert "cova/foo/bar/status" in out
        assert "log/board1" not in out

    def test_literal_filter(self, seeded_backend, capsys):
        query_events(seeded_backend, topic_pattern="log/board1")
        out = capsys.readouterr().out
        assert "log/board1" in out
        assert "cova/" not in out

    def test_limit_applies(self, seeded_backend, capsys):
        query_events(seeded_backend, limit=2)
        out = capsys.readouterr().out
        lines = [l for l in out.splitlines() if l.startswith("2026")]
        assert len(lines) == 2


class TestStats:
    def test_show_stats(self, seeded_backend, capsys):
        show_stats(seeded_backend)
        out = capsys.readouterr().out
        assert "Total events:" in out
        assert "5" in out
        assert "Retained msgs:" in out


class TestListTopics:
    def test_list_topics(self, seeded_backend, capsys):
        list_topics(seeded_backend)
        out = capsys.readouterr().out
        assert "cova/foo/status" in out
        assert "log/board1" in out
