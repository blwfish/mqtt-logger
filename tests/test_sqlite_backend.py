"""Unit tests for SQLiteBackend — schema, insert, batched commits."""
import sqlite3
import time
from datetime import datetime

import pytest

from mqtt_logger import SQLiteBackend


@pytest.fixture
def backend(tmp_path):
    db_path = tmp_path / "events.db"
    return SQLiteBackend(str(db_path))


@pytest.fixture
def fake_monotonic(monkeypatch):
    state = {"now": 1000.0}
    monkeypatch.setattr(time, "monotonic", lambda: state["now"])
    return state


class TestSchema:
    def test_table_and_indexes_created(self, backend):
        cur = backend._conn.execute(
            "SELECT name FROM sqlite_master WHERE type IN ('table', 'index')"
        )
        names = {row[0] for row in cur}
        assert "mqtt_events" in names
        assert "idx_timestamp" in names
        assert "idx_topic" in names

    def test_init_is_idempotent(self, tmp_path):
        path = tmp_path / "twice.db"
        SQLiteBackend(str(path))
        # Re-init against the same file — no IF-NOT-EXISTS violation.
        again = SQLiteBackend(str(path))
        assert again.db_path == str(path)

    def test_db_path_attribute_exposed(self, tmp_path):
        path = tmp_path / "exposed.db"
        b = SQLiteBackend(str(path))
        assert b.db_path == str(path)


class TestInsert:
    def test_round_trip(self, backend):
        ts = datetime(2026, 5, 12, 10, 30, 0)
        backend.insert(ts, "log/foo", "foo", '{"x":1}', 1, 0)
        backend.close()  # flushes any pending commit

        conn = sqlite3.connect(backend.db_path)
        row = conn.execute(
            "SELECT timestamp, topic, sender, payload, qos, retained "
            "FROM mqtt_events"
        ).fetchone()
        assert row == (ts.isoformat(), "log/foo", "foo", '{"x":1}', 1, 0)

    def test_null_sender_and_payload(self, backend):
        backend.insert(datetime.now(), "t", None, None, 0, 0)
        backend.close()
        conn = sqlite3.connect(backend.db_path)
        row = conn.execute(
            "SELECT sender, payload FROM mqtt_events"
        ).fetchone()
        assert row == (None, None)


class TestBatchedCommits:
    """The backend must commit every COMMIT_EVERY rows OR every
    COMMIT_INTERVAL_SEC, whichever comes first."""

    def test_commits_after_size_threshold(self, backend, fake_monotonic):
        # Insert COMMIT_EVERY-1 rows — nothing visible to a fresh connection.
        for _ in range(SQLiteBackend.COMMIT_EVERY - 1):
            backend.insert(datetime.now(), "t", None, "p", 0, 0)

        peek = sqlite3.connect(backend.db_path)
        # WAL means a second connection still sees only committed data.
        count_before = peek.execute(
            "SELECT COUNT(*) FROM mqtt_events"
        ).fetchone()[0]
        assert count_before == 0

        # The COMMIT_EVERY-th insert triggers a commit.
        backend.insert(datetime.now(), "t", None, "p", 0, 0)
        peek.close()
        peek = sqlite3.connect(backend.db_path)
        count_after = peek.execute(
            "SELECT COUNT(*) FROM mqtt_events"
        ).fetchone()[0]
        assert count_after == SQLiteBackend.COMMIT_EVERY

    def test_commits_after_size_threshold_plus_one(self, backend, fake_monotonic):
        """COMMIT_EVERY+1 inserts must still commit at the boundary — pins
        that the trigger is >= not >, and that pending resets after commit."""
        for _ in range(SQLiteBackend.COMMIT_EVERY + 1):
            backend.insert(datetime.now(), "t", None, "p", 0, 0)
        peek = sqlite3.connect(backend.db_path)
        count = peek.execute(
            "SELECT COUNT(*) FROM mqtt_events"
        ).fetchone()[0]
        # Commit fired at row COMMIT_EVERY; the +1 row is still pending.
        assert count == SQLiteBackend.COMMIT_EVERY

    def test_commits_after_time_interval(self, tmp_path, fake_monotonic):
        # Construct backend AFTER the monotonic patch so _last_commit captures
        # the fake clock, not the real one.
        backend = SQLiteBackend(str(tmp_path / "events.db"))
        backend.insert(datetime.now(), "t", None, "p", 0, 0)
        peek = sqlite3.connect(backend.db_path)
        assert peek.execute(
            "SELECT COUNT(*) FROM mqtt_events"
        ).fetchone()[0] == 0

        # Advance monotonic clock past the interval; next insert flushes.
        fake_monotonic["now"] += SQLiteBackend.COMMIT_INTERVAL_SEC + 0.01
        backend.insert(datetime.now(), "t", None, "p", 0, 0)

        peek.close()
        peek = sqlite3.connect(backend.db_path)
        assert peek.execute(
            "SELECT COUNT(*) FROM mqtt_events"
        ).fetchone()[0] == 2

    def test_close_flushes_pending_rows(self, backend, fake_monotonic):
        backend.insert(datetime.now(), "t", None, "p", 0, 0)
        backend.close()

        peek = sqlite3.connect(backend.db_path)
        assert peek.execute(
            "SELECT COUNT(*) FROM mqtt_events"
        ).fetchone()[0] == 1
