"""Same test body, run against both backends. Any test in here failing on
one backend but passing on the other is a parity bug."""
from datetime import datetime

import pytest

from mqtt_logger import (
    MariaDBBackend,
    MariaDBQueryBackend,
    SQLiteBackend,
    SQLiteQueryBackend,
)


# ─── Backend factories ────────────────────────────────────────────────────────
#
# Each backend kind is keyed by a name and a callable that returns
# (writer, reader, ctx_cleanup_callable). Per-test isolation is the
# writer/reader scope; the tier-level fixture handles container teardown.

def _sqlite_factory(tmp_path):
    db = str(tmp_path / "parity.db")
    writer = SQLiteBackend(db)

    def reader_factory():
        return SQLiteQueryBackend(db)

    return writer, reader_factory


def _mariadb_factory(mariadb):
    writer = MariaDBBackend(
        host=mariadb.host, port=mariadb.port,
        database=mariadb.database, user=mariadb.user,
    )

    def reader_factory():
        return MariaDBQueryBackend(
            host=mariadb.host, port=mariadb.port,
            database=mariadb.database, user=mariadb.user,
        )

    return writer, reader_factory


@pytest.fixture(params=["sqlite", "mariadb"])
def writer_and_reader(request, tmp_path, mariadb):
    """Per-test fresh writer + reader factory, parameterized across backends."""
    if request.param == "sqlite":
        writer, reader_factory = _sqlite_factory(tmp_path)
    else:
        writer, reader_factory = _mariadb_factory(mariadb)

    try:
        yield writer, reader_factory
    finally:
        try:
            writer.close()
        except Exception:
            pass


# ─── Parametrized tests ───────────────────────────────────────────────────────

class TestParity:
    def test_round_trip_single_row(self, writer_and_reader):
        writer, reader_factory = writer_and_reader
        ts = datetime(2026, 5, 12, 9, 0, 0)
        writer.insert(ts, "log/foo", "foo", '{"a":1}', 1, 1)
        writer.close()

        reader = reader_factory()
        try:
            rows = list(reader.query_events(None, None, 10))
        finally:
            reader.close()

        assert len(rows) == 1
        _ts, topic, sender, payload, qos, retained = rows[0]
        assert topic == "log/foo"
        assert sender == "foo"
        assert payload == '{"a":1}'
        assert qos == 1
        assert retained == 1

    def test_wildcard_plus_single_level(self, writer_and_reader):
        writer, reader_factory = writer_and_reader
        now = datetime(2026, 5, 12, 9, 0, 0)
        for topic in ("cova/a/status", "cova/b/status", "cova/a/b/status"):
            writer.insert(now, topic, None, "p", 0, 0)
        writer.close()

        reader = reader_factory()
        try:
            rows = list(reader.query_events("cova/+/status", None, 10))
        finally:
            reader.close()

        topics = {r[1] for r in rows}
        assert topics == {"cova/a/status", "cova/b/status"}

    def test_wildcard_hash_multi_level(self, writer_and_reader):
        writer, reader_factory = writer_and_reader
        now = datetime(2026, 5, 12, 9, 0, 0)
        for topic in ("cova/a", "cova/a/b", "cova/a/b/c", "other/x"):
            writer.insert(now, topic, None, "p", 0, 0)
        writer.close()

        reader = reader_factory()
        try:
            rows = list(reader.query_events("cova/#", None, 10))
        finally:
            reader.close()

        topics = {r[1] for r in rows}
        assert topics == {"cova/a", "cova/a/b", "cova/a/b/c"}

    def test_literal_topic(self, writer_and_reader):
        writer, reader_factory = writer_and_reader
        now = datetime(2026, 5, 12, 9, 0, 0)
        writer.insert(now, "cova/a/status", None, "p1", 0, 0)
        writer.insert(now, "cova/b/status", None, "p2", 0, 0)
        writer.close()

        reader = reader_factory()
        try:
            rows = list(reader.query_events("cova/a/status", None, 10))
        finally:
            reader.close()
        assert len(rows) == 1
        assert rows[0][3] == "p1"

    def test_since_filter(self, writer_and_reader):
        writer, reader_factory = writer_and_reader
        writer.insert(datetime(2026, 5, 12,  9, 0, 0), "t1", None, "1", 0, 0)
        writer.insert(datetime(2026, 5, 12,  9, 5, 0), "t2", None, "2", 0, 0)
        writer.insert(datetime(2026, 5, 12, 10, 0, 0), "t3", None, "3", 0, 0)
        writer.close()

        reader = reader_factory()
        try:
            rows = list(reader.query_events(
                None, datetime(2026, 5, 12, 9, 4, 0), 10))
        finally:
            reader.close()
        topics = sorted(r[1] for r in rows)
        assert topics == ["t2", "t3"]

    def test_limit_applied(self, writer_and_reader):
        writer, reader_factory = writer_and_reader
        for i in range(20):
            writer.insert(datetime(2026, 5, 12, 9, 0, i), f"t{i}", None,
                          str(i), 0, 0)
        writer.close()

        reader = reader_factory()
        try:
            rows = list(reader.query_events(None, None, 5))
        finally:
            reader.close()
        assert len(rows) == 5

    def test_stats(self, writer_and_reader):
        writer, reader_factory = writer_and_reader
        writer.insert(datetime(2026, 5, 12, 9, 0, 0), "a", None, "1", 0, 0)
        writer.insert(datetime(2026, 5, 12, 9, 1, 0), "a", None, "2", 0, 1)
        writer.insert(datetime(2026, 5, 12, 9, 2, 0), "b", None, "3", 0, 1)
        writer.close()

        reader = reader_factory()
        try:
            s = reader.stats()
        finally:
            reader.close()
        assert s["total_events"] == 3
        assert s["unique_topics"] == 2
        assert s["retained_count"] == 2

    def test_null_sender_and_payload(self, writer_and_reader):
        writer, reader_factory = writer_and_reader
        writer.insert(datetime(2026, 5, 12, 9, 0, 0), "t", None, None, 0, 0)
        writer.close()
        reader = reader_factory()
        try:
            rows = list(reader.query_events(None, None, 10))
        finally:
            reader.close()
        assert rows[0][2] is None
        assert rows[0][3] is None

    def test_descending_timestamp_order(self, writer_and_reader):
        """Both backends must return results newest-first — important for
        the CLI's truncation-by-limit behavior."""
        writer, reader_factory = writer_and_reader
        for i in range(5):
            writer.insert(datetime(2026, 5, 12, 9, 0, i),
                          f"t{i}", None, str(i), 0, 0)
        writer.close()
        reader = reader_factory()
        try:
            rows = list(reader.query_events(None, None, 5))
        finally:
            reader.close()
        topics = [r[1] for r in rows]
        assert topics == ["t4", "t3", "t2", "t1", "t0"]
