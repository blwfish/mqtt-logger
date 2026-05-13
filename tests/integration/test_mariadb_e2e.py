"""End-to-end tests for MariaDBBackend and MariaDBQueryBackend against a
real MariaDB container. These catch dialect-level surprises that mocked
pymysql can't — REGEXP semantics, DATETIME(6) round-trip, charset handling."""
from datetime import datetime

import pytest

from mqtt_logger import MariaDBBackend, MariaDBQueryBackend


def _open_writer(mariadb):
    return MariaDBBackend(host=mariadb.host, port=mariadb.port,
                          database=mariadb.database, user=mariadb.user)


def _open_reader(mariadb):
    return MariaDBQueryBackend(host=mariadb.host, port=mariadb.port,
                               database=mariadb.database, user=mariadb.user)


class TestWriter:
    def test_insert_round_trips_through_raw_sql(self, mariadb):
        writer = _open_writer(mariadb)
        ts = datetime(2026, 5, 12, 10, 30, 0, 123456)  # microseconds matter
        try:
            writer.insert(ts, "log/board1", "board1", '{"x":1}', 1, 1)
        finally:
            writer.close()

        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT timestamp, topic, sender, payload, qos, retained "
                "FROM mqtt_events"
            )
            rows = cur.fetchall()

        assert len(rows) == 1
        timestamp, topic, sender, payload, qos, retained = rows[0]
        assert timestamp == ts                  # DATETIME(6) preserved μs
        assert topic == "log/board1"
        assert sender == "board1"
        assert payload == '{"x":1}'
        assert qos == 1
        assert retained == 1

    def test_utf8mb4_payload_preserved(self, mariadb):
        """Charset regression — utf8mb4 must round-trip emoji, CJK, etc.
        Catches the 'utf8 instead of utf8mb4' classic 3-byte-vs-4-byte
        truncation bug."""
        writer = _open_writer(mariadb)
        payload = "señor 🚂 鉄道 — 4-byte chars"
        try:
            writer.insert(datetime.now(), "test/utf8", None, payload, 0, 0)
        finally:
            writer.close()

        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT payload FROM mqtt_events")
            stored = cur.fetchone()[0]
        assert stored == payload

    def test_long_payload_fits_in_longtext(self, mariadb):
        writer = _open_writer(mariadb)
        # 200 KB — beyond TEXT's 64 KB, comfortably within LONGTEXT.
        payload = "x" * (200 * 1024)
        try:
            writer.insert(datetime.now(), "test/long", None, payload, 0, 0)
        finally:
            writer.close()

        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT LENGTH(payload) FROM mqtt_events")
            assert cur.fetchone()[0] == len(payload)

    def test_long_topic_within_varchar_512(self, mariadb):
        writer = _open_writer(mariadb)
        topic = "a/" + "b" * 500  # 502 chars, well under VARCHAR(512)
        try:
            writer.insert(datetime.now(), topic, None, "p", 0, 0)
        finally:
            writer.close()
        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT topic FROM mqtt_events")
            assert cur.fetchone()[0] == topic


class TestSchemaAutoCreate:
    """The backend runs CREATE TABLE IF NOT EXISTS at init. With the root
    user we have CREATE — verify the schema actually gets built correctly
    on an empty database."""

    def test_creates_table_on_fresh_database(self, mariadb):
        # Drop the table first; the fixture's truncate creates it, so we
        # need to start from a truly empty database to test creation.
        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS mqtt_events")

        writer = _open_writer(mariadb)
        try:
            with mariadb.admin_connect() as conn, conn.cursor() as cur:
                cur.execute("SHOW CREATE TABLE mqtt_events")
                ddl = cur.fetchone()[1].lower()
            assert "bigint" in ddl
            assert "datetime(6)" in ddl
            assert "varchar(512)" in ddl
            assert "longtext" in ddl
            assert "utf8mb4" in ddl
            assert "idx_timestamp" in ddl
            assert "idx_topic" in ddl
        finally:
            writer.close()


class TestReader:
    @pytest.fixture
    def seeded_mariadb(self, mariadb):
        """Populate via the real writer so the test exercises both sides."""
        writer = _open_writer(mariadb)
        rows = [
            (datetime(2026, 5, 12, 9, 0, 0),  "cova/foo/status",     None,     "1", 0, 0),
            (datetime(2026, 5, 12, 9, 1, 0),  "cova/bar/status",     None,     "2", 0, 0),
            (datetime(2026, 5, 12, 9, 2, 0),  "cova/foo/bar/status", None,     "3", 0, 0),
            (datetime(2026, 5, 12, 9, 3, 0),  "log/board1",          "board1", "4", 1, 1),
            (datetime(2026, 5, 12, 9, 4, 0),  "other/topic",         None,     "5", 0, 0),
        ]
        for row in rows:
            writer.insert(*row)
        writer.close()
        return mariadb

    def test_wildcard_plus_respects_level_boundaries(self, seeded_mariadb):
        """Regression for the old bug: `+` used to map to `%` and matched
        across `/`. Verified now against real MariaDB REGEXP."""
        reader = _open_reader(seeded_mariadb)
        try:
            rows = list(reader.query_events("cova/+/status", None, 10))
        finally:
            reader.close()
        topics = {r[1] for r in rows}
        assert topics == {"cova/foo/status", "cova/bar/status"}
        assert "cova/foo/bar/status" not in topics

    def test_wildcard_hash_multi_level(self, seeded_mariadb):
        reader = _open_reader(seeded_mariadb)
        try:
            rows = list(reader.query_events("cova/#", None, 10))
        finally:
            reader.close()
        topics = {r[1] for r in rows}
        assert "cova/foo/status" in topics
        assert "cova/foo/bar/status" in topics
        assert "log/board1" not in topics

    def test_literal_topic_filter(self, seeded_mariadb):
        reader = _open_reader(seeded_mariadb)
        try:
            rows = list(reader.query_events("log/board1", None, 10))
        finally:
            reader.close()
        assert len(rows) == 1
        assert rows[0][1] == "log/board1"
        assert rows[0][2] == "board1"

    def test_since_filter_uses_real_datetime(self, seeded_mariadb):
        reader = _open_reader(seeded_mariadb)
        cutoff = datetime(2026, 5, 12, 9, 2, 30)
        try:
            rows = list(reader.query_events(None, cutoff, 10))
        finally:
            reader.close()
        # Two rows: 09:03 and 09:04
        assert len(rows) == 2

    def test_stats(self, seeded_mariadb):
        reader = _open_reader(seeded_mariadb)
        try:
            s = reader.stats()
        finally:
            reader.close()
        assert s["total_events"] == 5
        assert s["unique_topics"] == 5
        assert s["retained_count"] == 1
        assert s["first_event"] == datetime(2026, 5, 12, 9, 0, 0)
        assert s["last_event"]  == datetime(2026, 5, 12, 9, 4, 0)

    def test_list_topics(self, seeded_mariadb):
        reader = _open_reader(seeded_mariadb)
        try:
            rows = list(reader.list_topics())
        finally:
            reader.close()
        assert len(rows) == 5
        # Every topic has exactly one row in the seed.
        assert all(count == 1 for _, count in rows)


class TestReconnectAfterServerGone:
    """The retry path in MariaDBBackend.insert is exercised when the
    connection has been closed server-side. Force that by killing the
    pymysql connection out from under it, then verify the next insert
    transparently reconnects."""

    def test_insert_after_connection_drop(self, mariadb):
        writer = _open_writer(mariadb)
        try:
            writer.insert(datetime.now(), "before/drop", None, "1", 0, 0)
            # Slam the connection shut to simulate server-gone.
            writer._conn.close()
            # Next insert should reconnect and succeed without raising.
            writer.insert(datetime.now(), "after/drop", None, "2", 0, 0)
        finally:
            writer.close()

        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT topic FROM mqtt_events ORDER BY id")
            topics = [r[0] for r in cur.fetchall()]
        assert topics == ["before/drop", "after/drop"]
