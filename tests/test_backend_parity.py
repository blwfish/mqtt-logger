"""Cross-cutting parity tests — exercising properties both backends MUST
share so the SQLite/MariaDB code paths can never drift quietly."""
from datetime import datetime
from unittest.mock import MagicMock


class TestWriterReaderSchemaParity:
    """The writer and the query backend must agree on column shape: write a
    row through SQLiteBackend, read it back through SQLiteQueryBackend, and
    confirm every field round-trips. Catches accidental schema or column-
    order drift between writer and reader."""

    def test_round_trip(self, tmp_path):
        from mqtt_logger import SQLiteBackend, SQLiteQueryBackend

        db = str(tmp_path / "p.db")
        writer = SQLiteBackend(db)
        ts = datetime(2026, 5, 12, 10, 30, 0)
        writer.insert(ts, "log/foo", "foo-1", '{"a":1}', 1, 1)
        writer.close()

        reader = SQLiteQueryBackend(db)
        rows = list(reader.query_events(None, None, 10))
        reader.close()

        assert len(rows) == 1
        timestamp, topic, sender, payload, qos, retained = rows[0]
        assert topic == "log/foo"
        assert sender == "foo-1"
        assert payload == '{"a":1}'
        assert qos == 1
        assert retained == 1


class TestDialectInvariants:
    """Both backends must translate MQTT wildcards using the same regex
    helper. Direct invocation of the helper rather than running queries —
    asserts the dialect-shared code is genuinely shared."""

    def test_helper_is_shared(self):
        from mqtt_logger import _mqtt_pattern_to_regex
        import query_events
        assert query_events.mqtt_pattern_to_regex is _mqtt_pattern_to_regex


class TestQueryBackendInterface:
    """Both query backends conform to the QueryBackend ABC — i.e. each
    declares all four abstract methods. If a new abstract method is added
    upstream, instantiation here will fail until both backends implement
    it, catching parity drift at test time."""

    def test_sqlite_implements_abc(self, tmp_path):
        from mqtt_logger import SQLiteBackend, SQLiteQueryBackend, QueryBackend
        db = str(tmp_path / "p.db")
        SQLiteBackend(db).close()
        b = SQLiteQueryBackend(db)
        try:
            # Construction itself raises TypeError if any abstract method is
            # unimplemented — no need to assert isinstance separately. Instead,
            # call every abstract method to verify runtime contract, not just
            # that the class hierarchy is intact.
            assert list(b.query_events(None, None, 1)) == []
            assert list(b.list_topics()) == []
            s = b.stats()
            assert set(s) >= {"total_events", "unique_topics", "retained_count",
                              "first_event", "last_event"}
            assert s["total_events"] == 0
        finally:
            b.close()

    def test_mariadb_implements_abc(self, fake_pymysql, fake_keyring):
        """Construct MariaDBQueryBackend with fakes and assert it satisfies
        the same ABC. Doesn't need a live MariaDB."""
        from mqtt_logger import MariaDBQueryBackend, QueryBackend

        fake_keyring._password = "secret"
        fake_conn = MagicMock(name="conn")
        # stats() fetches one row; supply a plausible empty-table result.
        fake_conn.cursor.return_value.__enter__.return_value \
            .fetchone.return_value = (0, 0, None, None, None)
        fake_pymysql.connect = MagicMock(return_value=fake_conn)
        b = MariaDBQueryBackend(host="h", database="db", user="u")
        # Construction proves ABC conformance; calling stats() verifies the
        # contract beyond the signature.
        s = b.stats()
        assert set(s) >= {"total_events", "unique_topics", "retained_count",
                          "first_event", "last_event"}


class TestMariaDBQueryFilters:
    """Sanity check that the MariaDB query backend produces the right SQL
    shape for wildcard, literal, and time-window filters. We don't hit a
    real DB — assert on the cursor's recorded execute() call."""

    def setup_backend(self, fake_pymysql, fake_keyring):
        from mqtt_logger import MariaDBQueryBackend
        fake_keyring._password = "secret"
        fake_conn = MagicMock(name="conn")
        fake_pymysql.connect = MagicMock(return_value=fake_conn)
        return MariaDBQueryBackend(host="h", database="db", user="u"), fake_conn

    def _last_execute(self, fake_conn):
        cur = fake_conn.cursor.return_value.__enter__.return_value
        return cur.execute.call_args.args

    def test_wildcard_uses_regexp(self, fake_pymysql, fake_keyring):
        backend, fake_conn = self.setup_backend(fake_pymysql, fake_keyring)
        list(backend.query_events("cova/+/status", None, 10))
        sql, params = self._last_execute(fake_conn)
        assert "REGEXP" in sql
        # The translated regex must respect MQTT level boundaries.
        assert any("[^/]+" in p for p in params)

    def test_literal_topic_uses_equality(self, fake_pymysql, fake_keyring):
        backend, fake_conn = self.setup_backend(fake_pymysql, fake_keyring)
        list(backend.query_events("log/board1", None, 10))
        sql, params = self._last_execute(fake_conn)
        assert "REGEXP" not in sql
        assert "topic = %s" in sql
        assert "log/board1" in params

    def test_since_filter_uses_datetime(self, fake_pymysql, fake_keyring):
        backend, fake_conn = self.setup_backend(fake_pymysql, fake_keyring)
        cutoff = datetime(2026, 5, 12, 10, 0, 0)
        list(backend.query_events(None, cutoff, 10))
        sql, params = self._last_execute(fake_conn)
        assert "timestamp >= %s" in sql
        # pymysql will format the datetime; we pass it through directly.
        assert cutoff in params
