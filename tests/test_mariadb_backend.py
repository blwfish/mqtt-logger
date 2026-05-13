"""Unit tests for MariaDBBackend — mocked pymysql + keyring."""
from datetime import datetime
from unittest.mock import MagicMock

import pytest


def make_backend(fake_pymysql, fake_keyring, monkeypatch, password="secret"):
    from mqtt_logger import MariaDBBackend  # imported after fakes installed

    fake_keyring._password = password
    fake_conn = MagicMock(name="conn")
    fake_pymysql.connect = MagicMock(return_value=fake_conn)
    return MariaDBBackend(host="h", port=3306, database="db", user="u"), fake_conn


class TestStartup:
    def test_missing_password_raises(self, fake_pymysql, fake_keyring):
        from mqtt_logger import MariaDBBackend
        fake_keyring._password = None
        with pytest.raises(RuntimeError, match="No password found in Keychain"):
            MariaDBBackend(user="u")

    def test_error_message_includes_security_command(self, fake_pymysql,
                                                     fake_keyring):
        from mqtt_logger import MariaDBBackend
        fake_keyring._password = None
        with pytest.raises(RuntimeError, match="security add-generic-password"):
            MariaDBBackend(user="u")

    def test_successful_connect(self, fake_pymysql, fake_keyring, monkeypatch):
        backend, fake_conn = make_backend(fake_pymysql, fake_keyring,
                                          monkeypatch)
        assert backend.name == "mariadb"
        fake_pymysql.connect.assert_called_once()
        kwargs = fake_pymysql.connect.call_args.kwargs
        assert kwargs["host"] == "h"
        assert kwargs["password"] == "secret"
        assert kwargs["autocommit"] is True


class TestInsert:
    def test_inserts_via_cursor(self, fake_pymysql, fake_keyring, monkeypatch):
        backend, fake_conn = make_backend(fake_pymysql, fake_keyring,
                                          monkeypatch)
        cursor = fake_conn.cursor.return_value.__enter__.return_value
        ts = datetime(2026, 5, 12, 10, 30, 0)

        backend.insert(ts, "log/foo", "foo", "payload", 1, 0)

        cursor.execute.assert_called_once()
        sql, params = cursor.execute.call_args.args
        assert "INSERT INTO mqtt_events" in sql
        assert params == (ts, "log/foo", "foo", "payload", 1, 0)


class TestRetry:
    def test_retryable_error_reconnects_and_retries(self, fake_pymysql,
                                                    fake_keyring, monkeypatch):
        backend, fake_conn = make_backend(fake_pymysql, fake_keyring,
                                          monkeypatch)
        # First execute raises retryable; second connection's cursor.execute
        # succeeds.
        first_cursor = fake_conn.cursor.return_value.__enter__.return_value
        first_cursor.execute.side_effect = fake_pymysql.OperationalError(2006,
                                                                          "gone")
        new_conn = MagicMock(name="new_conn")
        new_cursor = new_conn.cursor.return_value.__enter__.return_value
        fake_pymysql.connect = MagicMock(side_effect=[new_conn])

        backend.insert(datetime.now(), "t", None, "p", 0, 0)
        # Original failing cursor was called once; replacement cursor called
        # once after reconnect.
        assert first_cursor.execute.call_count == 1
        assert new_cursor.execute.call_count == 1
        assert backend._conn is new_conn

    def test_retry_failure_propagates(self, fake_pymysql, fake_keyring,
                                      monkeypatch):
        """Regression: if reconnect-then-retry also fails, the exception
        must propagate so the outer handler logs a dropped row — the previous
        implementation swallowed it silently."""
        backend, fake_conn = make_backend(fake_pymysql, fake_keyring,
                                          monkeypatch)
        first_cursor = fake_conn.cursor.return_value.__enter__.return_value
        first_cursor.execute.side_effect = fake_pymysql.OperationalError(2006,
                                                                          "gone")
        # New connection raises on connect.
        fake_pymysql.connect = MagicMock(
            side_effect=fake_pymysql.OperationalError(2003, "refused"))

        with pytest.raises(fake_pymysql.OperationalError):
            backend.insert(datetime.now(), "t", None, "p", 0, 0)

    def test_non_retryable_error_propagates_without_reconnect(
            self, fake_pymysql, fake_keyring, monkeypatch):
        backend, fake_conn = make_backend(fake_pymysql, fake_keyring,
                                          monkeypatch)
        first_cursor = fake_conn.cursor.return_value.__enter__.return_value
        first_cursor.execute.side_effect = fake_pymysql.OperationalError(
            1062, "duplicate")
        fake_pymysql.connect = MagicMock()  # would record any reconnect attempt

        with pytest.raises(fake_pymysql.OperationalError):
            backend.insert(datetime.now(), "t", None, "p", 0, 0)
        fake_pymysql.connect.assert_not_called()
