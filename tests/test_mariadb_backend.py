"""Unit tests for MariaDBBackend — mocked pymysql + keyring."""
from datetime import datetime
from unittest.mock import MagicMock

import pytest


def make_backend(fake_pymysql, fake_keyring, monkeypatch, password="secret"):
    from mqtt_logger import MariaDBBackend  # imported after fakes installed

    fake_keyring._password = password
    fake_conn = MagicMock(name="conn")
    fake_pymysql.connect = MagicMock(return_value=fake_conn)
    backend = MariaDBBackend(host="h", port=3306, database="db", user="u")
    # Construction runs `CREATE TABLE IF NOT EXISTS` via the cursor — clear
    # the mock so per-test assertions only see the calls each test makes.
    fake_conn.cursor.return_value.__enter__.return_value.execute.reset_mock()
    return backend, fake_conn


class TestStartup:
    def test_missing_password_raises(self, fake_pymysql, fake_keyring,
                                     monkeypatch):
        from mqtt_logger import MariaDBBackend
        fake_keyring._password = None
        monkeypatch.delenv("MQTT_LOGGER_MARIADB_PASSWORD", raising=False)
        with pytest.raises(RuntimeError, match="No MariaDB password found"):
            MariaDBBackend(user="u")

    def test_error_message_includes_security_command(self, fake_pymysql,
                                                     fake_keyring, monkeypatch):
        from mqtt_logger import MariaDBBackend
        fake_keyring._password = None
        monkeypatch.delenv("MQTT_LOGGER_MARIADB_PASSWORD", raising=False)
        with pytest.raises(RuntimeError, match="security add-generic-password"):
            MariaDBBackend(user="u")

    def test_env_var_overrides_keyring(self, fake_pymysql, fake_keyring,
                                       monkeypatch):
        """Regression: MQTT_LOGGER_MARIADB_PASSWORD should take precedence
        over Keychain, so integration tests and Linux containers without a
        Keychain can supply credentials."""
        from mqtt_logger import MariaDBBackend
        from unittest.mock import MagicMock
        fake_keyring._password = "from-keyring"
        monkeypatch.setenv("MQTT_LOGGER_MARIADB_PASSWORD", "from-env")
        fake_pymysql.connect = MagicMock(return_value=MagicMock())

        MariaDBBackend(user="u")
        assert fake_pymysql.connect.call_args.kwargs["password"] == "from-env"

    def test_successful_connect(self, fake_pymysql, fake_keyring, monkeypatch):
        backend, fake_conn = make_backend(fake_pymysql, fake_keyring,
                                          monkeypatch)
        assert backend.name == "mariadb"
        fake_pymysql.connect.assert_called_once()
        kwargs = fake_pymysql.connect.call_args.kwargs
        assert kwargs["host"] == "h"
        assert kwargs["password"] == "secret"
        assert kwargs["autocommit"] is True


class TestSchemaParity:
    """MariaDBBackend should run a CREATE TABLE IF NOT EXISTS on init, the
    same way SQLiteBackend does. Production accounts that lack the CREATE
    privilege get the DDL error silently swallowed."""

    def test_init_runs_ddl(self, fake_pymysql, fake_keyring, monkeypatch):
        from mqtt_logger import MariaDBBackend
        fake_keyring._password = "secret"
        fake_conn = MagicMock(name="conn")
        fake_pymysql.connect = MagicMock(return_value=fake_conn)

        MariaDBBackend(host="h", database="db", user="u")
        cursor = fake_conn.cursor.return_value.__enter__.return_value

        # At least one statement, and one of them is CREATE TABLE.
        executed = [c.args[0] for c in cursor.execute.call_args_list]
        assert any("CREATE TABLE" in s.upper() for s in executed)
        assert any("mqtt_events" in s for s in executed)

    def test_ddl_permission_denied_is_swallowed(self, fake_pymysql,
                                                fake_keyring, monkeypatch):
        """Regression: the `logger` account is INSERT+SELECT only. CREATE
        must not crash startup."""
        from mqtt_logger import MariaDBBackend
        fake_keyring._password = "secret"
        fake_conn = MagicMock(name="conn")
        cursor = fake_conn.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = fake_pymysql.OperationalError(
            1142, "CREATE command denied"
        )
        fake_pymysql.connect = MagicMock(return_value=fake_conn)

        # Should NOT raise.
        MariaDBBackend(host="h", database="db", user="u")

    def test_ddl_other_operational_error_propagates(self, fake_pymysql,
                                                    fake_keyring, monkeypatch):
        from mqtt_logger import MariaDBBackend
        fake_keyring._password = "secret"
        fake_conn = MagicMock(name="conn")
        cursor = fake_conn.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = fake_pymysql.OperationalError(
            1064, "syntax error"
        )
        fake_pymysql.connect = MagicMock(return_value=fake_conn)

        with pytest.raises(fake_pymysql.OperationalError):
            MariaDBBackend(host="h", database="db", user="u")


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

    @pytest.mark.parametrize("code", [2006, 2013, 2055])
    def test_all_retryable_codes_trigger_reconnect(self, code, fake_pymysql,
                                                   fake_keyring, monkeypatch):
        """Each code in _RETRYABLE must individually trigger reconnect+retry."""
        backend, fake_conn = make_backend(fake_pymysql, fake_keyring,
                                          monkeypatch)
        first_cursor = fake_conn.cursor.return_value.__enter__.return_value
        first_cursor.execute.side_effect = fake_pymysql.OperationalError(code,
                                                                          "transient")
        new_conn = MagicMock(name="new_conn")
        fake_pymysql.connect = MagicMock(side_effect=[new_conn])

        backend.insert(datetime.now(), "t", None, "p", 0, 0)
        assert backend._conn is new_conn, \
            f"OperationalError({code}) should have triggered reconnect"

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

    def test_interface_error_triggers_reconnect(self, fake_pymysql,
                                                fake_keyring, monkeypatch):
        """Regression for an issue surfaced by integration tests:
        pymysql.InterfaceError fires when the underlying socket is already
        closed (typically because MariaDB's wait_timeout dropped an idle
        connection). The retry path must catch this too, not only
        OperationalError."""
        backend, fake_conn = make_backend(fake_pymysql, fake_keyring,
                                          monkeypatch)
        first_cursor = fake_conn.cursor.return_value.__enter__.return_value
        first_cursor.execute.side_effect = fake_pymysql.InterfaceError(0, "")
        new_conn = MagicMock(name="new_conn")
        new_cursor = new_conn.cursor.return_value.__enter__.return_value
        fake_pymysql.connect = MagicMock(side_effect=[new_conn])

        backend.insert(datetime.now(), "t", None, "p", 0, 0)
        assert new_cursor.execute.call_count == 1
        assert backend._conn is new_conn
