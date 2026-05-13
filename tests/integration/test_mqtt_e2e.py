"""End-to-end MQTT pipeline tests. Runs MQTTLogger.on_message wired to a
real broker + real DB, then publishes via a real paho client. Catches
broken paho V2 callback wiring and broker-side topic filtering quirks
that mocks miss."""
import threading
import time
from datetime import datetime

import paho.mqtt.client as mqtt
import pytest

from mqtt_logger import (
    MariaDBBackend,
    MQTTLogger,
    SQLiteBackend,
)


def _publish_and_drain(mosquitto, topic, payload, qos=0, retain=False):
    """Synchronously publish a message and wait for the broker to ack."""
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                      client_id=f"test-pub-{time.time_ns()}")
    pub.connect(mosquitto.host, mosquitto.port)
    pub.loop_start()
    try:
        info = pub.publish(topic, payload, qos=qos, retain=retain)
        info.wait_for_publish(timeout=5)
    finally:
        pub.loop_stop()
        pub.disconnect()


def _run_logger_in_thread(logger):
    """Start MQTTLogger.run() in a background thread; return (thread, stop_fn)."""
    t = threading.Thread(target=logger.run, daemon=True)
    t.start()
    # Wait for connection — paho's connect() returns before the CONNACK lands.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if logger.client.is_connected():
            break
        time.sleep(0.05)
    else:
        raise TimeoutError("MQTTLogger did not connect to broker")
    return t


def _stop_logger(logger, thread):
    logger.stop()
    thread.join(timeout=10)
    assert not thread.is_alive(), "logger thread did not exit"


class TestSQLiteEndToEnd:
    def test_publish_lands_in_sqlite(self, mosquitto_container, tmp_path):
        """SQLite uses batched commits (25 rows / 1 s) plus a flush on
        close, so we publish, gracefully stop the logger, *then* read.
        This matches how production shutdowns guarantee durability."""
        backend = SQLiteBackend(str(tmp_path / "events.db"))
        logger = MQTTLogger(mosquitto_container.host,
                            mosquitto_container.port, [backend])
        t = _run_logger_in_thread(logger)
        try:
            _publish_and_drain(mosquitto_container,
                               "log/board-x", b'{"board":"board-x","msg":"hi"}',
                               qos=1)
            # Let the broker deliver before we stop.
            time.sleep(0.5)
        finally:
            _stop_logger(logger, t)

        from mqtt_logger import SQLiteQueryBackend
        reader = SQLiteQueryBackend(str(tmp_path / "events.db"))
        try:
            rows = list(reader.query_events(None, None, 10))
        finally:
            reader.close()

        assert rows, "message did not reach SQLite"
        timestamp, topic, sender, payload, qos, retained = rows[0]
        assert topic == "log/board-x"
        assert sender == "board-x"  # extracted from payload + topic
        # Note: the broker downgrades the delivered QoS to the subscriber's
        # subscription QoS (the daemon subscribes at QoS 0), so even if
        # the publish was QoS 1 the row we store reads back as QoS 0.
        assert qos == 0
        assert payload == '{"board":"board-x","msg":"hi"}'


class TestMariaDBEndToEnd:
    def test_publish_lands_in_mariadb(self, mosquitto_container, mariadb):
        backend = MariaDBBackend(host=mariadb.host, port=mariadb.port,
                                 database=mariadb.database, user=mariadb.user)
        logger = MQTTLogger(mosquitto_container.host,
                            mosquitto_container.port, [backend])
        t = _run_logger_in_thread(logger)
        try:
            _publish_and_drain(mosquitto_container,
                               "log/board-y", b'{"board":"board-y"}', qos=0)
            deadline = time.monotonic() + 5
            with mariadb.admin_connect() as conn:
                while time.monotonic() < deadline:
                    with conn.cursor() as cur:
                        cur.execute("SELECT topic, sender FROM mqtt_events")
                        rows = cur.fetchall()
                    if rows:
                        break
                    time.sleep(0.1)
            assert rows, "message did not reach MariaDB within 5s"
            assert rows[0] == ("log/board-y", "board-y")
        finally:
            _stop_logger(logger, t)

    def test_binary_payload_stored_as_hex(self, mosquitto_container, mariadb):
        backend = MariaDBBackend(host=mariadb.host, port=mariadb.port,
                                 database=mariadb.database, user=mariadb.user)
        logger = MQTTLogger(mosquitto_container.host,
                            mosquitto_container.port, [backend])
        t = _run_logger_in_thread(logger)
        try:
            _publish_and_drain(mosquitto_container,
                               "test/bin", b'\xff\xfe\xfd\x00\xde\xad\xbe\xef',
                               qos=0)
            deadline = time.monotonic() + 5
            with mariadb.admin_connect() as conn:
                while time.monotonic() < deadline:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT payload FROM mqtt_events "
                            "WHERE topic = 'test/bin'"
                        )
                        rows = cur.fetchall()
                    if rows:
                        break
                    time.sleep(0.1)
            assert rows, "binary message did not reach MariaDB"
            assert rows[0][0] == "fffefd00deadbeef"
        finally:
            _stop_logger(logger, t)


class TestDualWrite:
    def test_publish_lands_in_both_backends(self, mosquitto_container,
                                            mariadb, tmp_path):
        """The headline use case: SQLite + MariaDB receive the same row,
        with no special-casing in MQTTLogger.on_message."""
        sqlite = SQLiteBackend(str(tmp_path / "events.db"))
        maria = MariaDBBackend(host=mariadb.host, port=mariadb.port,
                               database=mariadb.database, user=mariadb.user)
        logger = MQTTLogger(mosquitto_container.host,
                            mosquitto_container.port, [sqlite, maria])
        t = _run_logger_in_thread(logger)
        try:
            _publish_and_drain(mosquitto_container,
                               "dual/check", b'{"sender":"d1"}', qos=1)
            # Force commits.
            time.sleep(1.5)
        finally:
            _stop_logger(logger, t)

        # SQLite
        from mqtt_logger import SQLiteQueryBackend
        sr = SQLiteQueryBackend(str(tmp_path / "events.db"))
        try:
            sqlite_rows = list(sr.query_events(None, None, 10))
        finally:
            sr.close()

        # MariaDB
        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT topic, sender FROM mqtt_events")
            maria_rows = cur.fetchall()

        assert len(sqlite_rows) == 1
        assert len(maria_rows) == 1
        assert sqlite_rows[0][1] == "dual/check"
        assert maria_rows[0] == ("dual/check", "d1")
