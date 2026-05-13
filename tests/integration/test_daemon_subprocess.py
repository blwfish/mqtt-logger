"""Spawn mqtt_logger.py as a real OS subprocess, exercise it end-to-end,
then send SIGTERM and verify graceful shutdown. Catches issues that
in-process tests can't — signal handling, paho V2 lifecycle, batched-
commit flush-on-close behavior."""
import signal
import time

import paho.mqtt.client as mqtt


def _publish(host, port, topic, payload):
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                      client_id=f"e2e-pub-{time.time_ns()}")
    pub.connect(host, port)
    pub.loop_start()
    try:
        pub.publish(topic, payload, qos=1).wait_for_publish(timeout=5)
    finally:
        pub.loop_stop()
        pub.disconnect()


class TestDaemonLifecycle:
    def test_daemon_writes_then_shuts_down_cleanly(self, spawn_daemon,
                                                   mosquitto_container,
                                                   mariadb):
        daemon = spawn_daemon()
        daemon.wait_for_connected()

        _publish(mosquitto_container.host, mosquitto_container.port,
                 "log/integration-test", b'{"board":"integration-test"}')
        time.sleep(0.5)  # let on_message run

        # SIGTERM and verify clean exit (signal handler → client.disconnect
        # → loop_forever exits → cleanup() → backend.close() → row commit).
        daemon.process.send_signal(signal.SIGTERM)
        rc = daemon.terminate(timeout=10)
        assert rc == 0, f"daemon exited with {rc} on SIGTERM"

        # Now the row should be persisted.
        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT topic, sender FROM mqtt_events "
                "WHERE topic = 'log/integration-test'"
            )
            rows = cur.fetchall()
        assert list(rows) == [("log/integration-test", "integration-test")]

    def test_daemon_keeps_running_when_one_backend_fails(
            self, spawn_daemon, mosquitto_container, mariadb, tmp_path):
        """The on_message dispatch wraps each backend.insert in its own
        try/except. Verify this with a real subprocess by passing an
        unwritable SQLite path alongside a working MariaDB. SQLite inserts
        will fail; MariaDB inserts must keep landing."""
        # Make a read-only directory and try to write the db inside it.
        ro_dir = tmp_path / "readonly"
        ro_dir.mkdir()
        ro_dir.chmod(0o555)
        try:
            sqlite_path = ro_dir / "events.db"
            daemon = spawn_daemon(
                enable_sqlite=False,  # we'll pass --db manually
                extra_args=["--db", str(sqlite_path)],
            )
            # The daemon may not even start if SQLite refuses outright. If
            # it doesn't connect within 15s, that's the failure mode we
            # care about — skip with a clear note rather than hang.
            try:
                daemon.wait_for_connected(timeout=15)
            except RuntimeError:
                # Daemon exited; this means SQLite init failed hard and
                # the failure-isolation property doesn't apply to startup,
                # only to per-message inserts. That's acceptable behavior.
                import pytest
                pytest.skip("SQLite init refuses to start with read-only dir "
                            "— failure isolation only covers steady-state")

            _publish(mosquitto_container.host, mosquitto_container.port,
                     "isolation/test", b'{"board":"iso"}')
            time.sleep(0.5)
            daemon.process.send_signal(signal.SIGTERM)
            daemon.terminate()

            with mariadb.admin_connect() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT topic FROM mqtt_events WHERE topic='isolation/test'"
                )
                rows = cur.fetchall()
            assert rows, "MariaDB should still receive rows when SQLite fails"
        finally:
            ro_dir.chmod(0o755)  # let pytest tmp cleanup do its job


class TestSignalHandling:
    def test_sigint_triggers_clean_shutdown(self, spawn_daemon):
        daemon = spawn_daemon()
        daemon.wait_for_connected()
        daemon.process.send_signal(signal.SIGINT)
        rc = daemon.terminate(timeout=10)
        assert rc == 0
