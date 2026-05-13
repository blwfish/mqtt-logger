"""Reconnect lifecycle: stop a container under the running daemon and
verify the daemon recovers when the container comes back. Exercises code
paths that unit tests can only approximate via mocks."""
import signal
import socket
import time

import paho.mqtt.client as mqtt
import pytest

from tests.integration.conftest import _wait_for_tcp


def _publish(host, port, topic, payload, timeout=5):
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                      client_id=f"reconnect-{time.time_ns()}")
    pub.connect(host, port)
    pub.loop_start()
    try:
        pub.publish(topic, payload, qos=1) \
           .wait_for_publish(timeout=timeout)
    finally:
        pub.loop_stop()
        pub.disconnect()


class TestMariaDBReconnect:
    """Force MariaDB to drop the daemon's connection, then verify the
    next insert reconnects transparently."""

    def test_daemon_survives_kill_idle_connections(self, spawn_daemon,
                                                   mosquitto_container,
                                                   mariadb):
        daemon = spawn_daemon()
        daemon.wait_for_connected()

        # Publish a baseline message — proves the daemon is wired up.
        _publish(mosquitto_container.host, mosquitto_container.port,
                 "reconnect/before", b'{"phase":"before"}')
        time.sleep(0.5)

        # Brute-force every non-root, non-current connection. The daemon's
        # pymysql session is one of them.
        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT id FROM information_schema.processlist "
                        "WHERE user != 'system user'")
            ids = [r[0] for r in cur.fetchall()]
            for cid in ids:
                # KILL fails harmlessly on our own connection — that's fine.
                try:
                    cur.execute(f"KILL {cid}")
                except Exception:
                    pass

        # Give the daemon a moment to notice (it won't until next insert).
        time.sleep(0.2)

        # Publish another message — should land via the reconnect path.
        _publish(mosquitto_container.host, mosquitto_container.port,
                 "reconnect/after", b'{"phase":"after"}')
        time.sleep(0.5)

        daemon.process.send_signal(signal.SIGTERM)
        daemon.terminate()

        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT topic FROM mqtt_events "
                "WHERE topic IN ('reconnect/before', 'reconnect/after') "
                "ORDER BY id"
            )
            topics = [r[0] for r in cur.fetchall()]

        assert "reconnect/before" in topics
        assert "reconnect/after" in topics, \
            "daemon did not recover after its MariaDB connection was killed"


class TestBrokerReconnect:
    """Restart the broker out from under the daemon and verify paho's own
    auto-reconnect recovers the subscription."""

    def test_daemon_resubscribes_after_broker_restart(
            self, spawn_daemon, mosquitto_container, mariadb):
        from testcontainers.core.container import DockerContainer
        # This test needs the ability to stop/start the mosquitto container.
        # The session-scoped fixture's container is the only one available,
        # so we need to coordinate restart carefully. We'll use the docker
        # client directly to stop/start it; the fixture cleanup will not
        # be confused.
        import docker
        client = docker.from_env()

        # Find the mosquitto container the fixture started. testcontainers
        # tags them with random names; we can match by exposed-port.
        containers = client.containers.list()
        moscon = None
        for c in containers:
            if c.image.tags and any("mosquitto" in t for t in c.image.tags):
                moscon = c
                break
        if moscon is None:
            pytest.skip("could not locate mosquitto container by image tag")

        daemon = spawn_daemon()
        daemon.wait_for_connected()

        # Pre-restart write.
        _publish(mosquitto_container.host, mosquitto_container.port,
                 "broker-restart/before", b'{"phase":"before"}')
        time.sleep(0.3)

        # Stop the broker. paho's loop_forever will detect, retry, and
        # call on_disconnect.
        moscon.stop(timeout=5)
        # Wait for the daemon's log to note the disconnect.
        log_path = daemon.log_dir / "mqtt_logger.log"
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if "disconnect" in log_path.read_text().lower():
                break
            time.sleep(0.2)

        moscon.start()
        # Wait until the broker's port is open again. testcontainers
        # ephemeral ports stay stable across stop/start.
        _wait_for_tcp(mosquitto_container.host, mosquitto_container.port,
                      timeout=20)

        # Wait for the daemon's resubscription log line — paho will
        # auto-reconnect inside loop_forever, then on_connect fires.
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            text = log_path.read_text()
            if text.count("Connected to broker") >= 2:
                break
            time.sleep(0.2)
        else:
            pytest.fail("daemon did not reconnect to broker within 20s")

        _publish(mosquitto_container.host, mosquitto_container.port,
                 "broker-restart/after", b'{"phase":"after"}')
        time.sleep(0.5)

        daemon.process.send_signal(signal.SIGTERM)
        daemon.terminate()

        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT topic FROM mqtt_events "
                "WHERE topic LIKE 'broker-restart/%' "
                "ORDER BY id"
            )
            topics = [r[0] for r in cur.fetchall()]
        assert "broker-restart/before" in topics
        assert "broker-restart/after" in topics, \
            "daemon did not pick up messages after broker restart"
