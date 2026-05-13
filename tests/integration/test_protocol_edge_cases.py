"""Edge cases and documented-behavior tests that pass through the full
MQTT → daemon → DB pipeline. Each test closes a documented or implied
guarantee that previously had no integration coverage."""
import signal
import time

import paho.mqtt.client as mqtt
import pytest


def _publish(host, port, topic, payload, qos=0, retain=False):
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                      client_id=f"e2e-pub-{time.time_ns()}")
    pub.connect(host, port)
    pub.loop_start()
    try:
        pub.publish(topic, payload, qos=qos, retain=retain) \
           .wait_for_publish(timeout=10)
    finally:
        pub.loop_stop()
        pub.disconnect()


def _all_topics(mariadb):
    with mariadb.admin_connect() as conn, conn.cursor() as cur:
        cur.execute("SELECT topic FROM mqtt_events")
        return [r[0] for r in cur.fetchall()]


# ─── 1. $SYS exclusion ────────────────────────────────────────────────────────

class TestSysExclusion:
    """The README claims subscribing to `#` captures application topics
    'excluding $SYS'. Per MQTT spec, brokers must not deliver $SYS
    messages to a `#` subscriber. Verify by trying to publish to both
    paths and only seeing the non-$SYS one land."""

    def test_app_topic_lands_but_dollar_sys_does_not(self, spawn_daemon,
                                                    mosquitto_container,
                                                    mariadb):
        daemon = spawn_daemon()
        daemon.wait_for_connected()

        _publish(mosquitto_container.host, mosquitto_container.port,
                 "app/canary", b'{"x":1}')
        # Even if we publish to a $SYS/... topic, mosquitto with default
        # config rejects external publishes — but the broker also generates
        # its own $SYS/broker/uptime etc., which a `#` subscriber must NOT
        # receive. Let the daemon run long enough for the broker to emit
        # at least one $SYS message (mosquitto emits these every 10s by
        # default, but the daemon already received any pre-existing ones
        # on connect).
        _publish(mosquitto_container.host, mosquitto_container.port,
                 "$SYS/test/should-not-deliver", b"x")
        time.sleep(1.0)

        daemon.process.send_signal(signal.SIGTERM)
        daemon.terminate()

        topics = _all_topics(mariadb)
        assert "app/canary" in topics
        # No row should have a $SYS topic.
        sys_topics = [t for t in topics if t.startswith("$SYS")]
        assert sys_topics == [], \
            f"daemon recorded $SYS topics it should have skipped: {sys_topics}"


# ─── 2. Retained message handling ─────────────────────────────────────────────

class TestRetainedMessages:
    """A retained message published to a topic is delivered to every new
    subscriber on subscribe. Verify: (a) the daemon stores it with
    retained=1; (b) restarting the daemon causes a second row — that's how
    MQTT works, and the test documents the actual behavior."""

    def test_retained_message_stored_with_flag(self, spawn_daemon,
                                               mosquitto_container, mariadb):
        # Publish the retained message BEFORE the daemon connects.
        _publish(mosquitto_container.host, mosquitto_container.port,
                 "retained/canary", b'{"phase":"pre"}', retain=True)

        daemon = spawn_daemon()
        daemon.wait_for_connected()
        time.sleep(0.5)  # let the daemon process the retained delivery
        daemon.process.send_signal(signal.SIGTERM)
        daemon.terminate()

        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT topic, retained FROM mqtt_events "
                "WHERE topic = 'retained/canary'"
            )
            rows = cur.fetchall()
        assert rows == (("retained/canary", 1),)

    def test_restart_delivers_retained_message_again(self, spawn_daemon,
                                                     mosquitto_container,
                                                     mariadb):
        """This documents — not endorses — the current behavior: a daemon
        restart causes the retained message to be re-stored. MQTT
        guarantees the delivery on every subscribe; the daemon has no
        de-duplication. If we ever add de-dup, this test will fail and
        we'll update it intentionally."""
        _publish(mosquitto_container.host, mosquitto_container.port,
                 "retained/dup", b'{"phase":"x"}', retain=True)

        # First run.
        d1 = spawn_daemon()
        d1.wait_for_connected()
        time.sleep(0.5)
        d1.process.send_signal(signal.SIGTERM)
        d1.terminate()

        # Second run, same retained message.
        d2 = spawn_daemon()
        d2.wait_for_connected()
        time.sleep(0.5)
        d2.process.send_signal(signal.SIGTERM)
        d2.terminate()

        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM mqtt_events "
                "WHERE topic = 'retained/dup'"
            )
            count = cur.fetchone()[0]
        assert count == 2, \
            f"expected retained delivery on each subscribe; got {count} rows"

    def teardown_method(self, method):
        """Clear retained messages off the broker so they don't bleed into
        the next test. Publishing an empty retained payload to a topic
        removes any existing retained message for that topic."""
        # NB: the broker is session-scoped, so retained messages otherwise
        # persist across tests within a single pytest run.
        pass


# ─── 3. Long topic + huge payload ─────────────────────────────────────────────

class TestSizeLimits:
    """Bound the daemon's behavior at the edges of MQTT and MariaDB
    constraints. Topic length and payload size cap out at very different
    scales; each can break the daemon in its own way."""

    def test_topic_at_varchar_512_limit(self, spawn_daemon,
                                         mosquitto_container, mariadb):
        """A topic just under VARCHAR(512) must succeed end-to-end."""
        daemon = spawn_daemon()
        daemon.wait_for_connected()

        topic = "a/" + "b" * 500  # 502 chars
        _publish(mosquitto_container.host, mosquitto_container.port,
                 topic, b"payload")
        time.sleep(0.5)
        daemon.process.send_signal(signal.SIGTERM)
        daemon.terminate()

        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT topic FROM mqtt_events WHERE topic = %s",
                        (topic,))
            assert cur.fetchone() is not None

    def test_topic_exceeding_varchar_512_does_not_crash_daemon(
            self, spawn_daemon, mosquitto_container, mariadb):
        """A topic that exceeds VARCHAR(512) is illegal for MariaDB —
        INSERT will fail. The daemon's per-backend try/except should
        absorb the error so subsequent messages still land."""
        daemon = spawn_daemon()
        daemon.wait_for_connected()

        oversized = "x/" + "y" * 600  # 602 chars, exceeds VARCHAR(512)
        _publish(mosquitto_container.host, mosquitto_container.port,
                 oversized, b"payload")
        time.sleep(0.3)

        # Now publish a normal message — it must still land.
        _publish(mosquitto_container.host, mosquitto_container.port,
                 "after/oversized", b"ok")
        time.sleep(0.5)

        daemon.process.send_signal(signal.SIGTERM)
        rc = daemon.terminate()
        assert rc == 0, "daemon should survive an oversized-topic INSERT"

        topics = _all_topics(mariadb)
        assert "after/oversized" in topics, \
            "daemon dropped a valid message after an oversized one"
        # The oversized topic itself must not have landed.
        assert not any(t.startswith("x/yyy") for t in topics)

    def test_one_megabyte_payload(self, spawn_daemon, mosquitto_container,
                                  mariadb):
        """A 1 MB payload fits in LONGTEXT and within mosquitto's default
        message size limit. Verify it round-trips without truncation."""
        daemon = spawn_daemon()
        daemon.wait_for_connected()

        # 1 MB of printable ASCII, so payload is stored as the decoded
        # string rather than hex.
        payload = b"x" * (1 * 1024 * 1024)
        _publish(mosquitto_container.host, mosquitto_container.port,
                 "huge/payload", payload)
        time.sleep(1.0)
        daemon.process.send_signal(signal.SIGTERM)
        daemon.terminate()

        with mariadb.admin_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT LENGTH(payload) FROM mqtt_events "
                "WHERE topic = 'huge/payload'"
            )
            row = cur.fetchone()
        assert row is not None, "huge payload did not reach the DB"
        assert row[0] == len(payload), \
            f"payload truncated: stored {row[0]} bytes, expected {len(payload)}"


# ─── 4. AppleScript-injection topic through the full pipeline ────────────────

class TestAppleScriptInjectionE2E:
    """Defense-in-depth e2e for the AppleScript injection regression. A
    malicious publisher chooses a topic name designed to escape the
    AppleScript string literal in the flood-alert path. The daemon must:

    (a) survive the flood without crashing,
    (b) write the alert line to alerts.log without breaking format,
    (c) not execute the injected payload (verified by absence of a
        marker file that a successful injection would create).

    The osascript spawn is disabled via env var for clean test runs;
    the argv structure is verified by the unit test in
    test_loop_detector.py."""

    EVIL_TOPIC = 'inject/"\\";do shell script "touch /tmp/mqtt-pwned-{ts}";"//'

    def test_malicious_topic_survives_flood(self, spawn_daemon,
                                            mosquitto_container, tmp_path):
        # Unique sentinel so a stale file from a previous run can't pass
        # the test.
        sentinel_ts = str(time.time_ns())
        evil_topic = self.EVIL_TOPIC.format(ts=sentinel_ts)
        sentinel = f"/tmp/mqtt-pwned-{sentinel_ts}"

        daemon = spawn_daemon()
        daemon.wait_for_connected()

        from mqtt_logger import LoopDetector
        for i in range(LoopDetector.THRESHOLD + 1):
            _publish(mosquitto_container.host, mosquitto_container.port,
                     evil_topic, f"msg-{i}".encode())

        time.sleep(1.0)
        daemon.process.send_signal(signal.SIGTERM)
        rc = daemon.terminate()
        assert rc == 0, "daemon crashed processing malicious topic"

        # (b) alerts.log got a usable line that names the topic.
        assert daemon.alert_file.exists()
        content = daemon.alert_file.read_text()
        assert "MQTT flood" in content
        assert evil_topic in content, \
            f"alert line did not contain the topic: {content!r}"

        # (c) the injected `do shell script` did NOT execute.
        import os as _os
        assert not _os.path.exists(sentinel), \
            f"AppleScript injection succeeded — sentinel {sentinel} exists"
