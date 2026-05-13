"""Flood detection through the full pipeline: real broker, real daemon
subprocess, real flood, real alerts.log file. Catches timing or
threading issues that the in-process LoopDetector tests miss."""
import signal
import time

import paho.mqtt.client as mqtt
import pytest

from mqtt_logger import LoopDetector


def _publish_burst(host, port, topic, count):
    """Publish `count` messages to `topic` as fast as the broker can ack."""
    pub = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                      client_id=f"flood-{time.time_ns()}")
    pub.connect(host, port)
    pub.loop_start()
    try:
        for i in range(count):
            pub.publish(topic, f"msg-{i}".encode(), qos=1) \
               .wait_for_publish(timeout=5)
    finally:
        pub.loop_stop()
        pub.disconnect()


class TestFloodDetectionE2E:
    def test_burst_triggers_alert_file_write(self, spawn_daemon,
                                             mosquitto_container):
        """Publish more than THRESHOLD messages to a single topic in under
        WINDOW_SEC, then verify the alert file gets a flood line."""
        daemon = spawn_daemon()
        daemon.wait_for_connected()

        topic = "flood/canary"
        _publish_burst(mosquitto_container.host, mosquitto_container.port,
                       topic, count=LoopDetector.THRESHOLD + 1)

        # The daemon writes to the alert file synchronously from on_message,
        # but the publish is async — give it a moment to drain.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if daemon.alert_file.exists():
                content = daemon.alert_file.read_text()
                if topic in content:
                    break
            time.sleep(0.1)

        assert daemon.alert_file.exists(), "alert file was never written"
        content = daemon.alert_file.read_text()
        assert topic in content, \
            f"flood topic not in alert file. Content was: {content!r}"
        assert "MQTT flood" in content

    def test_non_flood_traffic_does_not_trigger(self, spawn_daemon,
                                                mosquitto_container):
        """Publish fewer than THRESHOLD messages — the alert file should
        stay empty (or unwritten)."""
        daemon = spawn_daemon()
        daemon.wait_for_connected()

        topic = "calm/topic"
        _publish_burst(mosquitto_container.host, mosquitto_container.port,
                       topic, count=LoopDetector.THRESHOLD - 1)

        # Wait beyond the daemon's reaction window.
        time.sleep(1.0)

        # File may or may not exist; if it does, it must not contain our
        # canary topic.
        if daemon.alert_file.exists():
            assert topic not in daemon.alert_file.read_text()

    def test_cooldown_suppresses_repeat_alerts(self, spawn_daemon,
                                               mosquitto_container):
        """Two back-to-back bursts (within COOLDOWN_SEC) should produce
        exactly one alert line, not two."""
        daemon = spawn_daemon()
        daemon.wait_for_connected()

        topic = "cooldown/canary"
        _publish_burst(mosquitto_container.host, mosquitto_container.port,
                       topic, count=LoopDetector.THRESHOLD + 1)
        time.sleep(0.5)
        # Second burst while still in cooldown.
        _publish_burst(mosquitto_container.host, mosquitto_container.port,
                       topic, count=LoopDetector.THRESHOLD + 1)
        time.sleep(0.5)

        # Stop daemon so we know it's done writing.
        daemon.process.send_signal(signal.SIGTERM)
        daemon.terminate()

        content = daemon.alert_file.read_text() if daemon.alert_file.exists() \
                  else ""
        matching = [ln for ln in content.splitlines() if topic in ln]
        assert len(matching) == 1, \
            f"expected exactly one alert line, got {len(matching)}: {matching}"
