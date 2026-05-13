"""Unit tests for MQTTLogger.on_message dispatch."""
from datetime import datetime
from unittest.mock import MagicMock

import pytest


class FakeBackend:
    def __init__(self, name="fake", fail=False):
        self._name = name
        self.calls = []
        self.fail = fail

    @property
    def name(self):
        return self._name

    def insert(self, *args):
        if self.fail:
            raise RuntimeError("boom")
        self.calls.append(args)

    def close(self):
        pass


@pytest.fixture
def logger_instance(monkeypatch):
    # Patch paho.mqtt.client.Client to a no-op MagicMock so MQTTLogger.__init__
    # doesn't try to wire real callbacks.
    import paho.mqtt.client as mqtt
    monkeypatch.setattr(mqtt, "Client", MagicMock())

    from mqtt_logger import MQTTLogger
    backend = FakeBackend()
    inst = MQTTLogger("localhost", 1883, [backend])
    return inst, backend


def make_msg(topic="log/board1", payload=b'{"board":"board1"}', qos=0,
             retain=False):
    msg = MagicMock()
    msg.topic = topic
    msg.payload = payload
    msg.qos = qos
    msg.retain = retain
    return msg


class TestDispatch:
    def test_utf8_payload_decoded(self, logger_instance):
        inst, backend = logger_instance
        inst.on_message(inst.client, None, make_msg())

        assert len(backend.calls) == 1
        ts, topic, sender, payload, qos, retained = backend.calls[0]
        assert isinstance(ts, datetime)
        assert topic == "log/board1"
        assert sender == "board1"
        assert payload == '{"board":"board1"}'
        assert qos == 0
        assert retained == 0

    def test_binary_payload_falls_back_to_hex(self, logger_instance):
        inst, backend = logger_instance
        # Bytes that aren't valid UTF-8.
        inst.on_message(inst.client, None, make_msg(payload=b'\xff\xfe\xfd'))

        assert backend.calls[0][3] == "fffefd"

    def test_retain_flag_propagates(self, logger_instance):
        inst, backend = logger_instance
        inst.on_message(inst.client, None, make_msg(retain=True))
        assert backend.calls[0][5] == 1

    def test_backend_failure_does_not_block_others(self, monkeypatch):
        import paho.mqtt.client as mqtt
        monkeypatch.setattr(mqtt, "Client", MagicMock())
        from mqtt_logger import MQTTLogger

        good = FakeBackend("ok")
        bad = FakeBackend("bad", fail=True)
        inst = MQTTLogger("h", 1883, [bad, good])
        # Should not raise; good backend still receives the insert.
        inst.on_message(inst.client, None, make_msg())
        assert len(good.calls) == 1
