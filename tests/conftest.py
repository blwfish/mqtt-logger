"""Shared fixtures and helpers for the mqtt-logger test suite."""
from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

# Make the project root importable as a sibling package without installing.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class FakeMQTTMessage:
    """Duck-type stand-in for paho.mqtt.client.MQTTMessage."""

    def __init__(self, topic: str, payload: bytes, qos: int = 0,
                 retain: bool = False):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.retain = retain


@pytest.fixture
def msg_factory():
    """Build a FakeMQTTMessage with sensible defaults."""
    def _make(topic: str = "test/topic", payload: bytes = b"hello",
              qos: int = 0, retain: bool = False) -> FakeMQTTMessage:
        return FakeMQTTMessage(topic, payload, qos, retain)
    return _make


@pytest.fixture
def fake_pymysql(monkeypatch):
    """Install a minimal fake pymysql module before MariaDBBackend imports it."""
    fake = types.ModuleType("pymysql")

    class OperationalError(Exception):
        pass

    fake.OperationalError = OperationalError
    fake.connect = None  # callers will override per-test
    monkeypatch.setitem(sys.modules, "pymysql", fake)
    return fake


@pytest.fixture
def fake_keyring(monkeypatch):
    """Install a fake keyring module with a settable password value."""
    fake = types.ModuleType("keyring")
    fake._password = "secret"

    def get_password(service: str, user: str):
        return fake._password

    fake.get_password = get_password
    monkeypatch.setitem(sys.modules, "keyring", fake)
    return fake
