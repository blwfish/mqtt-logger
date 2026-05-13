"""Unit tests for mqtt_logger.extract_sender."""
import json

import pytest

from mqtt_logger import extract_sender


class TestTopicPatterns:
    def test_log_topic(self):
        assert extract_sender("log/loco-3001", "") == "loco-3001"

    def test_log_topic_with_leading_slash(self):
        assert extract_sender("/log/loco-3001", "") == "loco-3001"

    def test_three_segment_log_topic_is_not_matched(self):
        # The `/log/{board}` matcher requires exactly 2 segments; deeper paths
        # should fall through to the JSON-payload check.
        assert extract_sender("some/log/loco", "") is None

    def test_config_status_topic(self):
        assert extract_sender("mr/config/status/turnout-7", "") == "turnout-7"

    def test_config_backup_topic(self):
        assert extract_sender("mr/config/backup/yard-1", "") == "yard-1"

    def test_config_other_subtype_falls_through(self):
        assert extract_sender("mr/config/request/board", "") is None


class TestPayloadPatterns:
    @pytest.mark.parametrize("key", [
        "board", "sender", "client_id", "clientId",
        "source", "from", "device_id",
    ])
    def test_each_known_key(self, key):
        payload = json.dumps({key: "device-x"})
        assert extract_sender("misc/topic", payload) == "device-x"

    def test_key_precedence(self):
        # `board` comes before `sender` in the lookup order — confirm.
        payload = json.dumps({"sender": "B", "board": "A"})
        assert extract_sender("misc/topic", payload) == "A"

    def test_non_string_value_is_stringified(self):
        payload = json.dumps({"board": 42})
        assert extract_sender("misc/topic", payload) == "42"

    def test_no_known_keys_returns_none(self):
        assert extract_sender("misc/topic", json.dumps({"foo": "bar"})) is None


class TestEdgeCases:
    def test_empty_payload(self):
        assert extract_sender("misc/topic", "") is None

    def test_malformed_json(self):
        assert extract_sender("misc/topic", "{not json") is None

    def test_json_array_payload(self):
        # JSON list at the top level isn't a dict; should not crash.
        assert extract_sender("misc/topic", "[1, 2, 3]") is None

    def test_json_scalar_payload(self):
        assert extract_sender("misc/topic", '"just a string"') is None

    def test_hex_payload_does_not_crash(self):
        # Binary payloads get stored as hex; the JSON loader should silently
        # reject them.
        assert extract_sender("misc/topic", "deadbeef") is None

    def test_empty_topic(self):
        assert extract_sender("", "") is None
