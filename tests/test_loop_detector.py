"""Unit tests for LoopDetector — flood detection, cooldown, eviction, safety."""
import platform
import time

import pytest

from mqtt_logger import LoopDetector


class FakeClock:
    """Monotonic clock stand-in. Tests advance time by calling .advance()."""

    def __init__(self, start: float = 1000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    fake = FakeClock()
    monkeypatch.setattr(time, "monotonic", fake)
    return fake


@pytest.fixture
def no_osascript(monkeypatch):
    """Block any accidental osascript launch in tests."""
    calls = []

    def fake_popen(argv, *args, **kwargs):
        calls.append(argv)

        class P:
            pass

        return P()

    import subprocess
    monkeypatch.setattr(subprocess, "Popen", fake_popen)
    return calls


class TestThreshold:
    def test_below_threshold_no_alert(self, clock, no_osascript, tmp_path):
        alert_file = tmp_path / "alerts.log"
        d = LoopDetector(alert_file=str(alert_file))

        for _ in range(d.THRESHOLD - 1):
            d.record("foo/bar")

        assert not alert_file.exists() or alert_file.read_text() == ""

    def test_at_threshold_triggers_alert(self, clock, no_osascript, tmp_path):
        alert_file = tmp_path / "alerts.log"
        d = LoopDetector(alert_file=str(alert_file))

        for _ in range(d.THRESHOLD):
            d.record("foo/bar")

        content = alert_file.read_text()
        assert "MQTT flood" in content
        assert "foo/bar" in content

    def test_above_threshold_triggers_alert(self, clock, no_osascript, tmp_path):
        """THRESHOLD+1 must also trigger — pins >=, not >."""
        alert_file = tmp_path / "alerts.log"
        d = LoopDetector(alert_file=str(alert_file))

        for _ in range(d.THRESHOLD + 1):
            d.record("foo/bar")

        assert "MQTT flood" in alert_file.read_text()


class TestCooldown:
    def test_cooldown_suppresses_repeat_alerts(self, clock, no_osascript,
                                               tmp_path):
        alert_file = tmp_path / "alerts.log"
        d = LoopDetector(alert_file=str(alert_file))

        # First burst → 1 alert line.
        for _ in range(d.THRESHOLD):
            d.record("foo")
        first_lines = alert_file.read_text().splitlines()
        assert len(first_lines) == 1

        # Advance less than COOLDOWN — another burst — still 1 alert line.
        clock.advance(d.COOLDOWN_SEC / 2)
        for _ in range(d.THRESHOLD * 2):
            d.record("foo")
        assert len(alert_file.read_text().splitlines()) == 1

    def test_alert_re_fires_after_cooldown(self, clock, no_osascript,
                                           tmp_path):
        alert_file = tmp_path / "alerts.log"
        d = LoopDetector(alert_file=str(alert_file))

        for _ in range(d.THRESHOLD):
            d.record("foo")
        clock.advance(d.COOLDOWN_SEC + 1)
        for _ in range(d.THRESHOLD):
            d.record("foo")

        assert len(alert_file.read_text().splitlines()) == 2

    def test_alert_re_fires_at_exact_cooldown(self, clock, no_osascript,
                                              tmp_path):
        """Advancing exactly COOLDOWN_SEC must allow re-alert — pins >=, not >."""
        alert_file = tmp_path / "alerts.log"
        d = LoopDetector(alert_file=str(alert_file))

        for _ in range(d.THRESHOLD):
            d.record("foo")
        clock.advance(d.COOLDOWN_SEC)  # exact boundary
        for _ in range(d.THRESHOLD):
            d.record("foo")

        assert len(alert_file.read_text().splitlines()) == 2


class TestSlidingWindow:
    def test_old_timestamps_drop_out(self, clock, no_osascript):
        d = LoopDetector()

        # Pile up THRESHOLD-1 records, then wait past the window.
        for _ in range(d.THRESHOLD - 1):
            d.record("foo")
        clock.advance(d.WINDOW_SEC + 1)

        # One more record should NOT trigger — the older ones aged out.
        d.record("foo")
        assert len(d._counts["foo"]) == 1

    def test_timestamp_at_exact_window_boundary_is_kept(self, clock,
                                                         no_osascript):
        """A timestamp at exactly now - WINDOW_SEC must NOT be evicted.
        The purge uses strict `<`, so the boundary value stays in the window."""
        d = LoopDetector()
        d.record("foo")                    # recorded at t=1000
        clock.advance(d.WINDOW_SEC)        # now = 1000 + WINDOW_SEC
        # cutoff = now - WINDOW_SEC = 1000; the record's timestamp IS the cutoff.
        # `timestamps[0] < cutoff` is False for ==, so it must NOT be evicted.
        d.record("foo")                    # triggers purge
        assert len(d._counts["foo"]) == 2  # both records kept


class TestEviction:
    def test_idle_topics_are_evicted(self, clock, no_osascript):
        d = LoopDetector()

        d.record("ephemeral/topic")
        assert "ephemeral/topic" in d._counts

        # Skip past the idle threshold, then trigger an eviction sweep by
        # generating enough records on a different topic.
        clock.advance(d._EVICT_IDLE_SEC + 1)
        for _ in range(d._EVICT_EVERY):
            d.record("hot/topic")

        assert "ephemeral/topic" not in d._counts
        assert "ephemeral/topic" not in d._last_seen
        assert "hot/topic" in d._counts

    def test_topic_at_exact_idle_boundary_is_kept(self, clock, no_osascript):
        """A topic last seen at exactly now - _EVICT_IDLE_SEC must NOT be evicted.
        The check uses strict `<`, so the boundary value is retained."""
        d = LoopDetector()
        d.record("boundary/topic")        # last_seen = t=1000
        clock.advance(d._EVICT_IDLE_SEC)  # now = 1000 + _EVICT_IDLE_SEC
        # idle_cutoff = now - _EVICT_IDLE_SEC = 1000
        # seen (1000) < idle_cutoff (1000) is False → must NOT be evicted
        for _ in range(d._EVICT_EVERY):
            d.record("hot/topic")
        assert "boundary/topic" in d._counts

    def test_eviction_does_not_fire_before_evict_every(self, clock,
                                                        no_osascript):
        """Total records must reach _EVICT_EVERY to trigger eviction — pins
        >=, not >.  The ephemeral record counts toward the total, so we need
        _EVICT_EVERY - 2 hot records to reach _EVICT_EVERY - 1 total."""
        d = LoopDetector()
        d.record("ephemeral/topic")           # _records_since_evict = 1
        clock.advance(d._EVICT_IDLE_SEC + 1)
        for _ in range(d._EVICT_EVERY - 2):  # total reaches _EVICT_EVERY - 1
            d.record("hot/topic")
        # Counter is _EVICT_EVERY - 1; eviction has NOT fired yet.
        assert "ephemeral/topic" in d._counts


class TestAlertInjectionSafe:
    """Regression: a topic containing AppleScript-meaningful characters must
    not be able to break out of the AppleScript string. The fix routes the
    message through argv, so we assert on the subprocess argv shape rather
    than trying to actually execute osascript."""

    def test_topic_with_quotes_lands_in_argv(self, clock, no_osascript,
                                              tmp_path):
        if platform.system() != "Darwin":
            pytest.skip("AppleScript path only fires on macOS")

        alert_file = tmp_path / "alerts.log"
        d = LoopDetector(alert_file=str(alert_file))
        evil = 'evil"; do shell script "echo pwn'

        for _ in range(d.THRESHOLD):
            d.record(evil)

        assert no_osascript, "expected one osascript invocation"
        argv = no_osascript[-1]
        assert argv[0] == "osascript"
        # AppleScript template comes via -e, and msg/title via positional argv.
        # The topic must appear in a positional slot, not inlined into the
        # template — so we assert the template DOES NOT contain the topic and
        # the message-positional DOES.
        template_arg = argv[argv.index("-e") + 1]
        assert evil not in template_arg
        assert any(evil in arg for arg in argv[argv.index("--") + 1:])
