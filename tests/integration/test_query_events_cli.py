"""End-to-end tests for query_events.py spawned as a subprocess.

Covers what the existing unit tests can't reach: argparse wiring, output
formatting, the MariaDB credential resolution path, the auto-detect
SQLite fallback, and exit codes on real failures."""
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

from mqtt_logger import MariaDBBackend, SQLiteBackend


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
PYTHON = REPO_ROOT / "venv" / "bin" / "python"


def _run_query(args, env=None, cwd=None, expect_success=True):
    """Spawn query_events.py with the given args and return CompletedProcess."""
    cmd = [str(PYTHON), str(REPO_ROOT / "query_events.py"), *args]
    result = subprocess.run(
        cmd, env=env, cwd=str(cwd) if cwd else None,
        capture_output=True, text=True, timeout=15,
    )
    if expect_success and result.returncode != 0:
        pytest.fail(
            f"query_events exited with {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def _seed_sqlite(path: Path):
    """Write a known set of rows through the real SQLiteBackend."""
    writer = SQLiteBackend(str(path))
    rows = [
        (datetime(2026, 5, 12,  9, 0, 0), "cova/a/status",   None,     "1", 0, 0),
        (datetime(2026, 5, 12,  9, 1, 0), "cova/b/status",   None,     "2", 0, 0),
        (datetime(2026, 5, 12,  9, 2, 0), "cova/a/b/status", None,     "3", 0, 0),
        (datetime(2026, 5, 12,  9, 3, 0), "log/board1",      "board1", "4", 1, 1),
    ]
    for r in rows:
        writer.insert(*r)
    writer.close()


def _seed_mariadb(mariadb):
    writer = MariaDBBackend(host=mariadb.host, port=mariadb.port,
                            database=mariadb.database, user=mariadb.user)
    try:
        writer.insert(datetime(2026, 5, 12,  9, 0, 0), "cova/a/status",
                      None, "1", 0, 0)
        writer.insert(datetime(2026, 5, 12,  9, 1, 0), "cova/b/status",
                      None, "2", 0, 0)
        writer.insert(datetime(2026, 5, 12,  9, 2, 0), "cova/a/b/status",
                      None, "3", 0, 0)
        writer.insert(datetime(2026, 5, 12,  9, 3, 0), "log/board1",
                      "board1", "4", 1, 1)
    finally:
        writer.close()


class TestSQLiteCLI:
    def test_topics_lists_unique_topics_with_counts(self, tmp_path):
        db = tmp_path / "events.db"
        _seed_sqlite(db)

        out = _run_query(["--db", str(db), "--topics"]).stdout
        assert "cova/a/status" in out
        assert "cova/b/status" in out
        assert "log/board1" in out
        # Each topic occurs once in the seed.
        for line in out.splitlines():
            if "cova/a/status" in line:
                # Last whitespace-separated token is the count.
                assert line.split()[-1] == "1"

    def test_stats_reports_correct_totals(self, tmp_path):
        db = tmp_path / "events.db"
        _seed_sqlite(db)

        out = _run_query(["--db", str(db), "--stats"]).stdout
        assert "Total events:" in out
        assert "4" in out
        assert "Retained msgs:" in out
        # 4 distinct topics in our seed.
        assert "Unique topics:" in out

    def test_topic_wildcard_plus_respects_levels(self, tmp_path):
        db = tmp_path / "events.db"
        _seed_sqlite(db)

        out = _run_query(["--db", str(db),
                          "--topic", "cova/+/status",
                          "--limit", "10"]).stdout
        assert "cova/a/status" in out
        assert "cova/b/status" in out
        # Regression: `+` must not cross `/`.
        assert "cova/a/b/status" not in out

    def test_topic_hash_wildcard(self, tmp_path):
        db = tmp_path / "events.db"
        _seed_sqlite(db)

        out = _run_query(["--db", str(db), "--topic", "cova/#",
                          "--limit", "10"]).stdout
        assert "cova/a/status" in out
        assert "cova/a/b/status" in out
        assert "log/board1" not in out

    def test_limit_truncates_output(self, tmp_path):
        db = tmp_path / "events.db"
        _seed_sqlite(db)

        out = _run_query(["--db", str(db), "--limit", "2"]).stdout
        # Each event prints a header line that starts with the year.
        header_lines = [l for l in out.splitlines() if l.startswith("2026")]
        assert len(header_lines) == 2

    def test_missing_db_exits_nonzero(self, tmp_path):
        nonexistent = tmp_path / "does-not-exist.db"
        result = _run_query(["--db", str(nonexistent)], expect_success=False)
        assert result.returncode != 0
        # SystemExit message goes to stderr.
        combined = result.stdout + result.stderr
        assert "not found" in combined.lower() or "no such" in combined.lower()


class TestMariaDBCLI:
    def test_stats_against_real_mariadb(self, mariadb):
        _seed_mariadb(mariadb)

        env = {**__import__("os").environ,
               "MQTT_LOGGER_MARIADB_PASSWORD": mariadb.password}
        out = _run_query([
            "--mariadb",
            "--mariadb-host", mariadb.host,
            "--mariadb-port", str(mariadb.port),
            "--mariadb-db", mariadb.database,
            "--mariadb-user", mariadb.user,
            "--stats",
        ], env=env).stdout
        assert "Total events:" in out
        assert "4" in out
        assert "Unique topics:" in out

    def test_wildcard_through_real_regexp(self, mariadb):
        _seed_mariadb(mariadb)

        env = {**__import__("os").environ,
               "MQTT_LOGGER_MARIADB_PASSWORD": mariadb.password}
        out = _run_query([
            "--mariadb",
            "--mariadb-host", mariadb.host,
            "--mariadb-port", str(mariadb.port),
            "--mariadb-db", mariadb.database,
            "--mariadb-user", mariadb.user,
            "--topic", "cova/+/status",
        ], env=env).stdout
        assert "cova/a/status" in out
        assert "cova/b/status" in out
        assert "cova/a/b/status" not in out

    def test_bad_credentials_exits_nonzero(self, mariadb):
        env = {**__import__("os").environ,
               "MQTT_LOGGER_MARIADB_PASSWORD": "wrong-password"}
        result = _run_query([
            "--mariadb",
            "--mariadb-host", mariadb.host,
            "--mariadb-port", str(mariadb.port),
            "--mariadb-db", mariadb.database,
            "--mariadb-user", mariadb.user,
            "--stats",
        ], env=env, expect_success=False)
        assert result.returncode != 0


class TestAutoDetectSQLite:
    """When no --db / --mariadb flag is passed, the CLI falls back to a
    local mqtt_events.db. Verify the lookup path and the failure mode."""

    def test_uses_local_db_when_present(self, tmp_path):
        # Set up a scratch dir with a mqtt_events.db next to the script copy.
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        (scratch / "mqtt_logger.py").write_text(
            (REPO_ROOT / "mqtt_logger.py").read_text()
        )
        (scratch / "query_events.py").write_text(
            (REPO_ROOT / "query_events.py").read_text()
        )
        db = scratch / "mqtt_events.db"
        _seed_sqlite(db)

        # Run the query script from the scratch dir; with no --db flag, it
        # should auto-detect mqtt_events.db beside itself.
        cmd = [str(PYTHON), str(scratch / "query_events.py"), "--stats"]
        result = subprocess.run(cmd, capture_output=True, text=True,
                                timeout=15)
        assert result.returncode == 0, result.stderr
        assert "Total events:" in result.stdout
        assert "4" in result.stdout
