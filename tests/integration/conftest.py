"""Session-scoped container fixtures for the integration tier.

Spins up disposable Mosquitto + MariaDB containers via testcontainers once
per pytest session. Per-test isolation is achieved by truncating the
mqtt_events table between tests (the MariaDB fixture exposes a helper).
"""
from __future__ import annotations

import os
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import pytest

# All tests in this directory are automatically marked `integration`,
# so the default pytest invocation skips them.
pytestmark = pytest.mark.integration


def pytest_collection_modifyitems(config, items):
    """Apply the `integration` marker to every test in this directory so
    individual files don't need to repeat the decoration."""
    marker = pytest.mark.integration
    for item in items:
        if "tests/integration/" in str(item.fspath).replace(os.sep, "/"):
            item.add_marker(marker)


def _wait_for_tcp(host: str, port: int, timeout: float = 30.0) -> None:
    """Block until a TCP listener accepts on (host, port). Raises on timeout."""
    deadline = time.monotonic() + timeout
    last_err = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2.0):
                return
        except OSError as e:
            last_err = e
            time.sleep(0.25)
    raise TimeoutError(
        f"TCP {host}:{port} did not accept within {timeout}s "
        f"(last error: {last_err})"
    )


# ─── Mosquitto ────────────────────────────────────────────────────────────────

@dataclass
class MosquittoEndpoint:
    host: str
    port: int


def _find_free_port() -> int:
    """Reserve a free port by binding ephemerally, closing, and returning it.
    There's a brief race where another process could grab it, but the
    window is small and the testcontainer claims it immediately."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return int(s.getsockname()[1])


@pytest.fixture(scope="session")
def mosquitto_container():
    """Run eclipse-mosquitto with anonymous access on a fixed host port.

    The port is fixed (not Docker-assigned) so the reconnect test can
    stop/start the container without the host-port mapping changing —
    `docker run -p <fixed>:1883` preserves the binding across restarts."""
    from testcontainers.core.container import DockerContainer

    config_dir = Path(tempfile.mkdtemp(prefix="mosquitto-conf-"))
    config_file = config_dir / "mosquitto.conf"
    config_file.write_text(
        "listener 1883 0.0.0.0\n"
        "allow_anonymous true\n"
        "persistence false\n"
    )

    host_port = _find_free_port()
    container = (
        DockerContainer("eclipse-mosquitto:2.0")
        .with_bind_ports(1883, host_port)
        .with_volume_mapping(str(config_file), "/mosquitto/config/mosquitto.conf", "ro")
    )
    container.start()
    try:
        host = container.get_container_host_ip()
        _wait_for_tcp(host, host_port)
        yield MosquittoEndpoint(host=host, port=host_port)
    finally:
        container.stop()


# ─── MariaDB ──────────────────────────────────────────────────────────────────

@dataclass
class MariaDBEndpoint:
    host: str
    port: int
    user: str
    password: str
    database: str

    def admin_connect(self):
        import pymysql
        return pymysql.connect(
            host=self.host, port=self.port, user=self.user,
            password=self.password, database=self.database,
            charset="utf8mb4", autocommit=True,
        )

    def truncate(self) -> None:
        """Empty mqtt_events between tests. Safe even before the table
        exists (the writer creates it on first connect)."""
        with self.admin_connect() as conn, conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS mqtt_events ("
                "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY, "
                "timestamp DATETIME(6) NOT NULL, "
                "topic VARCHAR(512) NOT NULL, "
                "sender VARCHAR(255) DEFAULT NULL, "
                "payload LONGTEXT DEFAULT NULL, "
                "qos TINYINT NOT NULL, "
                "retained TINYINT NOT NULL, "
                "KEY idx_timestamp (timestamp), "
                "KEY idx_topic (topic(64))"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
            )
            cur.execute("TRUNCATE TABLE mqtt_events")


@pytest.fixture(scope="session")
def mariadb_container():
    """Run mariadb:11 with a known root password and an empty 'mqtt_log' db."""
    from testcontainers.mysql import MySqlContainer

    # testcontainers' MariaDB wrapper lives under testcontainers.mysql even
    # though the image is `mariadb`. The class accepts a custom image.
    container = MySqlContainer(image="mariadb:11", dialect="pymysql")
    container.with_env("MARIADB_ROOT_PASSWORD", "rootpw")
    container.with_env("MARIADB_DATABASE", "mqtt_log")
    container.start()
    try:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(3306))
        _wait_for_tcp(host, port)
        ep = MariaDBEndpoint(
            host=host, port=port,
            user="root", password="rootpw", database="mqtt_log",
        )
        # The container may accept TCP before MariaDB is ready to answer
        # SELECT 1. Poll briefly for that too.
        deadline = time.monotonic() + 30
        last_err = None
        while time.monotonic() < deadline:
            try:
                with ep.admin_connect() as c, c.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
                break
            except Exception as e:
                last_err = e
                time.sleep(0.5)
        else:
            raise TimeoutError(f"MariaDB not ready: {last_err}")
        yield ep
    finally:
        container.stop()


@pytest.fixture
def mariadb(mariadb_container, monkeypatch):
    """Per-test fixture: truncates events table + injects credentials via
    the env-var fallback so backend code paths don't need to know about
    testcontainers."""
    mariadb_container.truncate()
    monkeypatch.setenv("MQTT_LOGGER_MARIADB_PASSWORD",
                       mariadb_container.password)
    return mariadb_container


# ─── Daemon subprocess helper ─────────────────────────────────────────────────

@dataclass
class DaemonHandle:
    process: subprocess.Popen
    alert_file: Path
    log_dir: Path

    def wait_for_connected(self, timeout: float = 15.0) -> None:
        """Block until the daemon writes 'Connected to broker' to its log."""
        log_path = self.log_dir / "mqtt_logger.log"
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if log_path.exists():
                text = log_path.read_text()
                if "Connected to broker" in text and "Subscribed to #" in text:
                    return
            if self.process.poll() is not None:
                raise RuntimeError(
                    f"daemon exited prematurely with code "
                    f"{self.process.returncode}"
                )
            time.sleep(0.1)
        raise TimeoutError(
            f"daemon did not connect within {timeout}s. "
            f"Log so far: {log_path.read_text() if log_path.exists() else '<missing>'}"
        )

    def terminate(self, timeout: float = 10.0) -> int:
        """Send SIGTERM and wait for clean exit."""
        if self.process.poll() is None:
            self.process.terminate()
        try:
            return self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.process.kill()
            return self.process.wait()


@pytest.fixture
def spawn_daemon(tmp_path, mosquitto_container, mariadb):
    """Factory fixture that spawns mqtt_logger.py as a subprocess wired up
    to the testcontainer broker + MariaDB. Caller picks backend flags.

    The subprocess inherits the env var that supplies the MariaDB password,
    and we redirect its log files into a per-test temp dir so assertions
    can read them without colliding with other tests."""
    repo_root = Path(__file__).resolve().parent.parent.parent
    spawned: list[DaemonHandle] = []

    def _spawn(extra_args: list[str] = (), enable_mariadb: bool = True,
               enable_sqlite: bool = False) -> DaemonHandle:
        log_dir = tmp_path / "logs"
        log_dir.mkdir(exist_ok=True)
        alert_file = tmp_path / "alerts.log"

        # The daemon's RotatingFileHandler writes to <script_dir>/mqtt_logger.log
        # — we can't easily redirect that without a CLI flag, so we symlink
        # the script into a per-test dir so its 'script_dir' is isolated.
        scratch = tmp_path / "scratch"
        scratch.mkdir(exist_ok=True)
        # Copy the two daemon source files (cheap; ~25kb total) so the
        # subprocess's Path(__file__).parent points into the temp dir.
        for src in ("mqtt_logger.py", "query_events.py"):
            (scratch / src).write_text((repo_root / src).read_text())

        args = [
            str(repo_root / "venv" / "bin" / "python"),
            str(scratch / "mqtt_logger.py"),
            "--broker", mosquitto_container.host,
            "--port", str(mosquitto_container.port),
            "--alert-file", str(alert_file),
            "--verbose",
        ]
        if enable_sqlite:
            args.extend(["--db", str(tmp_path / "events.db")])
        if enable_mariadb:
            args.extend([
                "--mariadb",
                "--mariadb-host", mariadb.host,
                "--mariadb-port", str(mariadb.port),
                "--mariadb-db", mariadb.database,
                "--mariadb-user", mariadb.user,
            ])
        args.extend(extra_args)

        env = {
            **os.environ,
            "MQTT_LOGGER_MARIADB_PASSWORD": mariadb.password,
            # MariaDBBackend uses the `logger` user by default; tests run as
            # the root user the container creates. Override via env so we
            # don't have to expose another flag.
        }

        proc = subprocess.Popen(args, env=env, cwd=str(scratch))
        handle = DaemonHandle(process=proc, alert_file=alert_file,
                              log_dir=scratch)
        spawned.append(handle)
        return handle

    yield _spawn

    for handle in spawned:
        handle.terminate()
