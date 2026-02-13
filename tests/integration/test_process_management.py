"""Integration tests for pipeline process management.

These tests verify process management functionality:
- PID file creation and locking
- Status command on running pipelines
- Stop command for graceful termination
"""

import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

import pytest

# Find uv executable
UV_PATH = shutil.which("uv")
if UV_PATH is None:
    pytest.skip("uv not found", allow_module_level=True)


def wait_for_pid_file(pid_file: Path, timeout: float = 5.0) -> int:
    """Wait for PID file to be created and return the PID."""
    for _ in range(int(timeout / 0.25)):
        time.sleep(0.25)
        if pid_file.exists():
            return int(pid_file.read_text().strip())
    raise TimeoutError("PID file was not created")


def wait_for_process_exit(pid: int, timeout: float = 10.0) -> None:
    """Wait for a process to exit."""
    for _ in range(int(timeout / 0.5)):
        time.sleep(0.5)
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
    raise TimeoutError(f"Process {pid} did not exit")


def kill_pipeline(pid_file: Path) -> None:
    """Kill a running pipeline process if it exists."""
    if not pid_file.exists():
        return
    try:
        pid = int(pid_file.read_text().strip())
        os.kill(pid, signal.SIGTERM)
        wait_for_process_exit(pid, timeout=5.0)
    except (ProcessLookupError, ValueError, TimeoutError):
        pass


@pytest.fixture
def minimal_config(tmp_path: Path):
    """Create a minimal pipeline config using the built-in echo task."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("""
tasks:
  - name: echo
    kind: local
    module: tigerflow.library.echo
    input_ext: .txt
    output_ext: .txt
""")
    return config_file


@pytest.fixture
def background_pipeline(minimal_config, tmp_dirs):
    """Start a background pipeline and clean up after the test."""
    input_dir, output_dir = tmp_dirs
    pid_file = output_dir / ".tigerflow" / "run.pid"

    subprocess.run(
        [
            UV_PATH,
            "run",
            "tigerflow",
            "run",
            str(minimal_config),
            str(input_dir),
            str(output_dir),
            "--background",
            "--idle-timeout",
            "5",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )

    # Wait for pipeline to start
    time.sleep(1)

    yield output_dir

    # Cleanup
    kill_pipeline(pid_file)


class TestForegroundRun:
    """Test foreground pipeline execution."""

    def test_foreground_creates_pid_file(self, minimal_config, tmp_dirs):
        """Test that foreground run creates PID file."""
        input_dir, output_dir = tmp_dirs
        pid_file = output_dir / ".tigerflow" / "run.pid"

        proc = subprocess.Popen(
            [
                UV_PATH,
                "run",
                "tigerflow",
                "run",
                str(minimal_config),
                str(input_dir),
                str(output_dir),
                "--idle-timeout",
                "1",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            pid = wait_for_pid_file(pid_file)
            assert pid > 0
            os.kill(pid, 0)  # Raises ProcessLookupError if not running
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_foreground_blocks_concurrent_run(self, minimal_config, tmp_dirs):
        """Test that a second run is blocked while first is running."""
        input_dir, output_dir = tmp_dirs
        pid_file = output_dir / ".tigerflow" / "run.pid"

        proc = subprocess.Popen(
            [
                UV_PATH,
                "run",
                "tigerflow",
                "run",
                str(minimal_config),
                str(input_dir),
                str(output_dir),
                "--idle-timeout",
                "5",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            wait_for_pid_file(pid_file)

            # Try to start second pipeline
            result = subprocess.run(
                [
                    UV_PATH,
                    "run",
                    "tigerflow",
                    "run",
                    str(minimal_config),
                    str(input_dir),
                    str(output_dir),
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )

            assert result.returncode == 1
            assert "already running" in result.stderr.lower()
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_foreground_handles_sigterm(self, minimal_config, tmp_dirs):
        """Test that foreground pipeline shuts down gracefully on SIGTERM."""
        input_dir, output_dir = tmp_dirs
        pid_file = output_dir / ".tigerflow" / "run.pid"

        proc = subprocess.Popen(
            [
                UV_PATH,
                "run",
                "tigerflow",
                "run",
                str(minimal_config),
                str(input_dir),
                str(output_dir),
                "--idle-timeout",
                "60",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            wait_for_pid_file(pid_file)
            proc.terminate()
            returncode = proc.wait(timeout=10)
            assert returncode in (0, -signal.SIGTERM, 128 + signal.SIGTERM)
        except subprocess.TimeoutExpired:
            proc.kill()
            pytest.fail("Process did not terminate after SIGTERM")

    def test_foreground_cleans_up_pid_file(self, minimal_config, tmp_dirs):
        """Test that PID file is removed after graceful shutdown."""
        input_dir, output_dir = tmp_dirs
        pid_file = output_dir / ".tigerflow" / "run.pid"

        proc = subprocess.Popen(
            [
                UV_PATH,
                "run",
                "tigerflow",
                "run",
                str(minimal_config),
                str(input_dir),
                str(output_dir),
                "--idle-timeout",
                "5",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        try:
            pid = wait_for_pid_file(pid_file)
            os.kill(pid, signal.SIGTERM)
            wait_for_process_exit(pid)
        except TimeoutError as e:
            proc.kill()
            pytest.fail(str(e))

        proc.wait(timeout=5)
        assert not pid_file.exists(), "PID file should be removed after exit"


class TestBackgroundRun:
    """Test background pipeline execution (--background flag)."""

    def test_background_starts_and_creates_pid_file(self, minimal_config, tmp_dirs):
        """Test that --background creates PID file and returns quickly."""
        input_dir, output_dir = tmp_dirs
        pid_file = output_dir / ".tigerflow" / "run.pid"

        result = subprocess.run(
            [
                UV_PATH,
                "run",
                "tigerflow",
                "run",
                str(minimal_config),
                str(input_dir),
                str(output_dir),
                "--background",
                "--idle-timeout",
                "1",
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert "Started" in result.stdout
        assert "pid" in result.stdout

        time.sleep(0.5)
        assert pid_file.exists(), "PID file should be created"

        pid = int(pid_file.read_text().strip())
        assert pid > 0

        kill_pipeline(pid_file)

    def test_background_writes_logs(self, background_pipeline):
        """Test that background pipeline writes to log file."""
        output_dir = background_pipeline
        log_file = output_dir / ".tigerflow" / "run.log"
        assert log_file.exists(), "Log file should be created"

    def test_status_command_shows_running(self, background_pipeline):
        """Test that status command shows pipeline as running."""
        output_dir = background_pipeline

        result = subprocess.run(
            [UV_PATH, "run", "tigerflow", "status", str(output_dir)],
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0
        assert "running" in result.stdout.lower() or "pid" in result.stdout.lower()

    def test_stop_command_terminates_pipeline(self, background_pipeline):
        """Test that stop command terminates running pipeline."""
        output_dir = background_pipeline
        pid_file = output_dir / ".tigerflow" / "run.pid"

        assert pid_file.exists(), "PID file should exist"
        pid = int(pid_file.read_text().strip())

        result = subprocess.run(
            [UV_PATH, "run", "tigerflow", "stop", str(output_dir)],
            capture_output=True,
            text=True,
            timeout=30,
        )

        assert result.returncode == 0
        assert "SIGTERM" in result.stdout or "stopped" in result.stdout.lower()

        try:
            wait_for_process_exit(pid, timeout=5.0)
        except TimeoutError:
            os.kill(pid, signal.SIGKILL)
            pytest.fail("Process should have terminated after stop command")

    def test_status_json_output(self, background_pipeline):
        """Test that status --json outputs valid JSON."""
        import json

        output_dir = background_pipeline

        result = subprocess.run(
            [UV_PATH, "run", "tigerflow", "status", str(output_dir), "--json"],
            capture_output=True,
            text=True,
            timeout=30,
        )

        data = json.loads(result.stdout)
        assert "running" in data
        assert data["running"] is True
        assert "pid" in data
