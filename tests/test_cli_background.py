import os
import signal
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from tigerflow.cli import app
from tigerflow.utils import (
    check_and_cleanup_stale_pid,
    is_process_running,
    read_pid_file,
)

runner = CliRunner()


class TestPidUtilityFunctions:
    def test_read_pid_file_not_exists(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        assert read_pid_file(pid_file) is None

    def test_read_pid_file_valid(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        pid_file.write_text("12345")
        assert read_pid_file(pid_file) == 12345

    def test_read_pid_file_with_whitespace(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        pid_file.write_text("  12345\n")
        assert read_pid_file(pid_file) == 12345

    def test_read_pid_file_invalid(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        pid_file.write_text("not-a-number")
        assert read_pid_file(pid_file) is None

    def test_is_process_running_current_process(self):
        assert is_process_running(os.getpid()) is True

    def test_is_process_running_nonexistent(self):
        # Use a very high PID that's unlikely to exist
        assert is_process_running(999999999) is False

    def test_check_and_cleanup_stale_pid_no_file(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        assert check_and_cleanup_stale_pid(pid_file) is False

    def test_check_and_cleanup_stale_pid_running_process(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        pid_file.write_text(str(os.getpid()))
        assert check_and_cleanup_stale_pid(pid_file) is True
        assert pid_file.exists()  # File should not be removed

    def test_check_and_cleanup_stale_pid_dead_process(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        pid_file.write_text("999999999")  # Non-existent process
        assert check_and_cleanup_stale_pid(pid_file) is False
        assert not pid_file.exists()  # Stale file should be removed


class TestRunCommandPidLocking:
    def test_run_blocked_when_already_running(self, tmp_path: Path):
        # Setup directories and a PID file with current process ID
        config_file = tmp_path / "config.yaml"
        config_file.write_text("tasks: []")
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()
        internal_dir = output_dir / ".tigerflow"
        internal_dir.mkdir()
        pid_file = internal_dir / "run.pid"
        pid_file.write_text(str(os.getpid()))

        result = runner.invoke(
            app,
            ["run", str(config_file), str(input_dir), str(output_dir)],
        )

        assert result.exit_code == 1
        assert "already running" in result.output


class TestStatusCommand:
    def test_status_nonexistent_directory(self, tmp_path: Path):
        nonexistent = tmp_path / "nonexistent"
        result = runner.invoke(app, ["status", str(nonexistent)])
        assert result.exit_code == 1
        assert "does not exist" in result.stdout

    def test_status_not_pipeline_directory(self, tmp_path: Path):
        # Directory exists but no .tigerflow
        result = runner.invoke(app, ["status", str(tmp_path)])
        assert result.exit_code == 1
        assert "Not a valid pipeline directory" in result.stdout

    def test_status_no_pid_file(self, tmp_path: Path):
        # Setup minimal .tigerflow directory
        internal_dir = tmp_path / ".tigerflow"
        internal_dir.mkdir()
        (internal_dir / ".symlinks").mkdir()
        (internal_dir / ".finished").mkdir()

        result = runner.invoke(app, ["status", str(tmp_path)])
        # Should succeed but report not running
        assert "not running" in result.stdout.lower()

    def test_status_json_output(self, tmp_path: Path):
        # Setup minimal .tigerflow directory
        internal_dir = tmp_path / ".tigerflow"
        internal_dir.mkdir()
        (internal_dir / ".symlinks").mkdir()
        (internal_dir / ".finished").mkdir()

        result = runner.invoke(app, ["status", str(tmp_path), "--json"])
        assert '"running": false' in result.stdout or '"running":false' in result.stdout

    def test_status_exit_code_not_running(self, tmp_path: Path):
        internal_dir = tmp_path / ".tigerflow"
        internal_dir.mkdir()
        (internal_dir / ".symlinks").mkdir()
        (internal_dir / ".finished").mkdir()

        result = runner.invoke(app, ["status", str(tmp_path)])
        assert result.exit_code == 1  # Not running


class TestStopCommand:
    def test_stop_nonexistent_directory(self, tmp_path: Path):
        nonexistent = tmp_path / "nonexistent"
        result = runner.invoke(app, ["stop", str(nonexistent)])
        assert result.exit_code == 1
        assert "does not exist" in result.output

    def test_stop_not_pipeline_directory(self, tmp_path: Path):
        result = runner.invoke(app, ["stop", str(tmp_path)])
        assert result.exit_code == 1
        assert "Not a valid pipeline directory" in result.output

    def test_stop_no_pid_file(self, tmp_path: Path):
        internal_dir = tmp_path / ".tigerflow"
        internal_dir.mkdir()

        result = runner.invoke(app, ["stop", str(tmp_path)])
        assert result.exit_code == 0
        assert "not running" in result.stdout.lower()

    def test_stop_stale_pid_file(self, tmp_path: Path):
        internal_dir = tmp_path / ".tigerflow"
        internal_dir.mkdir()
        pid_file = internal_dir / "run.pid"
        pid_file.write_text("999999999")  # Non-existent process

        result = runner.invoke(app, ["stop", str(tmp_path)])
        assert result.exit_code == 0
        assert "stale" in result.stdout.lower()
        assert not pid_file.exists()  # Should be cleaned up

    @patch("tigerflow.cli.stop.os.kill")
    def test_stop_sends_sigterm(self, mock_kill, tmp_path: Path):
        internal_dir = tmp_path / ".tigerflow"
        internal_dir.mkdir()
        pid_file = internal_dir / "run.pid"
        pid_file.write_text(str(os.getpid()))

        result = runner.invoke(app, ["stop", str(tmp_path)])

        mock_kill.assert_called_with(os.getpid(), signal.SIGTERM)
        assert "SIGTERM" in result.stdout

    @patch("tigerflow.cli.stop.os.kill")
    def test_stop_force_sends_sigkill(self, mock_kill, tmp_path: Path):
        internal_dir = tmp_path / ".tigerflow"
        internal_dir.mkdir()
        pid_file = internal_dir / "run.pid"
        pid_file.write_text(str(os.getpid()))

        result = runner.invoke(app, ["stop", str(tmp_path), "--force"])

        mock_kill.assert_called_with(os.getpid(), signal.SIGKILL)
        assert "SIGKILL" in result.stdout
