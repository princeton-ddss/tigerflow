import os
import signal
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from tigerflow.cli import app

runner = CliRunner()


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
