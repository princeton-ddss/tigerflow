import os
from pathlib import Path

from typer.testing import CliRunner

from tigerflow.cli import app

runner = CliRunner()


class TestRunCommandPidLocking:
    def test_run_blocked_when_already_running(self, tmp_path: Path):
        # Setup directories and a PID file with current process ID
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
tasks:
  - name: echo
    kind: local
    module: tigerflow.library.echo
    input_ext: .txt
    output_ext: .txt
""")
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
