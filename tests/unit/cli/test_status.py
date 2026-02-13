from pathlib import Path

import pytest
from typer.testing import CliRunner

from tigerflow.cli import app

runner = CliRunner()


@pytest.fixture
def pipeline_dir(tmp_path: Path):
    """Create a minimal pipeline directory structure."""
    internal_dir = tmp_path / ".tigerflow"
    internal_dir.mkdir()
    (internal_dir / ".symlinks").mkdir()
    (internal_dir / ".finished").mkdir()
    return tmp_path


class TestStatusCommand:
    def test_status_nonexistent_directory(self, tmp_path: Path):
        nonexistent = tmp_path / "nonexistent"
        result = runner.invoke(app, ["status", str(nonexistent)])
        assert result.exit_code == 1
        assert "does not exist" in result.stdout

    def test_status_not_pipeline_directory(self, tmp_path: Path):
        result = runner.invoke(app, ["status", str(tmp_path)])
        assert result.exit_code == 1
        assert "Not a valid pipeline directory" in result.stdout

    def test_status_no_pid_file(self, pipeline_dir):
        result = runner.invoke(app, ["status", str(pipeline_dir)])
        assert "not running" in result.stdout.lower()

    def test_status_json_output(self, pipeline_dir):
        result = runner.invoke(app, ["status", str(pipeline_dir), "--json"])
        assert '"running": false' in result.stdout or '"running":false' in result.stdout

    def test_status_exit_code_not_running(self, pipeline_dir):
        result = runner.invoke(app, ["status", str(pipeline_dir)])
        assert result.exit_code == 1
