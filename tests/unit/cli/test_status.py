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
        assert "does not exist" in result.output

    def test_status_not_pipeline_directory(self, tmp_path: Path):
        result = runner.invoke(app, ["status", str(tmp_path)])
        assert result.exit_code == 1
        assert "Not a valid pipeline directory" in result.output

    def test_status_no_pid_file(self, pipeline_dir):
        result = runner.invoke(app, ["status", str(pipeline_dir)])
        assert "stopped" in result.stdout.lower()

    def test_status_json_output(self, pipeline_dir):
        import json

        result = runner.invoke(app, ["status", str(pipeline_dir), "--json"])
        data = json.loads(result.stdout)
        assert data["running"] is False
        assert data["finished"] == 0
        assert data["staged"] == 0
        assert data["failed"] == 0

    def test_status_exit_code_not_running(self, pipeline_dir):
        result = runner.invoke(app, ["status", str(pipeline_dir)])
        assert result.exit_code == 1


class TestStatusCounting:
    @pytest.fixture
    def pipeline_dir(self, tmp_path: Path):
        """Create a pipeline directory structure."""
        internal_dir = tmp_path / ".tigerflow"
        internal_dir.mkdir()
        (internal_dir / ".symlinks").mkdir()
        (internal_dir / ".finished").mkdir()
        (internal_dir / "task1").mkdir()
        return tmp_path

    def test_counts_finished_files(self, pipeline_dir):
        import json

        finished_dir = pipeline_dir / ".tigerflow" / ".finished"
        (finished_dir / "file1.txt").touch()
        (finished_dir / "file2.txt").touch()

        result = runner.invoke(app, ["status", str(pipeline_dir), "--json"])
        data = json.loads(result.stdout)
        assert data["finished"] == 2
        assert data["staged"] == 0
        assert data["failed"] == 0

    def test_counts_staged_files(self, pipeline_dir):
        import json

        symlinks_dir = pipeline_dir / ".tigerflow" / ".symlinks"
        (symlinks_dir / "file1.txt").touch()
        (symlinks_dir / "file2.txt").touch()
        (symlinks_dir / "file3.txt").touch()

        result = runner.invoke(app, ["status", str(pipeline_dir), "--json"])
        data = json.loads(result.stdout)
        assert data["finished"] == 0
        assert data["staged"] == 3
        assert data["failed"] == 0

    def test_counts_failed_files(self, pipeline_dir):
        import json

        task_dir = pipeline_dir / ".tigerflow" / "task1"
        (task_dir / "file1.err").touch()
        (task_dir / "file2.err").touch()

        # Failed files remain in symlinks
        symlinks_dir = pipeline_dir / ".tigerflow" / ".symlinks"
        (symlinks_dir / "file1.txt").touch()
        (symlinks_dir / "file2.txt").touch()

        result = runner.invoke(app, ["status", str(pipeline_dir), "--json"])
        data = json.loads(result.stdout)
        assert data["finished"] == 0
        assert data["staged"] == 0  # symlinks (2) - failed (2)
        assert data["failed"] == 2

    def test_counts_mixed_progress(self, pipeline_dir):
        import json

        symlinks_dir = pipeline_dir / ".tigerflow" / ".symlinks"
        finished_dir = pipeline_dir / ".tigerflow" / ".finished"
        task_dir = pipeline_dir / ".tigerflow" / "task1"

        # 5 finished
        for i in range(5):
            (finished_dir / f"done{i}.txt").touch()

        # 3 staged (not failed)
        for i in range(3):
            (symlinks_dir / f"processing{i}.txt").touch()

        # 2 failed (in symlinks + .err files)
        (symlinks_dir / "failed1.txt").touch()
        (symlinks_dir / "failed2.txt").touch()
        (task_dir / "failed1.err").touch()
        (task_dir / "failed2.err").touch()

        result = runner.invoke(app, ["status", str(pipeline_dir), "--json"])
        data = json.loads(result.stdout)
        assert data["finished"] == 5
        assert data["staged"] == 3  # symlinks (5) - failed (2)
        assert data["failed"] == 2
