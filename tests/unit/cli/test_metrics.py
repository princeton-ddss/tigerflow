import json
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
    return tmp_path


@pytest.fixture
def pipeline_with_metrics(pipeline_dir: Path):
    """Create a pipeline directory with sample metrics in logs."""
    task_dir = pipeline_dir / ".tigerflow" / "test_task"
    task_dir.mkdir()
    logs_dir = task_dir / "logs"
    logs_dir.mkdir()

    # Write sample log with metrics
    log_content = """2024-01-15 10:30:00 | INFO     | Setting up task
2024-01-15 10:30:01 | INFO     | Starting processing: file1.txt
2024-01-15 10:30:02 | INFO     | Successfully processed: file1.txt
2024-01-15 10:30:02 | METRICS  | {"file": "file1.txt", "started_at": "2024-01-15T10:30:01+00:00", "finished_at": "2024-01-15T10:30:02+00:00", "status": "success"}
2024-01-15 10:30:03 | INFO     | Starting processing: file2.txt
2024-01-15 10:30:04 | ERROR    | Failed processing: file2.txt
2024-01-15 10:30:04 | METRICS  | {"file": "file2.txt", "started_at": "2024-01-15T10:30:03+00:00", "finished_at": "2024-01-15T10:30:04+00:00", "status": "error"}
"""
    (logs_dir / "20240115-103000.log").write_text(log_content)
    return pipeline_dir


class TestMetricsCommand:
    def test_metrics_nonexistent_directory(self, tmp_path: Path):
        nonexistent = tmp_path / "nonexistent"
        result = runner.invoke(app, ["metrics", str(nonexistent)])
        assert result.exit_code == 1
        assert "does not exist" in result.stdout

    def test_metrics_not_pipeline_directory(self, tmp_path: Path):
        result = runner.invoke(app, ["metrics", str(tmp_path)])
        assert result.exit_code == 1
        assert "Not a valid pipeline directory" in result.stdout

    def test_metrics_no_metrics_found(self, pipeline_dir: Path):
        result = runner.invoke(app, ["metrics", str(pipeline_dir)])
        assert result.exit_code == 0
        assert "No metrics found" in result.stdout

    def test_metrics_displays_summary(self, pipeline_with_metrics: Path):
        result = runner.invoke(app, ["metrics", str(pipeline_with_metrics)])
        assert result.exit_code == 0
        assert "Metrics Summary" in result.stdout
        assert "Total files: 2" in result.stdout
        assert "1 success" in result.stdout
        assert "1 failed" in result.stdout

    def test_metrics_displays_table(self, pipeline_with_metrics: Path):
        result = runner.invoke(app, ["metrics", str(pipeline_with_metrics)])
        assert result.exit_code == 0
        assert "file1.txt" in result.stdout
        assert "file2.txt" in result.stdout
        assert "success" in result.stdout
        assert "error" in result.stdout

    def test_metrics_json_output(self, pipeline_with_metrics: Path):
        result = runner.invoke(app, ["metrics", str(pipeline_with_metrics), "--json"])
        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert len(data) == 2
        assert data[0]["file"] == "file1.txt"
        assert data[0]["status"] == "success"
        assert "started_at" in data[0]
        assert "finished_at" in data[0]

    def test_metrics_filter_by_task(self, pipeline_with_metrics: Path):
        result = runner.invoke(
            app, ["metrics", str(pipeline_with_metrics), "--task", "nonexistent"]
        )
        assert result.exit_code == 0
        assert "No metrics found" in result.stdout

    def test_metrics_filter_by_existing_task(self, pipeline_with_metrics: Path):
        result = runner.invoke(
            app, ["metrics", str(pipeline_with_metrics), "--task", "test_task"]
        )
        assert result.exit_code == 0
        assert "file1.txt" in result.stdout
