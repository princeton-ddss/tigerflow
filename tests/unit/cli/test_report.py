"""Tests for pipeline report command and PipelineOutput.report()."""

import json
from datetime import datetime, timedelta
from pathlib import Path

from typer.testing import CliRunner

from tigerflow.cli import app
from tigerflow.cli.report import (
    _compute_metrics_summary,
    _make_progress_bar,
    _make_sparkline,
)
from tigerflow.models import FileMetrics, PipelineOutput

runner = CliRunner()


class TestPipelineOutputReport:
    """Test PipelineOutput.report() method."""

    def test_report_empty_pipeline(self, tmp_path: Path):
        """Empty pipeline with no files processed."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        (internal / ".finished").mkdir()

        output = PipelineOutput(tmp_path)
        report = output.report()

        assert report.status == "stopped"
        assert report.processed == 0
        assert report.in_progress == 0
        assert report.failed == 0
        assert report.staged is None  # None when stopped

    def test_report_counts_finished_files(self, tmp_path: Path):
        """Files in .finished/ count as processed."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        finished = internal / ".finished"
        finished.mkdir()

        # Create 5 finished files
        for i in range(5):
            (finished / f"file{i}.txt").touch()

        # Create a task directory with logs
        task_dir = internal / "task1"
        task_dir.mkdir()

        # Write metrics for all 5 files
        log_file = task_dir / "task.log"
        now = datetime.now()
        lines = []
        for i in range(5):
            metrics = {
                "file": f"file{i}.txt",
                "started_at": now.isoformat(),
                "finished_at": (now + timedelta(seconds=1)).isoformat(),
                "status": "success",
            }
            lines.append(f"2026-03-10 12:00:00 | METRICS | {json.dumps(metrics)}")
        log_file.write_text("\n".join(lines))

        output = PipelineOutput(tmp_path)
        report = output.report()

        assert report.processed == 5
        assert report.in_progress == 0
        assert report.failed == 0

    def test_report_counts_failed_files(self, tmp_path: Path):
        """Files with .err count as failed."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        symlinks = internal / ".symlinks"
        symlinks.mkdir()
        (internal / ".finished").mkdir()

        # Create symlinks for 10 files
        for i in range(10):
            (symlinks / f"file{i}.txt").symlink_to(tmp_path / f"file{i}.txt")

        # Create task directory with some errors
        task_dir = internal / "task1"
        task_dir.mkdir()

        # Write metrics - 7 success, 3 errors
        log_file = task_dir / "task.log"
        now = datetime.now()
        lines = []
        for i in range(10):
            status = "success" if i < 7 else "error"
            metrics = {
                "file": f"file{i}.txt",
                "started_at": now.isoformat(),
                "finished_at": (now + timedelta(seconds=1)).isoformat(),
                "status": status,
            }
            lines.append(f"2026-03-10 12:00:00 | METRICS | {json.dumps(metrics)}")
        log_file.write_text("\n".join(lines))

        # Create .err files for failed ones
        for i in range(7, 10):
            (task_dir / f"file{i}.err").write_text("Error message")

        output = PipelineOutput(tmp_path)
        report = output.report()

        assert report.failed == 3
        assert len(report.errors.get("task1", [])) == 3

    def test_report_staged_vs_in_progress(self, tmp_path: Path):
        """Test filesystem-based counts: staged vs in_progress vs processed."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        symlinks = internal / ".symlinks"
        symlinks.mkdir()
        finished = internal / ".finished"
        finished.mkdir()

        # Create symlinks for 10 files
        for i in range(10):
            (symlinks / f"file{i}.txt").symlink_to(tmp_path / f"file{i}.txt")

        # Create task directory with output files
        task_dir = internal / "task1"
        task_dir.mkdir()

        # 4 files have task output (in_progress)
        for i in range(4):
            (task_dir / f"file{i}.out").touch()

        # 6 files have no task output (staged) - files 4-9

        output = PipelineOutput(tmp_path)
        report = output.report()

        # Directory-level counts from filesystem:
        # processed = 0 (nothing in .finished/)
        # in_progress = 4 (symlinks with task output)
        # staged = 6 (symlinks without task output)
        # failed = 0
        assert report.processed == 0
        assert report.in_progress == 4
        assert report.failed == 0
        # staged is None when not running
        assert report.staged is None


class TestMultiTaskPipeline:
    """Test report for multi-task pipelines."""

    def test_two_task_pipeline_counts(self, tmp_path: Path):
        """Test directory-level counts for a 2-task pipeline."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        symlinks = internal / ".symlinks"
        symlinks.mkdir()
        finished = internal / ".finished"
        finished.mkdir()

        # 100 files total
        for i in range(100):
            (symlinks / f"file{i}.txt").symlink_to(tmp_path / f"file{i}.txt")

        # Task1 directory
        task1_dir = internal / "task1"
        task1_dir.mkdir()

        # Task2 directory
        task2_dir = internal / "task2"
        task2_dir.mkdir()

        # Create task1 output files for files 0-99 (all processed by task1)
        for i in range(100):
            (task1_dir / f"file{i}.out1").touch()

        # Create .err files for failed (files 60-79)
        for i in range(60, 80):
            (task2_dir / f"file{i}.err").write_text("Error")

        # Mark 60 as finished (completed both tasks), remove symlinks
        for i in range(60):
            (finished / f"file{i}.txt").touch()
            (symlinks / f"file{i}.txt").unlink()

        output = PipelineOutput(tmp_path)
        report = output.report()

        # Directory-level counts from filesystem:
        # processed = 60 (in .finished/)
        # failed = 20 (unique .err stems)
        # in_progress = 20 (symlinks 80-99 have task1 output, excluded failed)
        # staged = 0 (all remaining symlinks have task output)
        assert report.processed == 60
        assert report.failed == 20
        assert report.in_progress == 20


class TestTaskMeta:
    """Test task metadata parsing."""

    def test_get_task_meta_from_latest_init(self, tmp_path: Path):
        """Task metadata comes from the most recent INIT log."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        (internal / ".finished").mkdir()

        (internal / "run.log").write_text(
            '2026-03-10 12:00:00 | INIT    | {"tasks": [{"name": "task1", "depends_on": null}]}'
        )

        output = PipelineOutput(tmp_path)
        tasks = output._get_task_meta()

        assert len(tasks) == 1
        assert tasks[0].name == "task1"
        assert tasks[0].depends_on is None


class TestAllRunsMetrics:
    """Test metrics parsing across all runs."""

    def test_parse_all_metrics(self, tmp_path: Path):
        """METRICS are collected from all task log files."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        (internal / ".finished").mkdir()

        task_dir = internal / "task1"
        task_dir.mkdir()

        now = datetime.now()

        # All metrics appended to single task.log
        all_lines = []
        for i in range(5):
            all_lines.append(
                f"2026-03-10 10:00:01 | METRICS | {json.dumps({'file': f'file{i}.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': 'success'})}"
            )
        (task_dir / "task.log").write_text("\n".join(all_lines))

        output = PipelineOutput(tmp_path)
        metrics = output._parse_all_metrics()

        assert len(metrics) == 5
        files = {m.file for m in metrics}
        assert files == {"file0.txt", "file1.txt", "file2.txt", "file3.txt", "file4.txt"}


class TestTaskProgress:
    """Test per-task progress from metrics and filesystem."""

    def test_task_progress_with_symlinks(self, tmp_path: Path):
        """Task progress uses filesystem for root available, metrics for counts."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        symlinks = internal / ".symlinks"
        symlinks.mkdir()
        (internal / ".finished").mkdir()

        # 10 symlinks (2 still staged, 8 have task output or errored)
        for i in range(10):
            (symlinks / f"file{i}.txt").symlink_to(tmp_path / f"file{i}.txt")

        # Pipeline log with INIT
        (internal / "run.log").write_text(
            '2026-03-10 12:00:00 | INIT    | {"tasks": [{"name": "task1", "depends_on": null}]}'
        )

        # Task directory with output files for 6 and errors for 2
        task_dir = internal / "task1"
        task_dir.mkdir()

        for i in range(6):
            (task_dir / f"file{i}.out").touch()
        for i in range(6, 8):
            (task_dir / f"file{i}.err").write_text("Error")

        now = datetime.now()
        task_lines = []
        for i in range(8):
            status = "success" if i < 6 else "error"
            task_lines.append(
                f"2026-03-10 12:00:01 | METRICS | {json.dumps({'file': f'file{i}.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': status})}"
            )
        (task_dir / "task.log").write_text("\n".join(task_lines))

        output = PipelineOutput(tmp_path)
        report = output.report()

        assert len(report.tasks) == 1
        task = report.tasks[0]
        assert task.name == "task1"
        assert task.processed == 6
        assert task.failed == 2
        # Root available = in_progress (6) + staged (2) + errored (2) + finished (0) = 10
        # staged = 10 - 6 - 2 = 2
        assert task.staged == 2

    def test_multi_task_dependency_chain(self, tmp_path: Path):
        """Task2 available = task1 succeeded files."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        symlinks = internal / ".symlinks"
        symlinks.mkdir()
        (internal / ".finished").mkdir()

        # 10 symlinks still active
        for i in range(10):
            (symlinks / f"file{i}.txt").symlink_to(tmp_path / f"file{i}.txt")

        # Pipeline log with INIT
        (internal / "run.log").write_text(
            '2026-03-10 12:00:00 | INIT    | {"tasks": [{"name": "task1", "depends_on": null}, {"name": "task2", "depends_on": "task1"}]}'
        )

        # Task1: all 10 processed, 8 success, 2 error
        task1_dir = internal / "task1"
        task1_dir.mkdir()

        for i in range(8):
            (task1_dir / f"file{i}.out1").touch()
        for i in range(8, 10):
            (task1_dir / f"file{i}.err").write_text("Error")

        now = datetime.now()
        lines1 = []
        for i in range(10):
            status = "success" if i < 8 else "error"
            lines1.append(
                f"2026-03-10 12:00:01 | METRICS | {json.dumps({'file': f'file{i}.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': status})}"
            )
        (task1_dir / "task.log").write_text("\n".join(lines1))

        # Task2: 5 of 8 available processed
        task2_dir = internal / "task2"
        task2_dir.mkdir()

        lines2 = []
        for i in range(5):
            lines2.append(
                f"2026-03-10 12:00:02 | METRICS | {json.dumps({'file': f'file{i}.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': 'success'})}"
            )
        (task2_dir / "task.log").write_text("\n".join(lines2))

        output = PipelineOutput(tmp_path)
        report = output.report()

        assert len(report.tasks) == 2
        task1 = report.tasks[0]
        task2 = report.tasks[1]

        # Task1: root available = in_progress (8) + staged (0) + errored (2) + finished (0) = 10
        assert task1.processed == 8
        assert task1.failed == 2
        assert task1.staged == 0  # 10 - 8 - 2

        # Task2: available = task1 successes = 8, 5 processed
        assert task2.processed == 5
        assert task2.failed == 0
        assert task2.staged == 3


class TestMultipleRuns:
    """Test behavior with multiple pipeline runs."""

    def test_directory_counts_accumulate_across_runs(self, tmp_path: Path):
        """Directory-level processed count includes files from all runs."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        finished = internal / ".finished"
        finished.mkdir()

        # INIT entries in run.log (both runs)
        (internal / "run.log").write_text(
            '2026-03-10 10:00:00 | INIT | {"tasks": [{"name": "task1", "depends_on": null}]}\n'
            '2026-03-10 12:00:00 | INIT | {"tasks": [{"name": "task1", "depends_on": null}]}'
        )

        # Run 1: 5 files finished
        for i in range(5):
            (finished / f"run1_file{i}.txt").touch()

        # Run 2: 3 more files finished
        for i in range(3):
            (finished / f"run2_file{i}.txt").touch()

        output = PipelineOutput(tmp_path)
        report = output.report()

        # Directory-level: 8 total processed (5 + 3)
        assert report.processed == 8

    def test_task_progress_aggregates_across_runs(self, tmp_path: Path):
        """Task progress counts metrics from all runs."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        finished = internal / ".finished"
        finished.mkdir()

        # INIT entries in run.log
        (internal / "run.log").write_text(
            '2026-03-10 10:00:00 | INIT    | {"tasks": [{"name": "task1", "depends_on": null}]}\n'
            '2026-03-10 12:00:00 | INIT    | {"tasks": [{"name": "task1", "depends_on": null}]}'
        )

        task_dir = internal / "task1"
        task_dir.mkdir()

        now = datetime.now()

        # All metrics appended to single task.log
        all_task_lines = []

        # Run 1: 5 files processed
        for i in range(5):
            all_task_lines.append(
                f"2026-03-10 10:00:01 | METRICS | {json.dumps({'file': f'run1_file{i}.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': 'success'})}"
            )
            (finished / f"run1_file{i}.txt").touch()

        # Run 2: 7 files processed
        for i in range(7):
            all_task_lines.append(
                f"2026-03-10 12:00:01 | METRICS | {json.dumps({'file': f'run2_file{i}.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': 'success'})}"
            )
            (finished / f"run2_file{i}.txt").touch()

        (task_dir / "task.log").write_text("\n".join(all_task_lines))

        output = PipelineOutput(tmp_path)
        report = output.report()

        # Task progress aggregates across both runs
        assert len(report.tasks) == 1
        assert report.tasks[0].processed == 12  # 5 + 7


class TestSlurmLogParsing:
    """Test parsing METRICS from Slurm worker logs."""

    def test_parse_metrics_from_multiple_worker_logs(self, tmp_path: Path):
        """METRICS are collected from multiple Slurm worker .log files."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        (internal / ".finished").mkdir()

        # Task directory with multiple Slurm worker logs
        task_dir = internal / "task1"
        task_dir.mkdir()

        now = datetime.now()

        # Worker 1 processed file1
        worker1_log = task_dir / "task-12345.log"
        worker1_log.write_text(
            f"2026-03-10 12:00:01 | METRICS | {json.dumps({'file': 'file1.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': 'success'})}\n"
        )

        # Worker 2 processed file2 and file3
        worker2_log = task_dir / "task-12346.log"
        worker2_log.write_text(
            f"2026-03-10 12:00:02 | METRICS | {json.dumps({'file': 'file2.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': 'success'})}\n"
            f"2026-03-10 12:00:03 | METRICS | {json.dumps({'file': 'file3.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': 'error'})}\n"
        )

        output = PipelineOutput(tmp_path)
        metrics = output._parse_all_metrics()

        # Should find all 3 METRICS entries across both worker logs
        assert len(metrics) == 3
        files = {m.file for m in metrics}
        assert files == {"file1.txt", "file2.txt", "file3.txt"}

        # Check status counts
        success_count = sum(1 for m in metrics if m.status == "success")
        error_count = sum(1 for m in metrics if m.status == "error")
        assert success_count == 2
        assert error_count == 1


class TestProgressBar:
    """Test progress bar rendering."""

    def test_progress_bar_all_processed(self):
        bar = _make_progress_bar(processed=100, ongoing=0, failed=0, total=100)
        assert "━" in bar  # Has filled segments
        assert "─" not in bar  # No empty segments

    def test_progress_bar_partial(self):
        bar = _make_progress_bar(processed=50, ongoing=0, failed=0, total=100)
        assert "━" in bar
        assert "─" in bar

    def test_progress_bar_with_failures(self):
        bar = _make_progress_bar(processed=70, ongoing=0, failed=30, total=100)
        assert "sea_green3" in bar  # Green for processed
        assert "bright_red" in bar  # Red for failed

    def test_progress_bar_fills_when_complete(self):
        """Rounding errors should not leave empty space when done."""
        bar = _make_progress_bar(processed=81, ongoing=0, failed=19, total=100)
        # Should not have empty segments when all files accounted for
        assert "─" not in bar


class TestSparkline:
    """Test sparkline rendering."""

    def test_sparkline_empty(self):
        result = _make_sparkline([])
        assert len(result) == 20  # Padded to default width
        assert result.strip() == ""

    def test_sparkline_single_value(self):
        result = _make_sparkline([100.0])
        assert len(result) == 20  # Padded to default width
        assert result.strip() != ""  # Has one block character

    def test_sparkline_variation(self):
        values = [100, 200, 300, 400, 500]
        result = _make_sparkline(values)
        assert len(result) == 20  # Padded to default width
        # Last chars should show increasing pattern
        blocks = result.strip()
        assert blocks[0] != blocks[-1]

    def test_sparkline_width(self):
        values = [float(x) for x in range(50)]
        result = _make_sparkline(values, width=20)
        assert len(result) == 20


class TestMetricsSummary:
    """Test metrics summary computation."""

    def test_compute_metrics_summary_empty(self):
        result = _compute_metrics_summary({})
        assert result == {}

    def test_compute_metrics_summary(self):
        now = datetime.now()
        metrics = {
            "task1": [
                FileMetrics(
                    file="f1.txt",
                    task="task1",
                    started_at=now,
                    finished_at=now + timedelta(milliseconds=100),
                    status="success",
                ),
                FileMetrics(
                    file="f2.txt",
                    task="task1",
                    started_at=now,
                    finished_at=now + timedelta(milliseconds=200),
                    status="success",
                ),
            ]
        }
        result = _compute_metrics_summary(metrics)
        assert result["total"] == 2
        assert result["success"] == 2
        assert result["failed"] == 0
        assert result["min_duration_ms"] == 100
        assert result["max_duration_ms"] == 200
        assert result["avg_duration_ms"] == 150


class TestReportCommand:
    """Test CLI report command."""

    def test_report_nonexistent_directory(self, tmp_path: Path):
        nonexistent = tmp_path / "nonexistent"
        result = runner.invoke(app, ["report", str(nonexistent)])
        assert result.exit_code == 1
        assert "does not exist" in result.output

    def test_report_not_pipeline_directory(self, tmp_path: Path):
        result = runner.invoke(app, ["report", str(tmp_path)])
        assert result.exit_code == 1
        assert "Not a valid pipeline" in result.output

    def test_report_json_output(self, tmp_path: Path):
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        (internal / ".finished").mkdir()

        result = runner.invoke(app, ["report", str(tmp_path), "--json"])
        assert result.exit_code == 0

        data = json.loads(result.output)
        assert "status" in data
        assert "progress" in data
        assert data["status"]["running"] is False

    def test_report_json_include_filter(self, tmp_path: Path):
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        (internal / ".finished").mkdir()

        result = runner.invoke(
            app, ["report", str(tmp_path), "--json", "--include", "status"]
        )
        assert result.exit_code == 0

        data = json.loads(result.output)
        assert "status" in data
        assert "progress" not in data
        assert "metrics" not in data
        assert "errors" not in data

    def test_report_watch_json_incompatible(self, tmp_path: Path):
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        (internal / ".finished").mkdir()

        result = runner.invoke(app, ["report", str(tmp_path), "--json", "--watch"])
        assert result.exit_code == 1
        assert "--watch cannot be used with --json" in result.output
