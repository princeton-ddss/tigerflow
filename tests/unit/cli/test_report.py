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
        logs_dir = task_dir / "logs"
        logs_dir.mkdir()

        # Write metrics for all 5 files
        log_file = logs_dir / "20260310-120000.log"
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
        logs_dir = task_dir / "logs"
        logs_dir.mkdir()

        # Write metrics - 7 success, 3 errors
        log_file = logs_dir / "20260310-120000.log"
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
        logs_dir = task_dir / "logs"
        logs_dir.mkdir()

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
        (task1_dir / "logs").mkdir()

        # Task2 directory
        task2_dir = internal / "task2"
        task2_dir.mkdir()
        (task2_dir / "logs").mkdir()

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


class TestRunLevelProgress:
    """Test run-level task progress from logs."""

    def test_parse_run_log_with_init_staged_metrics(self, tmp_path: Path):
        """Parse INIT/STAGED from pipeline log, METRICS from task log."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        (internal / ".finished").mkdir()

        # Pipeline log with INIT and STAGED
        pipeline_logs = internal / "logs"
        pipeline_logs.mkdir()
        pipeline_lines = [
            '2026-03-10 12:00:00 | INIT    | {"tasks": [{"name": "task1", "depends_on": null}]}',
            '2026-03-10 12:00:00 | STAGED  | {"file": "file1.txt"}',
            '2026-03-10 12:00:00 | STAGED  | {"file": "file2.txt"}',
            '2026-03-10 12:00:00 | STAGED  | {"file": "file3.txt"}',
        ]
        (pipeline_logs / "20260310-120000.log").write_text("\n".join(pipeline_lines))

        # Task log with METRICS
        task_dir = internal / "task1"
        task_dir.mkdir()
        task_logs = task_dir / "logs"
        task_logs.mkdir()

        now = datetime.now()
        task_lines = [
            f"2026-03-10 12:00:01 | METRICS | {json.dumps({'file': 'file1.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': 'success'})}",
        ]
        (task_logs / "20260310-120000.log").write_text("\n".join(task_lines))

        output = PipelineOutput(tmp_path)
        run_log = output._parse_run_log("20260310-120000")

        assert len(run_log.tasks) == 1
        assert run_log.tasks[0].name == "task1"
        assert run_log.tasks[0].depends_on is None
        assert run_log.staged == {"file1.txt", "file2.txt", "file3.txt"}
        assert len(run_log.metrics) == 1
        assert run_log.metrics[0].file == "file1.txt"

    def test_task_progress_from_run_log(self, tmp_path: Path):
        """Task progress computed from INIT, STAGED, and METRICS."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        (internal / ".finished").mkdir()

        # Pipeline log with INIT and STAGED
        pipeline_logs = internal / "logs"
        pipeline_logs.mkdir()
        pipeline_lines = [
            '2026-03-10 12:00:00 | INIT    | {"tasks": [{"name": "task1", "depends_on": null}]}',
        ]
        for i in range(10):
            pipeline_lines.append(
                f"2026-03-10 12:00:00 | STAGED  | {json.dumps({'file': f'file{i}.txt'})}"
            )
        (pipeline_logs / "20260310-120000.log").write_text("\n".join(pipeline_lines))

        # Task log with METRICS
        task_dir = internal / "task1"
        task_dir.mkdir()
        task_logs = task_dir / "logs"
        task_logs.mkdir()

        now = datetime.now()
        task_lines = []
        for i in range(8):
            status = "success" if i < 6 else "error"
            task_lines.append(
                f"2026-03-10 12:00:01 | METRICS | {json.dumps({'file': f'file{i}.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': status})}"
            )
        (task_logs / "20260310-120000.log").write_text("\n".join(task_lines))

        output = PipelineOutput(tmp_path)
        report = output.report()

        # Run-level task progress
        assert len(report.tasks) == 1
        task = report.tasks[0]
        assert task.name == "task1"
        assert task.processed == 6
        assert task.failed == 2
        assert task.staged == 2  # 10 - 6 - 2

    def test_multi_task_dependency_chain(self, tmp_path: Path):
        """Task2 available = task1 succeeded files."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        (internal / ".finished").mkdir()

        # Pipeline log with INIT and STAGED
        pipeline_logs = internal / "logs"
        pipeline_logs.mkdir()
        pipeline_lines = [
            '2026-03-10 12:00:00 | INIT    | {"tasks": [{"name": "task1", "depends_on": null}, {"name": "task2", "depends_on": "task1"}]}',
        ]
        for i in range(10):
            pipeline_lines.append(
                f"2026-03-10 12:00:00 | STAGED  | {json.dumps({'file': f'file{i}.txt'})}"
            )
        (pipeline_logs / "20260310-120000.log").write_text("\n".join(pipeline_lines))

        # Task1 logs with METRICS
        task1_dir = internal / "task1"
        task1_dir.mkdir()
        logs1 = task1_dir / "logs"
        logs1.mkdir()

        now = datetime.now()
        lines1 = []
        for i in range(10):
            status = "success" if i < 8 else "error"
            lines1.append(
                f"2026-03-10 12:00:01 | METRICS | {json.dumps({'file': f'file{i}.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': status})}"
            )
        (logs1 / "20260310-120000.log").write_text("\n".join(lines1))

        # Task2 logs with METRICS
        task2_dir = internal / "task2"
        task2_dir.mkdir()
        logs2 = task2_dir / "logs"
        logs2.mkdir()

        lines2 = []
        for i in range(5):
            lines2.append(
                f"2026-03-10 12:00:02 | METRICS | {json.dumps({'file': f'file{i}.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': 'success'})}"
            )
        (logs2 / "20260310-120000.log").write_text("\n".join(lines2))

        output = PipelineOutput(tmp_path)
        report = output.report()

        assert len(report.tasks) == 2
        task1 = report.tasks[0]
        task2 = report.tasks[1]

        # Task1: 10 available, 8 processed, 2 failed, 0 staged
        assert task1.processed == 8
        assert task1.failed == 2
        assert task1.staged == 0

        # Task2: 8 available (task1 successes), 5 processed, 0 failed, 3 staged
        assert task2.processed == 5
        assert task2.failed == 0
        assert task2.staged == 3


class TestMultipleRuns:
    """Test behavior with multiple pipeline runs."""

    def test_get_run_id_returns_most_recent(self, tmp_path: Path):
        """_get_run_id returns the most recent run."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        (internal / ".finished").mkdir()

        pipeline_logs = internal / "logs"
        pipeline_logs.mkdir()

        # Create logs for 3 runs
        (pipeline_logs / "20260310-100000.log").write_text(
            '2026-03-10 10:00:00 | INIT | {"tasks": []}'
        )
        (pipeline_logs / "20260310-120000.log").write_text(
            '2026-03-10 12:00:00 | INIT | {"tasks": []}'
        )
        (pipeline_logs / "20260310-110000.log").write_text(
            '2026-03-10 11:00:00 | INIT | {"tasks": []}'
        )

        output = PipelineOutput(tmp_path)
        assert output._get_run_id() == "20260310-120000"

    def test_run_id_filter_scopes_to_specific_run(self, tmp_path: Path):
        """run_id_filter returns data for a specific historical run."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        (internal / ".finished").mkdir()

        pipeline_logs = internal / "logs"
        pipeline_logs.mkdir()

        # Run 1: staged 5 files
        run1_lines = [
            '2026-03-10 10:00:00 | INIT    | {"tasks": [{"name": "task1", "depends_on": null}]}',
        ]
        for i in range(5):
            run1_lines.append(
                f"2026-03-10 10:00:00 | STAGED  | {json.dumps({'file': f'file{i}.txt'})}"
            )
        (pipeline_logs / "20260310-100000.log").write_text("\n".join(run1_lines))

        # Run 2: staged 10 files
        run2_lines = [
            '2026-03-10 12:00:00 | INIT    | {"tasks": [{"name": "task1", "depends_on": null}]}',
        ]
        for i in range(10):
            run2_lines.append(
                f"2026-03-10 12:00:00 | STAGED  | {json.dumps({'file': f'file{i}.txt'})}"
            )
        (pipeline_logs / "20260310-120000.log").write_text("\n".join(run2_lines))

        output = PipelineOutput(tmp_path)

        # Default (latest run) should have 10 staged
        run_log_latest = output._parse_run_log("20260310-120000")
        assert len(run_log_latest.staged) == 10

        # Filtered to run 1 should have 5 staged
        run_log_run1 = output._parse_run_log("20260310-100000")
        assert len(run_log_run1.staged) == 5

    def test_directory_counts_accumulate_across_runs(self, tmp_path: Path):
        """Directory-level processed count includes files from all runs."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        finished = internal / ".finished"
        finished.mkdir()

        pipeline_logs = internal / "logs"
        pipeline_logs.mkdir()

        # Run 1: 5 files finished
        (pipeline_logs / "20260310-100000.log").write_text(
            '2026-03-10 10:00:00 | INIT | {"tasks": [{"name": "task1", "depends_on": null}]}'
        )
        for i in range(5):
            (finished / f"run1_file{i}.txt").touch()

        # Run 2: 3 more files finished
        (pipeline_logs / "20260310-120000.log").write_text(
            '2026-03-10 12:00:00 | INIT | {"tasks": [{"name": "task1", "depends_on": null}]}'
        )
        for i in range(3):
            (finished / f"run2_file{i}.txt").touch()

        output = PipelineOutput(tmp_path)
        report = output.report()

        # Directory-level: 8 total processed (5 + 3)
        assert report.processed == 8

    def test_run_level_counts_scoped_to_run(self, tmp_path: Path):
        """Run-level task progress only counts files from that run."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        (internal / ".finished").mkdir()

        pipeline_logs = internal / "logs"
        pipeline_logs.mkdir()

        task_dir = internal / "task1"
        task_dir.mkdir()
        task_logs = task_dir / "logs"
        task_logs.mkdir()

        now = datetime.now()

        # Run 1: 5 files staged, 5 processed
        run1_pipeline = [
            '2026-03-10 10:00:00 | INIT    | {"tasks": [{"name": "task1", "depends_on": null}]}',
        ]
        for i in range(5):
            run1_pipeline.append(
                f"2026-03-10 10:00:00 | STAGED  | {json.dumps({'file': f'run1_file{i}.txt'})}"
            )
        (pipeline_logs / "20260310-100000.log").write_text("\n".join(run1_pipeline))

        run1_task = []
        for i in range(5):
            run1_task.append(
                f"2026-03-10 10:00:01 | METRICS | {json.dumps({'file': f'run1_file{i}.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': 'success'})}"
            )
        (task_logs / "20260310-100000.log").write_text("\n".join(run1_task))

        # Run 2: 10 files staged, 7 processed
        run2_pipeline = [
            '2026-03-10 12:00:00 | INIT    | {"tasks": [{"name": "task1", "depends_on": null}]}',
        ]
        for i in range(10):
            run2_pipeline.append(
                f"2026-03-10 12:00:00 | STAGED  | {json.dumps({'file': f'run2_file{i}.txt'})}"
            )
        (pipeline_logs / "20260310-120000.log").write_text("\n".join(run2_pipeline))

        run2_task = []
        for i in range(7):
            run2_task.append(
                f"2026-03-10 12:00:01 | METRICS | {json.dumps({'file': f'run2_file{i}.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': 'success'})}"
            )
        (task_logs / "20260310-120000.log").write_text("\n".join(run2_task))

        output = PipelineOutput(tmp_path)

        # Default report (run 2): task should show 7 processed, 3 staged
        report = output.report()
        assert len(report.tasks) == 1
        assert report.tasks[0].processed == 7
        assert report.tasks[0].staged == 3

        # Filtered to run 1: task should show 5 processed, 0 staged
        report_run1 = output.report(run_id_filter="20260310-100000")
        assert len(report_run1.tasks) == 1
        assert report_run1.tasks[0].processed == 5
        assert report_run1.tasks[0].staged == 0


class TestSlurmLogParsing:
    """Test parsing METRICS from Slurm worker logs."""

    def test_parse_metrics_from_multiple_worker_logs(self, tmp_path: Path):
        """METRICS are collected from multiple Slurm worker .log files."""
        internal = tmp_path / ".tigerflow"
        internal.mkdir()
        (internal / ".symlinks").mkdir()
        (internal / ".finished").mkdir()

        # Pipeline log
        pipeline_logs = internal / "logs"
        pipeline_logs.mkdir()
        (pipeline_logs / "20260310-120000.log").write_text(
            '2026-03-10 12:00:00 | INIT    | {"tasks": [{"name": "task1", "depends_on": null}]}\n'
            '2026-03-10 12:00:00 | STAGED  | {"file": "file1.txt"}\n'
            '2026-03-10 12:00:00 | STAGED  | {"file": "file2.txt"}\n'
            '2026-03-10 12:00:00 | STAGED  | {"file": "file3.txt"}\n'
        )

        # Task logs directory with multiple Slurm worker logs
        task_dir = internal / "task1"
        task_dir.mkdir()
        task_logs = task_dir / "logs"
        task_logs.mkdir()

        now = datetime.now()

        # Worker 1 processed file1
        worker1_log = task_logs / "20260310-120000-task1-worker-12345.log"
        worker1_log.write_text(
            f"2026-03-10 12:00:01 | METRICS | {json.dumps({'file': 'file1.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': 'success'})}\n"
        )

        # Worker 2 processed file2 and file3
        worker2_log = task_logs / "20260310-120000-task1-worker-12346.log"
        worker2_log.write_text(
            f"2026-03-10 12:00:02 | METRICS | {json.dumps({'file': 'file2.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': 'success'})}\n"
            f"2026-03-10 12:00:03 | METRICS | {json.dumps({'file': 'file3.txt', 'started_at': now.isoformat(), 'finished_at': now.isoformat(), 'status': 'error'})}\n"
        )

        output = PipelineOutput(tmp_path)
        run_log = output._parse_run_log("20260310-120000")

        # Should find all 3 METRICS entries across both worker logs
        assert len(run_log.metrics) == 3
        files = {m.file for m in run_log.metrics}
        assert files == {"file1.txt", "file2.txt", "file3.txt"}

        # Check status counts
        success_count = sum(1 for m in run_log.metrics if m.status == "success")
        error_count = sum(1 for m in run_log.metrics if m.status == "error")
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
                    run_id="20260310-120000",
                    started_at=now,
                    finished_at=now + timedelta(milliseconds=100),
                    status="success",
                ),
                FileMetrics(
                    file="f2.txt",
                    task="task1",
                    run_id="20260310-120000",
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
