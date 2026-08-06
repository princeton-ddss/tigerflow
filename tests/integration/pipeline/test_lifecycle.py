"""Tests for the pipeline tracking loop, shutdown, and exit codes.

Tests drive `_run_tracking_cycle()` directly instead of `run()` wherever the
shutdown path is not under test, which keeps them free of sleeps and signal
delivery.

Note when adding tests here: `run()` calls `_handle_processed_files()` once
more in its `finally` block, so final state after `run()` is one completion
pass ahead of the same number of bare cycle calls.
"""

import signal
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from tigerflow.pipeline import Pipeline

from .helpers import (
    FakePopen,
    PipelineFactory,
    start_fake_tasks,
    task_spec,
    write_output,
)


class TestTrackingCycle:
    """Emergent behavior across repeated tracking cycles."""

    def test_stages_then_completes_across_cycles(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """A staged file is completed once its task produces output."""
        pipeline = pipeline_factory()
        start_fake_tasks(pipeline)

        (input_dir / "a.txt").write_text("payload")

        pipeline._run_tracking_cycle()
        assert (pipeline._symlinks_dir / "a.txt").is_symlink()
        assert not (pipeline._finished_dir / "a.txt").exists()

        write_output(pipeline, "a")

        pipeline._run_tracking_cycle()
        assert (pipeline._finished_dir / "a.txt").exists()
        assert not (pipeline._symlinks_dir / "a.txt").exists()

    def test_file_staged_only_once(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Repeated cycles must not restage a file already being processed."""
        pipeline = pipeline_factory()
        start_fake_tasks(pipeline)

        (input_dir / "a.txt").write_text("payload")

        for _ in range(3):
            pipeline._run_tracking_cycle()

        staged = list(pipeline._symlinks_dir.iterdir())
        assert len(staged) == 1

    def test_finished_file_not_reprocessed(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """A completed file must not be restaged when it stays in input_dir.

        `delete_input` defaults to False, so the input file remains on disk
        after completion and would otherwise look like a fresh candidate.
        """
        pipeline = pipeline_factory()
        start_fake_tasks(pipeline)

        (input_dir / "a.txt").write_text("payload")
        pipeline._run_tracking_cycle()
        write_output(pipeline, "a")

        pipeline._run_tracking_cycle()
        assert (pipeline._finished_dir / "a.txt").exists()

        pipeline._run_tracking_cycle()
        assert not (pipeline._symlinks_dir / "a.txt").exists(), (
            "Finished file should not be restaged"
        )


class TestKeepOutput:
    """Outputs of tasks configured with keep_output land in the output dir."""

    def test_kept_output_survives_cleanup(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """The copy in output_dir must outlive deletion of the intermediate file."""
        pipeline = pipeline_factory([task_spec(keep_output=True)])
        start_fake_tasks(pipeline)

        (input_dir / "a.txt").write_text("payload")
        pipeline._run_tracking_cycle()
        write_output(pipeline, "a")
        pipeline._run_tracking_cycle()

        task = pipeline._config.tasks[0]
        kept = pipeline._output_dir / task.name / "a.txt"
        assert kept.exists(), "Kept output should be copied to the output directory"
        assert not (task.output_dir / "a.txt").exists(), (
            "Intermediate output should still be cleaned up"
        )


class TestInactivity:
    """Idle timeout drives shutdown once nothing is left to process."""

    def test_idle_timeout_must_be_positive(self, pipeline_factory: PipelineFactory):
        """A non-positive idle timeout is rejected at construction."""
        with pytest.raises(ValueError, match="idle_timeout"):
            pipeline_factory(idle_timeout=0)

    def test_idle_timeout_triggers_shutdown(self, pipeline_factory: PipelineFactory):
        """Exceeding the idle timeout with no pending work sets the shutdown event."""
        pipeline = pipeline_factory(idle_timeout=1)

        pipeline._last_active = datetime.now() - timedelta(minutes=2)
        pipeline._check_inactivity()

        assert pipeline._shutdown_event.is_set()
        assert pipeline._received_signal == signal.SIGTERM

    def test_pending_work_resets_idle_clock(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """A staged but unfinished file counts as activity, deferring shutdown."""
        pipeline = pipeline_factory(idle_timeout=1)
        start_fake_tasks(pipeline)

        (input_dir / "a.txt").write_text("payload")
        pipeline._run_tracking_cycle()  # Stages the file; it has no output yet

        pipeline._last_active = datetime.now() - timedelta(minutes=2)
        pipeline._check_inactivity()

        assert not pipeline._shutdown_event.is_set(), (
            "Pending work should reset the idle clock"
        )

    @pytest.mark.xfail(
        reason="Failure counts are per task while the input count holds each "
        "file once, so a file failing in two fan-out tasks is counted twice "
        "and the idle check believes the run is done",
        strict=True,
    )
    def test_fan_out_failures_do_not_cut_the_run_short(
        self, pipeline_factory: PipelineFactory, input_dir: Path
    ):
        """Failing in two tasks must count as one failed file, not two.

        Two of three files fail, each in both tasks. That produces four `.err`
        files for two bad inputs, so the pipeline counts 4 accounted-for files
        against 3 real ones while the third is still staged and healthy.
        Believing the run is done, `_check_inactivity` skips its reset of the
        idle clock and shutdown proceeds with work still in flight.

        Same unit mismatch as `test_staged_discounts_each_failed_file_once` in
        test_file_staging.py, which explains it in full.
        """
        pipeline = pipeline_factory(
            [task_spec("alpha"), task_spec("beta")], idle_timeout=1
        )
        start_fake_tasks(pipeline)

        for name in ("f0", "f1", "f2"):
            (input_dir / f"{name}.txt").write_text("payload")
        pipeline._run_tracking_cycle()

        alpha, beta = pipeline._config.tasks
        for name in ("f0", "f1"):
            (alpha.output_dir / f"{name}.err").write_text("boom")
            (beta.output_dir / f"{name}.err").write_text("boom")
        pipeline._run_tracking_cycle()

        assert (pipeline._symlinks_dir / "f2.txt").exists(), "f2 should still be staged"

        pipeline._last_active = datetime.now() - timedelta(minutes=2)
        pipeline._check_inactivity()

        assert not pipeline._shutdown_event.is_set(), (
            "A staged file is still in flight, so the run must not be idle"
        )


class TestShutdown:
    """`run()` shutdown path: task termination, PID file, and exit code."""

    def test_exits_with_signal_code(self, pipeline_factory: PipelineFactory):
        """Shutdown from a signal exits with 128 + signum."""
        pipeline = pipeline_factory()
        pipeline._shutdown_event.set()  # Exit the loop on the first check
        pipeline._received_signal = signal.SIGTERM

        with pytest.raises(SystemExit) as exc_info:
            pipeline.run()

        assert exc_info.value.code == 128 + signal.SIGTERM

    def test_clean_shutdown_does_not_exit(self, pipeline_factory: PipelineFactory):
        """Without a signal, `run()` returns normally instead of calling sys.exit."""
        pipeline = pipeline_factory()
        pipeline._shutdown_event.set()

        pipeline.run()

    def test_terminates_running_tasks(
        self,
        pipeline_factory: PipelineFactory,
        fake_popen: list[FakePopen],
        stop_after_one_cycle: Callable[[Pipeline], None],
    ):
        """Live local tasks are terminated during shutdown."""
        pipeline = pipeline_factory()
        stop_after_one_cycle(pipeline)

        pipeline.run()

        assert len(fake_popen) == 1
        assert fake_popen[0].terminate_calls == 1

    @pytest.mark.xfail(
        reason="Tasks started but never polled stay INACTIVE, so the shutdown "
        "guard skips terminate() and leaks the subprocess. The test also pins "
        "that no tracking cycle runs, so both must hold before this marker goes",
        strict=True,
    )
    def test_terminates_tasks_when_shutdown_precedes_first_poll(
        self,
        pipeline_factory: PipelineFactory,
        fake_popen: list[FakePopen],
        monkeypatch: pytest.MonkeyPatch,
    ):
        """Tasks must be terminated even if shutdown arrives before any status poll.

        `run()` checks the shutdown event before its first cycle, so a signal
        delivered between task startup and that check skips the cycle entirely
        and no task is ever polled.

        Two tasks, because the factory default is a single task and asserting
        over one process would not catch a guard that skips the rest.
        """
        pipeline = pipeline_factory([task_spec("first"), task_spec("second")])
        pipeline._shutdown_event.set()  # Signal arrives before the loop runs

        cycles = 0

        def count_cycle():
            nonlocal cycles
            cycles += 1

        monkeypatch.setattr(pipeline, "_run_tracking_cycle", count_cycle)

        pipeline.run()

        assert [process.terminate_calls for process in fake_popen] == [1, 1]
        # If a cycle ran, it would mark tasks alive and terminate them for the
        # wrong reason, so pin that the shutdown path alone did the cleanup.
        assert cycles == 0

    def test_pid_file_removed_on_shutdown(
        self,
        pipeline_factory: PipelineFactory,
        tmp_path: Path,
    ):
        """The PID file is removed when `run()` exits.

        Setting the shutdown event up front skips the tracking loop but not
        the PID write, which happens before the `try`, so there is a real
        file for the `finally` to remove.
        """
        pid_file = tmp_path / "run.pid"
        pipeline = pipeline_factory(pid_file=pid_file)
        pipeline._shutdown_event.set()

        pipeline.run()

        assert not pid_file.exists()

    def test_log_file_created(self, pipeline_factory: PipelineFactory):
        """`run()` adds a file sink so the run is recorded in run.log."""
        pipeline = pipeline_factory()
        pipeline._shutdown_event.set()

        pipeline.run()

        assert (pipeline._internal_dir / "run.log").exists()

    def test_signal_handler_requests_shutdown(self, pipeline_factory: PipelineFactory):
        """The handler records the signal and sets the shutdown event."""
        pipeline = pipeline_factory()

        pipeline._signal_handler(signal.SIGINT, None)

        assert pipeline._shutdown_event.is_set()
        assert pipeline._received_signal == signal.SIGINT
