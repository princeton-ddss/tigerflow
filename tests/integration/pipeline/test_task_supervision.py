"""Tests for how Pipeline watches running tasks and reacts to their state.

Local tasks are watched through their subprocess exit code; Slurm tasks
through squeue. Slurm jobs that hit their wall-clock limit are resubmitted so
long pipelines survive time limits.
"""

import subprocess
from unittest.mock import patch

import pytest

from tigerflow.models import SlurmTaskConfig, TaskStatus, TaskStatusKind
from tigerflow.pipeline import Pipeline

from .helpers import FakePopen, PipelineFactory, task_spec

SLURM_TASK = task_spec(
    "gpu",
    kind="slurm",
    max_workers=2,
    worker_resources={"cpus": 1, "memory": "4G", "time": "01:00:00"},
)


def slurm_status(kind: TaskStatusKind, detail: str | None = None) -> TaskStatus:
    return TaskStatus(kind=kind, detail=detail)


class TestSubprocessStatus:
    """Local task health is derived from the subprocess exit code."""

    def test_running_process_is_active(self):
        """A process that has not exited reports ACTIVE."""
        status = Pipeline._get_subprocess_status(FakePopen())

        assert status.kind is TaskStatusKind.ACTIVE
        assert status.is_alive

    def test_exited_process_is_inactive_with_code(
        self, pipeline_factory: PipelineFactory
    ):
        """An exited process reports INACTIVE and surfaces its exit code."""
        pipeline = pipeline_factory()
        process = FakePopen()
        process.exit_code = 3

        status = pipeline._get_subprocess_status(process)

        assert status.kind is TaskStatusKind.INACTIVE
        assert not status.is_alive
        assert "3" in (status.detail or "")

    def test_status_change_is_recorded(self, pipeline_factory: PipelineFactory):
        """`_check_task_status` reflects a task that has died."""
        pipeline = pipeline_factory()
        process = FakePopen()
        pipeline._subprocesses["echo"] = process

        pipeline._check_task_status()
        assert pipeline._task_status["echo"].is_alive

        process.exit_code = 1
        pipeline._check_task_status()

        assert not pipeline._task_status["echo"].is_alive


class TestSlurmTimeout:
    """Slurm tasks that hit their time limit are resubmitted."""

    def test_resubmits_after_timeout(self, pipeline_factory: PipelineFactory):
        """A TIMEOUT status triggers a fresh submission and records the new ID."""
        pipeline = pipeline_factory([SLURM_TASK])
        pipeline._slurm_task_ids["gpu"] = 111
        pipeline._task_status["gpu"] = slurm_status(TaskStatusKind.INACTIVE, "TIMEOUT")

        with patch("tigerflow.pipeline.submit_to_slurm", return_value=222) as resubmit:
            pipeline._handle_task_timeout()

        resubmit.assert_called_once()
        assert pipeline._slurm_task_ids["gpu"] == 222

    def test_no_resubmit_while_running(self, pipeline_factory: PipelineFactory):
        pipeline = pipeline_factory([SLURM_TASK])
        pipeline._slurm_task_ids["gpu"] = 111
        pipeline._task_status["gpu"] = slurm_status(TaskStatusKind.ACTIVE)

        with patch("tigerflow.pipeline.submit_to_slurm") as resubmit:
            pipeline._handle_task_timeout()

        resubmit.assert_not_called()

    # Slurm tasks start out INACTIVE with no detail, and sacct also returns
    # no reason once a job ages out of accounting
    @pytest.mark.parametrize("detail", ["FAILED", None])
    def test_no_resubmit_on_other_failures(
        self, pipeline_factory: PipelineFactory, detail: str | None
    ):
        """A task that died for a non-timeout reason is not resubmitted."""
        pipeline = pipeline_factory([SLURM_TASK])
        pipeline._slurm_task_ids["gpu"] = 111
        pipeline._task_status["gpu"] = slurm_status(TaskStatusKind.INACTIVE, detail)

        with patch("tigerflow.pipeline.submit_to_slurm") as resubmit:
            pipeline._handle_task_timeout()

        resubmit.assert_not_called()
        assert pipeline._slurm_task_ids["gpu"] == 111

    def test_resubmits_once_per_timeout(self, pipeline_factory: PipelineFactory):
        """The replacement job ID is what the next status poll observes.

        `_handle_task_timeout` records the new ID so the following cycle polls
        the replacement; polling the dead job would keep reporting TIMEOUT and
        resubmit on every cycle.
        """
        pipeline = pipeline_factory([SLURM_TASK])
        pipeline._slurm_task_ids["gpu"] = 111

        with (
            patch("tigerflow.pipeline.submit_to_slurm", return_value=222) as resubmit,
            patch(
                "tigerflow.pipeline.get_slurm_task_status",
                side_effect=[
                    slurm_status(TaskStatusKind.INACTIVE, "Reason: TIMEOUT"),
                    slurm_status(TaskStatusKind.PENDING, "Reason: Priority"),
                ],
            ) as poll,
        ):
            pipeline._run_tracking_cycle()
            pipeline._run_tracking_cycle()

        assert resubmit.call_count == 1, "Only the timed-out job should be resubmitted"
        assert poll.call_args_list[1].args[0] == 222, "Second poll must use the new ID"


class TestSlurmShutdown:
    """Slurm jobs are cancelled when the pipeline shuts down."""

    @staticmethod
    def _no_such_job(argv, *args, **kwargs) -> subprocess.CompletedProcess:
        """Stand in for `subprocess.run` with output that reads as a finished job.

        Empty stdout is what `get_slurm_task_status` treats as gone. A bare mock
        returns a mock from `.stdout`, which reads as live instead and leaves
        `_drain_tasks` spinning until its timeout expires.
        """
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    def test_scancel_issued_for_worker_and_client(
        self, pipeline_factory: PipelineFactory
    ):
        """Shutdown cancels both the worker and client jobs by name."""
        pipeline = pipeline_factory([SLURM_TASK])
        pipeline._shutdown_event.set()

        task = pipeline._config.tasks[0]
        assert isinstance(task, SlurmTaskConfig)  # Narrow for the type checker

        with (
            patch("tigerflow.pipeline.submit_to_slurm", return_value=111),
            patch.object(subprocess, "run", side_effect=self._no_such_job) as run,
        ):
            pipeline.run()

        # Ignore kwargs so adding e.g. `check=True` does not fail the test;
        # the argv itself is the contract with the scancel binary
        cancel_argv = [call.args[0] for call in run.call_args_list if call.args]
        assert ["scancel", "-n", task.worker_job_name] in cancel_argv
        assert ["scancel", "-n", task.client_job_name] in cancel_argv

    def test_drain_gives_up_on_job_that_stays_queued(
        self, pipeline_factory: PipelineFactory, monkeypatch: pytest.MonkeyPatch
    ):
        """A job still queued after `scancel` must not hold shutdown open forever.

        The drain re-polls squeue on every pass, so a job that keeps reporting
        PENDING keeps it looping; `scancel` is asynchronous and the job may
        outlive the request. Only the deadline ends that, which makes this the
        Slurm analogue of `test_kills_task_that_ignores_terminate`.

        Zeroing the timeout pins the bound itself: squeue never reports the job
        gone, so a drain that returns can only have hit its deadline.
        """
        monkeypatch.setattr("tigerflow.pipeline.settings.pipeline_shutdown_timeout", 0)
        pipeline = pipeline_factory([SLURM_TASK])
        pipeline._shutdown_event.set()

        def still_queued(argv, *args, **kwargs) -> subprocess.CompletedProcess:
            """Report PENDING for every squeue poll, as if scancel were a no-op."""
            return subprocess.CompletedProcess(argv, 0, stdout=" PENDING", stderr="")

        with (
            patch("tigerflow.pipeline.submit_to_slurm", return_value=111),
            patch.object(subprocess, "run", side_effect=still_queued),
        ):
            pipeline.run()

        assert pipeline._task_status["gpu"].kind is TaskStatusKind.PENDING, (
            "The drain must have exited on its deadline with the job still live, "
            "not because the job was observed gone"
        )
