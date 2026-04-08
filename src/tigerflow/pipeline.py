import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from types import FrameType

import yaml

from tigerflow.logconfig import logger
from tigerflow.models import (
    LocalAsyncTaskConfig,
    LocalTaskConfig,
    PipelineConfig,
    SlurmTaskConfig,
    TaskStatus,
    TaskStatusKind,
)
from tigerflow.settings import settings
from tigerflow.staging import StagingContext
from tigerflow.tasks.utils import get_slurm_task_status
from tigerflow.utils import TEMP_FILE_PREFIX, submit_to_slurm, validate_task_cli


class Pipeline:
    @logger.catch(reraise=True)
    def __init__(
        self,
        *,
        config_file: Path,
        input_dir: Path,
        output_dir: Path,
        idle_timeout: int = 10,  # In minutes
        delete_input: bool = False,
        pid_file: Path | None = None,
    ):
        for path in (config_file, input_dir, output_dir):
            if not path.exists():
                raise FileNotFoundError(path)

        self._pid_file = pid_file

        self._input_dir = input_dir.resolve()
        self._output_dir = output_dir.resolve()
        self._internal_dir = self._output_dir / ".tigerflow"
        self._symlinks_dir = self._internal_dir / ".symlinks"
        self._finished_dir = self._internal_dir / ".finished"

        for path in (self._symlinks_dir, self._finished_dir):
            path.mkdir(parents=True, exist_ok=True)

        if idle_timeout < 1:
            raise ValueError("'idle_timeout' must be greater than zero")

        self._idle_timeout = timedelta(minutes=idle_timeout)
        self._last_active = datetime.now()

        self._delete_input = delete_input

        self._config = PipelineConfig.model_validate(
            yaml.safe_load(config_file.read_text())
        )

        for task in self._config.tasks:
            validate_task_cli(task.module)

        # Map task I/O directories from the dependency graph
        for task in self._config.tasks:
            task.input_dir = (
                self._internal_dir / task.depends_on
                if task.depends_on
                else self._symlinks_dir
            )
            task.output_dir = self._internal_dir / task.name

        # Create task directories
        for task in self._config.tasks:
            task.output_dir.mkdir(parents=True, exist_ok=True)
            if task.keep_output:
                self._output_dir.joinpath(task.name).mkdir(exist_ok=True)

        # Collect file IDs that need cleanup
        finished_ids = set()
        for file in self._finished_dir.iterdir():
            file_id = file.name.removesuffix(self._config.root_input_ext)
            finished_ids.add(file_id)
        cleanup_ids = set(finished_ids)
        for file in self._symlinks_dir.iterdir():
            if not file.is_symlink():
                file.unlink()
            else:
                file_id = file.name.removesuffix(self._config.root_input_ext)
                if not file.exists() or file_id in finished_ids:
                    file.unlink()
                    cleanup_ids.add(file_id)

        # Remove orphaned task outputs and input files
        for file_id in cleanup_ids:
            for task in self._config.tasks:
                task_file = task.output_dir / f"{file_id}{task.output_ext}"
                task_file.unlink(missing_ok=True)
            if self._delete_input:
                filename = f"{file_id}{self._config.root_input_ext}"
                self._input_dir.joinpath(filename).unlink(missing_ok=True)

        # Clean up any invalid or unsuccessful task outputs
        for task in self._config.tasks:
            for file in task.output_dir.iterdir():
                if file.is_file() and (
                    not file.name.endswith(task.output_ext)
                    or file.name.startswith(TEMP_FILE_PREFIX)
                ):
                    file.unlink()

        # Initialize a set to track files being processed or already processed
        self._filenames = {
            file.name
            for dir in (self._symlinks_dir, self._finished_dir)
            for file in dir.iterdir()
            if file.is_file()
        }

        # Initialize mapping to track failed files per task
        self._task_error_filenames: dict[str, set[str]] = {
            task.name: set() for task in self._config.tasks
        }

        # Initialize mapping to track processed files per task.
        # Start empty so that existing outputs from a prior run are
        # detected as "new" and can trigger pipeline completion.
        self._task_processed_filenames: dict[str, set[str]] = {
            task.name: set() for task in self._config.tasks
        }

        # Initialize mapping from task name to status
        self._task_status: dict[str, TaskStatus] = {
            task.name: TaskStatus(kind=TaskStatusKind.INACTIVE)
            for task in self._config.tasks
        }

        # Initialize mapping from task name to local subprocess
        self._subprocesses: dict[str, subprocess.Popen] = dict()

        # Initialize mapping from task name to Slurm job ID
        self._slurm_task_ids: dict[str, int] = dict()

        # Initialize an event to manage graceful shutdown
        self._shutdown_event = threading.Event()

        # Store the received signal number for proper exit code
        self._received_signal: int | None = None

    def _signal_handler(self, signum: int, frame: FrameType | None):
        logger.warning("Received signal {}, initiating shutdown", signum)
        self._received_signal = signum
        self._shutdown_event.set()

    @logger.catch(reraise=True)
    def run(self):
        # Register signal handlers for graceful shutdown
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, self._signal_handler)

        # Write PID file after signal handlers are set up, so that if SIGTERM
        # arrives after the PID file exists, the cleanup in the finally block runs.
        if self._pid_file is not None:
            self._pid_file.write_text(str(os.getpid()))

        try:
            logger.info("Starting pipeline execution")
            self._start_tasks()
            logger.info("All tasks started, beginning pipeline tracking loop")
            while not self._shutdown_event.is_set():
                self._check_task_status()
                self._handle_task_timeout()
                self._stage_new_files()
                self._report_failed_files()
                self._handle_processed_files()
                self._check_inactivity()
                self._shutdown_event.wait(timeout=settings.pipeline_poll_interval)
        finally:
            self._handle_processed_files()
            logger.info("Shutting down pipeline")
            for name, process in self._subprocesses.items():
                if self._task_status[name].is_alive:
                    logger.info("[{}] Terminating...", name)
                    process.terminate()
            for task in self._config.tasks:
                if not isinstance(task, SlurmTaskConfig):
                    continue
                logger.info("[{}] Terminating...", task.name)
                subprocess.run(["scancel", "-n", task.worker_job_name])
                subprocess.run(["scancel", "-n", task.client_job_name])
            while any(status.is_alive for status in self._task_status.values()):
                self._check_task_status()
                time.sleep(1)
            logger.info("Pipeline shutdown complete")
            if self._pid_file is not None:
                self._pid_file.unlink(missing_ok=True)
            if self._received_signal is not None:
                sys.exit(128 + self._received_signal)

    def _start_tasks(self):
        # Add file sink so all log output is written to run.log
        log_file = self._internal_dir / "run.log"
        logger.add(
            log_file,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {message}",
            level="INFO",
        )

        tasks_meta = [
            {"name": t.name, "depends_on": t.depends_on} for t in self._config.tasks
        ]
        logger.log("INIT", json.dumps({"tasks": tasks_meta}))
        for task in self._config.tasks:
            logger.info("[{}] Starting as a {} task", task.name, task.kind.upper())
            task.runner_pid = os.getpid()
            task.log_dir.mkdir(parents=True, exist_ok=True)  # PID-scoped log dir
            script = task.to_script()
            if isinstance(task, (LocalTaskConfig, LocalAsyncTaskConfig)):
                process = subprocess.Popen(["bash", "-c", script])
                self._subprocesses[task.name] = process
                logger.info("[{}] Started with PID {}", task.name, process.pid)
            elif isinstance(task, SlurmTaskConfig):
                job_id = submit_to_slurm(script)
                self._slurm_task_ids[task.name] = job_id
                logger.info("[{}] Submitted with Slurm job ID {}", task.name, job_id)
            else:
                raise ValueError(f"Unsupported task kind: {type(task)}")

    def _build_staging_context(self) -> StagingContext:
        """Build the current context for staging middleware."""
        n_finished = sum(1 for f in self._finished_dir.iterdir() if f.is_file())
        n_failed = sum(len(e) for e in self._task_error_filenames.values())
        n_staged = sum(1 for f in self._symlinks_dir.iterdir() if f.is_file())
        n_waiting = sum(
            1
            for f in self._input_dir.iterdir()
            if f.is_file()
            and f.name.endswith(self._config.root_input_ext)
            and f.name not in self._filenames
        )
        return StagingContext(
            waiting=n_waiting,
            staged=n_staged - n_failed,
            completed=n_finished,
            failed=n_failed,
            input_dir=self._input_dir,
            output_dir=self._output_dir,
        )

    def _stage_new_files(self):
        context = self._build_staging_context()
        candidates = [
            f
            for f in self._input_dir.iterdir()
            if f.is_file()
            and f.name.endswith(self._config.root_input_ext)
            and f.name not in self._filenames
        ]
        to_stage = self._config.staging.process(candidates, context)
        for file in to_stage:
            self._symlinks_dir.joinpath(file.name).symlink_to(file)
            self._filenames.add(file.name)

    def _check_task_status(self):
        for task in self._config.tasks:
            if isinstance(task, (LocalTaskConfig, LocalAsyncTaskConfig)):
                process = self._subprocesses[task.name]
                status = self._get_subprocess_status(process)
            elif isinstance(task, SlurmTaskConfig):
                job_id = self._slurm_task_ids[task.name]
                status = get_slurm_task_status(job_id, task.worker_job_name)
            else:
                raise ValueError(f"Unsupported task kind: {type(task)}")

            if self._task_status[task.name] != status:
                old_status = self._task_status[task.name]
                self._task_status[task.name] = status
                log_func = logger.info if status.is_alive else logger.error
                log_func(
                    "[{}] Status changed: {}{} -> {}{}",
                    task.name,
                    old_status.kind.name,
                    f" ({old_status.detail})" if old_status.detail else "",
                    status.kind.name,
                    f" ({status.detail})" if status.detail else "",
                )

    def _handle_task_timeout(self):
        for task in self._config.tasks:
            if isinstance(task, SlurmTaskConfig):
                task_status = self._task_status[task.name]
                if (
                    not task_status.is_alive
                    and task_status.detail
                    and "TIMEOUT" in task_status.detail
                ):
                    script = task.to_script()
                    job_id = submit_to_slurm(script)
                    self._slurm_task_ids[task.name] = job_id
                    logger.info(
                        "[{}] Re-submitted with Slurm job ID {}", task.name, job_id
                    )

    def _report_failed_files(self):
        for task in self._config.tasks:
            n_files = 0
            for file in task.output_dir.iterdir():
                if (
                    file.is_file()
                    and file.name.endswith(".err")
                    and not file.name.startswith(TEMP_FILE_PREFIX)
                    and file.name not in self._task_error_filenames[task.name]
                ):
                    self._task_error_filenames[task.name].add(file.name)
                    n_files += 1
            if n_files > 0:
                logger.error("[{}] {} failed files", task.name, n_files)

    def _handle_processed_files(self):
        # Identify *newly* processed files for each task
        processed_filenames_by_task: dict[str, set[str]] = {
            task.name: set() for task in self._config.tasks
        }
        for task in self._config.tasks:
            for file in task.output_dir.iterdir():
                if (
                    file.is_file()
                    and file.name.endswith(task.output_ext)
                    and not file.name.startswith(TEMP_FILE_PREFIX)
                    and file.name not in self._task_processed_filenames[task.name]
                ):
                    self._task_processed_filenames[task.name].add(file.name)
                    processed_filenames_by_task[task.name].add(file.name)
                    if task.keep_output:
                        new_file = self._output_dir / task.name / file.name
                        shutil.copy(file, new_file)

        # Identify files that have completed all pipeline tasks
        completed_file_ids: set[str] = set.intersection(
            *(
                {
                    filename.removesuffix(task.output_ext)
                    for filename in processed_filenames_by_task[task.name]
                }
                for task in self._config.terminal_tasks
            )
        )

        # Record completion and clean up staged/input files
        for file_id in completed_file_ids:
            filename = f"{file_id}{self._config.root_input_ext}"
            self._finished_dir.joinpath(filename).touch()
            self._symlinks_dir.joinpath(filename).unlink(missing_ok=True)
            if self._delete_input:
                self._input_dir.joinpath(filename).unlink(missing_ok=True)

        # Clean up intermediate data for completed files
        for task in self._config.tasks:
            for file_id in completed_file_ids:
                file = task.output_dir / f"{file_id}{task.output_ext}"
                file.unlink(missing_ok=True)

        # Log progress
        if completed_file_ids:
            logger.info("Completed processing {} files", len(completed_file_ids))
            n_finished = sum(1 for f in self._finished_dir.iterdir() if f.is_file())
            n_failed = sum(len(errs) for errs in self._task_error_filenames.values())
            if (n_finished + n_failed) >= len(self._filenames):
                logger.info("No more files to process, starting idle time count")

    def _check_inactivity(self):
        n_finished = sum(1 for file in self._finished_dir.iterdir() if file.is_file())
        n_failed = sum(len(errs) for errs in self._task_error_filenames.values())
        if (n_finished + n_failed) < len(self._filenames):  # Still in progress
            self._last_active = datetime.now()

        inactivity = datetime.now() - self._last_active
        if inactivity > self._idle_timeout:
            logger.warning("Idle timeout reached, initiating shutdown")
            self._received_signal = signal.SIGTERM
            self._shutdown_event.set()

    @staticmethod
    def _get_subprocess_status(process: subprocess.Popen) -> TaskStatus:
        exit_code = process.poll()
        if exit_code is None:
            return TaskStatus(kind=TaskStatusKind.ACTIVE)
        else:
            return TaskStatus(
                kind=TaskStatusKind.INACTIVE,
                detail=f"Exit Code: {exit_code}",
            )
