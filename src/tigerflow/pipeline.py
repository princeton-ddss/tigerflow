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
    PipelineProgress,
    SlurmTaskConfig,
    TaskProgress,
    TaskStatus,
    TaskStatusKind,
)
from tigerflow.tasks.utils import get_slurm_task_status
from tigerflow.utils import is_valid_cli, submit_to_slurm


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
    ):
        for path in (config_file, input_dir, output_dir):
            if not path.exists():
                raise FileNotFoundError(path)

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
            if not is_valid_cli(task.module):
                raise ValueError(f"Invalid CLI: {task.module}")

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
            for path in (task.output_dir, task.log_dir):
                path.mkdir(parents=True, exist_ok=True)
            if task.keep_output:
                self._output_dir.joinpath(task.name).mkdir(exist_ok=True)

        # If opted in, delete input files that have been marked as finished
        if self._delete_input:
            for file in self._finished_dir.iterdir():
                source_file = self._input_dir / file.name
                source_file.unlink(missing_ok=True)

        # Clean up any invalid or broken symlinks
        for file in self._symlinks_dir.iterdir():
            if not file.is_symlink():
                file.unlink()
            elif not file.exists():
                file.unlink()
                # Remove all downstream task outputs since source data is missing
                file_id = file.name.removesuffix(self._config.root_task.input_ext)
                for task in self._config.tasks:
                    file = task.output_dir / f"{file_id}{task.output_ext}"
                    file.unlink(missing_ok=True)

        # Clean up any invalid or unsuccessful task outputs
        for task in self._config.tasks:
            for file in task.output_dir.iterdir():
                if file.is_file() and not file.name.endswith(task.output_ext):
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

        # Initialize mapping to track processed files per task
        self._task_processed_filenames: dict[str, set[str]] = {}
        for task in self._config.tasks:
            processed_filenames: set[str] = set()
            for file in task.output_dir.iterdir():
                if file.is_file() and file.name.endswith(task.output_ext):
                    processed_filenames.add(file.name)
            self._task_processed_filenames[task.name] = processed_filenames

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
                self._shutdown_event.wait(timeout=10)  # Interruptible sleep
        finally:
            self._handle_processed_files()
            logger.info("Shutting down pipeline")
            for name, process in self._subprocesses.items():
                if self._task_status[name].is_alive:
                    logger.info("[{}] Terminating...", name)
                    process.terminate()
            for name, job_id in self._slurm_task_ids.items():
                if self._task_status[name].is_alive:
                    logger.info("[{}] Terminating...", name)
                    subprocess.run(["scancel", str(job_id)])
            while any(status.is_alive for status in self._task_status.values()):
                self._check_task_status()
                time.sleep(1)
            logger.info("Pipeline shutdown complete")
            if self._received_signal is not None:
                sys.exit(128 + self._received_signal)

    def _start_tasks(self):
        for task in self._config.tasks:
            logger.info("[{}] Starting as a {} task", task.name, task.kind.upper())
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

    def _stage_new_files(self):
        n_files = 0
        for file in self._input_dir.iterdir():
            if (
                file.is_file()
                and file.name.endswith(self._config.root_task.input_ext)
                and file.name not in self._filenames
            ):
                self._symlinks_dir.joinpath(file.name).symlink_to(file)
                self._filenames.add(file.name)
                n_files += 1
        if n_files > 0:
            logger.info("Staged {} new files for processing", n_files)

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
                if not task_status.is_alive and "TIMEOUT" in task_status.detail:
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

        # Clean up intermediate data for completed files
        for task in self._config.tasks:
            for file_id in completed_file_ids:
                file = task.output_dir / f"{file_id}{task.output_ext}"
                file.unlink()

        # Record completion
        for file_id in completed_file_ids:
            filename = f"{file_id}{self._config.root_task.input_ext}"
            self._symlinks_dir.joinpath(filename).unlink()
            if self._delete_input:
                self._input_dir.joinpath(filename).unlink(missing_ok=True)
            self._finished_dir.joinpath(filename).touch()
        if completed_file_ids:
            logger.info("Completed processing {} files", len(completed_file_ids))

    def _check_inactivity(self):
        n_finished = sum(1 for file in self._finished_dir.iterdir() if file.is_file())
        n_failed = sum(len(errors) for errors in self._task_error_filenames.values())
        n_total = len(self._filenames)
        if (n_finished + n_failed) < n_total:  # Still in progress
            self._last_active = datetime.now()

        inactivity = datetime.now() - self._last_active
        if inactivity > self._idle_timeout:
            logger.warning("Idle timeout reached, initiating shutdown")
            self._received_signal = signal.SIGTERM
            self._shutdown_event.set()

    @staticmethod
    def report_progress(output_dir: Path) -> PipelineProgress:
        """
        Report progress across pipeline tasks.
        """
        internal_dir = output_dir / ".tigerflow"
        for path in (output_dir, internal_dir):
            if not path.exists():
                raise FileNotFoundError(path)

        symlinks_dir = internal_dir / ".symlinks"
        finished_dir = internal_dir / ".finished"

        pipeline = PipelineProgress()
        pipeline.staged = [f for f in symlinks_dir.iterdir() if f.is_file()]
        pipeline.finished = [f for f in finished_dir.iterdir() if f.is_file()]
        for folder in internal_dir.iterdir():  # TODO: Iterate tasks topologically
            if folder.is_dir() and not folder.name.startswith("."):  # Task directory
                task = TaskProgress(name=folder.name)
                for file in folder.iterdir():
                    if file.is_file():
                        if file.suffix == "":
                            task.ongoing.append(file)
                        elif file.name.endswith(".err"):
                            task.failed.append(file)
                        else:
                            task.processed.append(file)
                pipeline.tasks.append(task)

        return pipeline

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
