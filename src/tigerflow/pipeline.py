import re
import signal
import subprocess
import sys
import threading
from pathlib import Path
from types import FrameType

import yaml

from tigerflow.config import (
    LocalAsyncTaskConfig,
    LocalTaskConfig,
    PipelineConfig,
    SlurmTaskConfig,
)
from tigerflow.logconfig import logger
from tigerflow.utils import is_valid_cli


class Pipeline:
    @logger.catch(reraise=True)
    def __init__(self, config_file: Path, input_dir: Path, output_dir: Path):
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

        # Clean up any broken symlinks
        for file in self._symlinks_dir.iterdir():
            if not file.is_symlink() or not file.exists():
                file.unlink()

        # Clean up any invalid or unsuccessful intermediate data
        for task in self._config.tasks:
            for file in task.output_dir.iterdir():
                if file.is_file() and not file.name.endswith(task.output_ext):
                    file.unlink()

        # Initialize a set to track files being processed or already processed
        self._filenames = {
            file.name
            for dir in (self._symlinks_dir, self._finished_dir)
            for file in dir.iterdir()
        }

        # Initialize mapping from task name to active status
        self._task_is_active: dict[str, bool] = {
            task.name: False for task in self._config.tasks
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
                self._stage_new_files()
                self._report_failed_files()
                self._process_completed_files()
                self._shutdown_event.wait(timeout=60)  # Interruptible sleep
        finally:
            logger.info("Shutting down pipeline")
            for name, process in self._subprocesses.items():
                logger.info("Terminating task: {}", name)
                process.terminate()
            for name, job_id in self._slurm_task_ids.items():
                logger.info("Terminating task: {}", name)
                subprocess.run(["scancel", str(job_id)])
            logger.info("Pipeline shutdown complete")
            if self._received_signal is not None:
                sys.exit(128 + self._received_signal)

    def _start_tasks(self):
        for task in self._config.tasks:
            logger.info("Starting task: {} ({})", task.name, task.kind)
            script = task.to_script()
            if isinstance(task, (LocalTaskConfig, LocalAsyncTaskConfig)):
                process = subprocess.Popen(["bash", "-c", script])
                self._subprocesses[task.name] = process
                logger.info("Local task {} started with PID {}", task.name, process.pid)
            elif isinstance(task, SlurmTaskConfig):
                result = subprocess.run(
                    ["sbatch"], input=script, capture_output=True, text=True
                )
                match = re.search(r"Submitted batch job (\d+)", result.stdout)
                if not match:
                    raise ValueError("Failed to extract job ID from sbatch output")
                job_id = int(match.group(1))
                self._slurm_task_ids[task.name] = job_id
                logger.info("Slurm task {} submitted with job ID {}", task.name, job_id)
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
        for name, process in self._subprocesses.items():
            is_active = False if process.poll() else True
            self._update_task_status(name, is_active)

        for name, job_id in self._slurm_task_ids.items():
            result = subprocess.run(
                ["squeue", "-j", str(job_id), "-h", "-o", "%.10T"],
                capture_output=True,
                text=True,
            )
            is_active = True if result.stdout.strip() else False
            self._update_task_status(name, is_active)

    def _update_task_status(self, name: str, is_active: bool):
        if self._task_is_active[name] != is_active:
            self._task_is_active[name] = is_active
            if is_active:
                logger.info("[{}] Status: Active", name)
            else:
                logger.error("[{}] Status: Terminated", name)

    def _report_failed_files(self):
        for task in self._config.tasks:
            n_error_files = len(list(task.output_dir.glob("*.err")))
            if n_error_files > 0:
                logger.error("[{}] {} failed file(s)", task.name, n_error_files)

    def _process_completed_files(self):
        # Identify files that have completed all pipeline tasks
        completed_file_ids_by_task = (
            {
                file.name.removesuffix(task.output_ext)
                for file in task.output_dir.iterdir()
                if file.is_file() and file.name.endswith(task.output_ext)
            }
            for task in self._config.terminal_tasks
        )
        completed_file_ids: set[str] = set.intersection(*completed_file_ids_by_task)

        # Clean up intermediate data
        for task in self._config.tasks:
            for file_id in completed_file_ids:
                file = task.output_dir / f"{file_id}{task.output_ext}"
                if task.keep_output:
                    file.replace(self._output_dir / task.name / file.name)
                else:
                    file.unlink()

        # Record completion status
        ext = self._config.root_task.input_ext
        for file_id in completed_file_ids:
            file = self._symlinks_dir / f"{file_id}{ext}"
            file.unlink()
            new_file = self._finished_dir / file.name
            new_file.touch()
        if completed_file_ids:
            logger.info("Completed processing {} files", len(completed_file_ids))
