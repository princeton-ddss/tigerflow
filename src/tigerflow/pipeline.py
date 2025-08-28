import re
import signal
import subprocess
import sys
import threading
from pathlib import Path
from types import FrameType

import yaml

from .config import (
    LocalAsyncTaskConfig,
    LocalTaskConfig,
    PipelineConfig,
    SlurmTaskConfig,
)
from .utils import is_valid_cli


class Pipeline:
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

        # Initialize mapping from task name to local subprocess
        self._subprocesses: dict[str, subprocess.Popen] = dict()

        # Initialize mapping from task name to Slurm job ID
        self._slurm_task_ids: dict[str, int] = dict()

        # Initialize an event to manage graceful shutdown
        self._shutdown_event = threading.Event()

        # Store the received signal number for proper exit code
        self._received_signal: int | None = None

    def _signal_handler(self, signum: int, frame: FrameType | None):
        self._received_signal = signum
        self._shutdown_event.set()

    def run(self):
        # Register signal handlers for graceful shutdown
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, self._signal_handler)

        try:
            self._start_tasks()
            while not self._shutdown_event.is_set():
                self._stage_new_files()
                self._process_completed_files()
                self._shutdown_event.wait(timeout=60)  # Interruptible sleep
        finally:
            for process in self._subprocesses.values():
                process.terminate()
            for job_id in self._slurm_task_ids.values():
                subprocess.run(["scancel", str(job_id)])
            if self._received_signal is not None:
                sys.exit(128 + self._received_signal)

    def _start_tasks(self):
        for task in self._config.tasks:
            script = task.to_script()
            if isinstance(task, (LocalTaskConfig, LocalAsyncTaskConfig)):
                process = subprocess.Popen(["bash", "-c", script])
                self._subprocesses[task.name] = process
            elif isinstance(task, SlurmTaskConfig):
                result = subprocess.run(
                    ["sbatch"], input=script, capture_output=True, text=True
                )
                match = re.search(r"Submitted batch job (\d+)", result.stdout)
                if match:
                    job_id = int(match.group(1))
                    self._slurm_task_ids[task.name] = job_id
                else:
                    raise ValueError("Failed to extract job ID from sbatch output")
            else:
                raise ValueError(f"Unsupported task kind: {type(task)}")

    def _stage_new_files(self):
        for file in self._input_dir.iterdir():
            if (
                file.is_file()
                and file.name.endswith(self._config.root_task.input_ext)
                and file.name not in self._filenames
            ):
                self._symlinks_dir.joinpath(file.name).symlink_to(file)
                self._filenames.add(file.name)

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
                    file.unlink()  # TODO: Log if FileNotFoundError

        # Record completion status
        ext = self._config.root_task.input_ext
        for file_id in completed_file_ids:
            file = self._symlinks_dir / f"{file_id}{ext}"
            file.unlink()  # TODO: Log if FileNotFoundError
            new_file = self._finished_dir / file.name
            new_file.touch()
