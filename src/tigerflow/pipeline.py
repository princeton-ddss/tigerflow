import re
import signal
import subprocess
import textwrap
import threading
from pathlib import Path

import yaml

from .config import (
    LocalAsyncTaskConfig,
    LocalTaskConfig,
    PipelineConfig,
    SlurmTaskConfig,
)
from .utils import get_slurm_max_array_size, is_valid_cli


class Pipeline:
    def __init__(self, config_file: Path, input_dir: Path, output_dir: Path):
        for path in (config_file, input_dir, output_dir):
            if not path.exists():
                raise FileNotFoundError(path)

        self._input_dir = input_dir.resolve()
        self._output_dir = output_dir.resolve()
        self._symlinks_dir = self._output_dir / ".symlinks"
        self._finished_dir = self._output_dir / ".finished"

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
                self._output_dir / task.depends_on
                if task.depends_on
                else self._symlinks_dir
            )
            task.output_dir = self._output_dir / task.name

        # Create task directories
        for task in self._config.tasks:
            for path in (task.output_dir, task.log_dir):
                path.mkdir(parents=True, exist_ok=True)

        # Clean up any broken symlinks
        for file in self._symlinks_dir.iterdir():
            if not file.is_symlink() or not file.exists():
                file.unlink()

        # Clean up any invalid or unsuccessful intermediate data
        for task in self._config.tasks:
            for file in task.output_dir.iterdir():
                if not file.name.endswith(task.output_ext):
                    file.unlink()

        # Initialize a set to track files being processed or already processed
        self._filenames = {
            file.name
            for dir in (self._symlinks_dir, self._finished_dir)
            for file in dir.iterdir()
        }

        # Initialize a set to track local task processes
        self._subprocesses: set[subprocess.Popen] = set()

        # Initialize a set to track Slurm task clusters
        self._slurm_task_ids: set[str] = set()

        # Initialize an event to manage graceful shutdown
        self._shutdown_event = threading.Event()

    def run(self):
        # Register signal handlers for graceful shutdown
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, lambda signum, frame: self._shutdown_event.set())

        try:
            self._start_tasks()
            while not self._shutdown_event.is_set():
                self._stage_new_files()
                self._process_completed_files()
                self._shutdown_event.wait(timeout=60)
        finally:
            for process in self._subprocesses:
                process.terminate()
            for job_id in self._slurm_task_ids:
                subprocess.run(["scancel", job_id])

    def _start_tasks(self):
        for task in self._config.tasks:
            if isinstance(task, LocalTaskConfig):
                script = self._compose_local_task_script(task)
                process = subprocess.Popen(["bash", "-c", script])
                self._subprocesses.add(process)
            elif isinstance(task, LocalAsyncTaskConfig):
                script = self._compose_local_async_task_script(task)
                process = subprocess.Popen(["bash", "-c", script])
                self._subprocesses.add(process)
            elif isinstance(task, SlurmTaskConfig):
                script = self._compose_slurm_task_script(task)
                result = subprocess.run(
                    ["sbatch"], input=script, capture_output=True, text=True
                )
                match = re.search(r"Submitted batch job (\d+)", result.stdout)
                if match:
                    job_id = match.group(1).strip()
                    self._slurm_task_ids.add(job_id)
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
            # TODO: Skip if the task is marked for keeping output
            for file_id in completed_file_ids:
                file = task.output_dir / f"{file_id}{task.output_ext}"
                file.unlink()  # TODO: Log if FileNotFoundError

        # Record completion status
        ext = self._config.root_task.input_ext
        for file_id in completed_file_ids:
            file = self._symlinks_dir / f"{file_id}{ext}"
            file.unlink()  # TODO: Log if FileNotFoundError
            new_file = self._finished_dir / file.name
            new_file.touch()

    @staticmethod
    def _compose_local_task_script(task: LocalTaskConfig) -> str:
        setup_command = task.setup_commands if task.setup_commands else ""
        task_command = " ".join(
            [
                "python",
                f"{task.module}",
                f"--input-dir {task.input_dir}",
                f"--input-ext {task.input_ext}",
                f"--output-dir {task.output_dir}",
                f"--output-ext {task.output_ext}",
            ]
        )

        script = textwrap.dedent(f"""\
            #!/bin/bash
            {setup_command}
            {task_command}
        """)

        return script

    @staticmethod
    def _compose_local_async_task_script(task: LocalAsyncTaskConfig) -> str:
        setup_command = task.setup_commands if task.setup_commands else ""
        task_command = " ".join(
            [
                "python",
                f"{task.module}",
                f"--input-dir {task.input_dir}",
                f"--input-ext {task.input_ext}",
                f"--output-dir {task.output_dir}",
                f"--output-ext {task.output_ext}",
                f"--concurrency-limit {task.concurrency_limit}",
            ]
        )

        script = textwrap.dedent(f"""\
            #!/bin/bash
            {setup_command}
            {task_command}
        """)

        return script

    @staticmethod
    def _compose_slurm_task_script(task: SlurmTaskConfig) -> str:
        try:
            array_size = get_slurm_max_array_size() // 2
        except Exception:
            array_size = 300  # Default

        setup_command = task.setup_commands if task.setup_commands else ""
        task_command = " ".join(
            [
                "python",
                f"{task.module}",
                f"--input-dir {task.input_dir}",
                f"--input-ext {task.input_ext}",
                f"--output-dir {task.output_dir}",
                f"--output-ext {task.output_ext}",
                f"--cpus {task.resources.cpus}",
                f"--memory {task.resources.memory}",
                f"--time {task.resources.time}",
                f"--max-workers {task.resources.max_workers}",
                f"--gpus {task.resources.gpus}" if task.resources.gpus else "",
                f"--setup-commands {repr(task.setup_commands)}"
                if task.setup_commands
                else "",
            ]
        )

        script = textwrap.dedent(f"""\
            #!/bin/bash
            #SBATCH --job-name=dask-client
            #SBATCH --output={task.log_dir}/dask-client-%A-%a.log
            #SBATCH --error={task.log_dir}/dask-client-%A-%a.log
            #SBATCH --nodes=1
            #SBATCH --ntasks=1
            #SBATCH --cpus-per-task=1
            #SBATCH --mem-per-cpu=2G
            #SBATCH --time=72:00:00
            #SBATCH --array=1-{array_size}%1

            echo "Starting Dask client for: {task.name}"
            echo "With SLURM_ARRAY_JOB_ID: $SLURM_ARRAY_JOB_ID"
            echo "With SLURM_ARRAY_TASK_ID: $SLURM_ARRAY_TASK_ID"
            echo "On machine:" $(hostname)

            {setup_command}

            {task_command}
        """)

        return script
