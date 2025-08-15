import re
import subprocess
import tempfile
import textwrap
import time
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
        for p in [config_file, input_dir, output_dir]:
            if not p.exists():
                raise FileNotFoundError(p)

        self.input_dir = input_dir.resolve()
        self.output_dir = output_dir.resolve()
        self.symlinks_dir = self.output_dir / ".symlinks"
        self.finished_dir = self.output_dir / ".finished"

        for p in [self.symlinks_dir, self.finished_dir]:
            p.mkdir(parents=True, exist_ok=True)

        self.config = PipelineConfig.model_validate(
            yaml.safe_load(config_file.read_text())
        )

        for task in self.config.tasks:
            if not is_valid_cli(task.module):
                raise ValueError(f"Invalid CLI: {task.module}")

        # Map task I/O directories from the dependency graph
        for task in self.config.tasks:
            task.input_dir = (
                self.output_dir / task.depends_on
                if task.depends_on
                else self.symlinks_dir
            )
            task.output_dir = self.output_dir / task.name

        # Create task directories
        for task in self.config.tasks:
            for p in [task.output_dir, task.log_dir]:
                p.mkdir(parents=True, exist_ok=True)

        # Initialize a set to track local task processes
        self._subprocesses: set[subprocess.Popen] = set()

        # Initialize a set to track Slurm task clusters
        self.slurm_task_ids: set[str] = set()

    def run(self):
        try:
            self._start_tasks()
            while True:
                # TODO: Periodically check for any new input files to process (and create corresponding symlinks)
                # TODO: Periodically clean up files that have successfully completed all steps of the pipeline
                time.sleep(3)
        finally:
            for process in self._subprocesses:
                process.terminate()
            for job_id in self.slurm_task_ids:
                subprocess.run(["scancel", job_id])

    def _start_tasks(self):
        for task in self.config.tasks:
            if isinstance(task, LocalTaskConfig):
                self._start_local_task(task)
            elif isinstance(task, LocalAsyncTaskConfig):
                self._start_local_async_task(task)
            elif isinstance(task, SlurmTaskConfig):
                self._start_slurm_task(task)
            else:
                raise ValueError(f"Unsupported task kind: {type(task)}")

    def _start_local_task(self, task: LocalTaskConfig):
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

        process = subprocess.Popen(["bash", "-c", script])
        self._subprocesses.add(process)

    def _start_local_async_task(self, task: LocalAsyncTaskConfig):
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

        process = subprocess.Popen(["bash", "-c", script])
        self._subprocesses.add(process)

    def _start_slurm_task(self, task: SlurmTaskConfig):
        script = self._compose_slurm_script(task)

        with tempfile.TemporaryDirectory() as temp_dir:
            # Write the Slurm script to a temporary file
            file = Path(temp_dir) / "task.slurm"
            with open(file, "w") as f:
                f.write(script)

            # Submit the Slurm job
            result = subprocess.run(
                ["sbatch", str(file)],
                capture_output=True,
                text=True,
            )

            # Extract and store the job ID
            match = re.search(r"Submitted batch job (\d+)", result.stdout)
            if match:
                job_id = match.group(1).strip()
                self.slurm_task_ids.add(job_id)
            else:
                raise ValueError("Failed to extract job ID from sbatch output")

    @staticmethod
    def _compose_slurm_script(task: SlurmTaskConfig) -> str:
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

        slurm_script = textwrap.dedent(f"""\
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

        return slurm_script
