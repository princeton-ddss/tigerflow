import re
import subprocess
import tempfile
import textwrap
from pathlib import Path

import yaml

from .config import LocalTaskConfig, PipelineConfig, SlurmTaskConfig
from .utils import get_slurm_max_array_size, is_valid_cli


class Pipeline:
    def __init__(self, config_file: Path, input_dir: Path, output_dir: Path):
        for p in [config_file, input_dir, output_dir]:
            if not p.exists():
                raise FileNotFoundError(p)

        self.config = PipelineConfig.model_validate(
            yaml.safe_load(config_file.read_text())
        )

        for task in self.config.tasks:
            if not is_valid_cli(task.module):
                raise ValueError(f"Invalid CLI: {task.module}")

        # Map task I/O directories from the dependency graph
        pipeline_dir = output_dir.absolute() / self.config.name
        for task in self.config.tasks:
            task.input_dir = (
                pipeline_dir / task.depends_on / "results"
                if task.depends_on
                else input_dir.absolute()
            )
            task.output_dir = pipeline_dir / task.name / "results"

        # Initialize a set to track Slurm task clusters
        self.slurm_task_ids: set[int] = set()

    def run(self):
        for task in self.config.tasks:
            for p in [task.output_dir, task.log_dir]:
                p.mkdir(parents=True, exist_ok=True)
            if isinstance(task, SlurmTaskConfig):
                self._start_slurm_task(task)
            elif isinstance(task, LocalTaskConfig):
                pass  # TODO: Start the task as a subprocess
            else:
                raise ValueError(f"Unsupported task type: {type(task)}")

        # while True:
        #     # TODO: Check status of each task
        #     pass

        #     # TODO: Clean up files that have successfully completed all steps of the pipeline
        #     pass

        #     # TODO: Handle collective shutdown (i.e., bring down all tasks when the main process terminates)
        #     pass

    def _start_slurm_task(self, task: SlurmTaskConfig):
        script = self._compose_slurm_script(task)

        with tempfile.TemporaryDirectory() as temp_dir:
            # Write the Slurm script to a temporary file
            file = Path(temp_dir) / "task.slurm"
            with open(file, "w") as f:
                f.write(script)

            # Submit the Slurm job
            try:
                result = subprocess.run(
                    ["sbatch", str(file)],
                    capture_output=True,
                    text=True,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f"Failed to run sbatch: {e.stderr}")
            except Exception as e:
                raise RuntimeError(f"Unexpected error: {e}")

            # Extract and store the job ID
            match = re.search(r"Submitted batch job (\d+)", result.stdout)
            if match:
                job_id = int(match.group(1))
                self.slurm_task_ids.add(job_id)
            else:
                raise ValueError("Failed to extract job ID from sbatch output")

    @staticmethod
    def _compose_slurm_script(task: SlurmTaskConfig) -> str:
        try:
            array_size = get_slurm_max_array_size() // 2
        except Exception:
            array_size = 300  # Default

        job_name = f"{task.name}-client"
        log_dir = task.log_dir
        setup_command = task.setup_commands if task.setup_commands else ""
        task_command = " ".join(
            [
                "python",
                f"{task.input_dir}",
                f"{task.output_dir}",
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
            #SBATCH --job-name={job_name}
            #SBATCH --output={log_dir}/{job_name}-slurm-%A-%a.out
            #SBATCH --error={log_dir}/{job_name}-slurm-%A-%a.err
            #SBATCH --nodes=1
            #SBATCH --ntasks=1
            #SBATCH --cpus-per-task=1
            #SBATCH --mem-per-cpu=2G
            #SBATCH --time=72:00:00
            #SBATCH --array=1-{array_size}%1

            echo "Starting Slurm job: {job_name}"
            echo "With SLURM_ARRAY_JOB_ID: $SLURM_ARRAY_JOB_ID"
            echo "With SLURM_ARRAY_TASK_ID: $SLURM_ARRAY_TASK_ID"
            echo "On machine:" $(hostname)

            {setup_command}

            {task_command}
        """)

        return slurm_script
