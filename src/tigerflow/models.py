import textwrap
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Literal

import networkx as nx
from pydantic import BaseModel, Field, field_validator, model_validator

from tigerflow.settings import settings
from tigerflow.utils import validate_file_ext


class TaskStatusKind(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"


class TaskStatus(BaseModel):
    kind: TaskStatusKind
    detail: str | None = None

    @property
    def is_alive(self) -> bool:
        return self.kind != TaskStatusKind.INACTIVE


class SlurmResourceConfig(BaseModel):
    cpus: int
    gpus: int | None = None
    memory: str
    time: str
    sbatch_options: list[str] = []


class BaseTaskConfig(BaseModel):
    name: str
    depends_on: str | None = None
    module: Path | None = None
    library: str | None = None
    params: dict[str, Any] = {}
    input_ext: str
    output_ext: str = ".out"
    keep_output: bool = True
    setup_commands: list[str] = []
    _input_dir: Path | None = None
    _output_dir: Path | None = None

    @model_validator(mode="after")
    def validate_module_or_library(self):
        if self.module is None and self.library is None:
            raise ValueError("Either 'module' or 'library' must be specified")
        if self.module is not None and self.library is not None:
            raise ValueError("Cannot specify both 'module' and 'library'")
        return self

    @field_validator("module")
    @classmethod
    def validate_module(cls, module: Path | None) -> Path | None:
        if module is None:
            return None
        if not module.exists():
            raise ValueError(f"Module does not exist: {module}")
        if not module.is_file():
            raise ValueError(f"Module is not a file: {module}")
        return module.resolve()  # Use absolute path for clarity

    @property
    def python_command(self) -> str:
        """Return the python command to run this task's module."""
        if self.module:
            return f"python {self.module}"
        else:
            return f"python -m {self.library}"

    @property
    def params_as_cli_args(self) -> list[str]:
        """Convert params dict to CLI argument strings."""
        args = []
        for key, value in self.params.items():
            # Convert underscores to hyphens for CLI convention
            cli_key = key.replace("_", "-")
            if isinstance(value, bool):
                if value:
                    args.append(f"--{cli_key}")
            elif isinstance(value, list):
                for item in value:
                    args.append(f"--{cli_key} {repr(item)}")
            else:
                args.append(f"--{cli_key} {repr(value)}")
        return args

    @field_validator("input_ext")
    @classmethod
    def validate_input_ext(cls, input_ext: str) -> str:
        return validate_file_ext(input_ext)

    @field_validator("output_ext")
    @classmethod
    def validate_output_ext(cls, output_ext: str) -> str:
        return validate_file_ext(output_ext)

    @property
    def input_dir(self) -> Path:
        if not self._input_dir:
            raise ValueError("Input directory has not been set")
        return self._input_dir

    @input_dir.setter
    def input_dir(self, value: Path):
        self._input_dir = value

    @property
    def output_dir(self) -> Path:
        if not self._output_dir:
            raise ValueError("Output directory has not been set")
        return self._output_dir

    @output_dir.setter
    def output_dir(self, value: Path):
        self._output_dir = value

    @property
    def log_dir(self) -> Path:
        return self.output_dir / "logs"

    def to_script(self) -> str:
        """
        Compose a Bash script that executes the task.
        """
        raise NotImplementedError


class LocalTaskConfig(BaseTaskConfig):
    kind: Literal["local"]

    def to_script(self) -> str:
        stdout_file = self.log_dir / f"{self.name}-$$.out"
        stderr_file = self.log_dir / f"{self.name}-$$.err"
        setup_command = ";".join(self.setup_commands)
        task_command = " ".join(
            [
                "exec",
                self.python_command,
                f"--task-name {self.name}",
                f"--input-dir {self.input_dir}",
                f"--input-ext {self.input_ext}",
                f"--output-dir {self.output_dir}",
                f"--output-ext {self.output_ext}",
            ]
            + self.params_as_cli_args
        )

        script = textwrap.dedent(f"""\
            #!/bin/bash
            {setup_command}
            {task_command} > {stdout_file} 2> {stderr_file}
        """)

        return script


class LocalAsyncTaskConfig(BaseTaskConfig):
    kind: Literal["local_async"]
    concurrency_limit: int

    def to_script(self) -> str:
        stdout_file = self.log_dir / f"{self.name}-$$.out"
        stderr_file = self.log_dir / f"{self.name}-$$.err"
        setup_command = ";".join(self.setup_commands)
        task_command = " ".join(
            [
                "exec",
                self.python_command,
                f"--task-name {self.name}",
                f"--input-dir {self.input_dir}",
                f"--input-ext {self.input_ext}",
                f"--output-dir {self.output_dir}",
                f"--output-ext {self.output_ext}",
                f"--concurrency-limit {self.concurrency_limit}",
            ]
            + self.params_as_cli_args
        )

        script = textwrap.dedent(f"""\
            #!/bin/bash
            {setup_command}
            {task_command} > {stdout_file} 2> {stderr_file}
        """)

        return script


class SlurmTaskConfig(BaseTaskConfig):
    kind: Literal["slurm"]
    account: str
    max_workers: int
    worker_resources: SlurmResourceConfig

    @property
    def client_job_time(self) -> str:
        return f"{settings.slurm_task_client_hours}:00:00"

    @property
    def client_job_name(self) -> str:
        return f"{self.name}-client"

    @property
    def worker_job_name(self) -> str:
        return f"{self.name}-worker"

    def to_script(self) -> str:
        setup_command = ";".join(self.setup_commands)
        task_command = " ".join(
            [
                self.python_command,
                f"--task-name {self.name}",
                f"--input-dir {self.input_dir}",
                f"--input-ext {self.input_ext}",
                f"--output-dir {self.output_dir}",
                f"--output-ext {self.output_ext}",
                f"--account {self.account}",
                f"--max-workers {self.max_workers}",
                f"--cpus {self.worker_resources.cpus}",
                f"--memory {self.worker_resources.memory}",
                f"--time {self.worker_resources.time}",
                f"--gpus {self.worker_resources.gpus}"
                if self.worker_resources.gpus
                else "",
                "--run-directly",
            ]
            + [
                f"--sbatch-option {repr(option)}"
                for option in self.worker_resources.sbatch_options
            ]
            + [f"--setup-command {repr(command)}" for command in self.setup_commands]
            + self.params_as_cli_args
        )

        script = textwrap.dedent(f"""\
            #!/bin/bash
            #SBATCH --account={self.account}
            #SBATCH --job-name={self.client_job_name}
            #SBATCH --output={self.log_dir}/%x-%j.out
            #SBATCH --error={self.log_dir}/%x-%j.err
            #SBATCH --nodes=1
            #SBATCH --ntasks=1
            #SBATCH --cpus-per-task=1
            #SBATCH --mem-per-cpu=2G
            #SBATCH --time={self.client_job_time}

            echo "Starting Dask client for: {self.name}"
            echo "With SLURM_JOB_ID: $SLURM_JOB_ID"
            echo "On machine:" $(hostname)

            {setup_command}

            {task_command}
        """)

        return script


TaskConfig = Annotated[
    LocalTaskConfig | LocalAsyncTaskConfig | SlurmTaskConfig,
    Field(discriminator="kind"),
]


class PipelineConfig(BaseModel):
    tasks: list[TaskConfig] = Field(min_length=1)

    @field_validator("tasks")
    @classmethod
    def validate_task_dependency_graph(
        cls,
        tasks: list[TaskConfig],
    ) -> list[TaskConfig]:
        """
        Validate if the graph of task input/output files forms an arborescence.
        """
        if not tasks:
            raise ValueError("Pipeline must have at least one task")

        # Validate task names are unique
        seen_names = set()
        for task in tasks:
            if task.name in seen_names:
                raise ValueError(f"Duplicate task name: {task.name}")
            seen_names.add(task.name)

        # Validate dependency references and extension compatibility
        task_dict = {task.name: task for task in tasks}
        for task in tasks:
            if not task.depends_on:
                continue
            parent_task = task_dict.get(task.depends_on)
            if not parent_task:
                raise ValueError(
                    f"Task '{task.name}' depends on unknown task '{task.depends_on}'"
                )
            if parent_task.output_ext != task.input_ext:
                raise ValueError(
                    "Extension mismatch: "
                    f"task '{parent_task.name}' outputs '{parent_task.output_ext}' but "
                    f"its dependent task '{task.name}' expects '{task.input_ext}'"
                )

        # Build the task dependency graph
        G = nx.DiGraph()
        for task in tasks:
            G.add_node(task.name)
            if task.depends_on:
                G.add_edge(task.depends_on, task.name)

        # Validate the graph of input/output files forms an arborescence
        if not nx.is_branching(G):
            raise ValueError("Task dependency graph contains a cycle")
        root_input_ext = {task.input_ext for task in tasks if not task.depends_on}
        if len(root_input_ext) > 1:  # Cannot be zero due to earlier validations
            raise ValueError("Root tasks must have the same input extension")

        # Sort tasks topologically
        order_map = {name: index for index, name in enumerate(nx.topological_sort(G))}
        tasks.sort(key=lambda task: order_map[task.name])

        return tasks

    @property
    def root_input_ext(self) -> str:
        # Assumes all root tasks share the same input extension
        for task in self.tasks:
            if not task.depends_on:
                return task.input_ext
        raise ValueError("No root task found")

    @property
    def root_tasks(self) -> list[TaskConfig]:
        return [task for task in self.tasks if not task.depends_on]

    @property
    def terminal_tasks(self) -> list[TaskConfig]:
        parents = {task.depends_on for task in self.tasks if task.depends_on}
        return [task for task in self.tasks if task.name not in parents]


class TaskProgress(BaseModel):
    name: str
    processed: set[Path] = set()
    ongoing: set[Path] = set()
    failed: set[Path] = set()


class PipelineProgress(BaseModel):
    staged: set[Path] = set()
    finished: set[Path] = set()
    tasks: list[TaskProgress] = []

    @property
    def failed(self) -> set[Path]:
        return set.union(*(task.failed for task in self.tasks))
