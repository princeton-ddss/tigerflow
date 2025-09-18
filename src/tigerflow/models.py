import textwrap
from enum import Enum
from pathlib import Path
from typing import Annotated, Literal

import networkx as nx
from pydantic import BaseModel, Field, field_validator

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
    max_workers: int


class BaseTaskConfig(BaseModel):
    name: str
    depends_on: str | None = None
    module: Path
    input_ext: str
    output_ext: str = ".out"
    keep_output: bool = True
    setup_commands: str | None = None
    _input_dir: Path | None = None
    _output_dir: Path | None = None

    @field_validator("module")
    @classmethod
    def validate_module(cls, module: Path) -> Path:
        if not module.exists():
            raise ValueError(f"Module does not exist: {module}")
        if not module.is_file():
            raise ValueError(f"Module is not a file: {module}")
        return module.resolve()  # Use absolute path for clarity

    @field_validator("input_ext")
    @classmethod
    def validate_input_ext(cls, input_ext: str) -> str:
        return validate_file_ext(input_ext)

    @field_validator("output_ext")
    @classmethod
    def validate_output_ext(cls, output_ext: str) -> str:
        return validate_file_ext(output_ext)

    @field_validator("setup_commands")
    @classmethod
    def transform_setup_commands(cls, setup_commands: str | None) -> str | None:
        return ";".join(setup_commands.splitlines()) if setup_commands else None

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
        setup_command = self.setup_commands if self.setup_commands else ""
        task_command = " ".join(
            [
                "exec",
                "python",
                f"{self.module}",
                f"--task-name {self.name}",
                f"--input-dir {self.input_dir}",
                f"--input-ext {self.input_ext}",
                f"--output-dir {self.output_dir}",
                f"--output-ext {self.output_ext}",
            ]
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
        setup_command = self.setup_commands if self.setup_commands else ""
        task_command = " ".join(
            [
                "exec",
                "python",
                f"{self.module}",
                f"--task-name {self.name}",
                f"--input-dir {self.input_dir}",
                f"--input-ext {self.input_ext}",
                f"--output-dir {self.output_dir}",
                f"--output-ext {self.output_ext}",
                f"--concurrency-limit {self.concurrency_limit}",
            ]
        )

        script = textwrap.dedent(f"""\
            #!/bin/bash
            {setup_command}
            {task_command} > {stdout_file} 2> {stderr_file}
        """)

        return script


class SlurmTaskConfig(BaseTaskConfig):
    kind: Literal["slurm"]
    resources: SlurmResourceConfig

    @property
    def client_job_name(self) -> str:
        return f"{self.name}-client"

    @property
    def worker_job_name(self) -> str:
        return f"{self.name}-worker"

    def to_script(self) -> str:
        setup_command = self.setup_commands if self.setup_commands else ""
        task_command = " ".join(
            [
                "python",
                f"{self.module}",
                f"--task-name {self.name}",
                f"--input-dir {self.input_dir}",
                f"--input-ext {self.input_ext}",
                f"--output-dir {self.output_dir}",
                f"--output-ext {self.output_ext}",
                f"--cpus {self.resources.cpus}",
                f"--memory {self.resources.memory}",
                f"--time {self.resources.time}",
                f"--max-workers {self.resources.max_workers}",
                f"--gpus {self.resources.gpus}" if self.resources.gpus else "",
                f"--setup-commands {repr(self.setup_commands)}"
                if self.setup_commands
                else "",
                "--run-directly",
            ]
        )

        script = textwrap.dedent(f"""\
            #!/bin/bash
            #SBATCH --job-name={self.client_job_name}
            #SBATCH --output={self.log_dir}/%x-%j.out
            #SBATCH --error={self.log_dir}/%x-%j.err
            #SBATCH --nodes=1
            #SBATCH --ntasks=1
            #SBATCH --cpus-per-task=1
            #SBATCH --mem-per-cpu=2G
            #SBATCH --time=24:00:00

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

        # Build the dependency graph
        G = nx.DiGraph()
        for task in tasks:
            G.add_node(task.name)
            if task.depends_on:
                G.add_edge(task.depends_on, task.name)

        # Validate the dependency graph is a rooted tree
        if not nx.is_tree(G):
            raise ValueError("Task dependency graph is not a tree")
        roots = [node for node in G.nodes() if G.in_degree(node) == 0]
        if len(roots) != 1:
            raise ValueError("Task dependency graph must have exactly one root")

        # Sort tasks topologically
        order_map = {name: index for index, name in enumerate(nx.topological_sort(G))}
        tasks.sort(key=lambda task: order_map[task.name])

        return tasks

    @property
    def root_task(self) -> TaskConfig:
        for task in self.tasks:
            if not task.depends_on:
                return task
        raise ValueError("No root task found")

    @property
    def terminal_tasks(self) -> list[TaskConfig]:
        parents = {task.depends_on for task in self.tasks if task.depends_on}
        return [task for task in self.tasks if task.name not in parents]


class TaskProgress(BaseModel):
    name: str
    processed: list[Path] = []
    ongoing: list[Path] = []
    failed: list[Path] = []


class PipelineProgress(BaseModel):
    staged: list[Path] = []
    finished: list[Path] = []
    tasks: list[TaskProgress] = []
