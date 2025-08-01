from pathlib import Path
from typing import Annotated, Literal

import networkx as nx
from pydantic import BaseModel, Field, field_validator


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
    setup_commands: str | None = None
    _input_dir: Path
    _output_dir: Path

    @field_validator("module")
    @classmethod
    def validate_module(cls, module: Path) -> Path:
        if not module.exists():
            raise ValueError(f"Module does not exist: {module}")
        if not module.is_file():
            raise ValueError(f"Module is not a file: {module}")
        return module

    @field_validator("setup_commands")
    @classmethod
    def transform_setup_commands(cls, setup_commands: str | None) -> str | None:
        return ";".join(setup_commands.splitlines()) if setup_commands else None

    @property
    def input_dir(self) -> Path:
        return self._input_dir

    @input_dir.setter
    def input_dir(self, value: Path):
        self._input_dir = value

    @property
    def output_dir(self) -> Path:
        return self._output_dir

    @output_dir.setter
    def output_dir(self, value: Path):
        self._output_dir = value

    @property
    def log_dir(self) -> Path:
        return self._output_dir.parent / "logs"


class LocalTaskConfig(BaseTaskConfig):
    type: Literal["local"]


class LocalAsyncTaskConfig(BaseTaskConfig):
    type: Literal["local_async"]
    concurrency_limit: int


class SlurmTaskConfig(BaseTaskConfig):
    type: Literal["slurm"]
    resources: SlurmResourceConfig


TaskConfig = Annotated[
    LocalTaskConfig | LocalAsyncTaskConfig | SlurmTaskConfig,
    Field(discriminator="type"),
]


class PipelineConfig(BaseModel):
    name: str
    tasks: list[TaskConfig] = Field(min_length=1)

    @field_validator("tasks")
    @classmethod
    def validate_task_dependency_graph(
        cls,
        tasks: list[TaskConfig],
    ) -> list[TaskConfig]:
        # Validate each dependency is on a known task
        task_names = {task.name for task in tasks}
        for task in tasks:
            if task.depends_on and task.depends_on not in task_names:
                raise ValueError(
                    f"Task '{task.name}' depends on unknown task '{task.depends_on}'"
                )

        # Validate there is only a single root node
        root_nodes = [task.name for task in tasks if task.depends_on is None]
        if len(root_nodes) > 1:
            raise ValueError(f"The pipeline has multiple root nodes: {root_nodes}")

        # Build the dependency graph
        G = nx.DiGraph()
        for task in tasks:
            G.add_node(task.name)
            if task.depends_on:
                G.add_edge(task.depends_on, task.name)

        # Validate the dependency graph is a DAG
        if not nx.is_directed_acyclic_graph(G):
            cycle = list(nx.simple_cycles(G))[0]
            raise ValueError(f"Dependency cycle detected: {' -> '.join(cycle)}")

        return tasks
