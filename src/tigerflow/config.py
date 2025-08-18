from pathlib import Path
from typing import Annotated, Literal

import networkx as nx
from pydantic import BaseModel, Field, field_validator

from tigerflow.utils import validate_file_ext


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
        return self._output_dir / "logs"


class LocalTaskConfig(BaseTaskConfig):
    kind: Literal["local"]


class LocalAsyncTaskConfig(BaseTaskConfig):
    kind: Literal["local_async"]
    concurrency_limit: int


class SlurmTaskConfig(BaseTaskConfig):
    kind: Literal["slurm"]
    resources: SlurmResourceConfig


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
        # Validate dependency references and extension compatibility
        task_dict = {t.name: t for t in tasks}
        for t in tasks:
            if not t.depends_on:
                continue
            dep_t = task_dict.get(t.depends_on)
            if not dep_t:
                raise ValueError(
                    f"Task '{t.name}' depends on unknown task '{t.depends_on}'"
                )
            if dep_t.output_ext != t.input_ext:
                raise ValueError(
                    "Extension mismatch: "
                    f"task '{dep_t.name}' outputs '{dep_t.output_ext}' but "
                    f"its dependent task '{t.name}' expects '{t.input_ext}'"
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

        return tasks

    @property
    def root_task(self) -> TaskConfig:
        for task in self.tasks:
            if task.depends_on is None:
                return task

    @property
    def terminal_tasks(self) -> list[TaskConfig]:
        parents = {task.depends_on for task in self.tasks if task.depends_on}
        non_parents = [task for task in self.tasks if task.name not in parents]
        return non_parents
