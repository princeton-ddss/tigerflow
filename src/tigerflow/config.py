import networkx as nx
from pydantic import BaseModel, Field, field_validator


class SlurmResourceConfig(BaseModel):
    cpus: int
    gpus: int | None = None
    memory: str
    time: str
    max_workers: int


class TaskConfig(BaseModel):
    name: str
    depends_on: str | None = None
    module: str
    setup_commands: str | None = None


class LocalTaskConfig(TaskConfig):
    pass


class SlurmTaskConfig(TaskConfig):
    resources: SlurmResourceConfig


class PipelineConfig(BaseModel):
    name: str
    tasks: list[LocalTaskConfig | SlurmTaskConfig] = Field(min_length=1)

    @field_validator("tasks")
    @classmethod
    def validate_task_dependency_graph(
        cls, tasks: list[LocalTaskConfig | SlurmTaskConfig]
    ) -> list[LocalTaskConfig | SlurmTaskConfig]:
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
