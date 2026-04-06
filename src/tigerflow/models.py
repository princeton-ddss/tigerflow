import json
import textwrap
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Literal

import networkx as nx
from pydantic import BaseModel, Field, field_validator

from tigerflow.settings import settings
from tigerflow.staging import StagingPipeline
from tigerflow.utils import is_process_running, read_pid_file, validate_file_ext


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


class FileMetrics(BaseModel):
    """Timing metrics for a single file processed by a task."""

    file: str
    task: str
    started_at: datetime
    finished_at: datetime
    status: Literal["success", "error"]

    @property
    def duration_ms(self) -> float:
        return (self.finished_at - self.started_at).total_seconds() * 1000


class SlurmResourceConfig(BaseModel):
    cpus: int
    gpus: int | None = None
    memory: str
    time: str
    sbatch_options: list[str] = []

    @field_validator("sbatch_options")
    @classmethod
    def transform_sbatch_options(cls, sbatch_options: list[str]) -> list[str]:
        return [option.strip() for option in sbatch_options]


class BaseTaskConfig(BaseModel):
    name: str
    depends_on: str | None = None
    module: str
    params: dict[str, Any] = {}
    input_ext: str
    output_ext: str = ".out"
    keep_output: bool = True
    setup_commands: list[str] = []
    _input_dir: Path | None = None
    _output_dir: Path | None = None

    @field_validator("module")
    @classmethod
    def validate_module(cls, module: str) -> str:
        from importlib.util import find_spec

        if module.endswith(".py"):
            path = Path(module)
            if not path.exists():
                raise ValueError(f"Module does not exist: {module}")
            if not path.is_file():
                raise ValueError(f"Module is not a file: {module}")
            return str(path.resolve())  # Use absolute path for clarity
        else:
            try:
                if find_spec(module) is None:
                    raise ValueError(f"Module not found: {module}")
            except ModuleNotFoundError:
                raise ValueError(f"Module not found: {module}")
            return module

    @property
    def python_command(self) -> str:
        """Return the python command to run this task's module."""
        if self.module.endswith(".py"):
            return f"python {self.module}"
        else:
            return f"python -m {self.module}"

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

    def to_script(self) -> str:
        """
        Compose a Bash script that executes the task.
        """
        raise NotImplementedError


class LocalTaskConfig(BaseTaskConfig):
    kind: Literal["local"]

    def to_script(self) -> str:
        log_file = self.output_dir / "task.log"
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
            {task_command} >> {log_file} 2>&1
        """)

        return script


class LocalAsyncTaskConfig(BaseTaskConfig):
    kind: Literal["local_async"]
    concurrency_limit: int

    def to_script(self) -> str:
        log_file = self.output_dir / "task.log"
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
            {task_command} >> {log_file} 2>&1
        """)

        return script


class SlurmTaskConfig(BaseTaskConfig):
    kind: Literal["slurm"]
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
        sbatch_account = next(
            (
                f"#SBATCH {option}"
                for option in self.worker_resources.sbatch_options
                if option.startswith(("--account", "-A"))
            ),
            "",
        )

        setup_command = ";".join(self.setup_commands)
        task_command = " ".join(
            [
                self.python_command,
                f"--task-name {self.name}",
                f"--input-dir {self.input_dir}",
                f"--input-ext {self.input_ext}",
                f"--output-dir {self.output_dir}",
                f"--output-ext {self.output_ext}",
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
            #SBATCH --job-name={self.client_job_name}
            #SBATCH --output={self.output_dir}/task-%j.out
            #SBATCH --error={self.output_dir}/task-%j.err
            #SBATCH --nodes=1
            #SBATCH --ntasks=1
            #SBATCH --cpus-per-task=1
            #SBATCH --mem-per-cpu=2G
            #SBATCH --time={self.client_job_time}
            {sbatch_account}

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
    staging: StagingPipeline = StagingPipeline()
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

        # Sort tasks in tree order (DFS pre-order from roots)
        roots = [n for n in G if G.in_degree(n) == 0]
        order_map = {
            name: index
            for index, name in enumerate(
                node for root in roots for node in nx.dfs_preorder_nodes(G, root)
            )
        }
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
    """Progress for a single task.

    Note: We don't distinguish staged vs in-progress at the task level
    because the distinction is fleeting (files are picked up quickly).
    """

    name: str
    processed: int = 0
    staged: int = 0  # files available but not yet completed (queued + processing)
    failed: int = 0


class FileError(BaseModel):
    """Error information for a failed file."""

    file: str
    path: str
    timestamp: datetime | None = None
    exception_type: str = ""
    message: str = ""
    traceback: str = ""


class TaskMeta(BaseModel):
    """Task metadata from INIT log entry."""

    name: str
    depends_on: str | None = None


class PipelineReport(BaseModel):
    """Complete pipeline status and progress report."""

    output_dir: Path
    status: Literal["running", "stopped"]
    pid: int | None = None
    processed: int = 0
    in_progress: int = 0
    failed: int = 0
    staged: int | None = None  # None if stopped
    tasks: list[TaskProgress] = []
    metrics: dict[str, list[FileMetrics]] = {}
    errors: dict[str, list[FileError]] = {}


class PipelineOutput:
    """Manage the output directory structure and validation."""

    def __init__(self, path: Path):
        self.root = path.resolve()
        self.internal = self.root / ".tigerflow"
        self.pid_file = self.internal / "run.pid"
        self.log_file = self.internal / "run.log"
        self.symlinks = self.internal / ".symlinks"
        self.finished = self.internal / ".finished"

    def validate(self) -> None:
        """Validate that the pipeline directory structure exists.

        Raises FileNotFoundError if directories don't exist.
        """
        if not self.root.exists():
            raise FileNotFoundError(f"Output directory does not exist: {self.root}")
        if not self.internal.exists():
            raise FileNotFoundError(
                f"Not a valid pipeline directory (missing .tigerflow): {self.root}"
            )

    def create(self) -> None:
        """Create the pipeline directory structure."""
        self.internal.mkdir(parents=True, exist_ok=True)

    def _get_status(self) -> tuple[bool, int | None]:
        """Return (is_running, pid)."""
        pid = read_pid_file(self.pid_file)
        is_running = pid is not None and is_process_running(pid)
        return is_running, pid if is_running else None

    def _get_task_dirs(self) -> list[Path]:
        """Get all task directories (non-hidden dirs in .tigerflow)."""
        return [
            d
            for d in self.internal.iterdir()
            if d.is_dir() and not d.name.startswith(".")
        ]

    def _get_task_meta(self) -> list[TaskMeta]:
        """Get task metadata from the most recent INIT entry in run.log."""
        if not self.log_file.exists():
            return []

        tasks: list[TaskMeta] = []
        try:
            with open(self.log_file) as f:
                for line in f:
                    if "INIT" not in line:
                        continue
                    start = line.find("{")
                    if start == -1:
                        continue
                    data = json.loads(line[start:])
                    tasks = [TaskMeta(**t) for t in data.get("tasks", [])]
        except (OSError, json.JSONDecodeError, KeyError):
            pass
        return tasks

    def _parse_all_metrics(self) -> list[FileMetrics]:
        """Parse METRICS from task log files.

        Reads from:
        - {task}/task.log (local/local_async tasks, appended across runs)
        - {task}/task-{job_id}.log (Slurm worker logs)
        """
        metrics: list[FileMetrics] = []

        for task_dir in self._get_task_dirs():
            log_files = list(task_dir.glob("task*.log"))

            for log_file in log_files:
                try:
                    with open(log_file) as f:
                        for line in f:
                            if "METRICS" not in line:
                                continue
                            start = line.find("{")
                            if start == -1:
                                continue
                            data = json.loads(line[start:])
                            metrics.append(
                                FileMetrics(
                                    file=data["file"],
                                    task=task_dir.name,
                                    started_at=datetime.fromisoformat(
                                        data["started_at"]
                                    ),
                                    finished_at=datetime.fromisoformat(
                                        data["finished_at"]
                                    ),
                                    status=data["status"],
                                )
                            )
                except (OSError, json.JSONDecodeError, KeyError):
                    continue

        return metrics

    def report(self) -> PipelineReport:
        """Generate a complete pipeline status report."""
        self.validate()

        is_running, pid = self._get_status()

        # === Filesystem Counts ===

        # processed = files in .finished/
        finished_stems: set[str] = set()
        if self.finished.exists():
            finished_stems = {f.stem for f in self.finished.iterdir() if f.is_file()}

        # failed = unique .err stems across all task dirs
        failed_stems: set[str] = set()
        errors: dict[str, list[FileError]] = {}
        for task_dir in self._get_task_dirs():
            task_errors: list[FileError] = []
            for file in task_dir.iterdir():
                if file.is_file() and file.name.endswith(".err"):
                    stem = file.name.removesuffix(".err")
                    failed_stems.add(stem)
                    try:
                        data = json.loads(file.read_text())
                        task_errors.append(
                            FileError(
                                file=data.get("file", stem),
                                path=str(file),
                                timestamp=datetime.fromisoformat(data["timestamp"]),
                                exception_type=data.get("exception_type", ""),
                                message=data.get("message", ""),
                                traceback=data.get("traceback", ""),
                            )
                        )
                    except (OSError, json.JSONDecodeError, KeyError):
                        task_errors.append(FileError(file=stem, path=str(file)))
            if task_errors:
                errors[task_dir.name] = task_errors

        # Get symlink stems (excluding finished and failed)
        symlink_stems: set[str] = set()
        if self.symlinks.exists():
            symlink_stems = {
                f.stem
                for f in self.symlinks.iterdir()
                if f.is_symlink()
                and f.stem not in finished_stems
                and f.stem not in failed_stems
            }

        # in_progress = symlinks with any task output file
        # staged = symlinks without any task output file
        stems_with_output: set[str] = set()
        for task_dir in self._get_task_dirs():
            for file in task_dir.iterdir():
                if (
                    file.is_file()
                    and not file.name.endswith(".err")
                    and not file.name.endswith(".log")
                    and not file.name.startswith("task-")
                ):
                    stems_with_output.add(file.stem)

        in_progress_stems = symlink_stems & stems_with_output
        staged_stems = symlink_stems - stems_with_output

        # === Per-Task Progress (from METRICS logs, all runs) ===

        all_metrics = self._parse_all_metrics()
        task_meta = self._get_task_meta()

        # Group metrics by task
        metrics_by_task: dict[str, list[FileMetrics]] = {}
        task_succeeded: dict[str, set[str]] = {}
        for m in all_metrics:
            if m.task not in metrics_by_task:
                metrics_by_task[m.task] = []
            metrics_by_task[m.task].append(m)
            if m.status == "success":
                if m.task not in task_succeeded:
                    task_succeeded[m.task] = set()
                task_succeeded[m.task].add(Path(m.file).stem)

        # Compute per-task progress
        tasks: list[TaskProgress] = []
        for tm in task_meta:
            # Available files for this task
            if tm.depends_on is None:
                # Root task: all files the pipeline has seen (filesystem)
                available = (
                    len(finished_stems)
                    + len(in_progress_stems)
                    + len(staged_stems)
                    + len(failed_stems)
                )
            else:
                # Downstream task: files that succeeded in parent
                available = len(task_succeeded.get(tm.depends_on, set()))

            # Processed from metrics, failed from filesystem
            task_metrics = metrics_by_task.get(tm.name, [])
            task_processed = sum(1 for m in task_metrics if m.status == "success")
            task_failed = len(errors.get(tm.name, []))

            tasks.append(
                TaskProgress(
                    name=tm.name,
                    processed=task_processed,
                    staged=max(0, available - task_processed - task_failed),
                    failed=task_failed,
                )
            )

        return PipelineReport(
            output_dir=self.root,
            status="running" if is_running else "stopped",
            pid=pid,
            processed=len(finished_stems),
            in_progress=len(in_progress_stems),
            failed=len(failed_stems),
            staged=len(staged_stems) if is_running else None,
            tasks=tasks,
            metrics=metrics_by_task,
            errors=errors,
        )
