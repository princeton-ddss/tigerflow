"""Config builders and fakes shared across Pipeline tests.

Kept out of `conftest.py` so that file holds fixtures only, which pytest
supplies implicitly, rather than a mix of implicit fixtures and symbols
tests import by name.
"""

import subprocess
from collections.abc import Callable
from pathlib import Path

from tigerflow.models import TaskStatus, TaskStatusKind
from tigerflow.pipeline import Pipeline

DEFAULT_TASK_NAME = "echo"

# Any importable module works: validate_task_cli is patched out by pipeline_factory
DEFAULT_MODULE = "tigerflow.library.echo"

# Type of the `pipeline_factory` fixture, which tests annotate on every request
PipelineFactory = Callable[..., Pipeline]


def task_spec(
    name: str = DEFAULT_TASK_NAME,
    *,
    kind: str = "local",
    depends_on: str | None = None,
    input_ext: str = ".txt",
    output_ext: str = ".txt",
    keep_output: bool = False,
    **extra,
) -> dict:
    """Build a single task config dict, overriding only what a test cares about."""
    spec = {
        "name": name,
        "kind": kind,
        "module": DEFAULT_MODULE,
        "input_ext": input_ext,
        "output_ext": output_ext,
        "keep_output": keep_output,
    }
    if depends_on is not None:
        spec["depends_on"] = depends_on
    spec.update(extra)
    return spec


class FakePopen(subprocess.Popen):
    """Stand-in for a task subprocess, so tests never spawn real processes.

    Nothing spawns because `__init__` never calls `super()`, which is where the
    real class forks. That also leaves inherited methods without the state they
    read, so anything Pipeline reaches for beyond `poll`/`terminate`/`pid`
    raises AttributeError instead of acting on a pid that is not ours.

    Set `exit_code` to make a task look like it died.
    """

    def __init__(self, *args, **kwargs):
        # Popen.__del__ reads _child_created to tell whether a child is owed a reap
        self._child_created = False
        self.args = args
        self.pid = 424242
        self.exit_code: int | None = None
        self.terminate_calls = 0

    def poll(self) -> int | None:
        return self.exit_code

    def terminate(self):
        self.terminate_calls += 1
        # A terminated process must stop reporting itself as alive, otherwise
        # the shutdown loop in Pipeline.run() never exits
        if self.exit_code is None:
            self.exit_code = -15


def start_fake_tasks(pipeline: Pipeline) -> None:
    """Register a fake subprocess per task so a cycle can poll task status.

    `_check_task_status()` looks tasks up in `_subprocesses`, which only
    `_start_tasks()` fills — and tests driving cycles never call it. Status is
    set separately because the constructor starts tasks INACTIVE.
    """
    for task in pipeline._config.tasks:
        pipeline._subprocesses[task.name] = FakePopen()
        pipeline._task_status[task.name] = TaskStatus(kind=TaskStatusKind.ACTIVE)


def stage_file(pipeline: Pipeline, input_dir: Path, file_id: str) -> None:
    """Write an input file and stage it through the real staging path.

    Goes through `_stage_new_files` rather than writing the symlink directly,
    so callers keep exercising staging.
    """
    (input_dir / f"{file_id}{pipeline._config.root_input_ext}").write_text("payload")
    pipeline._stage_new_files()


def write_output(pipeline: Pipeline, file_id: str, *, task: str | None = None) -> None:
    """Write a task output, as the task's worker would on success.

    `task` may be omitted only for a single-task pipeline, where there is no
    ambiguity and naming the task would force the caller to know which name
    the pipeline was built with.
    """
    tasks = pipeline._config.tasks
    if task is None:
        if len(tasks) > 1:
            raise ValueError(
                f"Pipeline has {len(tasks)} tasks; pass task= to pick one of "
                f"{[t.name for t in tasks]}"
            )
        spec = tasks[0]
    else:
        spec = next((t for t in tasks if t.name == task), None)
        if spec is None:
            raise ValueError(
                f"No task named {task!r}; pipeline has {[t.name for t in tasks]}"
            )
    (spec.output_dir / f"{file_id}{spec.output_ext}").write_text("done")
