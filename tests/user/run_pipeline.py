"""Manual pipeline testing script.

Creates a temporary pipeline with configurable task kind, input files,
and runs it in background mode for manual testing.

Usage:
    python tests/user/run_pipeline.py --kind local --num-files 10 --delay 2.0
    python tests/user/run_pipeline.py --kind local_async --num-files 20 --fail-rate 0.1
    python tests/user/run_pipeline.py --kind slurm --num-files 50 --delay 5.0
"""

import re
import subprocess
import tempfile
from enum import Enum
from pathlib import Path
from typing import Annotated

import typer
import yaml

# Get the directory containing this script (for task module paths)
SCRIPT_DIR = Path(__file__).parent.resolve()


class TaskKind(str, Enum):
    local = "local"
    local_async = "local_async"
    slurm = "slurm"


def main(
    kind: Annotated[
        TaskKind,
        typer.Option("--kind", "-k", help="Task kind to use"),
    ] = TaskKind.local,
    num_files: Annotated[
        int,
        typer.Option("--num-files", "-n", help="Number of input files to generate"),
    ] = 10,
    delay: Annotated[
        float,
        typer.Option("--delay", "-d", help="Seconds of CPU work per file"),
    ] = 1.0,
    fail_rate: Annotated[
        float,
        typer.Option("--fail-rate", "-f", help="Probability of random failure (0-1)"),
    ] = 0.0,
    idle_timeout: Annotated[
        int,
        typer.Option("--idle-timeout", help="Pipeline idle timeout in minutes"),
    ] = 5,
    concurrency_limit: Annotated[
        int,
        typer.Option("--concurrency-limit", help="Concurrency limit for local_async"),
    ] = 4,
    max_workers: Annotated[
        int,
        typer.Option("--max-workers", help="Max workers for slurm"),
    ] = 4,
):
    """Start a test pipeline in background mode."""
    # Create temp directory
    temp_dir = Path(tempfile.mkdtemp(prefix="tigerflow-test-"))
    input_dir = temp_dir / "input"
    output_dir = temp_dir / "output"
    config_file = temp_dir / "config.yaml"

    input_dir.mkdir()
    output_dir.mkdir()

    # Generate input files
    for i in range(1, num_files + 1):
        (input_dir / f"{i:04d}.txt").write_text(f"Input file {i}\n")

    # Generate config
    config = _generate_config(
        kind=kind,
        delay=delay,
        fail_rate=fail_rate,
        concurrency_limit=concurrency_limit,
        max_workers=max_workers,
    )
    config_file.write_text(yaml.dump(config))

    # Run pipeline in background
    result = subprocess.run(
        [
            "tigerflow",
            "run",
            str(config_file),
            str(input_dir),
            str(output_dir),
            "--background",
            "--idle-timeout",
            str(idle_timeout),
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        typer.echo(f"Error starting pipeline: {result.stderr}", err=True)
        raise typer.Exit(1)

    # Parse PID from output (e.g., "Started (pid 12345)")
    match = re.search(r"pid (\d+)", result.stdout)
    pid = match.group(1) if match else "unknown"

    typer.echo(f"Pipeline started (pid {pid})")
    typer.echo(f"Output directory: {output_dir}")
    typer.echo()
    typer.echo("Commands:")
    typer.echo(f"  tigerflow status {output_dir}")
    typer.echo(f"  tigerflow stop {output_dir}")
    typer.echo(f"  kill -TERM {pid}")


def _generate_config(
    kind: TaskKind,
    delay: float,
    fail_rate: float,
    concurrency_limit: int,
    max_workers: int,
) -> dict:
    """Generate pipeline config for the specified task kind."""
    task_modules = {
        TaskKind.local: SCRIPT_DIR / "slow_task.py",
        TaskKind.local_async: SCRIPT_DIR / "slow_task_async.py",
        TaskKind.slurm: SCRIPT_DIR / "slow_task_slurm.py",
    }

    base_task = {
        "name": "slow",
        "kind": kind.value,
        "module": str(task_modules[kind]),
        "input_ext": ".txt",
        "output_ext": ".out",
        "params": {
            "delay": delay,
            "fail_rate": fail_rate,
        },
    }

    if kind == TaskKind.local_async:
        base_task["concurrency_limit"] = concurrency_limit
    elif kind == TaskKind.slurm:
        base_task["max_workers"] = max_workers
        base_task["worker_resources"] = {
            "cpus": 1,
            "memory": "2G",
            "time": "00:30:00",
        }

    return {"tasks": [base_task]}


if __name__ == "__main__":
    typer.run(main)
