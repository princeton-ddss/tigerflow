"""Manual pipeline testing script.

Creates a temporary pipeline with configurable task kind, input files,
and runs it in background mode for manual testing.

Usage:
    python tests/user/run_pipeline.py --kind local --num-files 10 --delay 2.0
    python tests/user/run_pipeline.py --kind local_async --num-files 20 --fail-rate 0.1
    python tests/user/run_pipeline.py --kind slurm --num-files 50 --delay 5.0

Multi-task pipelines (comma-separated values):
    python tests/user/run_pipeline.py --kind local,local --delay 1.0,2.0
    python tests/user/run_pipeline.py --kind local,local_async --delay 1.0 --fail-rate 0.1,0.2
"""

import re
import subprocess
import tempfile
from pathlib import Path
from typing import Annotated

import typer
import yaml

# Get the directory containing this script (for task module paths)
SCRIPT_DIR = Path(__file__).parent.resolve()

TASK_MODULES = {
    "local": SCRIPT_DIR / "slow_task.py",
    "local_async": SCRIPT_DIR / "slow_task_async.py",
    "slurm": SCRIPT_DIR / "slow_task_slurm.py",
}


def _parse_list(value: str, parser=str) -> list:
    """Parse comma-separated values into a list."""
    return [parser(v.strip()) for v in value.split(",")]


def _expand_params(
    kinds: list[str],
    delays: list[float],
    delay_variations: list[float],
    fail_rates: list[float],
) -> tuple[list[str], list[float], list[float], list[float]]:
    """Expand singleton lists to match the unique length, validate lengths match."""
    lengths = {len(kinds), len(delays), len(delay_variations), len(fail_rates)}
    lengths.discard(1)  # Singletons can be expanded

    if len(lengths) > 1:
        raise typer.BadParameter(
            f"List lengths must match (got kinds={len(kinds)}, delays={len(delays)}, "
            f"delay_variations={len(delay_variations)}, fail_rates={len(fail_rates)})"
        )

    target_len = max(lengths) if lengths else 1

    def expand(lst: list) -> list:
        return lst if len(lst) == target_len else lst * target_len

    return expand(kinds), expand(delays), expand(delay_variations), expand(fail_rates)


def main(
    kind: Annotated[
        str,
        typer.Option("--kind", "-k", help="Task kind(s): local,local_async,slurm"),
    ] = "local",
    num_files: Annotated[
        int,
        typer.Option("--num-files", "-n", help="Number of input files to generate"),
    ] = 10,
    delay: Annotated[
        str,
        typer.Option("--delay", "-d", help="Seconds of CPU work per file"),
    ] = "1.0",
    delay_variation: Annotated[
        str,
        typer.Option("--delay-variation", "-v", help="Random variation in delay (0-1)"),
    ] = "0.0",
    fail_rate: Annotated[
        str,
        typer.Option("--fail-rate", "-f", help="Probability of random failure (0-1)"),
    ] = "0.0",
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
    # Parse comma-separated values
    kinds = _parse_list(kind)
    delays = _parse_list(delay, float)
    delay_variations = _parse_list(delay_variation, float)
    fail_rates = _parse_list(fail_rate, float)

    # Validate kinds
    for k in kinds:
        if k not in TASK_MODULES:
            raise typer.BadParameter(
                f"Invalid kind: {k}. Must be one of {list(TASK_MODULES.keys())}"
            )

    # Expand singletons to match list length
    kinds, delays, delay_variations, fail_rates = _expand_params(
        kinds, delays, delay_variations, fail_rates
    )

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
        kinds=kinds,
        delays=delays,
        delay_variations=delay_variations,
        fail_rates=fail_rates,
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
    typer.echo(f"Tasks: {len(kinds)}")
    typer.echo()
    typer.echo("Commands:")
    typer.echo(f"  tigerflow report {output_dir}")
    typer.echo(f"  tigerflow report {output_dir} --watch")
    typer.echo(f"  tigerflow stop {output_dir}")


def _generate_config(
    kinds: list[str],
    delays: list[float],
    delay_variations: list[float],
    fail_rates: list[float],
    concurrency_limit: int,
    max_workers: int,
) -> dict:
    """Generate pipeline config for multiple tasks."""
    tasks = []

    for i, (kind, task_delay, task_delay_var, task_fail_rate) in enumerate(
        zip(kinds, delays, delay_variations, fail_rates)
    ):
        # Name tasks: task1, task2, etc. (or just "slow" for single task)
        name = f"task{i + 1}" if len(kinds) > 1 else "slow"

        # Chain tasks: task2 depends on task1, etc.
        depends_on = f"task{i}" if i > 0 else None

        # Input/output extensions chain: .txt -> .out1 -> .out2 -> ...
        input_ext = ".txt" if i == 0 else f".out{i}"
        output_ext = f".out{i + 1}" if len(kinds) > 1 else ".out"

        task = {
            "name": name,
            "kind": kind,
            "module": str(TASK_MODULES[kind]),
            "input_ext": input_ext,
            "output_ext": output_ext,
            "params": {
                "delay": task_delay,
                "delay_variation": task_delay_var,
                "fail_rate": task_fail_rate,
            },
        }

        if depends_on:
            task["depends_on"] = depends_on

        if kind == "local_async":
            task["concurrency_limit"] = concurrency_limit
        elif kind == "slurm":
            task["max_workers"] = max_workers
            task["worker_resources"] = {
                "cpus": 1,
                "memory": "2G",
                "time": "00:30:00",
            }

        tasks.append(task)

    return {"tasks": tasks}


if __name__ == "__main__":
    typer.run(main)
