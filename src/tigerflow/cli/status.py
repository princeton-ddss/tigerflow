import json
from pathlib import Path
from typing import Annotated

import typer

from tigerflow.utils import is_process_running, read_pid_file


def status(
    output_dir: Annotated[
        Path,
        typer.Argument(
            help="Pipeline output directory (must contain .tigerflow)",
            show_default=False,
        ),
    ],
    output_json: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output status in JSON format for machine consumption.",
        ),
    ] = False,
):
    """
    Check the status of a pipeline.
    """
    output_dir = output_dir.resolve()
    internal_dir = output_dir / ".tigerflow"
    pid_file = internal_dir / "run.pid"

    if not output_dir.exists():
        _output_error("Output directory does not exist", output_json)
        raise typer.Exit(1)

    if not internal_dir.exists():
        _output_error(
            "Not a valid pipeline directory (missing .tigerflow)", output_json
        )
        raise typer.Exit(1)

    pid = read_pid_file(pid_file)
    running = pid is not None and is_process_running(pid)

    # Count progress directly from filesystem
    symlinks_dir = internal_dir / ".symlinks"
    finished_dir = internal_dir / ".finished"

    staged = sum(1 for f in symlinks_dir.iterdir() if f.is_file())
    finished = sum(1 for f in finished_dir.iterdir() if f.is_file())

    # Count failed files across all task directories
    failed = 0
    for task_dir in internal_dir.iterdir():
        if task_dir.is_dir() and not task_dir.name.startswith("."):
            failed += sum(1 for f in task_dir.iterdir() if f.name.endswith(".err"))

    # Failed files remain in symlinks, so subtract them from staged count
    staged = staged - failed

    if output_json:
        _output_json(pid, running, finished, staged, failed)
    else:
        _output_rich(pid, running, finished, staged, failed)

    # Return appropriate exit code: 0 = running, 1 = not running
    if not running:
        raise typer.Exit(1)


def _output_error(message: str, output_json: bool):
    """Output an error message in the appropriate format."""
    if output_json:
        print(json.dumps({"error": message}))
    else:
        typer.echo(f"Error: {message}", err=True)


def _output_json(
    pid: int | None, running: bool, finished: int, staged: int, failed: int
):
    """Output status in JSON format."""
    data = {
        "pid": pid,
        "running": running,
        "finished": finished,
        "staged": staged,
        "failed": failed,
    }
    print(json.dumps(data, indent=2))


def _output_rich(
    pid: int | None, running: bool, finished: int, staged: int, failed: int
):
    """Output status with formatting."""
    if running:
        typer.echo(f"Pipeline running (pid {pid})")
    elif pid is not None:
        typer.echo(f"Pipeline stopped (stale pid {pid})")
    else:
        typer.echo("Pipeline stopped")

    typer.echo(f"{finished} finished, {staged} staged, {failed} failed")
