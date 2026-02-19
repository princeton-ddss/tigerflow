import json
from pathlib import Path
from typing import Annotated

import typer

from tigerflow.models import PipelineOutput
from tigerflow.utils import is_process_running, read_pid_file


def status(
    output_dir: Annotated[
        Path,
        typer.Argument(
            help="Pipeline output directory (must contain .tigerflow)",
            show_default=False,
        ),
    ],
    as_json: Annotated[
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
    output = PipelineOutput(output_dir)

    try:
        output.validate()
    except FileNotFoundError as e:
        _output_error(str(e), as_json)
        raise typer.Exit(1)

    pid = read_pid_file(output.pid_file)
    running = pid is not None and is_process_running(pid)

    progress = output.report_progress()
    finished = len(progress.finished)
    failed = len(progress.failed)
    staged = len(progress.staged) - failed  # Failed files remain in symlinks

    if as_json:
        _output_json(pid, running, finished, staged, failed)
    else:
        _output_rich(pid, running, finished, staged, failed)

    # Return appropriate exit code: 0 = running, 1 = not running
    if not running:
        raise typer.Exit(1)


def _output_error(message: str, as_json: bool):
    """Output an error message in the appropriate format."""
    if as_json:
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
