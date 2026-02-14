import json
from pathlib import Path
from typing import Annotated

import typer
from rich import print
from rich.table import Table

from tigerflow.pipeline import Pipeline
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

    try:
        progress = Pipeline.report_progress(output_dir)
    except Exception as e:
        _output_error(f"Failed to read progress: {e}", output_json)
        raise typer.Exit(1)

    if output_json:
        _output_json(pid, running, progress)
    else:
        _output_rich(pid, running, progress)

    # Return appropriate exit code: 0 = running, 1 = not running
    if not running:
        raise typer.Exit(1)


def _output_error(message: str, output_json: bool):
    """Output an error message in the appropriate format."""
    if output_json:
        print(json.dumps({"error": message}))
    else:
        print(f"[red]Error: {message}[/red]")


def _output_json(pid: int | None, running: bool, progress):
    """Output status in JSON format."""
    data = {
        "pid": pid,
        "running": running,
        "staged": len(progress.staged),
        "finished": len(progress.finished),
        "failed": len(progress.failed),
        "tasks": [
            {
                "name": task.name,
                "processed": len(task.processed),
                "ongoing": len(task.ongoing),
                "failed": len(task.failed),
            }
            for task in progress.tasks
        ],
    }
    print(json.dumps(data, indent=2))


def _output_rich(pid: int | None, running: bool, progress):
    """Output status with rich formatting."""
    # Status header
    if running:
        print(f"[bold green]Pipeline running[/bold green] (pid {pid})")
    elif pid is not None:
        print(f"[bold yellow]Pipeline stopped[/bold yellow] (stale pid {pid})")
    else:
        print("[bold yellow]Pipeline not running[/bold yellow]")

    print()

    # Progress summary
    total = len(progress.staged) + len(progress.finished)
    print(
        f"Files: {len(progress.finished)}/{total} finished, {len(progress.failed)} failed"
    )

    # Task table
    if progress.tasks:
        print()
        table = Table()
        table.add_column("Task")
        table.add_column("Processed", justify="right", style="green")
        table.add_column("Ongoing", justify="right", style="yellow")
        table.add_column("Failed", justify="right", style="red")

        for task in progress.tasks:
            table.add_row(
                task.name,
                str(len(task.processed)),
                str(len(task.ongoing)),
                str(len(task.failed)),
            )

        print(table)
