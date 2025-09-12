from pathlib import Path

import click
import typer
from rich import print
from rich.table import Table
from typing_extensions import Annotated

from tigerflow.pipeline import Pipeline

app = typer.Typer()


@app.command()
def progress(
    pipeline_dir: Annotated[
        Path,
        typer.Argument(
            help="Pipeline output directory (must contain .tigerflow)",
            show_default=False,
        ),
    ],
):
    """
    Report progress across pipeline tasks.
    """
    progress = Pipeline.report_progress(pipeline_dir)

    bar = _make_progress_bar(
        current=len(progress.finished),
        total=len(progress.staged) + len(progress.finished),
    )

    table = Table()
    table.add_column("Task")
    table.add_column("Processed", justify="right", style="blue")
    table.add_column("Ongoing", justify="right", style="yellow")
    table.add_column("Failed", justify="right", style="red")
    for task in progress.tasks:
        table.add_row(
            task.name,
            str(len(task.processed) + len(progress.finished)),
            str(len(task.ongoing)),
            str(len(task.failed)),
        )

    print(table)
    print("[bold]COMPLETED[/bold]:", bar)


@app.command()
def errors(
    pipeline_dir: Annotated[
        Path,
        typer.Argument(
            help="Pipeline output directory (must contain .tigerflow)",
            show_default=False,
        ),
    ],
    task_name: Annotated[
        str,
        typer.Option(
            "--task",
            help="Show failed files for this task only.",
            show_default="all",
        ),
    ] = "*",
):
    """
    Report failed files for pipeline tasks.
    """
    progress = Pipeline.report_progress(pipeline_dir)

    available_tasks = {task.name for task in progress.tasks}
    if task_name != "*" and task_name not in available_tasks:
        print(
            f"[red]Error: Task '{task_name}' not found. "
            f"Available tasks: {', '.join(available_tasks)}[/red]"
        )
        raise typer.Exit(1)

    error_sections = []
    for task in progress.tasks:
        if task_name in ("*", task.name):
            if task.failed:
                section = f"[{task.name}] {len(task.failed)} failed files (open to view errors):\n"
                for file in sorted(task.failed):
                    section += f"  {file}\n"
                error_sections.append(section)

    if error_sections:
        click.echo_via_pager("\n".join(error_sections))
    else:
        print("[green]No failed files found.[/green]")


@app.callback()
def callback():
    """
    Report different types of information about the given pipeline.
    """


def _make_progress_bar(*, current: int, total: int, length: int = 30) -> str:
    """
    Returns a string with a fixed-width static progress bar.
    """
    filled = int(length * current / total)
    empty = length - filled
    bar = f"[bold green]{'█' * filled}[/bold green][dim]{'░' * empty}[/dim]"
    percentage = f"{(current / total) * 100:>5.1f}%"
    return f"{bar} {current}/{total} ({percentage})"
