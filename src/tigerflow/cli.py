from pathlib import Path

import typer
from rich import print
from rich.table import Table
from typing_extensions import Annotated

from tigerflow.pipeline import Pipeline

app = typer.Typer()


@app.command()
def run(
    config_file: Annotated[
        Path,
        typer.Argument(
            help="Configuration file",
            show_default=False,
        ),
    ],
    input_dir: Annotated[
        Path,
        typer.Argument(
            help="Directory containing input data for the pipeline",
            show_default=False,
        ),
    ],
    output_dir: Annotated[
        Path,
        typer.Argument(
            help="Directory for storing pipeline outputs and internal data",
            show_default=False,
        ),
    ],
    delete_input: Annotated[
        bool,
        typer.Option(
            "--delete-input",
            help="Delete input files after pipeline processing.",
        ),
    ] = False,
):
    """
    Run a pipeline based on the given specification.
    """
    pipeline = Pipeline(
        config_file=config_file,
        input_dir=input_dir,
        output_dir=output_dir,
        delete_input=delete_input,
    )
    pipeline.run()


@app.command()
def report(
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
        current=progress.n_finished,
        total=progress.n_staged + progress.n_finished,
    )

    table = Table()
    table.add_column("Task")
    table.add_column("Processed", justify="right", style="blue")
    table.add_column("Ongoing", justify="right", style="yellow")
    table.add_column("Failed", justify="right", style="red")
    for task in progress.tasks:
        table.add_row(
            task.name,
            str(task.n_processed),
            str(task.n_ongoing),
            str(task.n_failed),
        )

    print(table)
    print("[bold]COMPLETED[/bold]:", bar)


@app.callback()
def callback():
    """
    A pipeline framework optimized for HPC with Slurm integration.
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
