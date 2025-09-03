from pathlib import Path

import typer
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
):
    """
    Run a pipeline based on the given specification.
    """
    pipeline = Pipeline(config_file, input_dir, output_dir)
    pipeline.run()


@app.callback()
def callback():
    """
    A pipeline framework optimized for HPC with Slurm integration.
    """
