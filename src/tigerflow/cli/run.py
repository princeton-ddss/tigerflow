from pathlib import Path

import typer
from typing_extensions import Annotated

from tigerflow.pipeline import Pipeline


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
    idle_timeout: Annotated[
        int,
        typer.Option(
            help="Terminate after this many minutes of inactivity.",
        ),
    ] = 10,
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
        idle_timeout=idle_timeout,
        delete_input=delete_input,
    )
    pipeline.run()
