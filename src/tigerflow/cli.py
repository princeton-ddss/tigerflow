import typer
from rich import print
from typing_extensions import Annotated

app = typer.Typer()


@app.command()
def run(
    config_file: Annotated[
        str,
        typer.Argument(
            help="Configuration file",
            show_default=False,
        ),
    ],
    input_dir: Annotated[
        str,
        typer.Argument(
            help="Directory containing input data for the pipeline",
            show_default=False,
        ),
    ],
    output_dir: Annotated[
        str,
        typer.Argument(
            help="Directory to store pipeline scripts and intermediate data",
            show_default=False,
        ),
    ],
):
    """
    Run a pipeline based on the given specification.
    """
    print(config_file)
    print(input_dir)
    print(output_dir)


@app.callback()
def callback():
    """
    A pipeline framework optimized for HPC with Slurm integration.
    """
