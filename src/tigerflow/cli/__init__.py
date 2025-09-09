import typer
from typing_extensions import Annotated

from tigerflow.utils import get_version

from .report import app as report_app
from .run import run as run_func

app = typer.Typer()
app.command(name="run")(run_func)
app.add_typer(report_app, name="report")


def _version_callback(value: bool):
    if value:
        print(get_version())
        raise typer.Exit()


@app.callback()
def callback(
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            callback=_version_callback,
        ),
    ] = None,
):
    """
    A pipeline framework optimized for HPC with Slurm integration.
    """
