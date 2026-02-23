from typing import Annotated

import typer

from tigerflow.utils import get_version

from .report import app as report_app
from .run import run as run_func
from .status import status as status_func
from .stop import stop as stop_func
from .tasks import app as tasks_app

app = typer.Typer()
app.command(name="run")(run_func)
app.command(name="status")(status_func)
app.command(name="stop")(stop_func)
app.add_typer(report_app, name="report")
app.add_typer(tasks_app, name="tasks")


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
