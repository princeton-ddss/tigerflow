import typer

from .report import app as report_app
from .run import run as run_func

app = typer.Typer()
app.command(name="run")(run_func)
app.add_typer(report_app, name="report")


@app.callback()
def callback():
    """
    A pipeline framework optimized for HPC with Slurm integration.
    """
