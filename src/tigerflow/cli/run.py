import os
import sys
from pathlib import Path
from typing import Annotated

import typer

from tigerflow.pipeline import Pipeline
from tigerflow.utils import check_and_cleanup_stale_pid


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
    background: Annotated[
        bool,
        typer.Option(
            "--background",
            "-b",
            help="Run the pipeline in the background, detached from the terminal.",
        ),
    ] = False,
):
    """
    Run a pipeline based on the given specification.
    """
    # Resolve paths early (before fork to ensure consistency)
    output_dir = output_dir.resolve()
    internal_dir = output_dir / ".tigerflow"
    pid_file = internal_dir / "run.pid"
    log_file = internal_dir / "run.log"

    internal_dir.mkdir(parents=True, exist_ok=True)

    if check_and_cleanup_stale_pid(pid_file):
        pid = int(pid_file.read_text().strip())
        typer.echo(f"Error: Pipeline is already running (pid {pid})", err=True)
        raise typer.Exit(1)

    if background:
        _run_in_background(
            config_file=config_file,
            input_dir=input_dir,
            output_dir=output_dir,
            idle_timeout=idle_timeout,
            delete_input=delete_input,
            pid_file=pid_file,
            log_file=log_file,
        )
    else:
        pipeline = Pipeline(
            config_file=config_file,
            input_dir=input_dir,
            output_dir=output_dir,
            idle_timeout=idle_timeout,
            delete_input=delete_input,
            pid_file=pid_file,
        )
        pipeline.run()


def _run_in_background(
    *,
    config_file: Path,
    input_dir: Path,
    output_dir: Path,
    idle_timeout: int,
    delete_input: bool,
    pid_file: Path,
    log_file: Path,
):
    """
    Fork the process, detach from terminal, and run the pipeline in the background.
    """
    pid = os.fork()

    if pid > 0:
        # Parent process: print PID and exit immediately
        typer.echo(f"Started (pid {pid})")
        raise typer.Exit(0)

    # Child process: detach from terminal
    os.setsid()

    # Redirect stdout/stderr to log file
    with open(log_file, "a") as log:
        os.dup2(log.fileno(), sys.stdout.fileno())
        os.dup2(log.fileno(), sys.stderr.fileno())

        pipeline = Pipeline(
            config_file=config_file,
            input_dir=input_dir,
            output_dir=output_dir,
            idle_timeout=idle_timeout,
            delete_input=delete_input,
            pid_file=pid_file,
        )
        pipeline.run()
