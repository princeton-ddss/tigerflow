import os
import sys
from pathlib import Path
from typing import Annotated

import typer

from tigerflow.models import PipelineOutput
from tigerflow.pipeline import Pipeline
from tigerflow.utils import has_running_pid, read_pid_file


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
    output = PipelineOutput(output_dir)
    output.create()

    if has_running_pid(output.pid_file):
        pid = read_pid_file(output.pid_file)
        typer.echo(f"Error: Pipeline is already running (pid {pid})", err=True)
        raise typer.Exit(1)

    output.pid_file.unlink(missing_ok=True)  # clean up stale file if present

    if background:
        _run_in_background(
            config_file=config_file,
            input_dir=input_dir,
            output=output,
            idle_timeout=idle_timeout,
            delete_input=delete_input,
        )
    else:
        pipeline = Pipeline(
            config_file=config_file,
            input_dir=input_dir,
            output_dir=output.root,
            idle_timeout=idle_timeout,
            delete_input=delete_input,
            pid_file=output.pid_file,
        )
        pipeline.run()


def _run_in_background(
    *,
    config_file: Path,
    input_dir: Path,
    output: PipelineOutput,
    idle_timeout: int,
    delete_input: bool,
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
    with open(output.log_file, "a") as log:
        os.dup2(log.fileno(), sys.stdout.fileno())
        os.dup2(log.fileno(), sys.stderr.fileno())

        pipeline = Pipeline(
            config_file=config_file,
            input_dir=input_dir,
            output_dir=output.root,
            idle_timeout=idle_timeout,
            delete_input=delete_input,
            pid_file=output.pid_file,
        )
        pipeline.run()
