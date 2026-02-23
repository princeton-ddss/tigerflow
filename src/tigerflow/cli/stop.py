import os
import signal
from pathlib import Path
from typing import Annotated

import typer

from tigerflow.models import PipelineOutput
from tigerflow.utils import is_process_running, read_pid_file


def stop(
    output_dir: Annotated[
        Path,
        typer.Argument(
            help="Pipeline output directory (must contain .tigerflow)",
            show_default=False,
        ),
    ],
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            "-f",
            help="Send SIGKILL instead of SIGTERM for immediate termination.",
        ),
    ] = False,
):
    """
    Stop a running pipeline.
    """
    output = PipelineOutput(output_dir)

    try:
        output.validate()
    except FileNotFoundError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)

    pid = read_pid_file(output.pid_file)
    if pid is None:
        typer.echo("Pipeline is not running (no PID file)")
        raise typer.Exit(0)

    if not is_process_running(pid):
        typer.echo(f"Pipeline is not running (stale PID file, pid {pid})")
        output.pid_file.unlink(missing_ok=True)
        raise typer.Exit(0)

    sig = signal.SIGKILL if force else signal.SIGTERM

    try:
        os.kill(pid, sig)
        typer.echo(f"Sent {sig.name} to pipeline (pid {pid})")
    except ProcessLookupError:
        typer.echo(f"Pipeline already stopped (pid {pid})")
        output.pid_file.unlink(missing_ok=True)
    except PermissionError:
        typer.echo(f"Error: Permission denied to stop pipeline (pid {pid})", err=True)
        raise typer.Exit(1)
