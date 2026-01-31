import os
import signal
from pathlib import Path
from typing import Annotated

import typer

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
    output_dir = output_dir.resolve()
    internal_dir = output_dir / ".tigerflow"
    pid_file = internal_dir / "run.pid"

    if not output_dir.exists():
        typer.echo("Error: Output directory does not exist", err=True)
        raise typer.Exit(1)

    if not internal_dir.exists():
        typer.echo("Error: Not a valid pipeline directory (missing .tigerflow)", err=True)
        raise typer.Exit(1)

    pid = read_pid_file(pid_file)
    if pid is None:
        typer.echo("Pipeline is not running (no PID file)")
        raise typer.Exit(0)

    if not is_process_running(pid):
        typer.echo(f"Pipeline is not running (stale PID file, pid {pid})")
        pid_file.unlink(missing_ok=True)
        raise typer.Exit(0)

    sig = signal.SIGKILL if force else signal.SIGTERM
    sig_name = "SIGKILL" if force else "SIGTERM"

    try:
        os.kill(pid, sig)
        typer.echo(f"Sent {sig_name} to pipeline (pid {pid})")
    except ProcessLookupError:
        typer.echo(f"Pipeline already stopped (pid {pid})")
        pid_file.unlink(missing_ok=True)
    except PermissionError:
        typer.echo(f"Error: Permission denied to stop pipeline (pid {pid})", err=True)
        raise typer.Exit(1)
