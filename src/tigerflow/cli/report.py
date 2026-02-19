import json
import re
from datetime import datetime
from pathlib import Path
from typing import Annotated

import click
import typer
from rich import print
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from tigerflow.cli.metrics import _compute_duration_ms, _parse_metrics
from tigerflow.pipeline import Pipeline
from tigerflow.utils import is_process_running, read_pid_file

app = typer.Typer(invoke_without_command=True)


@app.callback()
def report(
    ctx: typer.Context,
    pipeline_dir: Annotated[
        Path,
        typer.Argument(
            help="Pipeline output directory (must contain .tigerflow)",
            show_default=False,
        ),
    ],
    output_json: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output in JSON format.",
        ),
    ] = False,
    watch: Annotated[
        bool,
        typer.Option(
            "--watch",
            "-w",
            help="Live-update the dashboard.",
        ),
    ] = False,
):
    """
    Report pipeline status, progress, and errors.

    Without a subcommand, shows a unified dashboard.
    """
    if ctx.invoked_subcommand is not None:
        # Subcommand will handle it
        ctx.ensure_object(dict)
        ctx.obj["pipeline_dir"] = pipeline_dir
        ctx.obj["output_json"] = output_json
        return

    # Unified dashboard
    _show_dashboard(pipeline_dir, output_json, watch)


def _show_dashboard(pipeline_dir: Path, output_json: bool, watch: bool = False):
    """Show unified dashboard with status, progress, and errors."""
    import time

    from tigerflow.settings import settings

    internal_dir = pipeline_dir / ".tigerflow"

    if not pipeline_dir.exists():
        if output_json:
            print(json.dumps({"error": "Directory does not exist"}))
        else:
            print(f"[red]Error: Directory does not exist: {pipeline_dir}[/red]")
        raise typer.Exit(1)

    if not internal_dir.exists():
        if output_json:
            print(json.dumps({"error": "Not a valid pipeline directory"}))
        else:
            print(
                "[red]Error: Not a valid pipeline directory (missing .tigerflow)[/red]"
            )
        raise typer.Exit(1)

    def gather_data():
        progress = Pipeline.report_progress(pipeline_dir)
        return (
            _get_status_data(internal_dir),
            _get_progress_data(progress),
            _get_errors_data(progress),
            _get_metrics_data(internal_dir),
        )

    if output_json:
        status_data, progress_data, errors_data, metrics_data = gather_data()
        print(
            json.dumps(
                {
                    "status": status_data,
                    "progress": progress_data,
                    "errors": errors_data,
                    "metrics": metrics_data,
                },
                indent=2,
                default=str,
            )
        )
    elif watch:
        console = Console(highlight=False)
        status_data, progress_data, errors_data, metrics_data = gather_data()
        panel = _build_dashboard_panel(
            status_data, progress_data, errors_data, metrics_data
        )
        with Live(panel, console=console, refresh_per_second=1) as live:
            while True:
                time.sleep(settings.pipeline_poll_interval)
                status_data, progress_data, errors_data, metrics_data = gather_data()
                panel = _build_dashboard_panel(
                    status_data, progress_data, errors_data, metrics_data
                )
                live.update(panel)
    else:
        status_data, progress_data, errors_data, metrics_data = gather_data()
        _render_dashboard(status_data, progress_data, errors_data, metrics_data)


def _get_status_data(internal_dir: Path) -> dict:
    """Get pipeline status information."""
    pid_file = internal_dir / "run.pid"
    pid = read_pid_file(pid_file)

    # Get most recent run_id from logs directory
    logs_dir = internal_dir / "logs"
    run_id = None
    if logs_dir.exists():
        log_files = sorted(logs_dir.glob("*.log"), key=lambda f: f.stat().st_mtime)
        if log_files:
            run_id = log_files[-1].stem  # e.g., "20240115-103000"

    if pid and is_process_running(pid):
        return {"running": True, "pid": pid, "run_id": run_id}
    else:
        return {"running": False, "pid": None, "run_id": run_id}


def _get_progress_data(progress) -> dict:
    """Extract progress data as a dict."""
    staged = len(progress.staged)
    finished = len(progress.finished)
    total = staged + finished
    failed = len(progress.failed)

    tasks = []
    for task in progress.tasks:
        tasks.append(
            {
                "name": task.name,
                "processed": len(task.processed) + finished,
                "ongoing": len(task.ongoing),
                "failed": len(task.failed),
                "total": staged + finished,
            }
        )

    return {
        "total": total,
        "completed": finished,
        "failed": failed,
        "tasks": tasks,
    }


def _get_errors_data(progress) -> list[dict]:
    """Extract error data as a list of dicts."""
    errors = []
    for task in progress.tasks:
        for err_file in sorted(task.failed):
            original_name = err_file.name.removesuffix(".err")
            try:
                content = err_file.read_text().strip()
                exception_line = _extract_exception_line(content) if content else ""
            except OSError:
                content = ""
                exception_line = "(could not read file)"

            errors.append(
                {
                    "task": task.name,
                    "file": original_name,
                    "error": exception_line,
                    "path": str(err_file),
                }
            )
    return errors


def _get_metrics_data(internal_dir: Path) -> dict:
    """Extract metrics summary data."""
    all_metrics = []
    for task_dir in internal_dir.iterdir():
        if (
            not task_dir.is_dir()
            or task_dir.name.startswith(".")
            or task_dir.name == "logs"
        ):
            continue

        logs_dir = task_dir / "logs"
        if not logs_dir.exists():
            continue

        for log_file in logs_dir.glob("*.log"):
            all_metrics.extend(_parse_metrics(log_file, task_dir.name))

    if not all_metrics:
        return {}

    total = len(all_metrics)
    success = sum(1 for m in all_metrics if m.get("status") == "success")
    failed = total - success
    durations = [_compute_duration_ms(m) for m in all_metrics]
    avg_duration_ms = sum(durations) / len(durations) if durations else 0
    min_duration_ms = min(durations) if durations else 0
    max_duration_ms = max(durations) if durations else 0

    return {
        "total": total,
        "success": success,
        "failed": failed,
        "avg_duration_ms": avg_duration_ms,
        "min_duration_ms": min_duration_ms,
        "max_duration_ms": max_duration_ms,
    }


def _build_dashboard_panel(
    status_data: dict, progress_data: dict, errors_data: list, metrics_data: dict
) -> Panel:
    """Build the dashboard panel."""
    run_id = status_data.get("run_id") or "unknown"

    # Parse started_at from run_id (format: YYYYMMDD-HHMMSS)
    started_at = ""
    if run_id and run_id != "unknown":
        try:
            dt = datetime.strptime(run_id, "%Y%m%d-%H%M%S")
            started_at = dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    # Status line
    if status_data["running"]:
        marker = "[sea_green3]●[/sea_green3]"
        status_text = f"running (pid {status_data['pid']})"
    else:
        marker = "[dim]○[/dim]"
        status_text = "stopped"

    lines = [""]
    lines.append(f"[bold]ID:[/bold] {run_id}")
    if started_at:
        lines.append(f"[bold]Started:[/bold] {started_at}")
    lines.append(f"[bold]Status:[/bold] {marker} {status_text}")
    lines.append("")

    # Per-task progress bars
    max_name_len = max((len(t["name"]) for t in progress_data["tasks"]), default=4)
    for task in progress_data["tasks"]:
        name = task["name"].ljust(max_name_len)
        completed = task["processed"]
        failed = task["failed"]
        ongoing = task["ongoing"]
        total = task["total"]
        if total == 0:
            total = 1  # Avoid division by zero

        bar = _make_task_progress_bar(completed=completed, total=total)

        # Status indicator
        indicators = []
        if ongoing > 0:
            indicators.append(f"{ongoing} running")
        if failed > 0:
            indicators.append(f"[bright_red]! {failed} failed[/bright_red]")
        if not indicators and completed == total:
            indicators.append("[sea_green3]✓[/sea_green3]")
        indicator = "  ".join(indicators)

        lines.append(f"  {name}  {bar} {completed}/{total}  {indicator}")

    # Metrics summary
    if metrics_data:
        lines.append("")

        def fmt_duration(ms):
            if ms >= 1000:
                return f"{ms / 1000:.1f}s"
            return f"{ms:.0f}ms"

        avg = fmt_duration(metrics_data.get("avg_duration_ms", 0))
        min_d = fmt_duration(metrics_data.get("min_duration_ms", 0))
        max_d = fmt_duration(metrics_data.get("max_duration_ms", 0))
        lines.append(
            f"[bold]Metrics:[/bold] {metrics_data['total']} processed, "
            f"{min_d} / {avg} / {max_d} (min/avg/max)"
        )

    # Errors summary
    if errors_data:
        lines.append("")
        lines.append(f"[bold]Errors:[/bold] {len(errors_data)}")
        for err in errors_data[:5]:
            lines.append(f"  [dim]{err['task']}[/dim] {err['file']}: {err['error']}")
        if len(errors_data) > 5:
            lines.append(f"  [dim]... +{len(errors_data) - 5} more[/dim]")

    lines.append("")
    content = "\n".join(lines)
    return Panel(content, title="[bold]tigerflow report[/bold]", title_align="left")


def _render_dashboard(
    status_data: dict, progress_data: dict, errors_data: list, metrics_data: dict
):
    """Render the dashboard to the console."""
    console = Console(highlight=False)
    panel = _build_dashboard_panel(
        status_data, progress_data, errors_data, metrics_data
    )
    console.print(panel)


@app.command()
def progress(ctx: typer.Context):
    """
    Report progress across pipeline tasks.
    """
    pipeline_dir = ctx.obj["pipeline_dir"]
    output_json = ctx.obj["output_json"]

    prog = Pipeline.report_progress(pipeline_dir)
    progress_data = _get_progress_data(prog)

    if output_json:
        print(json.dumps(progress_data, indent=2))
    else:
        table = Table()
        table.add_column("Task")
        table.add_column("Processed", justify="right", style="green")
        table.add_column("Ongoing", justify="right", style="yellow")
        table.add_column("Failed", justify="right", style="red")

        for task in progress_data["tasks"]:
            table.add_row(
                task["name"],
                str(task["processed"]),
                str(task["ongoing"]),
                str(task["failed"]),
            )

        print(table)

        total = progress_data["total"]
        completed = progress_data["completed"] + progress_data["failed"]
        if total > 0:
            print(_make_progress_bar(current=completed, total=total))


@app.command()
def errors(
    ctx: typer.Context,
    task_name: Annotated[
        str,
        typer.Option(
            "--task",
            help="Show failed files for this task only.",
            show_default="all",
        ),
    ] = "*",
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            "-v",
            help="Show full tracebacks instead of just the exception line.",
        ),
    ] = False,
):
    """
    Report failed files for pipeline tasks.
    """
    pipeline_dir = ctx.obj["pipeline_dir"]
    output_json = ctx.obj["output_json"]

    prog = Pipeline.report_progress(pipeline_dir)

    available_tasks = {task.name for task in prog.tasks}
    if task_name != "*" and task_name not in available_tasks:
        if output_json:
            print(json.dumps({"error": f"Task '{task_name}' not found"}))
        else:
            print(
                f"[red]Error: Task '{task_name}' not found. "
                f"Available tasks: {', '.join(available_tasks)}[/red]"
            )
        raise typer.Exit(1)

    errors_list = []
    for task in prog.tasks:
        if task_name in ("*", task.name):
            for err_file in sorted(task.failed):
                original_name = err_file.name.removesuffix(".err")
                try:
                    content = err_file.read_text().strip()
                    if not content:
                        content = "(empty error file)"
                except OSError as e:
                    content = f"(could not read file: {e})"

                errors_list.append(
                    {
                        "task": task.name,
                        "file": original_name,
                        "error": _extract_exception_line(content),
                        "traceback": content,
                        "path": str(err_file),
                    }
                )

    if output_json:
        # For JSON, include both error line and full traceback
        print(json.dumps(errors_list, indent=2))
    elif errors_list:
        sections = []
        for err in errors_list:
            if verbose:
                header = f"{'─' * 60}\n[{err['task']}] {err['file']}\n{'─' * 60}"
                sections.append(f"{header}\n{err['traceback']}\n")
            else:
                sections.append(f"[{err['task']}] {err['file']}: {err['error']}")
        click.echo_via_pager("\n".join(sections))
    else:
        print("[sea_green3]No failed files found.[/sea_green3]")


def _extract_exception_line(traceback_text: str) -> str:
    """Extract just the exception line from a traceback."""
    exception_pattern = re.compile(r"^[\w.]+Error|^[\w.]+Exception|^[\w.]+Warning")

    lines = traceback_text.strip().splitlines()

    for line in reversed(lines):
        line = line.strip()
        if exception_pattern.match(line):
            return line

    for line in reversed(lines):
        if line.strip():
            return line.strip()

    return "(no error message found)"


def _make_progress_bar(*, current: int, total: int, length: int = 30) -> str:
    """Returns a string with a fixed-width static progress bar."""
    if total == 0:
        return ""
    filled = int(length * current / total)
    empty = length - filled
    bar = f"[bold green]{'█' * filled}[/bold green][dim]{'░' * empty}[/dim]"
    percentage = f"{(current / total) * 100:>5.1f}%"
    return f"{bar} {current}/{total} ({percentage})"


def _make_task_progress_bar(*, completed: int, total: int, length: int = 20) -> str:
    """Returns a compact progress bar for a single task."""
    if total == 0:
        return "[dim]" + "░" * length + "[/dim]"
    filled = int(length * completed / total)
    empty = length - filled
    return f"[sea_green3]{'█' * filled}[/sea_green3][dim]{'░' * empty}[/dim]"
