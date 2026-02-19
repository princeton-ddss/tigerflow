import json
from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer
from rich import print
from rich.table import Table


def _compute_duration_ms(m: dict) -> float:
    """Compute duration in ms from started_at and finished_at timestamps."""
    started = m.get("started_at")
    finished = m.get("finished_at")
    if not started or not finished:
        return 0.0
    try:
        start_dt = datetime.fromisoformat(started)
        end_dt = datetime.fromisoformat(finished)
        return (end_dt - start_dt).total_seconds() * 1000
    except (ValueError, TypeError):
        return 0.0


def _format_timestamp(iso_str: str) -> str:
    """Format ISO timestamp for display (local time, no timezone)."""
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str).astimezone()
        return dt.strftime("%H:%M:%S")
    except (ValueError, TypeError):
        return iso_str


def metrics(
    output_dir: Annotated[
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
            help="Output metrics in JSON format.",
        ),
    ] = False,
    task: Annotated[
        str | None,
        typer.Option(
            "--task",
            "-t",
            help="Filter to a specific task name.",
        ),
    ] = None,
):
    """
    Show timing metrics for processed files.
    """
    output_dir = output_dir.resolve()
    internal_dir = output_dir / ".tigerflow"

    if not output_dir.exists():
        _output_error("Output directory does not exist", output_json)
        raise typer.Exit(1)

    if not internal_dir.exists():
        _output_error(
            "Not a valid pipeline directory (missing .tigerflow)", output_json
        )
        raise typer.Exit(1)

    # Collect metrics from all task log files (in .tigerflow/{task}/logs/)
    all_metrics = []
    for task_dir in internal_dir.iterdir():
        if not task_dir.is_dir():
            continue

        if task and task_dir.name != task:
            continue

        logs_dir = task_dir / "logs"
        if not logs_dir.exists():
            continue

        for log_file in logs_dir.glob("*.err"):
            all_metrics.extend(_parse_metrics(log_file, task_dir.name))

    if output_json:
        _output_json_format(all_metrics)
    else:
        _output_rich(all_metrics)


def _parse_metrics(log_file: Path, task_name: str) -> list[dict]:
    """Parse metrics from a log file."""
    metrics = []
    try:
        with open(log_file) as f:
            for line in f:
                if '"_metrics"' in line:
                    # Extract JSON from log line (after the log prefix)
                    try:
                        # Find the JSON object in the line
                        start = line.find("{")
                        if start != -1:
                            data = json.loads(line[start:])
                            data["task"] = task_name
                            metrics.append(data)
                    except json.JSONDecodeError:
                        continue
    except OSError:
        pass
    return metrics


def _output_error(message: str, output_json: bool):
    """Output an error message in the appropriate format."""
    if output_json:
        print(json.dumps({"error": message}))
    else:
        print(f"[red]Error: {message}[/red]")


def _output_json_format(metrics: list[dict]):
    """Output metrics in JSON format."""
    print(json.dumps(metrics, indent=2))


def _output_rich(metrics: list[dict]):
    """Output metrics with rich formatting."""
    if not metrics:
        print("[yellow]No metrics found[/yellow]")
        return

    # Summary stats
    total = len(metrics)
    success = sum(1 for m in metrics if m.get("status") == "success")
    failed = total - success
    durations = [_compute_duration_ms(m) for m in metrics]
    avg_duration = sum(durations) / len(durations) if durations else 0

    print("[bold]Metrics Summary[/bold]")
    print(f"Total files: {total} ({success} success, {failed} failed)")
    print(f"Average duration: {avg_duration:.1f}ms")
    print()

    # Table
    table = Table()
    table.add_column("Task")
    table.add_column("File")
    table.add_column("Started")
    table.add_column("Finished")
    table.add_column("Duration (ms)", justify="right")
    table.add_column("Status")

    for m in sorted(metrics, key=lambda x: x.get("started_at", "")):
        status_style = "green" if m.get("status") == "success" else "red"
        duration_ms = _compute_duration_ms(m)
        started = _format_timestamp(m.get("started_at", ""))
        finished = _format_timestamp(m.get("finished_at", ""))
        table.add_row(
            m.get("task", ""),
            m.get("file", ""),
            started,
            finished,
            f"{duration_ms:.1f}",
            f"[{status_style}]{m.get('status', 'unknown')}[/{status_style}]",
        )

    print(table)
