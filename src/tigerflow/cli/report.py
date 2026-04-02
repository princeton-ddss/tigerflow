import json
import time
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.live import Live
from rich.panel import Panel

from tigerflow.models import FileMetrics, PipelineOutput, PipelineReport


def _make_sparkline(values: list[int | float], width: int = 20) -> str:
    """Create a sparkline using Unicode blocks ▁▂▃▄▅▆▇█"""
    if not values:
        return " " * width
    blocks = "▁▂▃▄▅▆▇█"
    min_val, max_val = min(values), max(values)
    val_range = max_val - min_val
    scale = 7 / val_range if val_range else 0
    recent = values[-width:]
    sparkline = "".join(
        blocks[min(7, int((v - min_val) * scale))] if scale else blocks[4]
        for v in recent
    )
    return sparkline.rjust(width)


def _compute_task_metrics(task_metrics: list[FileMetrics]) -> dict:
    """Compute metrics for a single task. Summary uses successes only."""
    if not task_metrics:
        return {}

    successes = [m for m in task_metrics if m.status == "success"]
    durations = [m.duration_ms for m in successes]
    return {
        "count": len(successes),
        "avg_ms": sum(durations) / len(durations) if durations else 0,
        "min_ms": min(durations) if durations else 0,
        "max_ms": max(durations) if durations else 0,
        "durations": durations,
        "files": [
            {
                "file": m.file,
                "started_at": m.started_at.isoformat(),
                "finished_at": m.finished_at.isoformat(),
                "duration_ms": m.duration_ms,
                "status": m.status,
            }
            for m in task_metrics
        ],
    }


def _compute_metrics_summary(metrics: dict[str, list[FileMetrics]]) -> dict:
    """Compute summary statistics from metrics."""
    successes = [
        m
        for task_metrics in metrics.values()
        for m in task_metrics
        if m.status == "success"
    ]

    if not successes:
        return {}

    durations = [m.duration_ms for m in successes]
    return {
        "total": len(successes),
        "avg_duration_ms": sum(durations) / len(durations),
        "min_duration_ms": min(durations),
        "max_duration_ms": max(durations),
    }


def _build_dashboard_panel(report: PipelineReport) -> Panel:
    """Build the dashboard panel."""

    def fmt_duration(ms: float) -> str:
        if ms >= 1000:
            return f"{ms / 1000:.1f}s"
        return f"{ms:.0f}ms"

    metrics_data = _compute_metrics_summary(report.metrics)

    lines = [""]

    # === Status ===
    if report.status == "running":
        marker = "[sea_green3]●[/sea_green3]"
        status_text = f"running (pid {report.pid})"
    else:
        marker = "[dim]○[/dim]"
        status_text = "stopped"

    lines.append(f"[bold]Status:[/bold]  {marker} {status_text}")
    lines.append(f"[bold]Output:[/bold]  {report.output_dir}")
    lines.append("")

    # === Progress ===
    flow_parts = []
    if report.staged is not None and report.staged > 0:
        flow_parts.append(f"[dim]○ {report.staged} staged[/dim]")
    if report.in_progress > 0:
        flow_parts.append(f"[yellow1]◐ {report.in_progress} in progress[/yellow1]")
    terminal_parts = [f"[sea_green3]✓ {report.processed} processed[/sea_green3]"]
    if report.failed > 0:
        terminal_parts.append(f"[red]✗ {report.failed} failed[/red]")
    flow_parts.append(", ".join(terminal_parts))
    lines.append(f"[bold]Progress:[/bold] {' → '.join(flow_parts)}")

    # Per-task progress bars (run-scoped)
    if report.tasks:
        lines.append("")
        max_name_len = max((len(task.name) for task in report.tasks), default=4)
        available = (
            report.processed + report.in_progress + (report.staged or 0) + report.failed
        )
        for task in report.tasks:
            name = task.name.ljust(max_name_len)

            bar = _make_progress_bar(
                processed=task.processed,
                ongoing=0,  # No yellow - just show completed work
                failed=task.failed,
                total=available if available > 0 else 1,
            )

            error_part = f", {task.failed} failed" if task.failed > 0 else ""
            lines.append(f"  {name}  {bar} {task.processed} / {available}{error_part}")

            # Next task's available = this task's processed (only successes move on)
            available = task.processed
    lines.append("")

    # Metrics with per-task sparklines (for this run)
    if metrics_data:
        lines.append("[bold]Metrics:[/bold]")
        lines.append("")

        # Per-task sparkline with min/avg/max
        if report.tasks:
            max_name_len = max((len(task.name) for task in report.tasks), default=4)
            for task in report.tasks:
                task_metrics = report.metrics.get(task.name, [])
                if task_metrics:
                    tm = _compute_task_metrics(task_metrics)
                    if not tm["durations"]:
                        continue
                    sparkline = _make_sparkline(tm["durations"])
                    min_d = fmt_duration(tm["min_ms"])
                    avg_d = fmt_duration(tm["avg_ms"])
                    max_d = fmt_duration(tm["max_ms"])
                    name = task.name.ljust(max_name_len)
                    lines.append(
                        f"  {name}  [dim]{sparkline}[/dim]  {min_d} – {max_d} ({avg_d} avg)"
                    )
            lines.append("")

    # Errors summary (for this run)
    total_errors = sum(len(errs) for errs in report.errors.values())
    if total_errors > 0:
        lines.append(f"[bold]Errors:[/bold] {total_errors}")
        shown = 0
        for task_name, task_errors in report.errors.items():
            for err in task_errors:
                if shown >= 5:
                    break
                err_detail = (
                    f"{err.exception_type}: {err.message}"
                    if err.exception_type
                    else err.message or "Unknown error"
                )
                lines.append(
                    f"  [dim]{task_name}[/dim]  {err.file}  [red]{err_detail}[/red]"
                )
                shown += 1
            if shown >= 5:
                break
        if total_errors > 5:
            lines.append(f"  [dim]... +{total_errors - 5} more[/dim]")
        lines.append("")

    content = "\n".join(lines)
    return Panel(content, title="[bold]tigerflow report[/bold]", title_align="left")


def _make_progress_bar(
    *, processed: int, ongoing: int, failed: int, total: int, length: int = 40
) -> str:
    """Returns a colored progress bar: green (processed), yellow (ongoing), red (failed)."""
    if total == 0:
        total = 1
    green = int(length * processed / total)
    yellow = int(length * ongoing / total)
    red = int(length * failed / total)
    filled = green + yellow + red
    empty = length - filled

    # When all files are accounted for, eliminate rounding gaps
    # by adding extra blocks to the largest segment
    if processed + ongoing + failed >= total and empty > 0:
        if green >= yellow and green >= red:
            green += empty
        elif yellow >= red:
            yellow += empty
        else:
            red += empty
        empty = 0

    return (
        f"[sea_green3]{'━' * green}[/sea_green3]"
        f"[yellow]{'━' * yellow}[/yellow]"
        f"[bright_red]{'━' * red}[/bright_red]"
        f"[dim]{'─' * empty}[/dim]"
    )


def report(
    output_dir: Annotated[
        Path,
        typer.Argument(
            help="Pipeline output directory (must contain .tigerflow)",
            show_default=False,
        ),
    ],
    use_json: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output in JSON format.",
        ),
    ] = False,
    include: Annotated[
        str | None,
        typer.Option(
            "--include",
            help="Sections to include in JSON (comma-separated: status,progress,metrics,errors).",
        ),
    ] = None,
    watch: Annotated[
        bool,
        typer.Option(
            "--watch",
            "-w",
            help="Continuously update the display.",
        ),
    ] = False,
):
    """
    Report pipeline status, progress, metrics, and errors.
    """
    output = PipelineOutput(output_dir.resolve())

    try:
        output.validate()
    except FileNotFoundError as e:
        if use_json:
            print(json.dumps({"error": str(e)}))
        else:
            Console().print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

    if watch and use_json:
        Console().print("[red]Error: --watch cannot be used with --json[/red]")
        raise typer.Exit(1)

    if watch:
        console = Console(highlight=False)
        with Live(console=console, refresh_per_second=1) as live:
            try:
                while True:
                    pipeline_report = output.report()
                    panel = _build_dashboard_panel(pipeline_report)
                    live.update(panel)
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
    elif use_json:
        pipeline_report = output.report()

        sections = (
            {s.strip().lower() for s in include.split(",")}
            if include
            else {"status", "progress", "metrics", "errors"}
        )

        result: dict = {}
        if "status" in sections:
            result["status"] = {
                "running": pipeline_report.status == "running",
                "pid": pipeline_report.pid,
            }
        if "progress" in sections:
            result["progress"] = {
                "pipeline": {
                    "finished": pipeline_report.processed,
                    "in_progress": pipeline_report.in_progress,
                    "staged": pipeline_report.staged,
                    "errored": pipeline_report.failed,
                },
                "tasks": [
                    {
                        "name": t.name,
                        "processed": t.processed,
                        "staged": t.staged,
                        "failed": t.failed,
                    }
                    for t in pipeline_report.tasks
                ],
            }
        if "metrics" in sections:
            result["metrics"] = {
                name: _compute_task_metrics(file_metrics)
                for name, file_metrics in pipeline_report.metrics.items()
            }
        if "errors" in sections:
            result["errors"] = {
                name: [
                    {
                        "file": e.file,
                        "path": e.path,
                        "timestamp": e.timestamp.isoformat() if e.timestamp else None,
                        "exception_type": e.exception_type,
                        "message": e.message,
                        "traceback": e.traceback,
                    }
                    for e in errs
                ]
                for name, errs in pipeline_report.errors.items()
            }

        print(json.dumps(result, indent=2, default=str))
    else:
        pipeline_report = output.report()
        console = Console(highlight=False)
        panel = _build_dashboard_panel(pipeline_report)
        console.print(panel)
