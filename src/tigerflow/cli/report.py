import json as json_lib
import time
from datetime import datetime
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
    # Take the last `width` values
    recent = values[-width:]
    sparkline = "".join(
        blocks[min(7, int((v - min_val) * scale))] if scale else blocks[4]
        for v in recent
    )
    # Pad left to ensure consistent width
    return sparkline.rjust(width)


def _compute_task_metrics(task_metrics: list[FileMetrics]) -> dict:
    """Compute metrics summary for a single task."""
    if not task_metrics:
        return {}

    durations = [m.duration_ms for m in task_metrics]
    return {
        "count": len(task_metrics),
        "avg_ms": sum(durations) / len(durations),
        "min_ms": min(durations),
        "max_ms": max(durations),
        "durations": durations,
    }


def _compute_metrics_summary(metrics: dict[str, list[FileMetrics]]) -> dict:
    """Compute summary statistics from metrics."""
    all_metrics = [m for task_metrics in metrics.values() for m in task_metrics]

    if not all_metrics:
        return {}

    total = len(all_metrics)
    success = sum(1 for m in all_metrics if m.status == "success")
    failed = total - success
    durations = [m.duration_ms for m in all_metrics]
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


def _build_dashboard_panel(report: PipelineReport) -> Panel:
    """Build the dashboard panel."""

    def fmt_duration(ms: float) -> str:
        if ms >= 1000:
            return f"{ms / 1000:.1f}s"
        return f"{ms:.0f}ms"

    # Compute metrics summary once
    metrics_data = _compute_metrics_summary(report.metrics)

    lines = [""]

    # === Run state ===
    run_id = report.run_id or "unknown"

    # Parse started_at from run_id (format: YYYYMMDD-HHMMSS)
    started_at = ""
    if run_id and run_id != "unknown":
        try:
            dt = datetime.strptime(run_id, "%Y%m%d-%H%M%S")
            started_at = dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

    # Status line
    if report.status == "running":
        marker = "[sea_green3]●[/sea_green3]"
        status_text = f"running (pid {report.pid})"
    else:
        marker = "[dim]○[/dim]"
        status_text = "stopped"

    lines.append(f"[bold]Status:[/bold]  {marker} {status_text}")
    if started_at:
        lines.append(f"[bold]Started:[/bold] {started_at}")
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

    # Per-task progress bars
    total = report.processed + report.in_progress + report.failed
    if report.staged is not None:
        total += report.staged

    if report.tasks:
        lines.append("")
        max_name_len = max((len(task.name) for task in report.tasks), default=4)
        available = total  # Root task has all files available
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
                lines.append(f"  [dim]{task_name}[/dim] {err.file}: {err.message}")
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
    json: Annotated[
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
    run: Annotated[
        str | None,
        typer.Option(
            "--run",
            help="Filter to specific run ID.",
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
        if json:
            print(json_lib.dumps({"error": str(e)}))
        else:
            Console().print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

    if watch and json:
        Console().print("[red]Error: --watch cannot be used with --json[/red]")
        raise typer.Exit(1)

    if watch:
        console = Console(highlight=False)
        with Live(console=console, refresh_per_second=1) as live:
            try:
                while True:
                    pipeline_report = output.report(run_id_filter=run)
                    panel = _build_dashboard_panel(pipeline_report)
                    live.update(panel)
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
    elif json:
        pipeline_report = output.report(run_id_filter=run)

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
                "run_id": pipeline_report.run_id,
            }
        if "progress" in sections:
            # Calculate per-task staged (available - processed - failed)
            total = (
                pipeline_report.processed
                + pipeline_report.in_progress
                + pipeline_report.failed
                + (pipeline_report.staged or 0)
            )
            task_list = []
            available = total
            for t in pipeline_report.tasks:
                task_staged = max(0, available - t.processed - t.failed)
                task_list.append(
                    {
                        "name": t.name,
                        "processed": t.processed,
                        "staged": task_staged,
                        "failed": t.failed,
                    }
                )
                available = t.processed
            result["progress"] = {
                "processed": pipeline_report.processed,
                "in_progress": pipeline_report.in_progress,
                "failed": pipeline_report.failed,
                "staged": pipeline_report.staged,
                "tasks": task_list,
            }
        if "metrics" in sections:
            result["metrics"] = {
                name: {
                    "summary": {
                        "count": len(file_metrics),
                        "avg_ms": sum(m.duration_ms for m in file_metrics)
                        / len(file_metrics),
                        "min_ms": min(m.duration_ms for m in file_metrics),
                        "max_ms": max(m.duration_ms for m in file_metrics),
                    }
                    if file_metrics
                    else {},
                    "files": [
                        {
                            "file": m.file,
                            "started_at": m.started_at.isoformat(),
                            "finished_at": m.finished_at.isoformat(),
                            "duration_ms": m.duration_ms,
                            "status": m.status,
                        }
                        for m in file_metrics
                    ],
                }
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

        print(json_lib.dumps(result, indent=2, default=str))
    else:
        pipeline_report = output.report(run_id_filter=run)
        console = Console(highlight=False)
        panel = _build_dashboard_panel(pipeline_report)
        console.print(panel)
