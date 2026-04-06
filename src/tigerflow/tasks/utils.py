import json
import subprocess
import sys
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from tigerflow.logconfig import logger
from tigerflow.models import TaskStatus, TaskStatusKind
from tigerflow.utils import atomic_write


@contextmanager
def log_metrics(filename: str):
    """Log timing metrics for file processing.

    Yields a dict that can be used to set the status. If an exception
    propagates, status is automatically set to "error". Otherwise,
    the caller can set metrics["status"] = "error" explicitly.

    Example:
        with log_metrics("file.txt") as metrics:
            try:
                process(file)
            except Exception:
                metrics["status"] = "error"
                handle_error()
    """
    metrics = {"status": "success"}
    started_at = datetime.now(timezone.utc)
    try:
        yield metrics
    except Exception:
        metrics["status"] = "error"
        raise
    finally:
        finished_at = datetime.now(timezone.utc)
        logger.log(
            "METRICS",
            json.dumps(
                {
                    "file": filename,
                    "started_at": started_at.isoformat(),
                    "finished_at": finished_at.isoformat(),
                    "status": metrics["status"],
                }
            ),
        )


def get_slurm_task_status(client_job_id: int, worker_job_name: str) -> TaskStatus:
    client_status = subprocess.run(
        ["squeue", "-j", str(client_job_id), "-h", "-o", "%.10T"],
        capture_output=True,
        text=True,
    ).stdout

    if "RUNNING" in client_status:
        worker_status = subprocess.run(
            ["squeue", "--me", "-n", worker_job_name, "-h", "-o", "%.10T"],
            capture_output=True,
            text=True,
        ).stdout

        return TaskStatus(
            kind=TaskStatusKind.ACTIVE,
            detail=f"{worker_status.count('RUNNING')} workers",
        )
    elif "PENDING" in client_status:
        reason = subprocess.run(
            ["squeue", "-j", str(client_job_id), "-h", "-o", "%.30R"],
            capture_output=True,
            text=True,
        ).stdout

        return TaskStatus(
            kind=TaskStatusKind.PENDING,
            detail=f"Reason: {reason.splitlines()[-1].strip()}" if reason else None,
        )
    else:  # Client exited — check if workers are still draining
        worker_status = subprocess.run(
            ["squeue", "--me", "-n", worker_job_name, "-h", "-o", "%.10T"],
            capture_output=True,
            text=True,
        ).stdout

        if worker_status.strip():
            return TaskStatus(
                kind=TaskStatusKind.ACTIVE,
                detail=f"draining {len(worker_status.strip().splitlines())} workers",
            )

        reason = subprocess.run(
            ["sacct", "-j", str(client_job_id), "--format=State", "--noheader"],
            capture_output=True,
            text=True,
        ).stdout

        return TaskStatus(
            kind=TaskStatusKind.INACTIVE,
            detail=f"Reason: {reason.splitlines()[0].strip()}" if reason else None,
        )


def write_error_file(error_path: Path, input_file: str) -> None:
    """Write structured error JSON for a failed file.

    Must be called from within an exception handler.
    """
    exc_type, exc_value, _ = sys.exc_info()
    error_data = {
        "file": input_file,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "exception_type": exc_type.__name__ if exc_type else "Unknown",
        "message": str(exc_value) if exc_value else "",
        "traceback": traceback.format_exc(),
    }
    with atomic_write(error_path) as temp_path:
        with open(temp_path, "w") as f:
            json.dump(error_data, f, indent=2)
