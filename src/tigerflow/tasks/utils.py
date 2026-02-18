import json
import subprocess
import time
from contextlib import contextmanager

from tigerflow.logconfig import logger
from tigerflow.models import TaskStatus, TaskStatusKind


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
    start = time.perf_counter()
    try:
        yield metrics
    except Exception:
        metrics["status"] = "error"
        raise
    finally:
        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(
            json.dumps(
                {
                    "_metrics": True,
                    "file": filename,
                    "duration_ms": round(duration_ms, 3),
                    "status": metrics["status"],
                }
            )
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
    else:
        reason = subprocess.run(
            ["sacct", "-j", str(client_job_id), "--format=State", "--noheader"],
            capture_output=True,
            text=True,
        ).stdout

        return TaskStatus(
            kind=TaskStatusKind.INACTIVE,
            detail=f"Reason: {reason.splitlines()[0].strip()}" if reason else None,
        )
