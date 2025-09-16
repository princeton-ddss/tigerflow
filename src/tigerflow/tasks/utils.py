import subprocess

from tigerflow.models import TaskStatus, TaskStatusKind


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
