import subprocess

from tigerflow.models import SlurmTaskConfig, TaskStatus, TaskStatusKind


def get_slurm_task_status(config: SlurmTaskConfig) -> TaskStatus:
    client_status = subprocess.run(
        ["squeue", "--me", "-n", config.client_job_name, "-h", "-o", "%.10T"],
        capture_output=True,
        text=True,
    ).stdout

    if "RUNNING" in client_status:
        worker_status = subprocess.run(
            ["squeue", "--me", "-n", config.worker_job_name, "-h", "-o", "%.10T"],
            capture_output=True,
            text=True,
        ).stdout

        return TaskStatus(
            kind=TaskStatusKind.ACTIVE,
            detail=f"{worker_status.count('RUNNING')} workers",
        )
    elif "PENDING" in client_status:
        reason = subprocess.run(
            ["squeue", "--me", "-n", config.client_job_name, "-h", "-o", "%.30R"],
            capture_output=True,
            text=True,
        ).stdout

        return TaskStatus(
            kind=TaskStatusKind.PENDING,
            detail=f"Reason: {reason.splitlines()[-1].strip()}",
        )
    else:
        return TaskStatus(kind=TaskStatusKind.INACTIVE)
