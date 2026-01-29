import os

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_env_file = os.environ.get("TIGERFLOW_ENV_FILE", ".env")


class TigerflowSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="TIGERFLOW_",
        env_file=_env_file,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    task_validation_timeout: int = Field(
        default=60,
        gt=0,
        description="Timeout for task CLI validation in seconds",
    )

    pipeline_poll_interval: int = Field(
        default=10,
        gt=0,
        description="Pipeline polling interval in seconds",
    )

    task_poll_interval: int = Field(
        default=3,
        gt=0,
        description="Task polling interval in seconds",
    )

    slurm_task_client_hours: int = Field(
        default=24,
        gt=0,
        lt=100,
        description="Time limit in hours for each Slurm task client job (respawns when expired)",
    )

    slurm_task_scale_interval: int = Field(
        default=15,
        gt=0,
        description="Slurm task scaling check interval in seconds",
    )

    slurm_task_scale_wait_count: int = Field(
        default=8,
        gt=0,
        description="Consecutive idle checks before removing a worker",
    )

    slurm_task_worker_startup_timeout: int = Field(
        default=600,
        gt=0,
        description="Timeout in seconds for each Slurm task worker to initialize",
    )


settings = TigerflowSettings()
