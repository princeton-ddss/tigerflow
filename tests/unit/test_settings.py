import textwrap
from pathlib import Path

import pytest
from pydantic import ValidationError

from tigerflow.settings import TigerflowSettings


def test_default_values(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("TIGERFLOW_PIPELINE_POLL_INTERVAL", raising=False)
    monkeypatch.delenv("TIGERFLOW_TASK_POLL_INTERVAL", raising=False)
    monkeypatch.delenv("TIGERFLOW_SLURM_TASK_CLIENT_HOURS", raising=False)
    monkeypatch.delenv("TIGERFLOW_SLURM_TASK_SCALE_INTERVAL", raising=False)
    monkeypatch.delenv("TIGERFLOW_SLURM_TASK_SCALE_WAIT_COUNT", raising=False)
    monkeypatch.delenv("TIGERFLOW_SLURM_TASK_WORKER_STARTUP_TIMEOUT", raising=False)

    settings = TigerflowSettings()

    assert settings.pipeline_poll_interval == 10
    assert settings.task_poll_interval == 3
    assert settings.slurm_task_client_hours == 24
    assert settings.slurm_task_scale_interval == 15
    assert settings.slurm_task_scale_wait_count == 8
    assert settings.slurm_task_worker_startup_timeout == 600


def test_load_from_env_vars(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TIGERFLOW_PIPELINE_POLL_INTERVAL", "20")
    monkeypatch.setenv("TIGERFLOW_TASK_POLL_INTERVAL", "5")
    monkeypatch.setenv("TIGERFLOW_SLURM_TASK_CLIENT_HOURS", "48")
    monkeypatch.setenv("TIGERFLOW_SLURM_TASK_SCALE_INTERVAL", "30")
    monkeypatch.setenv("TIGERFLOW_SLURM_TASK_SCALE_WAIT_COUNT", "10")
    monkeypatch.setenv("TIGERFLOW_SLURM_TASK_WORKER_STARTUP_TIMEOUT", "900")

    settings = TigerflowSettings()

    assert settings.pipeline_poll_interval == 20
    assert settings.task_poll_interval == 5
    assert settings.slurm_task_client_hours == 48
    assert settings.slurm_task_scale_interval == 30
    assert settings.slurm_task_scale_wait_count == 10
    assert settings.slurm_task_worker_startup_timeout == 900


def test_load_from_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("TIGERFLOW_PIPELINE_POLL_INTERVAL", raising=False)
    monkeypatch.delenv("TIGERFLOW_TASK_POLL_INTERVAL", raising=False)
    monkeypatch.delenv("TIGERFLOW_SLURM_TASK_CLIENT_HOURS", raising=False)
    monkeypatch.delenv("TIGERFLOW_SLURM_TASK_SCALE_INTERVAL", raising=False)
    monkeypatch.delenv("TIGERFLOW_SLURM_TASK_SCALE_WAIT_COUNT", raising=False)
    monkeypatch.delenv("TIGERFLOW_SLURM_TASK_WORKER_STARTUP_TIMEOUT", raising=False)

    env_file = tmp_path / ".env"
    env_file.write_text(
        textwrap.dedent("""
            TIGERFLOW_PIPELINE_POLL_INTERVAL=15
            TIGERFLOW_TASK_POLL_INTERVAL=4
            TIGERFLOW_SLURM_TASK_CLIENT_HOURS=36
            TIGERFLOW_SLURM_TASK_SCALE_INTERVAL=20
            TIGERFLOW_SLURM_TASK_SCALE_WAIT_COUNT=6
            TIGERFLOW_SLURM_TASK_WORKER_STARTUP_TIMEOUT=450
            """)
    )
    monkeypatch.setenv("TIGERFLOW_ENV_FILE", str(env_file))

    import importlib

    import tigerflow.settings

    importlib.reload(tigerflow.settings)

    settings = tigerflow.settings.TigerflowSettings()

    assert settings.pipeline_poll_interval == 15
    assert settings.task_poll_interval == 4
    assert settings.slurm_task_client_hours == 36
    assert settings.slurm_task_scale_interval == 20
    assert settings.slurm_task_scale_wait_count == 6
    assert settings.slurm_task_worker_startup_timeout == 450


def test_env_var_takes_precedence_over_env_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv("TIGERFLOW_PIPELINE_POLL_INTERVAL", "30")

    env_file = tmp_path / ".env"
    env_file.write_text("TIGERFLOW_PIPELINE_POLL_INTERVAL=15")
    monkeypatch.setenv("TIGERFLOW_ENV_FILE", str(env_file))

    import importlib

    import tigerflow.settings

    importlib.reload(tigerflow.settings)

    settings = tigerflow.settings.TigerflowSettings()

    assert settings.pipeline_poll_interval == 30


def test_pipeline_poll_interval_must_be_positive(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TIGERFLOW_PIPELINE_POLL_INTERVAL", "0")
    with pytest.raises(ValidationError, match="greater than 0"):
        TigerflowSettings()


def test_task_poll_interval_must_be_positive(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TIGERFLOW_TASK_POLL_INTERVAL", "-1")
    with pytest.raises(ValidationError, match="greater than 0"):
        TigerflowSettings()


def test_slurm_task_client_hours_must_be_positive(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TIGERFLOW_SLURM_TASK_CLIENT_HOURS", "0")
    with pytest.raises(ValidationError, match="greater than 0"):
        TigerflowSettings()


def test_slurm_task_client_hours_must_be_under_100(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TIGERFLOW_SLURM_TASK_CLIENT_HOURS", "100")
    with pytest.raises(ValidationError, match="less than 100"):
        TigerflowSettings()


def test_slurm_task_scale_interval_must_be_positive(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TIGERFLOW_SLURM_TASK_SCALE_INTERVAL", "0")
    with pytest.raises(ValidationError, match="greater than 0"):
        TigerflowSettings()


def test_slurm_task_scale_wait_count_must_be_positive(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TIGERFLOW_SLURM_TASK_SCALE_WAIT_COUNT", "0")
    with pytest.raises(ValidationError, match="greater than 0"):
        TigerflowSettings()


def test_slurm_task_worker_startup_timeout_must_be_positive(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("TIGERFLOW_SLURM_TASK_WORKER_STARTUP_TIMEOUT", "0")
    with pytest.raises(ValidationError, match="greater than 0"):
        TigerflowSettings()
