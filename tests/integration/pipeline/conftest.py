"""Shared fixtures for Pipeline tests.

Pipeline is exercised in-process against real temp directories. Task
subprocesses are faked (see `fake_popen`) but the config models, staging
middleware, and filesystem layout are all real.

Config builders and fakes live in `helpers.py`.
"""

import subprocess
from collections.abc import Callable, Iterator
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from tigerflow.logconfig import logger
from tigerflow.pipeline import Pipeline

from .helpers import FakePopen, PipelineFactory, task_spec


@pytest.fixture
def input_dir(tmp_path: Path) -> Path:
    path = tmp_path / "input"
    path.mkdir(exist_ok=True)
    return path


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    path = tmp_path / "output"
    path.mkdir(exist_ok=True)
    return path


@pytest.fixture
def pipeline_factory(
    tmp_path: Path, input_dir: Path, output_dir: Path
) -> PipelineFactory:
    """Return a callable that builds a Pipeline over the `input_dir`/`output_dir` fixtures.

    Tasks are given as config dicts (see `task_spec`) so tests can construct
    dependency chains and multi-terminal-task graphs, not just a single task.
    CLI validation is patched out because no task subprocess is spawned.
    """

    def factory(
        tasks: list[dict] | None = None,
        *,
        staging: dict | None = None,
        **kwargs,
    ) -> Pipeline:
        config_file = tmp_path / "config.yaml"

        config: dict = {"tasks": tasks if tasks is not None else [task_spec()]}
        if staging is not None:
            config["staging"] = staging
        config_file.write_text(yaml.dump(config))

        with patch("tigerflow.pipeline.validate_task_cli", return_value=True):
            return Pipeline(
                config_file=config_file,
                input_dir=input_dir,
                output_dir=output_dir,
                **kwargs,
            )

    return factory


@pytest.fixture(autouse=True)
def fake_popen() -> Iterator[list[FakePopen]]:
    """Patch subprocess spawning so `_start_tasks` creates FakePopen objects.

    Autouse because the failure mode is silent: `_start_tasks` never waits on
    the processes it spawns, so a test that missed this fixture still passes
    green while leaking a real bash process that outlives the run.

    Yields a list receiving each FakePopen in creation order.
    """
    created: list[FakePopen] = []

    def spawn(*args, **kwargs) -> FakePopen:
        process = FakePopen(*args, **kwargs)
        created.append(process)
        return process

    with patch.object(subprocess, "Popen", spawn):
        yield created


@pytest.fixture
def stop_after_one_cycle(monkeypatch: pytest.MonkeyPatch) -> Callable[[Pipeline], None]:
    """Return a callable that arms a pipeline so a later `run()` does one cycle.

    Shutdown is requested from inside the cycle rather than before `run()`, so
    the pipeline reaches its normal steady state (tasks started and polled at
    least once) before the shutdown path is exercised. The poll interval is
    shortened so the post-cycle wait does not stall the test.
    """

    def setup(pipeline: Pipeline) -> None:
        monkeypatch.setattr("tigerflow.pipeline.settings.pipeline_poll_interval", 1)

        original_cycle = pipeline._run_tracking_cycle

        def cycle_then_stop():
            # Shutdown must be requested even if the cycle raises, otherwise a
            # failing cycle leaves run() looping until the idle timeout.
            try:
                original_cycle()
            finally:
                pipeline._shutdown_event.set()

        monkeypatch.setattr(pipeline, "_run_tracking_cycle", cycle_then_stop)

    return setup


@pytest.fixture
def error_logs() -> Iterator[list[str]]:
    """Yield a list collecting the message of every ERROR record logged.

    Tigerflow logs through loguru, so pytest's `caplog` sees nothing and
    capture requires adding a sink.
    """
    records: list[str] = []

    sink_id = logger.add(
        lambda message: records.append(message.record["message"]), level="ERROR"
    )
    try:
        yield records
    finally:
        logger.remove(sink_id)


@pytest.fixture(autouse=True)
def reset_logger_sinks():
    """Drop file sinks added by `Pipeline.run()` after each test.

    Loguru sinks are global, so without this a sink keeps pointing at a
    deleted tmp_path directory and later tests fail on write. Only sinks
    added during the test are removed; the stderr sink installed by
    tigerflow.logconfig at import time must survive for later tests.

    Reads `logger._core.handlers` because loguru exposes no public API for
    listing sink IDs.
    """
    existing_ids = set(logger._core.handlers)  # ty: ignore[unresolved-attribute]

    yield

    for sink_id in set(logger._core.handlers) - existing_ids:  # ty: ignore[unresolved-attribute]
        logger.remove(sink_id)
