"""Integration tests for SlurmTask.

These tests require a Slurm cluster to run. They are designed to be run
manually on a login node with access to sbatch and squeue commands.

Run with:
    SLURM_TEST_DIR=/scratch/user/tests pytest tests/integration/test_slurm_task.py -v

Set SLURM_TEST_DIR to a shared filesystem path accessible by compute nodes.
"""

import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tigerflow.utils import TEMP_FILE_PREFIX

SLURM_TEST_DIR = os.environ.get("SLURM_TEST_DIR")

# Skip conditions
skip_if_disabled = pytest.mark.skipif(
    os.environ.get("SKIP_SLURM_TESTS", "0") == "1",
    reason="SKIP_SLURM_TESTS=1",
)
skip_if_no_sbatch = pytest.mark.skipif(
    shutil.which("sbatch") is None,
    reason="sbatch not found",
)
skip_if_no_test_dir = pytest.mark.skipif(
    SLURM_TEST_DIR is None,
    reason="SLURM_TEST_DIR not set",
)

pytestmark = [skip_if_disabled, skip_if_no_sbatch, skip_if_no_test_dir]


def _build_slurm_task_cmd(
    script: Path,
    input_dir: Path,
    output_dir: Path,
    input_ext: str,
    output_ext: str,
    extra_args: list[str] | None = None,
) -> list[str]:
    """Build the command to run a Slurm task via subprocess."""
    # Create logs directory
    log_dir = output_dir / "logs"
    log_dir.mkdir(exist_ok=True)

    cmd = [
        sys.executable,
        str(script),
        "--input-dir",
        str(input_dir),
        "--output-dir",
        str(output_dir),
        "--input-ext",
        input_ext,
        "--output-ext",
        output_ext,
        "--max-workers",
        "2",
        "--cpus",
        "1",
        "--memory",
        "2G",
        "--time",
        "00:10:00",
    ]
    if extra_args:
        cmd.extend(extra_args)

    return cmd


def run_slurm_task_until_complete(
    script: Path,
    input_dir: Path,
    output_dir: Path,
    input_ext: str,
    output_ext: str,
    expected_count: int,
    extra_args: list[str] | None = None,
    timeout: float = 180,
):
    """Run Slurm task via subprocess and wait for output files.

    Uses SlurmTaskRunner (default mode) which submits to Slurm.
    """
    cmd = _build_slurm_task_cmd(
        script, input_dir, output_dir, input_ext, output_ext, extra_args
    )
    proc = subprocess.Popen(cmd)

    try:
        start_time = time.time()
        while time.time() - start_time < timeout:
            output_files = [
                f
                for f in output_dir.iterdir()
                if f.is_file()
                and f.suffix == output_ext
                and not f.name.startswith(TEMP_FILE_PREFIX)
            ]
            err_files = [
                f
                for f in output_dir.iterdir()
                if f.is_file()
                and f.suffix == ".err"
                and not f.name.startswith(TEMP_FILE_PREFIX)
            ]
            if len(output_files) + len(err_files) >= expected_count:
                break
            time.sleep(2)
    finally:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=30)


def run_slurm_task_until_exit(
    script: Path,
    input_dir: Path,
    output_dir: Path,
    input_ext: str,
    output_ext: str,
    extra_args: list[str] | None = None,
    timeout: float = 180,
) -> subprocess.Popen:
    """Run Slurm task via subprocess and wait for it to exit.

    Unlike run_slurm_task_until_complete, this waits for the process
    to terminate on its own (e.g., due to a fatal error) rather than
    waiting for output files.
    """
    cmd = _build_slurm_task_cmd(
        script, input_dir, output_dir, input_ext, output_ext, extra_args
    )
    proc = subprocess.Popen(cmd)

    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=30)

    return proc


@pytest.fixture
def task_dirs():
    """Create input and output directories on shared filesystem."""
    assert SLURM_TEST_DIR is not None, (
        "SLURM_TEST_DIR should be set (test should be skipped otherwise)"
    )
    base = Path(SLURM_TEST_DIR)
    base.mkdir(parents=True, exist_ok=True)

    input_dir = base / "input"
    output_dir = base / "output"

    # Clean up any previous test artifacts
    for d in (input_dir, output_dir):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir()

    yield input_dir, output_dir

    # Cleanup after test
    for d in (input_dir, output_dir):
        if d.exists():
            shutil.rmtree(d)


@pytest.fixture
def input_files(task_dirs):
    """Create sample input files."""
    input_dir, _ = task_dirs
    files = []
    for i, content in enumerate(["hello world", "foo bar", "test content"]):
        f = input_dir / f"file{i}.txt"
        f.write_text(content)
        files.append(f)
    return files


class TestSlurmTaskIntegration:
    """Integration tests that run actual Slurm jobs."""

    def test_processes_files(self, task_dirs, input_files, tasks_dir):
        """Test that SlurmTask processes input files to output via Slurm."""
        input_dir, output_dir = task_dirs

        run_slurm_task_until_complete(
            script=tasks_dir / "slurm_echo.py",
            input_dir=input_dir,
            output_dir=output_dir,
            input_ext=".txt",
            output_ext=".out",
            expected_count=len(input_files),
        )

        for input_file in input_files:
            output_file = output_dir / input_file.with_suffix(".out").name
            assert output_file.exists(), f"Missing output: {output_file}"
            assert output_file.read_text() == input_file.read_text()

    def test_params_passed_to_task(self, task_dirs, input_files, tasks_dir):
        """Test that CLI params are passed to Slurm task context."""
        input_dir, output_dir = task_dirs

        run_slurm_task_until_complete(
            script=tasks_dir / "slurm_echo.py",
            input_dir=input_dir,
            output_dir=output_dir,
            input_ext=".txt",
            output_ext=".out",
            expected_count=len(input_files),
            extra_args=["--prefix", ">>", "--suffix", "<<", "--uppercase"],
        )

        for input_file in input_files:
            output_file = output_dir / input_file.with_suffix(".out").name
            assert output_file.exists()
            expected = f">>{input_file.read_text().upper()}<<"
            assert output_file.read_text() == expected

    def test_error_files_created_on_failure(self, task_dirs, tasks_dir):
        """Test that .err files are created when Slurm task fails."""
        input_dir, output_dir = task_dirs

        input_file = input_dir / "fail.txt"
        input_file.write_text("will fail")

        run_slurm_task_until_complete(
            script=tasks_dir / "slurm_failing.py",
            input_dir=input_dir,
            output_dir=output_dir,
            input_ext=".txt",
            output_ext=".out",
            expected_count=1,
        )

        err_file = output_dir / "fail.err"
        assert err_file.exists()
        assert "Intentional Slurm failure" in err_file.read_text()

    def test_setup_failure_aborts(self, task_dirs, input_files, tasks_dir):
        """Test that setup failure writes sentinel and aborts the task."""
        input_dir, output_dir = task_dirs

        proc = run_slurm_task_until_exit(
            script=tasks_dir / "slurm_failing_setup.py",
            input_dir=input_dir,
            output_dir=output_dir,
            input_ext=".txt",
            output_ext=".out",
        )

        assert proc.returncode != 0

        # Sentinel should exist under the runner's log directory
        log_base = output_dir / "logs"
        sentinel_files = list(log_base.rglob(".setup-failed"))
        assert len(sentinel_files) > 0, "Expected .setup-failed sentinel file"

        # Sentinel should contain expected error details
        error_data = json.loads(sentinel_files[0].read_text())
        assert error_data["exception_type"] == "RuntimeError"
        assert "Intentional setup failure" in error_data["message"]
        assert "timestamp" in error_data
        assert "traceback" in error_data

        # No output files should have been produced
        output_files = [
            f
            for f in output_dir.iterdir()
            if f.is_file()
            and f.suffix == ".out"
            and not f.name.startswith(TEMP_FILE_PREFIX)
        ]
        assert len(output_files) == 0
