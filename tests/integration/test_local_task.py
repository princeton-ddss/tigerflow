"""Integration tests for LocalTask."""

import signal
import subprocess
import sys
import time
from pathlib import Path


def run_task_until_complete(
    input_dir: Path,
    output_dir: Path,
    input_ext: str,
    output_ext: str,
    expected_count: int,
    module: str | None = None,
    script: Path | None = None,
    extra_args: list[str] | None = None,
    timeout: float = 5,
):
    """Run task via subprocess until expected files are produced."""
    if module:
        cmd = [sys.executable, "-m", module]
    elif script:
        cmd = [sys.executable, str(script)]
    else:
        raise ValueError("Either module or script must be provided")

    cmd.extend(
        [
            "--input-dir",
            str(input_dir),
            "--output-dir",
            str(output_dir),
            "--input-ext",
            input_ext,
            "--output-ext",
            output_ext,
        ]
    )
    if extra_args:
        cmd.extend(extra_args)

    proc = subprocess.Popen(cmd)

    start_time = time.time()
    while time.time() - start_time < timeout:
        output_files = [f for f in output_dir.iterdir() if f.suffix == output_ext]
        if len(output_files) >= expected_count:
            break
        time.sleep(0.1)

    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=2)


class TestLocalTaskWithLibrary:
    """Test LocalTask using a library task (tigerflow.library.echo)."""

    def test_processes_files(self, tmp_dirs, input_files):
        """Test that task processes input files to output."""
        input_dir, output_dir = tmp_dirs

        run_task_until_complete(
            module="tigerflow.library.echo",
            input_dir=input_dir,
            output_dir=output_dir,
            input_ext=".txt",
            output_ext=".txt",
            expected_count=len(input_files),
        )

        for input_file in input_files:
            output_file = output_dir / input_file.name
            assert output_file.exists(), f"Missing output: {output_file}"
            assert output_file.read_text() == input_file.read_text()

    def test_params_passed_to_task(self, tmp_dirs, input_files):
        """Test that CLI params are passed to task context."""
        input_dir, output_dir = tmp_dirs

        run_task_until_complete(
            module="tigerflow.library.echo",
            input_dir=input_dir,
            output_dir=output_dir,
            input_ext=".txt",
            output_ext=".txt",
            expected_count=len(input_files),
            extra_args=["--prefix", ">>", "--suffix", "<<", "--uppercase"],
        )

        for input_file in input_files:
            output_file = output_dir / input_file.name
            assert output_file.exists()
            expected = f">>{input_file.read_text().upper()}<<"
            assert output_file.read_text() == expected


class TestLocalTaskWithModule:
    """Test LocalTask using a module-based task."""

    def test_error_files_created_on_failure(self, tmp_dirs, tasks_dir):
        """Test that .err files are created when task fails."""
        input_dir, output_dir = tmp_dirs

        input_file = input_dir / "fail.txt"
        input_file.write_text("will fail")

        run_task_until_complete(
            script=tasks_dir / "failing.py",
            input_dir=input_dir,
            output_dir=output_dir,
            input_ext=".txt",
            output_ext=".out",
            expected_count=1,
        )

        err_file = output_dir / "fail.err"
        assert err_file.exists()
        assert "Intentional failure" in err_file.read_text()
