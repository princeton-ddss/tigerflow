import os
import textwrap
from pathlib import Path

import pytest

from tigerflow.utils import (
    has_running_pid,
    is_process_running,
    is_valid_task_cli,
    read_pid_file,
)


class TestTaskCliValidation:
    @pytest.fixture
    def valid_cli(self, tmp_path: Path) -> Path:
        script = tmp_path / "valid_cli.py"
        script.write_text(
            textwrap.dedent("""
            import sys
            if "--help" in sys.argv:
                print("Usage: valid_cli.py [OPTIONS]")
                sys.exit(0)
            """)
        )
        return script

    @pytest.fixture
    def cli_nonzero_exit(self, tmp_path: Path) -> Path:
        script = tmp_path / "nonzero_exit.py"
        script.write_text(
            textwrap.dedent("""
            import sys
            sys.exit(1)
            """)
        )
        return script

    @pytest.fixture
    def cli_slow(self, tmp_path: Path) -> Path:
        script = tmp_path / "slow_cli.py"
        script.write_text(
            textwrap.dedent("""
            import time
            time.sleep(10)
            """)
        )
        return script

    # File module tests
    def test_valid_file_module_returns_true(self, valid_cli: Path):
        assert is_valid_task_cli(str(valid_cli)) is True

    def test_file_module_nonzero_exit_returns_false(self, cli_nonzero_exit: Path):
        assert is_valid_task_cli(str(cli_nonzero_exit)) is False

    def test_file_module_timeout_raises_timeout_error(self, cli_slow: Path):
        with pytest.raises(TimeoutError, match="timed out after 1s"):
            is_valid_task_cli(str(cli_slow), timeout=1)

    def test_custom_timeout(self, valid_cli: Path):
        assert is_valid_task_cli(str(valid_cli), timeout=5) is True

    # Library module tests
    def test_valid_library_module_returns_true(self):
        assert is_valid_task_cli("tigerflow.library.echo") is True

    def test_nonexistent_library_module_returns_false(self):
        assert is_valid_task_cli("nonexistent.module.path") is False


class TestPidUtilityFunctions:
    def test_read_pid_file_not_exists(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        assert read_pid_file(pid_file) is None

    def test_read_pid_file_valid(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        pid_file.write_text("12345")
        assert read_pid_file(pid_file) == 12345

    def test_read_pid_file_with_whitespace(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        pid_file.write_text("  12345\n")
        assert read_pid_file(pid_file) == 12345

    def test_read_pid_file_invalid(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        pid_file.write_text("not-a-number")
        assert read_pid_file(pid_file) is None

    def test_is_process_running_current_process(self):
        assert is_process_running(os.getpid()) is True

    def test_is_process_running_nonexistent(self):
        assert is_process_running(999999999) is False

    def test_has_running_pid_no_file(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        assert has_running_pid(pid_file) is False

    def test_has_running_pid_running_process(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        pid_file.write_text(str(os.getpid()))
        assert has_running_pid(pid_file) is True

    def test_has_running_pid_dead_process(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        pid_file.write_text("999999999")  # Non-existent process
        assert has_running_pid(pid_file) is False
