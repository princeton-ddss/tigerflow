import os
import textwrap
from pathlib import Path

import pytest

from tigerflow.utils import (
    check_and_cleanup_stale_pid,
    cleanup_logs,
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

    def test_check_and_cleanup_stale_pid_no_file(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        assert check_and_cleanup_stale_pid(pid_file) is False

    def test_check_and_cleanup_stale_pid_running_process(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        pid_file.write_text(str(os.getpid()))
        assert check_and_cleanup_stale_pid(pid_file) is True
        assert pid_file.exists()  # File should not be removed

    def test_check_and_cleanup_stale_pid_dead_process(self, tmp_path: Path):
        pid_file = tmp_path / "run.pid"
        pid_file.write_text("999999999")  # Non-existent process
        assert check_and_cleanup_stale_pid(pid_file) is False
        assert not pid_file.exists()  # Stale file should be removed


class TestCleanupLogs:
    def test_no_log_files(self, tmp_path: Path):
        """Test that cleanup returns 0 when no log files exist."""
        deleted = cleanup_logs(tmp_path, max_size_mb=1)
        assert deleted == 0

    def test_under_size_limit(self, tmp_path: Path):
        """Test that no files are deleted when under size limit."""
        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()
        log_file = logs_dir / "test.log"
        log_file.write_text("small content")

        deleted = cleanup_logs(tmp_path, max_size_mb=1)
        assert deleted == 0
        assert log_file.exists()

    def test_deletes_oldest_files(self, tmp_path: Path):
        """Test that oldest files are deleted first."""
        import time

        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()

        # Create files with different timestamps
        old_file = logs_dir / "old.log"
        old_file.write_bytes(b"x" * 600_000)  # 600KB
        time.sleep(0.1)

        new_file = logs_dir / "new.log"
        new_file.write_bytes(b"y" * 600_000)  # 600KB

        # Total is ~1.2MB, limit is 1MB
        deleted = cleanup_logs(tmp_path, max_size_mb=1)

        assert deleted == 1
        assert not old_file.exists()  # Oldest should be deleted
        assert new_file.exists()  # Newest should remain

    def test_deletes_multiple_files(self, tmp_path: Path):
        """Test that multiple files can be deleted to get under limit."""
        import time

        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()

        files = []
        for i in range(5):
            f = logs_dir / f"file{i}.log"
            f.write_bytes(b"x" * 500_000)  # 500KB each = 2.5MB total
            files.append(f)
            time.sleep(0.05)

        # Limit to 1MB - should delete oldest files
        deleted = cleanup_logs(tmp_path, max_size_mb=1)

        assert deleted >= 2  # At least 2 files need to go
        # Newest files should remain
        assert files[-1].exists()

    def test_finds_nested_log_files(self, tmp_path: Path):
        """Test that log files in subdirectories are found."""
        import time

        # Create nested structure like .tigerflow/{task}/logs/
        task_logs = tmp_path / "task1" / "logs"
        task_logs.mkdir(parents=True)

        old_file = task_logs / "old.log"
        old_file.write_bytes(b"x" * 600_000)
        time.sleep(0.1)

        new_file = task_logs / "new.log"
        new_file.write_bytes(b"y" * 600_000)

        deleted = cleanup_logs(tmp_path, max_size_mb=1)

        assert deleted == 1
        assert not old_file.exists()
        assert new_file.exists()
