import os
import textwrap
from pathlib import Path

import pytest

from tigerflow.utils import (
    has_running_pid,
    import_callable,
    is_process_running,
    is_valid_task_cli,
    read_pid_file,
    validate_callable_reference,
)


class TestTaskCliValidation:
    @pytest.fixture
    def valid_cli(self, tmp_path: Path) -> Path:
        script = tmp_path / "valid_cli.py"
        script.write_text(
            textwrap.dedent(
                """
            import sys
            if "--help" in sys.argv:
                print("Usage: valid_cli.py [OPTIONS]")
                sys.exit(0)
            """
            )
        )
        return script

    @pytest.fixture
    def cli_nonzero_exit(self, tmp_path: Path) -> Path:
        script = tmp_path / "nonzero_exit.py"
        script.write_text(
            textwrap.dedent(
                """
            import sys
            sys.exit(1)
            """
            )
        )
        return script

    @pytest.fixture
    def cli_slow(self, tmp_path: Path) -> Path:
        script = tmp_path / "slow_cli.py"
        script.write_text(
            textwrap.dedent(
                """
            import time
            time.sleep(10)
            """
            )
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


class TestValidateCallableReference:
    def test_valid_simple_reference(self):
        assert validate_callable_reference("os.path:exists") == "os.path:exists"

    def test_valid_single_module(self):
        assert validate_callable_reference("os:getcwd") == "os:getcwd"

    def test_rejects_no_colon(self):
        with pytest.raises(ValueError, match="exactly one ':'"):
            validate_callable_reference("os.path.exists")

    def test_rejects_multiple_colons(self):
        with pytest.raises(ValueError, match="exactly one ':'"):
            validate_callable_reference("os:path:exists")

    def test_rejects_empty_module(self):
        with pytest.raises(ValueError, match="Invalid Python identifier"):
            validate_callable_reference(":exists")

    def test_rejects_invalid_module_part(self):
        with pytest.raises(ValueError, match="Invalid Python identifier"):
            validate_callable_reference("123.bad:func")

    def test_rejects_invalid_function_name(self):
        with pytest.raises(ValueError, match="Invalid Python identifier"):
            validate_callable_reference("os.path:123bad")


class TestImportCallable:
    def test_imports_stdlib_function(self):
        func = import_callable("os.path:exists")
        assert func is os.path.exists

    def test_raises_import_error_for_bad_module(self):
        with pytest.raises(ModuleNotFoundError):
            import_callable("nonexistent_module_xyz:func")

    def test_raises_attribute_error_for_bad_function(self):
        with pytest.raises(AttributeError):
            import_callable("os.path:nonexistent_func_xyz")

    def test_raises_type_error_for_non_callable(self):
        with pytest.raises(TypeError, match="does not resolve to a callable"):
            import_callable("os.path:sep")
