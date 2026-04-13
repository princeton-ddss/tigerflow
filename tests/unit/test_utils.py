import json
import os
import textwrap
from pathlib import Path

import pytest

from tigerflow.utils import (
    TEMP_FILE_PREFIX,
    ErrorRecord,
    atomic_write,
    has_running_pid,
    import_callable,
    is_process_running,
    read_pid_file,
    validate_callable_reference,
    validate_task_cli,
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
            import nonexistent_package
            nonexistent_package.run()
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
    def test_valid_file_module_passes(self, valid_cli: Path):
        validate_task_cli(str(valid_cli))  # Should not raise

    def test_file_module_nonzero_exit_raises_value_error(self, cli_nonzero_exit: Path):
        with pytest.raises(ValueError, match="Invalid task CLI") as exc_info:
            validate_task_cli(str(cli_nonzero_exit))
        assert "ModuleNotFoundError" in str(exc_info.value)

    def test_file_module_timeout_raises_timeout_error(self, cli_slow: Path):
        with pytest.raises(TimeoutError, match="timed out after 1s"):
            validate_task_cli(str(cli_slow), timeout=1)

    def test_custom_timeout(self, valid_cli: Path):
        validate_task_cli(str(valid_cli), timeout=5)  # Should not raise

    # Library module tests
    def test_valid_library_module_passes(self):
        validate_task_cli("tigerflow.library.echo")  # Should not raise

    def test_nonexistent_library_module_raises_value_error(self):
        with pytest.raises(ValueError, match="Invalid task CLI") as exc_info:
            validate_task_cli("nonexistent.module.path")
        assert "ModuleNotFoundError" in str(exc_info.value)


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


class TestAtomicWrite:
    def test_temp_file_has_prefix_and_suffix(self, tmp_path: Path):
        target = tmp_path / "output.csv"
        with atomic_write(target) as temp:
            assert temp.name.startswith(TEMP_FILE_PREFIX)
            assert temp.suffix == ".csv"
            temp.write_text("data")
        assert target.read_text() == "data"
        assert not temp.exists(), "Temp file should be replaced by target"

    def test_temp_file_no_suffix(self, tmp_path: Path):
        target = tmp_path / "noext"
        with atomic_write(target) as temp:
            assert temp.name.startswith(TEMP_FILE_PREFIX)
            assert temp.suffix == ""
            temp.write_text("data")
        assert target.read_text() == "data"

    def test_temp_file_cleaned_on_error(self, tmp_path: Path):
        target = tmp_path / "output.txt"
        with pytest.raises(RuntimeError):
            with atomic_write(target) as temp:
                temp.write_text("partial")
                raise RuntimeError("boom")
        assert not temp.exists(), "Temp file should be removed on error"
        assert not target.exists(), "Target should not be created on error"

    def test_accepts_str_path(self, tmp_path: Path):
        target = tmp_path / "output.json"
        with atomic_write(str(target)) as temp:
            assert temp.name.startswith(TEMP_FILE_PREFIX)
            assert temp.suffix == ".json"
            temp.write_text("{}")
        assert target.read_text() == "{}"


class TestErrorRecord:
    def test_from_exception_captures_fields(self):
        try:
            raise ValueError("test error")
        except ValueError:
            record = ErrorRecord.from_exception()

        assert record.exception_type == "ValueError"
        assert record.message == "test error"
        assert "ValueError: test error" in record.traceback
        assert record.timestamp  # non-empty ISO string

    def test_from_exception_outside_handler(self):
        record = ErrorRecord.from_exception()
        assert record.exception_type == "Unknown"
        assert record.message == ""
        assert record.file is None

    def test_from_exception_captures_file(self):
        try:
            raise ValueError("test error")
        except ValueError:
            record = ErrorRecord.from_exception(file="input.txt")
        assert record.file == "input.txt"

    def test_write_read_roundtrip(self, tmp_path: Path):
        original = ErrorRecord(
            timestamp="2026-01-01T00:00:00+00:00",
            exception_type="RuntimeError",
            message="boom",
            traceback="Traceback ...",
            file="input.txt",
        )
        path = tmp_path / "error.err"
        original.write(path)
        loaded = ErrorRecord.read(path)
        assert loaded == original

    def test_write_read_roundtrip_without_file(self, tmp_path: Path):
        original = ErrorRecord(
            timestamp="2026-01-01T00:00:00+00:00",
            exception_type="RuntimeError",
            message="boom",
            traceback="Traceback ...",
        )
        path = tmp_path / "error.err"
        original.write(path)
        loaded = ErrorRecord.read(path)
        assert loaded == original
        assert loaded.file is None

    def test_read_extra_keys_raises_value_error(self, tmp_path: Path):
        path = tmp_path / "error.err"
        data = {
            "timestamp": "2026-01-01T00:00:00+00:00",
            "exception_type": "RuntimeError",
            "message": "boom",
            "traceback": "Traceback ...",
            "file": "input.txt",
            "unexpected": "value",
        }
        path.write_text(json.dumps(data))
        with pytest.raises(ValueError, match="invalid error record"):
            ErrorRecord.read(path)

    def test_read_without_file_key(self, tmp_path: Path):
        path = tmp_path / "error.err"
        data = {
            "timestamp": "2026-01-01T00:00:00+00:00",
            "exception_type": "RuntimeError",
            "message": "boom",
            "traceback": "Traceback ...",
        }
        path.write_text(json.dumps(data))
        loaded = ErrorRecord.read(path)
        assert loaded.file is None

    def test_read_missing_keys_raises_value_error(self, tmp_path: Path):
        path = tmp_path / "error.err"
        path.write_text(json.dumps({"timestamp": "2026-01-01T00:00:00+00:00"}))
        with pytest.raises(ValueError, match="invalid error record"):
            ErrorRecord.read(path)

    def test_read_malformed_json_raises_value_error(self, tmp_path: Path):
        path = tmp_path / "error.err"
        path.write_text("not json")
        with pytest.raises(ValueError, match="invalid error record"):
            ErrorRecord.read(path)
