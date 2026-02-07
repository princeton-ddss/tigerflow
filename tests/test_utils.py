import os.path
import textwrap
from pathlib import Path

import pytest

from tigerflow.utils import import_callable, is_valid_task_cli, validate_callable_reference


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


class TestTaskCliValidation:
    @pytest.fixture
    def valid_cli(self, tmp_path: Path) -> Path:
        script = tmp_path / "valid_cli.py"
        script.write_text(
            textwrap.dedent("""
            import sys

            HELP_TEXT = '''
            Usage: valid_cli.py [OPTIONS]

            Options:
              --input-dir PATH    Input directory
              --input-ext TEXT    Input extension
              --output-dir PATH   Output directory
              --output-ext TEXT   Output extension
              --help              Show this message and exit.
            '''

            if "--help" in sys.argv:
                print(HELP_TEXT)
                sys.exit(0)
            """)
        )
        return script

    @pytest.fixture
    def cli_missing_options(self, tmp_path: Path) -> Path:
        script = tmp_path / "missing_options.py"
        script.write_text(
            textwrap.dedent("""
            import sys

            HELP_TEXT = '''
            Usage: missing_options.py [OPTIONS]

            Options:
              --input-dir PATH    Input directory
              --help              Show this message and exit.
            '''

            if "--help" in sys.argv:
                print(HELP_TEXT)
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

    def test_valid_cli_returns_true(self, valid_cli: Path):
        assert is_valid_task_cli(valid_cli) is True

    def test_missing_options_returns_false(self, cli_missing_options: Path):
        assert is_valid_task_cli(cli_missing_options) is False

    def test_nonzero_exit_returns_false(self, cli_nonzero_exit: Path):
        assert is_valid_task_cli(cli_nonzero_exit) is False

    def test_timeout_raises_timeout_error(self, cli_slow: Path):
        with pytest.raises(TimeoutError, match="timed out after 1s"):
            is_valid_task_cli(cli_slow, timeout=1)

    def test_custom_timeout(self, valid_cli: Path):
        assert is_valid_task_cli(valid_cli, timeout=5) is True
