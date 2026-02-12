import textwrap
from pathlib import Path

import pytest

from tigerflow.utils import is_valid_task_cli


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

    # File module tests
    def test_valid_file_module_returns_true(self, valid_cli: Path):
        assert is_valid_task_cli(str(valid_cli)) is True

    def test_file_module_missing_options_returns_false(self, cli_missing_options: Path):
        assert is_valid_task_cli(str(cli_missing_options)) is False

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

    def test_library_module_without_cli_returns_false(self):
        # A module that exists but isn't a valid task CLI
        assert is_valid_task_cli("tigerflow.utils") is False
