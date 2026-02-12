import inspect
import textwrap
from pathlib import Path
from typing import Annotated

import pytest
import typer

from tigerflow.utils import (
    build_cli,
    get_params_from_class,
    is_valid_library_cli,
    is_valid_module_cli,
)


class TestModuleCliValidation:
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
        assert is_valid_module_cli(valid_cli) is True

    def test_missing_options_returns_false(self, cli_missing_options: Path):
        assert is_valid_module_cli(cli_missing_options) is False

    def test_nonzero_exit_returns_false(self, cli_nonzero_exit: Path):
        assert is_valid_module_cli(cli_nonzero_exit) is False

    def test_timeout_raises_timeout_error(self, cli_slow: Path):
        with pytest.raises(TimeoutError, match="timed out after 1s"):
            is_valid_module_cli(cli_slow, timeout=1)

    def test_custom_timeout(self, valid_cli: Path):
        assert is_valid_module_cli(valid_cli, timeout=5) is True


class TestLibraryCliValidation:
    def test_valid_library_returns_true(self):
        assert is_valid_library_cli("tigerflow.library.echo") is True

    def test_nonexistent_module_returns_false(self):
        assert is_valid_library_cli("nonexistent.module.path") is False

    def test_module_without_cli_returns_false(self):
        # A module that exists but isn't a CLI
        assert is_valid_library_cli("tigerflow.utils") is False


class TestGetParamsFromClass:
    def test_class_without_params_returns_empty(self):
        class NoParams:
            pass

        assert get_params_from_class(NoParams) == {}

    def test_class_with_params_and_defaults(self):
        class WithDefaults:
            class Params:
                prefix: str = "hello"
                count: int = 10

        result = get_params_from_class(WithDefaults)
        assert "prefix" in result
        assert "count" in result
        assert result["prefix"][1] == "hello"
        assert result["count"][1] == 10

    def test_class_with_params_no_defaults(self):
        class NoDefaults:
            class Params:
                required_param: str

        result = get_params_from_class(NoDefaults)
        assert "required_param" in result
        assert result["required_param"][1] == inspect.Parameter.empty

    def test_class_with_annotated_params(self):
        class AnnotatedParams:
            class Params:
                prefix: Annotated[
                    str,
                    typer.Option(help="A prefix"),
                ] = ""

        result = get_params_from_class(AnnotatedParams)
        assert "prefix" in result
        assert result["prefix"][1] == ""
        # Verify Annotated metadata is preserved
        type_hint = result["prefix"][0]
        assert hasattr(type_hint, "__metadata__")  # Annotated types have __metadata__


class TestBuildCli:
    def test_no_custom_params(self):
        class NoParams:
            pass

        def base_main(input_dir: str, _params: dict):
            return {"input_dir": input_dir, "params": _params}

        wrapped = build_cli(NoParams, base_main)

        # Signature should not include _params
        sig = inspect.signature(wrapped)
        assert "_params" not in sig.parameters
        assert "input_dir" in sig.parameters

        # Wrapper should pass _params=None when no Params class
        result = wrapped(input_dir="/test")
        assert result["input_dir"] == "/test"
        assert result["params"] is None

    def test_with_custom_params(self):
        class WithParams:
            class Params:
                prefix: str = "default"
                count: int = 5

        def base_main(input_dir: str, _params: dict):
            return {"input_dir": input_dir, "params": _params}

        wrapped = build_cli(WithParams, base_main)

        # Signature should include custom params
        sig = inspect.signature(wrapped)
        assert "prefix" in sig.parameters
        assert "count" in sig.parameters
        assert "_params" not in sig.parameters

        # Custom params should be passed via _params dict
        result = wrapped(input_dir="/test", prefix="custom", count=10)
        assert result["input_dir"] == "/test"
        assert result["params"] == {"prefix": "custom", "count": 10}

    def test_custom_params_have_defaults(self):
        class WithDefaults:
            class Params:
                option: str = "default_value"

        def base_main(input_dir: str, _params: dict):
            return _params

        wrapped = build_cli(WithDefaults, base_main)
        sig = inspect.signature(wrapped)

        # Default should be preserved
        assert sig.parameters["option"].default == "default_value"

    def test_annotated_types_preserved_in_signature(self):
        class WithAnnotated:
            class Params:
                prefix: Annotated[
                    str,
                    typer.Option(help="A prefix"),
                ] = ""

        def base_main(input_dir: str, _params: dict):
            return _params

        wrapped = build_cli(WithAnnotated, base_main)
        sig = inspect.signature(wrapped)

        # Verify Annotated metadata flows to wrapper signature
        prefix_param = sig.parameters["prefix"]
        assert hasattr(prefix_param.annotation, "__metadata__")

    def test_parameter_collision_raises_error(self):
        class MyTask:
            class Params:
                input_dir: str = "/collision"  # Collides with base param

        def base_main(input_dir: str, output_dir: str, _params: dict):
            pass

        with pytest.raises(ValueError, match="Parameter name collision") as exc_info:
            build_cli(MyTask, base_main)

        error_msg = str(exc_info.value)
        assert "MyTask.Params" in error_msg
        assert "input_dir" in error_msg
        assert "reserved" in error_msg
