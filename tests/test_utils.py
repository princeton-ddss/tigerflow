import inspect

import pytest
import typer
from typing_extensions import Annotated

from tigerflow.utils import (
    SetupContext,
    build_cli,
    get_params_from_class,
    is_valid_library_cli,
    validate_file_ext,
)


class TestIsValidLibraryCli:
    def test_valid_library_task(self):
        assert is_valid_library_cli("tigerflow.library.echo") is True

    def test_invalid_library_module(self):
        assert is_valid_library_cli("nonexistent.module") is False


class TestValidateFileExt:
    def test_valid_extension(self):
        assert validate_file_ext(".txt") == ".txt"

    def test_valid_compound_extension(self):
        assert validate_file_ext(".tar.gz") == ".tar.gz"

    def test_invalid_no_dot(self):
        with pytest.raises(ValueError):
            validate_file_ext("txt")

    def test_reserved_err_extension(self):
        with pytest.raises(ValueError):
            validate_file_ext(".err")


class TestSetupContext:
    def test_set_attribute(self):
        ctx = SetupContext()
        ctx.foo = "bar"
        assert ctx.foo == "bar"

    def test_freeze_prevents_modification(self):
        ctx = SetupContext()
        ctx.foo = "bar"
        ctx.freeze()
        with pytest.raises(AttributeError):
            ctx.foo = "baz"

    def test_freeze_prevents_deletion(self):
        ctx = SetupContext()
        ctx.foo = "bar"
        ctx.freeze()
        with pytest.raises(AttributeError):
            del ctx.foo

    def test_freeze_prevents_new_attributes(self):
        ctx = SetupContext()
        ctx.freeze()
        with pytest.raises(AttributeError):
            ctx.new_attr = "value"


class TestGetParamsFromClass:
    def test_no_params_class(self):
        class NoParams:
            pass

        result = get_params_from_class(NoParams)
        assert result == {}

    def test_empty_params_class(self):
        class EmptyParams:
            class Params:
                pass

        result = get_params_from_class(EmptyParams)
        assert result == {}

    def test_params_with_defaults(self):
        class WithDefaults:
            class Params:
                name: str = "default"
                count: int = 10

        result = get_params_from_class(WithDefaults)
        assert "name" in result
        assert "count" in result
        assert result["name"][1] == "default"
        assert result["count"][1] == 10

    def test_params_without_defaults(self):
        class WithoutDefaults:
            class Params:
                name: str
                count: int

        result = get_params_from_class(WithoutDefaults)
        assert "name" in result
        assert "count" in result
        assert result["name"][1] == inspect.Parameter.empty
        assert result["count"][1] == inspect.Parameter.empty

    def test_params_with_annotated(self):
        class WithAnnotated:
            class Params:
                name: Annotated[str, typer.Option(help="A name")] = "test"

        result = get_params_from_class(WithAnnotated)
        assert "name" in result
        type_hint, default = result["name"]
        assert default == "test"


class TestBuildCliWithParams:
    def test_no_params_returns_original(self):
        class NoParams:
            pass

        def main(x: int):
            return x

        result = build_cli(NoParams, main)
        assert result is main

    def test_adds_custom_params_to_signature(self):
        class WithParams:
            class Params:
                custom: str = "default"

        def main(x: int, _params: dict = None):
            return x, _params

        wrapped = build_cli(WithParams, main)
        sig = inspect.signature(wrapped)

        # Should have 'x' and 'custom', but not '_params'
        param_names = list(sig.parameters.keys())
        assert "x" in param_names
        assert "custom" in param_names
        assert "_params" not in param_names

    def test_wrapper_separates_custom_params(self):
        class WithParams:
            class Params:
                custom: str = "default"

        received_params = None

        def main(x: int, _params: dict = None):
            nonlocal received_params
            received_params = _params
            return x

        wrapped = build_cli(WithParams, main)
        wrapped(x=42, custom="hello")

        assert received_params == {"custom": "hello"}

    def test_preserves_docstring(self):
        class WithParams:
            class Params:
                custom: str = "default"

        def main(x: int, _custom_params: dict = None):
            """This is a docstring."""
            pass

        wrapped = build_cli(WithParams, main)
        assert wrapped.__doc__ == "This is a docstring."

    def test_multiple_params(self):
        class WithMultipleParams:
            class Params:
                name: str = "default"
                count: int = 10
                flag: bool = False

        def main(x: int, _custom_params: dict = None):
            return _custom_params

        wrapped = build_cli(WithMultipleParams, main)
        sig = inspect.signature(wrapped)
        param_names = list(sig.parameters.keys())

        assert "name" in param_names
        assert "count" in param_names
        assert "flag" in param_names
