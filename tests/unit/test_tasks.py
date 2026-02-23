import inspect
from typing import Annotated

import pytest
import typer

from tigerflow.tasks._base import Task


class TestGetParamsFromClass:
    def test_class_without_params_returns_empty(self):
        class NoParams(Task):
            @classmethod
            def cli(cls):
                pass

        assert NoParams._get_params_from_class() == {}

    def test_class_with_params_and_defaults(self):
        class WithDefaults(Task):
            @classmethod
            def cli(cls):
                pass

            class Params:
                prefix: str = "hello"
                count: int = 10

        result = WithDefaults._get_params_from_class()
        assert "prefix" in result
        assert "count" in result
        assert result["prefix"][1] == "hello"
        assert result["count"][1] == 10

    def test_class_with_params_no_defaults(self):
        class NoDefaults(Task):
            @classmethod
            def cli(cls):
                pass

            class Params:
                required_param: str

        result = NoDefaults._get_params_from_class()
        assert "required_param" in result
        assert result["required_param"][1] == inspect.Parameter.empty

    def test_class_with_annotated_params(self):
        class AnnotatedParams(Task):
            @classmethod
            def cli(cls):
                pass

            class Params:
                prefix: Annotated[
                    str,
                    typer.Option(help="A prefix"),
                ] = ""

        result = AnnotatedParams._get_params_from_class()
        assert "prefix" in result
        assert result["prefix"][1] == ""
        # Verify Annotated metadata is preserved
        type_hint = result["prefix"][0]
        assert hasattr(type_hint, "__metadata__")


class TestBuildCli:
    def test_no_custom_params(self):
        class NoParams(Task):
            @classmethod
            def cli(cls):
                pass

        def base_main(input_dir: str, _params: dict):
            return {"input_dir": input_dir, "params": _params}

        wrapped = NoParams.build_cli(base_main)

        # Signature should not include _params
        sig = inspect.signature(wrapped)
        assert "_params" not in sig.parameters
        assert "input_dir" in sig.parameters

        # Wrapper should pass _params={} when no Params class
        result = wrapped(input_dir="/test")
        assert result["input_dir"] == "/test"
        assert result["params"] == {}

    def test_with_custom_params(self):
        class WithParams(Task):
            @classmethod
            def cli(cls):
                pass

            class Params:
                prefix: str = "default"
                count: int = 5

        def base_main(input_dir: str, _params: dict):
            return {"input_dir": input_dir, "params": _params}

        wrapped = WithParams.build_cli(base_main)

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
        class WithDefaults(Task):
            @classmethod
            def cli(cls):
                pass

            class Params:
                option: str = "default_value"

        def base_main(input_dir: str, _params: dict):
            return _params

        wrapped = WithDefaults.build_cli(base_main)
        sig = inspect.signature(wrapped)

        # Default should be preserved
        assert sig.parameters["option"].default == "default_value"

    def test_annotated_types_preserved_in_signature(self):
        class WithAnnotated(Task):
            @classmethod
            def cli(cls):
                pass

            class Params:
                prefix: Annotated[
                    str,
                    typer.Option(help="A prefix"),
                ] = ""

        def base_main(input_dir: str, _params: dict):
            return _params

        wrapped = WithAnnotated.build_cli(base_main)
        sig = inspect.signature(wrapped)

        # Verify Annotated metadata flows to wrapper signature
        prefix_param = sig.parameters["prefix"]
        assert hasattr(prefix_param.annotation, "__metadata__")

    def test_parameter_collision_raises_error(self):
        class MyTask(Task):
            @classmethod
            def cli(cls):
                pass

            class Params:
                input_dir: str = "/collision"  # Collides with base param

        def base_main(input_dir: str, output_dir: str, _params: dict):
            pass

        with pytest.raises(ValueError, match="Parameter name collision") as exc_info:
            MyTask.build_cli(base_main)

        error_msg = str(exc_info.value)
        assert "MyTask.Params" in error_msg
        assert "input_dir" in error_msg
        assert "reserved" in error_msg
