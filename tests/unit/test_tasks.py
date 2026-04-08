import inspect
from pathlib import Path
from typing import Annotated

import pytest
import typer

from tigerflow.tasks._base import Task
from tigerflow.utils import TEMP_FILE_PREFIX


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

    def test_unsupported_param_type_rejected_by_typer(self):
        class CustomModel:
            pass

        class BadTask(Task):
            @classmethod
            def cli(cls):
                pass

            class Params:
                model: CustomModel = CustomModel()

        def base_main(input_dir: str, _params: dict):
            pass

        wrapped = BadTask.build_cli(base_main)

        with pytest.raises(RuntimeError, match="Type not yet supported"):
            typer.run(wrapped)


class TestRemoveTemporaryFiles:
    def test_removes_files_with_temp_prefix(self, tmp_path: Path):
        temp = tmp_path / f"{TEMP_FILE_PREFIX}abc123.csv"
        temp.write_text("partial")
        normal = tmp_path / "data.csv"
        normal.write_text("real")

        Task._remove_temporary_files(tmp_path)

        assert not temp.exists()
        assert normal.exists()

    def test_ignores_directories(self, tmp_path: Path):
        subdir = tmp_path / f"{TEMP_FILE_PREFIX}dir"
        subdir.mkdir()

        Task._remove_temporary_files(tmp_path)

        assert subdir.exists()

    def test_empty_directory(self, tmp_path: Path):
        Task._remove_temporary_files(tmp_path)  # Should not raise


class TestGetUnprocessedFiles:
    def test_includes_unprocessed_input(self, tmp_path: Path):
        input_dir = tmp_path / "in"
        output_dir = tmp_path / "out"
        input_dir.mkdir()
        output_dir.mkdir()

        (input_dir / "doc1.txt").write_text("data")

        result = Task._get_unprocessed_files(
            input_dir=input_dir,
            input_ext=".txt",
            output_dir=output_dir,
            output_ext=".csv",
        )
        assert len(result) == 1
        assert result[0].name == "doc1.txt"

    def test_includes_input_with_temp_output(self, tmp_path: Path):
        input_dir = tmp_path / "in"
        output_dir = tmp_path / "out"
        input_dir.mkdir()
        output_dir.mkdir()

        (input_dir / "doc1.txt").write_text("data")
        (output_dir / f"{TEMP_FILE_PREFIX}xyz.csv").write_text("partial")

        result = Task._get_unprocessed_files(
            input_dir=input_dir,
            input_ext=".txt",
            output_dir=output_dir,
            output_ext=".csv",
        )
        assert len(result) == 1
        assert result[0].name == "doc1.txt"

    def test_includes_input_with_temp_error(self, tmp_path: Path):
        input_dir = tmp_path / "in"
        output_dir = tmp_path / "out"
        input_dir.mkdir()
        output_dir.mkdir()

        (input_dir / "doc1.txt").write_text("data")
        (output_dir / f"{TEMP_FILE_PREFIX}xyz.err").write_text("partial err")

        result = Task._get_unprocessed_files(
            input_dir=input_dir,
            input_ext=".txt",
            output_dir=output_dir,
            output_ext=".csv",
        )
        assert len(result) == 1
        assert result[0].name == "doc1.txt"

    def test_excludes_temp_input(self, tmp_path: Path):
        input_dir = tmp_path / "in"
        output_dir = tmp_path / "out"
        input_dir.mkdir()
        output_dir.mkdir()

        (input_dir / "doc1.txt").write_text("real")
        (input_dir / f"{TEMP_FILE_PREFIX}abc.txt").write_text("partial")

        result = Task._get_unprocessed_files(
            input_dir=input_dir,
            input_ext=".txt",
            output_dir=output_dir,
            output_ext=".csv",
        )
        assert len(result) == 1
        assert result[0].name == "doc1.txt"

    def test_excludes_input_with_final_output(self, tmp_path: Path):
        input_dir = tmp_path / "in"
        output_dir = tmp_path / "out"
        input_dir.mkdir()
        output_dir.mkdir()

        (input_dir / "doc1.txt").write_text("data")
        (output_dir / "doc1.csv").write_text("done")

        result = Task._get_unprocessed_files(
            input_dir=input_dir,
            input_ext=".txt",
            output_dir=output_dir,
            output_ext=".csv",
        )
        assert result == []

    def test_excludes_input_with_final_error(self, tmp_path: Path):
        input_dir = tmp_path / "in"
        output_dir = tmp_path / "out"
        input_dir.mkdir()
        output_dir.mkdir()

        (input_dir / "doc1.txt").write_text("data")
        (output_dir / "doc1.err").write_text("failed")

        result = Task._get_unprocessed_files(
            input_dir=input_dir,
            input_ext=".txt",
            output_dir=output_dir,
            output_ext=".csv",
        )
        assert result == []
