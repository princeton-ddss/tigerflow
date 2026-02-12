import inspect
from pathlib import Path
from typing import Annotated

import pytest
import typer

from tigerflow.models import SlurmResourceConfig, SlurmTaskConfig
from tigerflow.tasks._base import Task

TESTS_DIR = Path(__file__).parent


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


class TestSlurmTaskConfigScript:
    """Test SlurmTaskConfig.to_script() generation."""

    @pytest.fixture
    def task_dirs(self, tmp_path: Path):
        """Create input and output directories."""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()
        return input_dir, output_dir

    @pytest.fixture
    def slurm_resource_config(self):
        """Create a basic Slurm resource configuration."""
        return SlurmResourceConfig(
            cpus=4,
            memory="8G",
            time="01:00:00",
            gpus=1,
            sbatch_options=["--account=test_account"],
        )

    @pytest.fixture
    def slurm_task_config(self, task_dirs, slurm_resource_config):
        """Create a basic Slurm task configuration."""
        input_dir, output_dir = task_dirs
        module_path = TESTS_DIR / "tasks" / "slurm_echo.py"
        config = SlurmTaskConfig(
            name="test-slurm-task",
            kind="slurm",
            module=module_path,
            input_ext=".txt",
            output_ext=".out",
            max_workers=4,
            worker_resources=slurm_resource_config,
            setup_commands=["source /etc/profile", "module load python"],
            params={"prefix": ">>", "suffix": "<<"},
        )
        config.input_dir = input_dir
        config.output_dir = output_dir
        return config

    def test_basic_script_generation(self, slurm_task_config):
        """Test that to_script generates a valid sbatch script."""
        script = slurm_task_config.to_script()

        # Check shebang
        assert script.startswith("#!/bin/bash")

        # Check SBATCH directives
        assert f"#SBATCH --job-name={slurm_task_config.client_job_name}" in script
        assert "#SBATCH --nodes=1" in script
        assert "#SBATCH --ntasks=1" in script

        # Check account option from sbatch_options
        assert "#SBATCH --account=test_account" in script

    def test_script_includes_setup_commands(self, slurm_task_config):
        """Test that setup commands are included in the script."""
        script = slurm_task_config.to_script()

        assert "source /etc/profile" in script
        assert "module load python" in script

    def test_script_includes_task_command(self, slurm_task_config, task_dirs):
        """Test that the python command includes required arguments."""
        input_dir, output_dir = task_dirs
        script = slurm_task_config.to_script()

        assert f"--input-dir {input_dir}" in script
        assert f"--output-dir {output_dir}" in script
        assert "--input-ext .txt" in script
        assert "--output-ext .out" in script
        assert "--max-workers 4" in script
        assert "--run-directly" in script

    def test_script_includes_params(self, slurm_task_config):
        """Test that custom params are included in the script."""
        script = slurm_task_config.to_script()

        assert "--prefix" in script
        assert "--suffix" in script

    def test_client_job_name(self, slurm_task_config):
        """Test that client job name is derived from task name."""
        assert slurm_task_config.client_job_name == "test-slurm-task-client"

    def test_worker_job_name(self, slurm_task_config):
        """Test that worker job name is derived from task name."""
        assert slurm_task_config.worker_job_name == "test-slurm-task-worker"


class TestSlurmResourceConfig:
    """Test SlurmResourceConfig validation."""

    def test_basic_config(self):
        """Test basic resource configuration."""
        config = SlurmResourceConfig(
            cpus=4,
            memory="8G",
            time="01:00:00",
        )

        assert config.cpus == 4
        assert config.memory == "8G"
        assert config.time == "01:00:00"
        assert config.gpus is None
        assert config.sbatch_options == []

    def test_config_with_gpus(self):
        """Test resource configuration with GPUs."""
        config = SlurmResourceConfig(
            cpus=4,
            memory="16G",
            time="02:00:00",
            gpus=2,
        )

        assert config.gpus == 2

    def test_sbatch_options_are_stripped(self):
        """Test that sbatch options are stripped of whitespace."""
        config = SlurmResourceConfig(
            cpus=4,
            memory="8G",
            time="01:00:00",
            sbatch_options=["  --account=test  ", " --partition=gpu "],
        )

        assert config.sbatch_options == ["--account=test", "--partition=gpu"]
