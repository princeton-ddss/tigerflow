from pathlib import Path

import pytest
from pydantic import ValidationError

from tigerflow.models import (
    BaseTaskConfig,
    LocalAsyncTaskConfig,
    LocalTaskConfig,
    PipelineConfig,
    SlurmResourceConfig,
    SlurmTaskConfig,
    TaskStatus,
    TaskStatusKind,
)


class TestTaskStatusKind:
    def test_enum_values(self):
        assert TaskStatusKind.ACTIVE.value == "active"
        assert TaskStatusKind.INACTIVE.value == "inactive"
        assert TaskStatusKind.PENDING.value == "pending"

    def test_enum_members(self):
        members = list(TaskStatusKind)
        assert len(members) == 3


class TestTaskStatus:
    def test_active_is_alive(self):
        status = TaskStatus(kind=TaskStatusKind.ACTIVE)
        assert status.is_alive is True

    def test_pending_is_alive(self):
        status = TaskStatus(kind=TaskStatusKind.PENDING)
        assert status.is_alive is True

    def test_inactive_is_not_alive(self):
        status = TaskStatus(kind=TaskStatusKind.INACTIVE)
        assert status.is_alive is False

    def test_detail_default_none(self):
        status = TaskStatus(kind=TaskStatusKind.ACTIVE)
        assert status.detail is None

    def test_detail_with_value(self):
        status = TaskStatus(kind=TaskStatusKind.INACTIVE, detail="Process exited")
        assert status.detail == "Process exited"


class TestSlurmResourceConfig:
    def test_minimal_config(self):
        config = SlurmResourceConfig(cpus=4, memory="8G", time="1:00:00")
        assert config.cpus == 4
        assert config.gpus is None
        assert config.memory == "8G"
        assert config.time == "1:00:00"
        assert config.sbatch_options == []

    def test_full_config(self):
        config = SlurmResourceConfig(
            cpus=8,
            gpus=2,
            memory="16G",
            time="4:00:00",
            sbatch_options=["--partition=gpu", "--constraint=v100"],
        )
        assert config.gpus == 2
        assert len(config.sbatch_options) == 2


class TestBaseTaskConfig:
    def test_module_must_exist(self, tmp_path: Path):
        nonexistent = tmp_path / "nonexistent.py"
        with pytest.raises(ValidationError, match="Module does not exist"):
            BaseTaskConfig(name="test", module=nonexistent, input_ext=".txt")

    def test_module_must_be_file(self, tmp_path: Path):
        directory = tmp_path / "some_dir"
        directory.mkdir()
        with pytest.raises(ValidationError, match="Module is not a file"):
            BaseTaskConfig(name="test", module=directory, input_ext=".txt")

    def test_module_resolved_to_absolute(self, tmp_module: Path):
        config = BaseTaskConfig(name="test", module=tmp_module, input_ext=".txt")
        assert config.module.is_absolute()

    def test_valid_input_ext(self, tmp_module: Path):
        config = BaseTaskConfig(name="test", module=tmp_module, input_ext=".json")
        assert config.input_ext == ".json"

    def test_valid_compound_ext(self, tmp_module: Path):
        config = BaseTaskConfig(name="test", module=tmp_module, input_ext=".tar.gz")
        assert config.input_ext == ".tar.gz"

    def test_invalid_input_ext_no_dot(self, tmp_module: Path):
        with pytest.raises(ValidationError, match="Invalid file extension"):
            BaseTaskConfig(name="test", module=tmp_module, input_ext="txt")

    def test_reserved_err_ext(self, tmp_module: Path):
        with pytest.raises(ValidationError, match="reserved"):
            BaseTaskConfig(name="test", module=tmp_module, input_ext=".err")

    def test_input_dir_not_set_raises(self, tmp_module: Path):
        config = BaseTaskConfig(name="test", module=tmp_module, input_ext=".txt")
        with pytest.raises(ValueError, match="Input directory has not been set"):
            _ = config.input_dir

    def test_output_dir_not_set_raises(self, tmp_module: Path):
        config = BaseTaskConfig(name="test", module=tmp_module, input_ext=".txt")
        with pytest.raises(ValueError, match="Output directory has not been set"):
            _ = config.output_dir

    def test_input_dir_setter(self, tmp_module: Path, tmp_dirs: tuple[Path, Path]):
        input_dir, _ = tmp_dirs
        config = BaseTaskConfig(name="test", module=tmp_module, input_ext=".txt")
        config.input_dir = input_dir
        assert config.input_dir == input_dir

    def test_output_dir_setter(self, tmp_module: Path, tmp_dirs: tuple[Path, Path]):
        _, output_dir = tmp_dirs
        config = BaseTaskConfig(name="test", module=tmp_module, input_ext=".txt")
        config.output_dir = output_dir
        assert config.output_dir == output_dir

    def test_log_dir_property(self, tmp_module: Path, tmp_dirs: tuple[Path, Path]):
        _, output_dir = tmp_dirs
        config = BaseTaskConfig(name="test", module=tmp_module, input_ext=".txt")
        config.output_dir = output_dir
        assert config.log_dir == output_dir / "logs"

    def test_default_output_ext(self, tmp_module: Path):
        config = BaseTaskConfig(name="test", module=tmp_module, input_ext=".txt")
        assert config.output_ext == ".out"

    def test_default_keep_output(self, tmp_module: Path):
        config = BaseTaskConfig(name="test", module=tmp_module, input_ext=".txt")
        assert config.keep_output is True

    def test_library_field(self):
        config = BaseTaskConfig(
            name="test",
            library="tigerflow.library.echo",
            input_ext=".txt",
        )
        assert config.library == "tigerflow.library.echo"
        assert config.module is None

    def test_module_or_library_required(self):
        with pytest.raises(ValidationError, match="Either 'module' or 'library'"):
            BaseTaskConfig(name="test", input_ext=".txt")

    def test_module_and_library_mutually_exclusive(self, tmp_module: Path):
        with pytest.raises(ValidationError, match="Cannot specify both"):
            BaseTaskConfig(
                name="test",
                module=tmp_module,
                library="tigerflow.library.echo",
                input_ext=".txt",
            )

    def test_params_field(self):
        config = BaseTaskConfig(
            name="test",
            library="tigerflow.library.echo",
            params={"prefix": "Hello: ", "uppercase": True},
            input_ext=".txt",
        )
        assert config.params == {"prefix": "Hello: ", "uppercase": True}

    def test_params_default_empty(self):
        config = BaseTaskConfig(
            name="test",
            library="tigerflow.library.echo",
            input_ext=".txt",
        )
        assert config.params == {}

    def test_python_command_with_module(self, tmp_module: Path):
        config = BaseTaskConfig(name="test", module=tmp_module, input_ext=".txt")
        assert config.python_command == f"python {tmp_module}"

    def test_python_command_with_library(self):
        config = BaseTaskConfig(
            name="test",
            library="tigerflow.library.echo",
            input_ext=".txt",
        )
        assert config.python_command == "python -m tigerflow.library.echo"

    def test_params_as_cli_args_simple(self):
        config = BaseTaskConfig(
            name="test",
            library="tigerflow.library.echo",
            params={"prefix": "Hello"},
            input_ext=".txt",
        )
        args = config.params_as_cli_args
        assert "--prefix 'Hello'" in args

    def test_params_as_cli_args_boolean_true(self):
        config = BaseTaskConfig(
            name="test",
            library="tigerflow.library.echo",
            params={"uppercase": True},
            input_ext=".txt",
        )
        args = config.params_as_cli_args
        assert "--uppercase" in args

    def test_params_as_cli_args_boolean_false(self):
        config = BaseTaskConfig(
            name="test",
            library="tigerflow.library.echo",
            params={"uppercase": False},
            input_ext=".txt",
        )
        args = config.params_as_cli_args
        assert "--uppercase" not in args

    def test_params_as_cli_args_underscores_to_hyphens(self):
        config = BaseTaskConfig(
            name="test",
            library="tigerflow.library.echo",
            params={"max_length": 512},
            input_ext=".txt",
        )
        args = config.params_as_cli_args
        assert "--max-length 512" in args


class TestLocalTaskConfig:
    def test_to_script(self, tmp_module: Path, tmp_dirs: tuple[Path, Path]):
        input_dir, output_dir = tmp_dirs
        config = LocalTaskConfig(
            name="my_task",
            kind="local",
            module=tmp_module,
            input_ext=".json",
            output_ext=".csv",
        )
        config.input_dir = input_dir
        config.output_dir = output_dir

        script = config.to_script()

        assert "#!/bin/bash" in script
        assert "exec python" in script
        assert str(tmp_module) in script
        assert "--task-name my_task" in script
        assert f"--input-dir {input_dir}" in script
        assert "--input-ext .json" in script
        assert f"--output-dir {output_dir}" in script
        assert "--output-ext .csv" in script

    def test_to_script_with_setup_commands(
        self, tmp_module: Path, tmp_dirs: tuple[Path, Path]
    ):
        input_dir, output_dir = tmp_dirs
        config = LocalTaskConfig(
            name="my_task",
            kind="local",
            module=tmp_module,
            input_ext=".txt",
            setup_commands=["source venv/bin/activate", "export VAR=1"],
        )
        config.input_dir = input_dir
        config.output_dir = output_dir

        script = config.to_script()

        assert "source venv/bin/activate;export VAR=1" in script

    def test_to_script_with_library(self, tmp_dirs: tuple[Path, Path]):
        input_dir, output_dir = tmp_dirs
        config = LocalTaskConfig(
            name="my_task",
            kind="local",
            library="tigerflow.library.echo",
            input_ext=".txt",
            output_ext=".txt",
        )
        config.input_dir = input_dir
        config.output_dir = output_dir

        script = config.to_script()

        assert "python -m tigerflow.library.echo" in script

    def test_to_script_with_params(self, tmp_dirs: tuple[Path, Path]):
        input_dir, output_dir = tmp_dirs
        config = LocalTaskConfig(
            name="my_task",
            kind="local",
            library="tigerflow.library.echo",
            params={"prefix": "Hello", "uppercase": True},
            input_ext=".txt",
            output_ext=".txt",
        )
        config.input_dir = input_dir
        config.output_dir = output_dir

        script = config.to_script()

        assert "--prefix" in script
        assert "--uppercase" in script


class TestLocalAsyncTaskConfig:
    def test_concurrency_limit_required(self, tmp_module: Path):
        with pytest.raises(ValidationError):
            LocalAsyncTaskConfig(  # type: ignore
                name="test",
                kind="local_async",
                module=tmp_module,
                input_ext=".txt",
            )

    def test_to_script(self, tmp_module: Path, tmp_dirs: tuple[Path, Path]):
        input_dir, output_dir = tmp_dirs
        config = LocalAsyncTaskConfig(
            name="async_task",
            kind="local_async",
            module=tmp_module,
            input_ext=".txt",
            concurrency_limit=10,
        )
        config.input_dir = input_dir
        config.output_dir = output_dir

        script = config.to_script()

        assert "#!/bin/bash" in script
        assert "--concurrency-limit 10" in script


class TestSlurmTaskConfig:
    @pytest.fixture
    def slurm_config(self, tmp_module: Path, tmp_dirs: tuple[Path, Path]):
        input_dir, output_dir = tmp_dirs
        config = SlurmTaskConfig(
            name="slurm_task",
            kind="slurm",
            module=tmp_module,
            input_ext=".txt",
            output_ext=".json",
            account="myaccount",
            max_workers=4,
            worker_resources=SlurmResourceConfig(
                cpus=8,
                gpus=1,
                memory="16G",
                time="2:00:00",
            ),
        )
        config.input_dir = input_dir
        config.output_dir = output_dir
        return config

    def test_client_job_name(self, slurm_config: SlurmTaskConfig):
        assert slurm_config.client_job_name == "slurm_task-client"

    def test_worker_job_name(self, slurm_config: SlurmTaskConfig):
        assert slurm_config.worker_job_name == "slurm_task-worker"

    def test_to_script_contains_sbatch_directives(self, slurm_config: SlurmTaskConfig):
        script = slurm_config.to_script()

        assert "#SBATCH --account=myaccount" in script
        assert "#SBATCH --job-name=slurm_task-client" in script
        assert "#SBATCH --nodes=1" in script
        assert "#SBATCH --ntasks=1" in script

    def test_to_script_contains_task_command(self, slurm_config: SlurmTaskConfig):
        script = slurm_config.to_script()

        assert "--max-workers 4" in script
        assert "--cpus 8" in script
        assert "--memory 16G" in script
        assert "--time 2:00:00" in script
        assert "--gpus 1" in script
        assert "--run-directly" in script

    def test_to_script_without_gpus(
        self, tmp_module: Path, tmp_dirs: tuple[Path, Path]
    ):
        input_dir, output_dir = tmp_dirs
        config = SlurmTaskConfig(
            name="cpu_task",
            kind="slurm",
            module=tmp_module,
            input_ext=".txt",
            account="myaccount",
            max_workers=2,
            worker_resources=SlurmResourceConfig(
                cpus=4,
                memory="8G",
                time="1:00:00",
            ),
        )
        config.input_dir = input_dir
        config.output_dir = output_dir

        script = config.to_script()

        assert "--gpus" not in script

    def test_to_script_with_sbatch_options(
        self, tmp_module: Path, tmp_dirs: tuple[Path, Path]
    ):
        input_dir, output_dir = tmp_dirs
        config = SlurmTaskConfig(
            name="task",
            kind="slurm",
            module=tmp_module,
            input_ext=".txt",
            account="myaccount",
            max_workers=2,
            worker_resources=SlurmResourceConfig(
                cpus=4,
                memory="8G",
                time="1:00:00",
                sbatch_options=[
                    "--partition=gpu",
                    "--mail-user=tigerflow@princeton.edu",
                ],
            ),
        )
        config.input_dir = input_dir
        config.output_dir = output_dir

        script = config.to_script()

        assert "--sbatch-option '--partition=gpu'" in script
        assert "--sbatch-option '--mail-user=tigerflow@princeton.edu'" in script


class TestPipelineConfig:
    def test_empty_tasks_rejected(self):
        with pytest.raises(ValidationError):
            PipelineConfig(tasks=[])

    def test_duplicate_task_names_rejected(self, tmp_module: Path):
        with pytest.raises(ValidationError, match="Duplicate task name"):
            PipelineConfig(
                tasks=[
                    LocalTaskConfig(
                        name="task1",
                        kind="local",
                        module=tmp_module,
                        input_ext=".txt",
                    ),
                    LocalTaskConfig(
                        name="task1",
                        kind="local",
                        module=tmp_module,
                        input_ext=".txt",
                    ),
                ]
            )

    def test_unknown_dependency_rejected(self, tmp_module: Path):
        with pytest.raises(ValidationError, match="depends on unknown task"):
            PipelineConfig(
                tasks=[
                    LocalTaskConfig(
                        name="task1",
                        kind="local",
                        module=tmp_module,
                        input_ext=".txt",
                        depends_on="nonexistent",
                    ),
                ]
            )

    def test_extension_mismatch_rejected(self, tmp_module: Path):
        with pytest.raises(ValidationError, match="Extension mismatch"):
            PipelineConfig(
                tasks=[
                    LocalTaskConfig(
                        name="task1",
                        kind="local",
                        module=tmp_module,
                        input_ext=".txt",
                        output_ext=".json",
                    ),
                    LocalTaskConfig(
                        name="task2",
                        kind="local",
                        module=tmp_module,
                        input_ext=".csv",
                        depends_on="task1",
                    ),
                ]
            )

    def test_valid_dependency_chain(self, tmp_module: Path):
        pipeline = PipelineConfig(
            tasks=[
                LocalTaskConfig(
                    name="task1",
                    kind="local",
                    module=tmp_module,
                    input_ext=".txt",
                    output_ext=".json",
                ),
                LocalTaskConfig(
                    name="task2",
                    kind="local",
                    module=tmp_module,
                    input_ext=".json",
                    output_ext=".csv",
                    depends_on="task1",
                ),
            ]
        )
        assert len(pipeline.tasks) == 2

    def test_tasks_sorted_topologically(self, tmp_module: Path):
        # Submit tasks in reverse order
        pipeline = PipelineConfig(
            tasks=[
                LocalTaskConfig(
                    name="task2",
                    kind="local",
                    module=tmp_module,
                    input_ext=".json",
                    depends_on="task1",
                ),
                LocalTaskConfig(
                    name="task1",
                    kind="local",
                    module=tmp_module,
                    input_ext=".txt",
                    output_ext=".json",
                ),
            ]
        )
        # Should be sorted so task1 comes before task2
        assert pipeline.tasks[0].name == "task1"
        assert pipeline.tasks[1].name == "task2"

    def test_root_tasks_must_share_input_ext(self, tmp_module: Path):
        with pytest.raises(ValidationError, match="same input extension"):
            PipelineConfig(
                tasks=[
                    LocalTaskConfig(
                        name="task1",
                        kind="local",
                        module=tmp_module,
                        input_ext=".txt",
                    ),
                    LocalTaskConfig(
                        name="task2",
                        kind="local",
                        module=tmp_module,
                        input_ext=".json",
                    ),
                ]
            )

    def test_root_input_ext_property(self, tmp_module: Path):
        pipeline = PipelineConfig(
            tasks=[
                LocalTaskConfig(
                    name="task1",
                    kind="local",
                    module=tmp_module,
                    input_ext=".txt",
                ),
            ]
        )
        assert pipeline.root_input_ext == ".txt"

    def test_root_tasks_property(self, tmp_module: Path):
        pipeline = PipelineConfig(
            tasks=[
                LocalTaskConfig(
                    name="task1",
                    kind="local",
                    module=tmp_module,
                    input_ext=".txt",
                    output_ext=".json",
                ),
                LocalTaskConfig(
                    name="task2",
                    kind="local",
                    module=tmp_module,
                    input_ext=".json",
                    depends_on="task1",
                ),
            ]
        )
        root_tasks = pipeline.root_tasks
        assert len(root_tasks) == 1
        assert root_tasks[0].name == "task1"

    def test_terminal_tasks_property(self, tmp_module: Path):
        pipeline = PipelineConfig(
            tasks=[
                LocalTaskConfig(
                    name="task1",
                    kind="local",
                    module=tmp_module,
                    input_ext=".txt",
                    output_ext=".json",
                ),
                LocalTaskConfig(
                    name="task2",
                    kind="local",
                    module=tmp_module,
                    input_ext=".json",
                    depends_on="task1",
                ),
            ]
        )
        terminal_tasks = pipeline.terminal_tasks
        assert len(terminal_tasks) == 1
        assert terminal_tasks[0].name == "task2"

    def test_branching_pipeline(self, tmp_module: Path):
        # task1 -> task2; task1 -> task3
        pipeline = PipelineConfig(
            tasks=[
                LocalTaskConfig(
                    name="task1",
                    kind="local",
                    module=tmp_module,
                    input_ext=".txt",
                    output_ext=".json",
                ),
                LocalTaskConfig(
                    name="task2",
                    kind="local",
                    module=tmp_module,
                    input_ext=".json",
                    depends_on="task1",
                ),
                LocalTaskConfig(
                    name="task3",
                    kind="local",
                    module=tmp_module,
                    input_ext=".json",
                    depends_on="task1",
                ),
            ]
        )
        assert len(pipeline.root_tasks) == 1
        assert len(pipeline.terminal_tasks) == 2

    def test_dependency_cycle_rejected(self, tmp_module: Path):
        # task1 -> task2 -> task3 -> task1
        with pytest.raises(ValidationError, match="cycle"):
            PipelineConfig(
                tasks=[
                    LocalTaskConfig(
                        name="task1",
                        kind="local",
                        module=tmp_module,
                        input_ext=".txt",
                        output_ext=".json",
                        depends_on="task3",
                    ),
                    LocalTaskConfig(
                        name="task2",
                        kind="local",
                        module=tmp_module,
                        input_ext=".json",
                        output_ext=".csv",
                        depends_on="task1",
                    ),
                    LocalTaskConfig(
                        name="task3",
                        kind="local",
                        module=tmp_module,
                        input_ext=".csv",
                        output_ext=".txt",
                        depends_on="task2",
                    ),
                ]
            )
