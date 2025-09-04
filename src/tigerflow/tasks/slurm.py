import time
import traceback
from abc import abstractmethod
from pathlib import Path

import typer
from dask.distributed import Client, Future, Worker, WorkerPlugin, get_worker
from dask_jobqueue import SLURMCluster
from typing_extensions import Annotated

from tigerflow.config import SlurmResourceConfig
from tigerflow.logconfig import logger
from tigerflow.utils import SetupContext, atomic_write, validate_file_ext

from ._base import Task


class SlurmTask(Task):
    """
    Execute the user-defined task in parallel by distributing
    the workload across Slurm jobs acting as cluster workers.
    """

    @logger.catch(reraise=True)
    def __init__(
        self,
        resources: SlurmResourceConfig,
        setup_commands: str | None = None,
    ):
        self._resources = resources
        self._setup_commands = setup_commands

    @logger.catch(reraise=True)
    def start(
        self,
        input_dir: Path,
        input_ext: str,
        output_dir: Path,
        output_ext: str,
    ):
        for path in (input_dir, output_dir):
            if not path.exists():
                raise FileNotFoundError(path)
        for ext in (input_ext, output_ext):
            validate_file_ext(ext)

        # Create subdirectory to store log files
        log_dir = output_dir / "logs"
        log_dir.mkdir(exist_ok=True)

        # Reference functions to use in plugin
        setup_func = type(self).setup
        teardown_func = type(self).teardown

        class TaskWorkerPlugin(WorkerPlugin):
            def setup(self, worker: Worker):
                logger.info("Setting up task")
                worker.context = SetupContext()
                setup_func(worker.context)
                worker.context.freeze()  # Make it read-only
                logger.info("Task setup complete")

            def teardown(self, worker: Worker):
                logger.info("Shutting down task")
                teardown_func(worker.context)
                logger.info("Task shutdown complete")

        def task(input_file: Path, output_file: Path):
            worker = get_worker()
            try:
                logger.info("Starting processing: {}", input_file.name)
                with atomic_write(output_file) as temp_file:
                    self.run(worker.context, input_file, temp_file)
                logger.info("Successfully processed: {}", input_file.name)
            except Exception:
                error_fname = output_file.name.removesuffix(output_ext) + ".err"
                error_file = output_dir / error_fname
                with atomic_write(error_file) as temp_file:
                    with open(temp_file, "w") as f:
                        f.write(traceback.format_exc())
                logger.error("Failed processing: {}", input_file.name)

        # Define parameters for each Slurm job
        cluster = SLURMCluster(
            cores=self._resources.cpus,
            memory=self._resources.memory,
            walltime=self._resources.time,
            processes=1,
            job_extra_directives=[
                f"--output={log_dir}/dask-worker-%J.out",
                f"--error={log_dir}/dask-worker-%J.err",
                f"--gres=gpu:{self._resources.gpus}" if self._resources.gpus else "",
            ],
            job_script_prologue=(
                self._setup_commands.splitlines() if self._setup_commands else None
            ),
        )

        # Enable autoscaling
        cluster.adapt(
            minimum_jobs=0,
            maximum_jobs=self._resources.max_workers,
        )

        # Instantiate a cluster client
        client = Client(cluster)
        client.register_plugin(TaskWorkerPlugin())

        # Clean up incomplete temporary files left behind by a prior cluster instance
        self._remove_temporary_files(output_dir)

        # Monitor for new files and enqueue them for processing
        active_futures: dict[Path, Future] = dict()
        while True:
            unprocessed_files = self._get_unprocessed_files(
                input_dir,
                input_ext,
                output_dir,
                output_ext,
            )

            for file in unprocessed_files:
                if file not in active_futures:  # Exclude in-progress files
                    output_fname = file.name.removesuffix(input_ext) + output_ext
                    output_file = output_dir / output_fname
                    future = client.submit(task, file, output_file)
                    active_futures[file] = future

            for key in list(active_futures.keys()):
                if active_futures[key].done():
                    del active_futures[key]

            time.sleep(3)

    @classmethod
    def cli(cls):
        """
        Run the task as a CLI application
        """

        def main(
            input_dir: Annotated[
                Path,
                typer.Option(
                    help="Input directory to read data",
                    show_default=False,
                ),
            ],
            input_ext: Annotated[
                str,
                typer.Option(
                    help="Input file extension",
                    show_default=False,
                ),
            ],
            output_dir: Annotated[
                Path,
                typer.Option(
                    help="Output directory to store results",
                    show_default=False,
                ),
            ],
            output_ext: Annotated[
                str,
                typer.Option(
                    help="Output file extension",
                    show_default=False,
                ),
            ],
            cpus: Annotated[
                int,
                typer.Option(
                    help="Number of CPUs per worker",
                    show_default=False,
                ),
            ],
            memory: Annotated[
                str,
                typer.Option(
                    help="Memory per worker",
                    show_default=False,
                ),
            ],
            time: Annotated[
                str,
                typer.Option(
                    help="Wall time per worker",
                    show_default=False,
                ),
            ],
            max_workers: Annotated[
                int,
                typer.Option(
                    help="Max number of workers for autoscaling",
                    show_default=False,
                ),
            ],
            gpus: Annotated[
                int | None,
                typer.Option(
                    help="Number of GPUs per worker",
                ),
            ] = None,
            setup_commands: Annotated[
                str | None,
                typer.Option(
                    help="""
                    Shell commands to run before the task starts
                    (separate commands with a semicolon)
                    """,
                ),
            ] = None,
        ):
            """
            Run the task as a CLI application
            """
            resources = SlurmResourceConfig(
                cpus=cpus,
                gpus=gpus,
                memory=memory,
                time=time,
                max_workers=max_workers,
            )

            task = cls(resources, setup_commands)

            task.start(input_dir, input_ext, output_dir, output_ext)

        typer.run(main)

    @staticmethod
    def setup(context: SetupContext):
        """
        Establish a shared setup to be used across different runs.

        Parameters
        ----------
        context : SetupContext
            Namespace to store any common, reusable data/objects
            (e.g., large language model, DB connection).
        """
        pass

    @staticmethod
    @abstractmethod
    def run(context: SetupContext, input_file: Path, output_file: Path):
        """
        Define the processing logic to be applied to each input file.

        Parameters
        ----------
        context : SetupContext
            Read-only namespace for retrieving setup data/objects
            (e.g., large language model, DB connection).
        input_file : Path
            Path to the input file to be processed
        output_file : Path
            Path to the output file to be generated

        Notes
        -----
        Unlike during setup, the `context` here is read-only
        and will raise an error if modified.
        """
        pass

    @staticmethod
    def teardown(context: SetupContext):
        """
        Define cleanup logic (e.g., closing a DB connection)
        to be executed upon termination.

        Parameters
        ----------
        context : SetupContext
            Read-only namespace for retrieving setup data/objects
            (e.g., large language model, DB connection).
        """
        pass
