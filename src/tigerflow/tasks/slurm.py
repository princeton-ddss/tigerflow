import time
from abc import abstractmethod
from pathlib import Path

import typer
from dask.distributed import Client, Future, Worker, WorkerPlugin, get_worker
from dask_jobqueue import SLURMCluster
from typing_extensions import Annotated

from tigerflow.config import SlurmResourceConfig
from tigerflow.utils import SetupContext, atomic_write

from ._base import Task


class SlurmTask(Task):
    """
    Execute the user-defined task in parallel by distributing
    the workload across Slurm jobs acting as cluster workers.
    """

    def __init__(
        self,
        resources: SlurmResourceConfig,
        setup_commands: str | None = None,
    ):
        self.resources = resources
        self.setup_commands = setup_commands

    def start(self, input_dir: Path, output_dir: Path):
        for p in [input_dir, output_dir]:
            if not p.exists():
                raise FileNotFoundError(p)

        # Reference methods that must be implemented in subclass
        setup_func = type(self).setup
        run_func = type(self).run
        teardown_func = type(self).teardown

        class TaskWorkerPlugin(WorkerPlugin):
            def setup(self, worker: Worker):
                worker.context = SetupContext()
                setup_func(worker.context)
                worker.context.freeze()  # Make it read-only

            def teardown(self, worker: Worker):
                teardown_func(worker.context)

        def task(input_file: Path, output_file: Path):
            worker = get_worker()
            try:
                with atomic_write(output_file) as temp_file:
                    run_func(worker.context, input_file, temp_file)
            except Exception as e:
                with atomic_write(output_file.with_suffix(".err")) as temp_file:
                    with open(temp_file, "w") as f:
                        f.write(str(e))

        # Define parameters for each Slurm job
        cluster = SLURMCluster(
            cores=self.resources.cpus,
            memory=self.resources.memory,
            walltime=self.resources.time,
            processes=1,
            worker_extra_args=(
                [f"--gres=gpu:{self.resources.gpus}"] if self.resources.gpus else None
            ),
            job_script_prologue=(
                self.setup_commands.splitlines() if self.setup_commands else None
            ),
            local_directory=output_dir,
            log_directory=output_dir,
        )

        # Enable autoscaling
        cluster.adapt(
            minimum_jobs=0,
            maximum_jobs=self.resources.max_workers,
        )

        # Instantiate a cluster client
        client = Client(cluster)
        client.register_plugin(TaskWorkerPlugin())

        # Clean up incomplete temporary files left behind by a prior cluster instance
        self._remove_temporary_files(output_dir)

        # Monitor for new files and enqueue them for processing
        active_futures: dict[str, Future] = dict()
        while True:
            unprocessed_files = self._get_unprocessed_files(input_dir, output_dir)

            for file in unprocessed_files:
                if file.stem not in active_futures:  # Exclude in-progress files
                    output_file = output_dir / file.with_suffix(".out").name
                    future = client.submit(task, file, output_file)
                    active_futures[file.stem] = future

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
                typer.Argument(
                    help="Input directory to read data",
                    show_default=False,
                ),
            ],
            output_dir: Annotated[
                Path,
                typer.Argument(
                    help="Output directory to store results",
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

            task.start(input_dir, output_dir)

        typer.run(main)

    @staticmethod
    @abstractmethod
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
    @abstractmethod
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
