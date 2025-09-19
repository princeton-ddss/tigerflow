import signal
import subprocess
import sys
import threading
import time
import traceback
from abc import abstractmethod
from pathlib import Path
from types import FrameType

import typer
from dask.distributed import Client, Future, Worker, WorkerPlugin, get_worker
from dask_jobqueue import SLURMCluster
from typing_extensions import Annotated

from tigerflow.logconfig import logger
from tigerflow.models import (
    SlurmResourceConfig,
    SlurmTaskConfig,
    TaskStatus,
    TaskStatusKind,
)
from tigerflow.utils import SetupContext, atomic_write, submit_to_slurm

from ._base import Task
from .utils import get_slurm_task_status


class SlurmTask(Task):
    """
    Execute the user-defined task in parallel by distributing
    the workload across Slurm jobs acting as cluster workers.
    """

    @logger.catch(reraise=True)
    def __init__(self, config: SlurmTaskConfig):
        self.config = config

    @logger.catch(reraise=True)
    def start(self, input_dir: Path, output_dir: Path):
        for path in (input_dir, output_dir):
            if not path.exists():
                raise FileNotFoundError(path)

        self.config.input_dir = input_dir
        self.config.output_dir = output_dir
        self.config.log_dir.mkdir(exist_ok=True)

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
                error_fname = (
                    output_file.name.removesuffix(self.config.output_ext) + ".err"
                )
                error_file = self.config.output_dir / error_fname
                with atomic_write(error_file) as temp_file:
                    with open(temp_file, "w") as f:
                        f.write(traceback.format_exc())
                logger.error("Failed processing: {}", input_file.name)

        # Define parameters for each Slurm job
        cluster = SLURMCluster(
            cores=self.config.resources.cpus,
            memory=self.config.resources.memory,
            walltime=self.config.resources.time,
            processes=1,
            job_extra_directives=[
                f"--job-name={self.config.worker_job_name}",
                f"--output={self.config.log_dir}/%x-%j.out",
                f"--error={self.config.log_dir}/%x-%j.err",
                f"--gres=gpu:{self.config.resources.gpus}"
                if self.config.resources.gpus
                else "",
            ],
            job_script_prologue=(
                self.config.setup_commands.splitlines()
                if self.config.setup_commands
                else None
            ),
        )

        # Enable autoscaling
        cluster.adapt(
            minimum_jobs=0,
            maximum_jobs=self.config.resources.max_workers,
            interval="15s",  # How often to check for scaling decisions
            wait_count=8,  # Consecutive idle checks before removing a worker
        )

        # Instantiate a cluster client
        client = Client(cluster)
        client.register_plugin(TaskWorkerPlugin())

        # Clean up incomplete temporary files left behind by a prior cluster instance
        self._remove_temporary_files(self.config.output_dir)

        # Monitor for new files and enqueue them for processing
        active_futures: dict[Path, Future] = dict()
        while True:
            unprocessed_files = self._get_unprocessed_files(
                input_dir=self.config.input_dir,
                input_ext=self.config.input_ext,
                output_dir=self.config.output_dir,
                output_ext=self.config.output_ext,
            )

            for file in unprocessed_files:
                if file not in active_futures:  # Exclude in-progress files
                    output_fname = (
                        file.name.removesuffix(self.config.input_ext)
                        + self.config.output_ext
                    )
                    output_file = self.config.output_dir / output_fname
                    future = client.submit(task, file, output_file)
                    active_futures[file] = future

            for key in list(active_futures.keys()):
                if active_futures[key].done():
                    active_futures[key].release()
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
            task_name: Annotated[
                str,
                typer.Option(
                    help="Task name",
                ),
            ] = cls.get_name(),
            _run_directly: Annotated[
                bool,
                typer.Option(
                    "--run-directly",
                    help="""
                    Run the task directly in the current process
                    rather than submitting to Slurm.
                    """,
                    hidden=True,  # Internal use only
                ),
            ] = False,
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

            config = SlurmTaskConfig(
                name=task_name,
                kind="slurm",
                module=cls.get_module_path(),
                input_ext=input_ext,
                output_ext=output_ext,
                setup_commands=setup_commands,
                resources=resources,
            )

            if _run_directly:
                task = cls(config)
                task.start(input_dir, output_dir)
            else:
                runner = SlurmTaskRunner(config)
                runner.start(input_dir, output_dir)

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


class SlurmTaskRunner:
    """
    Orchestrate Slurm task execution from a login/head node.
    """

    @logger.catch(reraise=True)
    def __init__(self, config: SlurmTaskConfig):
        self.config = config
        self._job_id: int | None = None
        self._status: TaskStatus = TaskStatus(kind=TaskStatusKind.INACTIVE)
        self._processed_filenames: set[str] = set()
        self._error_filenames: set[str] = set()
        self._shutdown_event = threading.Event()
        self._received_signal: int | None = None

    def _signal_handler(self, signum: int, frame: FrameType | None):
        logger.warning("Received signal {}, initiating shutdown", signum)
        self._received_signal = signum
        self._shutdown_event.set()

    @logger.catch(reraise=True)
    def start(self, input_dir: Path, output_dir: Path):
        for path in (input_dir, output_dir):
            if not path.exists():
                raise FileNotFoundError(path)

        self.config.input_dir = input_dir
        self.config.output_dir = output_dir

        # Register signal handlers for graceful shutdown
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, self._signal_handler)

        try:
            script = self.config.to_script()
            self._job_id = submit_to_slurm(script)
            logger.info("Submitted task with Slurm job ID {}", self._job_id)
            while not self._shutdown_event.is_set():
                self._check_status()
                self._handle_timeout()
                self._report_processed_files()
                self._report_failed_files()
                self._shutdown_event.wait(timeout=10)  # Interruptible sleep
        finally:
            logger.info("Shutting down task")
            if self._status.is_alive:
                subprocess.run(["scancel", str(self._job_id)])
            while self._status.is_alive:
                self._check_status()
                time.sleep(1)
            logger.info("Task shutdown complete")
            if self._received_signal is not None:
                sys.exit(128 + self._received_signal)

    def _check_status(self):
        status = get_slurm_task_status(self._job_id, self.config.worker_job_name)

        if self._status != status:
            old_status = self._status
            self._status = status
            log_func = logger.info if status.is_alive else logger.error
            log_func(
                "Status changed: {}{} -> {}{}",
                old_status.kind.name,
                f" ({old_status.detail})" if old_status.detail else "",
                status.kind.name,
                f" ({status.detail})" if status.detail else "",
            )

    def _handle_timeout(self):
        if not self._status.is_alive and "TIMEOUT" in self._status.detail:
            script = self.config.to_script()
            self._job_id = submit_to_slurm(script)
            logger.info("Re-submitted with Slurm job ID {}", self._job_id)

    def _report_processed_files(self):
        n_files = 0
        for file in self.config.output_dir.iterdir():
            if (
                file.is_file()
                and file.name.endswith(self.config.output_ext)
                and file.name not in self._processed_filenames
            ):
                self._processed_filenames.add(file.name)
                n_files += 1
        if n_files > 0:
            logger.info("{} processed files", n_files)

    def _report_failed_files(self):
        n_files = 0
        for file in self.config.output_dir.iterdir():
            if (
                file.is_file()
                and file.name.endswith(".err")
                and file.name not in self._error_filenames
            ):
                self._error_filenames.add(file.name)
                n_files += 1
        if n_files > 0:
            logger.error("{} failed files", n_files)
