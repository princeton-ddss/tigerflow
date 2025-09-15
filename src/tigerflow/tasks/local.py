import signal
import sys
import threading
import traceback
from abc import abstractmethod
from pathlib import Path
from types import FrameType

import typer
from typing_extensions import Annotated

from tigerflow.logconfig import logger
from tigerflow.models import LocalTaskConfig
from tigerflow.utils import SetupContext, atomic_write

from ._base import Task


class LocalTask(Task):
    @logger.catch(reraise=True)
    def __init__(self, config: LocalTaskConfig):
        self.config = config
        self._context = SetupContext()
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

        def task(input_file: Path, output_file: Path):
            try:
                logger.info("Starting processing: {}", input_file.name)
                with atomic_write(output_file) as temp_file:
                    self.run(self._context, input_file, temp_file)
                logger.info("Successfully processed: {}", input_file.name)
            except Exception:
                error_fname = (
                    output_file.name.removesuffix(self.config.output_ext) + ".err"
                )
                error_file = output_dir / error_fname
                with atomic_write(error_file) as temp_file:
                    with open(temp_file, "w") as f:
                        f.write(traceback.format_exc())
                logger.error("Failed processing: {}", input_file.name)

        # Clean up incomplete temporary files left behind by a prior process instance
        self._remove_temporary_files(output_dir)

        # Run common setup
        logger.info("Setting up task")
        self.setup(self._context)
        self._context.freeze()  # Make it read-only
        logger.info("Task setup complete")

        # Register signal handlers
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, self._signal_handler)

        # Monitor for new files and process them sequentially
        try:
            while not self._shutdown_event.is_set():
                unprocessed_files = self._get_unprocessed_files(
                    input_dir=self.config.input_dir,
                    input_ext=self.config.input_ext,
                    output_dir=self.config.output_dir,
                    output_ext=self.config.output_ext,
                )

                for file in unprocessed_files:
                    if self._shutdown_event.is_set():
                        return
                    output_fname = (
                        file.name.removesuffix(self.config.input_ext)
                        + self.config.output_ext
                    )
                    output_file = output_dir / output_fname
                    task(file, output_file)

                self._shutdown_event.wait(timeout=3)  # Interruptible sleep
        finally:
            logger.info("Shutting down task")
            self.teardown(self._context)
            logger.info("Task shutdown complete")
            if self._received_signal is not None:
                sys.exit(128 + self._received_signal)

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
            task_name: Annotated[
                str,
                typer.Option(
                    help="Task name",
                ),
            ] = cls.get_name(),
        ):
            """
            Run the task as a CLI application
            """
            config = LocalTaskConfig(
                name=task_name,
                kind="local",
                module=cls.get_module_path(),
                input_ext=input_ext,
                output_ext=output_ext,
            )

            task = cls(config)
            task.start(input_dir, output_dir)

        typer.run(main)

    @staticmethod
    def setup(context: SetupContext):
        """
        Establish a shared setup to be used across different runs.

        Parameters
        ----------
        context : SetupContext
            Namespace to store any common, reusable data/objects
            (e.g., DB connection).
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
            (e.g., DB connection).
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
            (e.g., DB connection).
        """
        pass
