import asyncio
import signal
import sys
import traceback
from abc import abstractmethod
from pathlib import Path

import aiofiles
import typer
from typing_extensions import Annotated

from tigerflow.logconfig import logger
from tigerflow.utils import SetupContext, atomic_write, validate_file_ext

from ._base import Task


class LocalAsyncTask(Task):
    @logger.catch(reraise=True)
    def __init__(self, *, concurrency_limit: int):
        self._concurrency_limit = concurrency_limit
        self._context = SetupContext()
        self._queue = asyncio.Queue()
        self._in_queue: set[Path] = set()  # Track files in queue
        self._shutdown_event = asyncio.Event()
        self._received_signal: int | None = None

    def _signal_handler(self, signum: int):
        logger.warning("Received signal {}, initiating shutdown", signum)
        self._received_signal = signum
        self._shutdown_event.set()

    @logger.catch(reraise=True)
    def start(
        self,
        *,
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

        async def task(input_file: Path, output_file: Path):
            try:
                logger.info("Starting processing: {}", input_file.name)
                with atomic_write(output_file) as temp_file:
                    await self.run(self._context, input_file, temp_file)
                logger.info("Successfully processed: {}", input_file.name)
            except Exception:
                error_fname = output_file.name.removesuffix(output_ext) + ".err"
                error_file = output_dir / error_fname
                with atomic_write(error_file) as temp_file:
                    async with aiofiles.open(temp_file, "w") as f:
                        await f.write(traceback.format_exc())
                logger.error("Failed processing: {}", input_file.name)

        async def worker():
            while not self._shutdown_event.is_set():
                file = await self._queue.get()
                assert isinstance(file, Path)

                output_fname = file.name.removesuffix(input_ext) + output_ext
                output_file = output_dir / output_fname

                try:
                    await task(file, output_file)
                finally:
                    self._queue.task_done()
                    self._in_queue.remove(file)

        async def poll():
            while not self._shutdown_event.is_set():
                unprocessed_files = self._get_unprocessed_files(
                    input_dir=input_dir,
                    input_ext=input_ext,
                    output_dir=output_dir,
                    output_ext=output_ext,
                )

                for file in unprocessed_files:
                    if file not in self._in_queue:
                        self._in_queue.add(file)
                        await self._queue.put(file)

                await asyncio.sleep(3)

        async def main():
            # Run common setup
            logger.info("Setting up task")
            await self.setup(self._context)
            self._context.freeze()  # Make it read-only
            logger.info("Task setup complete")

            # Register signal handlers
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
                loop.add_signal_handler(sig, self._signal_handler, sig.value)

            # Start coroutines
            workers = [
                asyncio.create_task(worker()) for _ in range(self._concurrency_limit)
            ]
            poller = asyncio.create_task(poll())

            # Perform graceful shutdown
            await self._shutdown_event.wait()
            logger.info("Shutting down task")
            for task in workers + [poller]:
                task.cancel()
            await asyncio.gather(*workers, poller, return_exceptions=True)
            await self.teardown(self._context)
            logger.info("Task shutdown complete")
            if self._received_signal is not None:
                sys.exit(128 + self._received_signal)

        # Clean up incomplete temporary files left behind by a prior process instance
        self._remove_temporary_files(output_dir)

        # Start coroutines
        asyncio.run(main())

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
            concurrency_limit: Annotated[
                int,
                typer.Option(
                    help="""
                    Maximum number of coroutines that may run concurrently
                    at any given time (excess coroutines are queued until
                    capacity becomes available)
                    """,
                    show_default=False,
                ),
            ],
        ):
            """
            Run the task as a CLI application
            """
            task = cls(
                concurrency_limit=concurrency_limit,
            )

            task.start(
                input_dir=input_dir,
                input_ext=input_ext,
                output_dir=output_dir,
                output_ext=output_ext,
            )

        typer.run(main)

    @staticmethod
    async def setup(context: SetupContext):
        """
        Establish a shared setup to be used across different runs.

        Parameters
        ----------
        context : SetupContext
            Namespace to store any common, reusable data/objects
            (e.g., HTTP client session, DB connection).
        """
        pass

    @staticmethod
    @abstractmethod
    async def run(context: SetupContext, input_file: Path, output_file: Path):
        """
        Define the processing logic to be applied to each input file.

        Parameters
        ----------
        context : SetupContext
            Read-only namespace for retrieving setup data/objects
            (e.g., HTTP client session, DB connection).
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
    async def teardown(context: SetupContext):
        """
        Define cleanup logic (e.g., closing an HTTP client session)
        to be executed upon termination.

        Parameters
        ----------
        context : SetupContext
            Read-only namespace for retrieving setup data/objects
            (e.g., HTTP client session, DB connection).
        """
        pass
