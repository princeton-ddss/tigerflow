import asyncio
from abc import abstractmethod
from pathlib import Path

import aiofiles
import typer
from typing_extensions import Annotated

from tigerflow.utils import SetupContext, atomic_write

from ._base import Task


class LocalAsyncTask(Task):
    def __init__(self, concurrency_limit: int):
        self.concurrency_limit = concurrency_limit
        self.context = SetupContext()
        self.queue = asyncio.Queue()
        self.in_queue: set[str] = set()  # Track file IDs in queue

    def start(self, input_dir: Path, output_dir: Path):
        for p in [input_dir, output_dir]:
            if not p.exists():
                raise FileNotFoundError(p)

        # Reference methods that must be implemented in subclass
        setup_func = type(self).setup
        run_func = type(self).run
        teardown_func = type(self).teardown

        async def task(input_file: Path, output_file: Path):
            try:
                with atomic_write(output_file) as temp_file:
                    await run_func(self.context, input_file, temp_file)
            except Exception as e:
                with atomic_write(output_file.with_suffix(".err")) as temp_file:
                    async with aiofiles.open(temp_file, "w") as f:
                        await f.write(str(e))

        async def worker():
            while True:
                file = await self.queue.get()
                assert isinstance(file, Path)
                output_file = output_dir / file.with_suffix(".out").name
                try:
                    await task(file, output_file)
                finally:
                    self.queue.task_done()
                    self.in_queue.remove(file.stem)

        async def poll():
            while True:
                unprocessed_files = self._get_unprocessed_files(input_dir, output_dir)
                for file in unprocessed_files:
                    if file.stem not in self.in_queue:
                        self.in_queue.add(file.stem)
                        await self.queue.put(file)
                await asyncio.sleep(3)

        async def main():
            await setup_func(self.context)
            self.context.freeze()  # Make it read-only

            workers = [
                asyncio.create_task(worker()) for _ in range(self.concurrency_limit)
            ]
            poller = asyncio.create_task(poll())

            try:
                await asyncio.gather(poller, *workers)
            except asyncio.CancelledError:
                print("Shutting down...")
            finally:
                await teardown_func(self.context)

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
            concurrency_limit: Annotated[
                int,
                typer.Option(
                    help="""
                    Maximum number of async tasks allowed to run in parallel
                    at any given time (excess tasks are queued until capacity
                    becomes available)
                    """,
                    show_default=False,
                ),
            ],
        ):
            """
            Run the task as a CLI application
            """
            task = cls(concurrency_limit)
            task.start(input_dir, output_dir)

        typer.run(main)

    @staticmethod
    @abstractmethod
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
    @abstractmethod
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
