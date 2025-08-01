import asyncio
from abc import abstractmethod
from pathlib import Path
from types import SimpleNamespace

import aiofiles
import typer
from typing_extensions import Annotated

from tigerflow.utils import atomic_write

from ._base import Task


class LocalAsyncTask(Task):
    def __init__(self, concurrency_limit: int):
        self.concurrency_limit = concurrency_limit
        self.context = SimpleNamespace()
        self.queue = asyncio.Queue()
        self.in_queue: set[str] = set()  # Track file IDs in queue

    def start(self, input_dir: Path, output_dir: Path):
        for p in [input_dir, output_dir]:
            if not p.exists():
                raise FileNotFoundError(p)

        # Reference methods that must be implemented in subclass
        setup_func = type(self).setup
        run_func = type(self).run

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
            workers = [
                asyncio.create_task(worker()) for _ in range(self.concurrency_limit)
            ]
            poller = asyncio.create_task(poll())
            try:
                await asyncio.gather(poller, *workers)
            except asyncio.CancelledError:
                print("Shutting down...")

        # Clean up incomplete temporary files left behind by a prior process instance
        for f in output_dir.iterdir():
            if f.is_file() and f.suffix == "":
                f.unlink()

        # Run the common setup
        setup_func(self.context)

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
    def setup(context: SimpleNamespace):
        """
        Establish a shared processing setup (e.g., model loading).
        """
        pass

    @staticmethod
    @abstractmethod
    async def run(context: SimpleNamespace, input_file: Path, output_file: Path):
        """
        Specify the processing logic to be applied to each input file.
        """
        pass
