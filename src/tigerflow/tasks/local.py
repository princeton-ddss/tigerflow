import time
from abc import abstractmethod
from pathlib import Path

import typer
from typing_extensions import Annotated

from tigerflow.utils import SetupContext, atomic_write

from ._base import Task


class LocalTask(Task):
    def __init__(self):
        self.context = SetupContext()

    def start(self, input_dir: Path, output_dir: Path):
        for p in [input_dir, output_dir]:
            if not p.exists():
                raise FileNotFoundError(p)

        # Reference methods that must be implemented in subclass
        setup_func = type(self).setup
        run_func = type(self).run
        teardown_func = type(self).teardown

        def task(input_file: Path, output_file: Path):
            try:
                with atomic_write(output_file) as temp_file:
                    run_func(self.context, input_file, temp_file)
            except Exception as e:
                with atomic_write(output_file.with_suffix(".err")) as temp_file:
                    with open(temp_file, "w") as f:
                        f.write(str(e))

        # Clean up incomplete temporary files left behind by a prior process instance
        self._remove_temporary_files(output_dir)

        # Run the setup logic
        setup_func(self.context)
        self.context.freeze()  # Make it read-only

        # Monitor for new files and process them sequentially
        try:
            while True:
                unprocessed_files = self._get_unprocessed_files(input_dir, output_dir)
                for file in unprocessed_files:
                    output_file = output_dir / file.with_suffix(".out").name
                    task(file, output_file)
                time.sleep(3)
        finally:
            teardown_func(self.context)

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
        ):
            """
            Run the task as a CLI application
            """
            task = cls()
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
            (e.g., DB connection).
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
    @abstractmethod
    async def teardown(context: SetupContext):
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
