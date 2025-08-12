import time
from abc import abstractmethod
from pathlib import Path

import typer
from typing_extensions import Annotated

from tigerflow.utils import SetupContext, atomic_write, validate_file_ext

from ._base import Task


class LocalTask(Task):
    def __init__(self):
        self.context = SetupContext()

    def start(
        self,
        input_dir: Path,
        input_ext: str,
        output_dir: Path,
        output_ext: str,
    ):
        for p in [input_dir, output_dir]:
            if not p.exists():
                raise FileNotFoundError(p)
        for s in [input_ext, output_ext]:
            validate_file_ext(s)

        # Reference methods that must be implemented in subclass
        setup_func = type(self).setup
        run_func = type(self).run
        teardown_func = type(self).teardown

        def task(input_file: Path, output_file: Path):
            try:
                with atomic_write(output_file) as temp_file:
                    run_func(self.context, input_file, temp_file)
            except Exception as e:
                error_fname = output_file.name.removesuffix(output_ext) + ".err"
                error_file = output_dir / error_fname
                with atomic_write(error_file) as temp_file:
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
                unprocessed_files = self._get_unprocessed_files(
                    input_dir,
                    input_ext,
                    output_dir,
                    output_ext,
                )

                for file in unprocessed_files:
                    output_fname = file.name.removesuffix(input_ext) + output_ext
                    output_file = output_dir / output_fname
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
        ):
            """
            Run the task as a CLI application
            """
            task = cls()
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
