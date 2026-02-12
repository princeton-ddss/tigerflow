"""A task that always fails - for testing error handling."""

from pathlib import Path

from tigerflow.tasks import LocalTask
from tigerflow.utils import SetupContext


class FailingTask(LocalTask):
    @staticmethod
    def run(context: SetupContext, input_file: Path, output_file: Path):
        raise ValueError("Intentional failure")


if __name__ == "__main__":
    FailingTask.cli()
