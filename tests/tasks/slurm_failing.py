"""A Slurm task that always fails - for testing error handling."""

from pathlib import Path

from tigerflow.tasks import SlurmTask
from tigerflow.utils import SetupContext


class SlurmFailingTask(SlurmTask):
    @staticmethod
    def run(context: SetupContext, input_file: Path, output_file: Path):
        raise ValueError("Intentional Slurm failure")


if __name__ == "__main__":
    SlurmFailingTask.cli()
