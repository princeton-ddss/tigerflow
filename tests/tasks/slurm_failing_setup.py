"""A Slurm task whose setup always fails - for testing abort-on-setup-failure."""

from pathlib import Path

from tigerflow.tasks import SlurmTask
from tigerflow.utils import SetupContext


class SlurmFailingSetupTask(SlurmTask):
    @staticmethod
    def setup(context: SetupContext):
        raise RuntimeError("Intentional setup failure")

    @staticmethod
    def run(context: SetupContext, input_file: Path, output_file: Path):
        raise AssertionError("run() should never be reached")


if __name__ == "__main__":
    SlurmFailingSetupTask.cli()
