"""An async task that always fails - for testing error handling."""

from pathlib import Path

from tigerflow.tasks import LocalAsyncTask
from tigerflow.utils import SetupContext


class AsyncFailingTask(LocalAsyncTask):
    @staticmethod
    async def run(context: SetupContext, input_file: Path, output_file: Path):
        raise ValueError("Intentional async failure")


if __name__ == "__main__":
    AsyncFailingTask.cli()
