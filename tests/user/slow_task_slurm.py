"""Slow task for manual pipeline testing - SlurmTask variant.

Does actual CPU work (repeated hashing) for a configurable duration,
with optional random failures to test error handling.
"""

import hashlib
import random
import time
from pathlib import Path
from typing import Annotated

import typer

from tigerflow.tasks import SlurmTask
from tigerflow.utils import SetupContext


class SlowTaskSlurm(SlurmTask):
    """Process files with configurable delay and failure rate."""

    class Params:
        delay: Annotated[
            float,
            typer.Option(help="Seconds of CPU work per file"),
        ] = 1.0
        fail_rate: Annotated[
            float,
            typer.Option(help="Probability of random failure (0-1)"),
        ] = 0.0

    @staticmethod
    def run(context: SetupContext, input_file: Path, output_file: Path):
        content = input_file.read_bytes()

        # Do actual CPU work for the specified duration
        end_time = time.time() + context.delay
        result = content
        while time.time() < end_time:
            result = hashlib.sha256(result).digest()

        if random.random() < context.fail_rate:
            raise RuntimeError(f"Simulated failure for {input_file.name}")

        output_file.write_bytes(result)


if __name__ == "__main__":
    SlowTaskSlurm.cli()
