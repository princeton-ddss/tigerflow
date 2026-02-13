"""Slow task for manual pipeline testing - LocalAsyncTask variant.

Does actual CPU work (repeated hashing) for a configurable duration,
with optional random failures to test error handling.
"""

import asyncio
import hashlib
import random
import time
from pathlib import Path
from typing import Annotated

import typer

from tigerflow.tasks import LocalAsyncTask
from tigerflow.utils import SetupContext


def _do_cpu_work(content: bytes, delay: float, fail_rate: float, filename: str) -> bytes:
    """CPU-bound work - runs in executor to not block event loop."""
    end_time = time.time() + delay
    result = content
    while time.time() < end_time:
        result = hashlib.sha256(result).digest()

    if random.random() < fail_rate:
        raise RuntimeError(f"Simulated failure for {filename}")

    return result


class SlowTaskAsync(LocalAsyncTask):
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
    async def run(context: SetupContext, input_file: Path, output_file: Path):
        content = input_file.read_bytes()

        # Run CPU work in executor to not block event loop
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None,
            _do_cpu_work,
            content,
            context.delay,
            context.fail_rate,
            input_file.name,
        )

        output_file.write_bytes(result)


if __name__ == "__main__":
    SlowTaskAsync.cli()
