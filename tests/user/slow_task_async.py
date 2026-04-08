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


def _do_cpu_work(
    content: bytes,
    delay: float,
    delay_variation: float,
    fail_rate: float,
    filename: str,
) -> bytes:
    """CPU-bound work - runs in executor to not block event loop."""
    actual_delay = delay * (1 + random.uniform(-delay_variation, delay_variation))
    end_time = time.time() + actual_delay
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
        delay_variation: Annotated[
            float,
            typer.Option(help="Random variation in delay (0-1, as fraction of delay)"),
        ] = 0.0
        fail_rate: Annotated[
            float,
            typer.Option(help="Probability of random failure (0-1)"),
        ] = 0.0

    @staticmethod
    async def run(context: SetupContext, input_file: Path, output_file: Path):
        content = input_file.read_bytes()

        # Run CPU work in executor to not block event loop
        loop = asyncio.get_event_loop()
        delay_variation = getattr(context, "delay_variation", 0.0)
        result = await loop.run_in_executor(
            None,
            _do_cpu_work,
            content,
            context.delay,
            delay_variation,
            context.fail_rate,
            input_file.name,
        )

        output_file.write_bytes(result)


if __name__ == "__main__":
    SlowTaskAsync.cli()
