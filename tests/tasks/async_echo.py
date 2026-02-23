"""Async echo task - copies input to output with optional transformations."""

from pathlib import Path
from typing import Annotated

import aiofiles
import typer

from tigerflow.tasks import LocalAsyncTask
from tigerflow.utils import SetupContext


class AsyncEcho(LocalAsyncTask):
    """Async copy of input files to output with optional prefix/suffix."""

    class Params:
        prefix: Annotated[
            str,
            typer.Option(help="Text to prepend to the content"),
        ] = ""
        suffix: Annotated[
            str,
            typer.Option(help="Text to append to the content"),
        ] = ""
        uppercase: Annotated[
            bool,
            typer.Option("--uppercase", help="Convert content to uppercase"),
        ] = False

    @staticmethod
    async def run(context: SetupContext, input_file: Path, output_file: Path):
        async with aiofiles.open(input_file) as f:
            content = await f.read()

        if context.uppercase:
            content = content.upper()

        result = f"{context.prefix}{content}{context.suffix}"

        async with aiofiles.open(output_file, "w") as f:
            await f.write(result)


if __name__ == "__main__":
    AsyncEcho.cli()
