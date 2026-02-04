"""
Echo task - copies input to output with optional transformations.

A simple example task that demonstrates the Params pattern.

Usage:
    python -m tigerflow.library.echo \
        --input-dir ./input \
        --output-dir ./output \
        --input-ext .txt \
        --output-ext .txt \
        --prefix "Hello: " \
        --suffix " :End"
"""

from pathlib import Path
from typing import Annotated

import typer

from tigerflow.tasks import LocalTask
from tigerflow.utils import SetupContext


class Echo(LocalTask):
    """Copy input files to output with optional prefix/suffix."""

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
            typer.Option(help="Convert content to uppercase"),
        ] = False

    @staticmethod
    def run(context: SetupContext, input_file: Path, output_file: Path):
        with open(input_file) as f:
            content = f.read()

        if context.uppercase:
            content = content.upper()

        result = f"{context.prefix}{content}{context.suffix}"

        with open(output_file, "w") as f:
            f.write(result)


if __name__ == "__main__":
    Echo.cli()
