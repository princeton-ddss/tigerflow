Scaffold a new task in the tigerflow library.

## Arguments
- Task name (required): The name of the task class (e.g., `Transform`, `Validate`)

## User's request
Create task: $ARGUMENTS

## Instructions

1. Create a new file at `src/tigerflow/library/<name>.py` (lowercase) using this template:

```python
"""
<Name> task - <brief description>.

Usage:
    python -m tigerflow.library.<name> \
        --input-dir ./input \
        --output-dir ./output \
        --input-ext .txt \
        --output-ext .txt
"""

from pathlib import Path
from typing import Annotated

import typer

from tigerflow.tasks import LocalTask
from tigerflow.utils import SetupContext


class <Name>(LocalTask):
    """<Brief description>."""

    class Params:
        # Add task-specific parameters here
        # example: Annotated[str, typer.Option(help="Description")] = "default"
        pass

    @staticmethod
    def run(context: SetupContext, input_file: Path, output_file: Path):
        # TODO: Implement processing logic
        raise NotImplementedError("Implement the run method")


if __name__ == "__main__":
    <Name>.cli()
```

2. Ask the user what the task should do and fill in the implementation.

3. If appropriate, add corresponding tests in `tests/unit/test_<name>.py`.
