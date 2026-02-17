Review code changes against tigerflow patterns and conventions.

## Arguments
- File path or pattern (optional) - specific files to review
- If empty, review all uncommitted changes

## User's request
$ARGUMENTS

## Instructions

1. **Identify changes to review**:
   - If a path is given, review that file
   - Otherwise, run `git diff` to see uncommitted changes

2. **Check against project patterns**:

   **Task implementations** (`src/tigerflow/library/`, `src/tigerflow/tasks/`):
   - `run()` must be `@staticmethod` with signature `(context: SetupContext, input_file: Path, output_file: Path)`
   - `setup()` and `teardown()` are optional but must also be `@staticmethod`
   - `Params` class uses `Annotated[type, typer.Option(...)]` pattern
   - Tasks should use `atomic_write()` for output files
   - Include `if __name__ == "__main__": TaskClass.cli()` for CLI support

   **Pydantic models** (`src/tigerflow/models.py`):
   - Use `Field()` with descriptions for all fields
   - Validators use `@field_validator` or `@model_validator`
   - Discriminated unions use `Literal` for the `kind` field

   **CLI commands** (`src/tigerflow/cli/`):
   - Use `typer` with `Annotated` type hints
   - Error handling should use `typer.Exit(code=1)`
   - JSON output uses `--json` flag pattern

   **Tests** (`tests/`):
   - Unit tests in `tests/unit/`, integration in `tests/integration/`
   - Use `tmp_path` fixture for file operations
   - Async tests use `@pytest.mark.asyncio`

3. **Report findings** with specific line references and suggestions.
