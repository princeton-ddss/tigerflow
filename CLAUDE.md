# Tigerflow

A pipeline orchestration framework for processing files through configurable task workflows.

## Project Structure

```
src/tigerflow/
├── cli/           # CLI commands (run, status, stop, tasks)
├── tasks/         # Task base classes (LocalTask, LocalAsyncTask, SlurmTask)
├── library/       # Built-in tasks (echo)
├── pipeline.py    # Pipeline orchestration
├── models.py      # Pydantic models for config
└── utils.py       # Shared utilities

tests/
├── unit/          # Unit tests
├── integration/   # Integration tests
└── user/          # Manual testing scripts (not run by pytest)

docs/mkdocs/       # Documentation (MkDocs)
examples/          # Example pipelines
```

## Development Commands

- **Run tests**: `uv run pytest` (or `uv run pytest tests/unit`, `tests/integration`)
- **Lint**: `pre-commit run --all-files`
- **Build docs**: `uv run mkdocs serve` (from docs/mkdocs)

## Task Types

- **LocalTask**: Sequential file processing on local machine
- **LocalAsyncTask**: Concurrent async processing with configurable concurrency
- **SlurmTask**: Distributed processing via Slurm + Dask

## PR Conventions

- Use descriptive commit messages focused on "why" not "what"
- Include Summary, Additional Changes, and Acceptance Criteria sections in PR description
- Run tests and linting before opening PR
- Check for merge conflicts with target branch

## Slash Commands

- `/test [unit|integration|all]` - Run tests
- `/lint [file]` - Run pre-commit linting
- `/pr-checklist` - Pre-PR verification
- `/test-pipeline` - Manual pipeline testing
- `/add-task <name>` - Scaffold a new task in the library
- `/docs [serve|build]` - Build or serve documentation
- `/review [file]` - Review code against project patterns
- `/debug-pipeline <path>` - Debug a failed pipeline run

## Code Patterns

### Task Implementation

Tasks inherit from `LocalTask`, `LocalAsyncTask`, or `SlurmTask`. Key patterns:

```python
class MyTask(LocalTask):
    class Params:
        # Use Annotated with typer.Option for CLI params
        option: Annotated[str, typer.Option(help="Description")] = "default"

    @staticmethod
    def setup(context: SetupContext):
        # One-time setup (DB connections, model loading)
        context.connection = create_connection()

    @staticmethod
    def run(context: SetupContext, input_file: Path, output_file: Path):
        # Process single file - context is read-only here
        # Use atomic_write() for output files
        pass

    @staticmethod
    def teardown(context: SetupContext):
        # Cleanup resources
        context.connection.close()
```

### Pydantic Models

Config models use discriminated unions via the `kind` field:

```python
class LocalTaskConfig(BaseTaskConfig):
    kind: Literal["local"] = "local"
    # LocalTask-specific fields

class SlurmTaskConfig(BaseTaskConfig):
    kind: Literal["slurm"] = "slurm"
    # SlurmTask-specific fields

TaskConfig = Annotated[LocalTaskConfig | SlurmTaskConfig, Field(discriminator="kind")]
```

### CLI Commands

Use `typer` with `Annotated` types:

```python
def command(
    path: Annotated[Path, typer.Argument(help="Output directory")],
    json_output: Annotated[bool, typer.Option("--json", help="JSON output")] = False,
):
    if error_condition:
        raise typer.Exit(code=1)
```

## Architecture Decisions

- **File-based orchestration**: Pipeline state is tracked via filesystem (presence of output files, `.err` files for failures). No database required.
- **Atomic writes**: Tasks write to temp files and rename on success to prevent partial outputs.
- **Signal handling**: Tasks handle SIGTERM/SIGINT gracefully for clean shutdown.
- **Polling model**: Tasks poll for new input files at configurable intervals rather than using filesystem watchers (more reliable across different filesystems/NFS).
