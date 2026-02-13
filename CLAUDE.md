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
