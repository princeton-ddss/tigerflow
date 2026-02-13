Run the test suite.

## Arguments
- `unit` - Run unit tests only: `uv run pytest tests/unit`
- `integration` - Run integration tests only: `uv run pytest tests/integration`
- `all` or empty - Run all tests: `uv run pytest`

## User's request
Run tests: $ARGUMENTS

## Instructions
Based on the argument above, run the appropriate pytest command. If no argument or "all", run all tests. Show the user a summary of results.
