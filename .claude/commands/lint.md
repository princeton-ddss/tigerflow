Run linting and formatting via pre-commit.

## User's request
$ARGUMENTS

## Instructions

1. **Check if pre-commit is installed in the repo**:
   ```bash
   git config --get core.hooksPath || ls .git/hooks/pre-commit 2>/dev/null
   ```
   If not installed, run `uv run pre-commit install` first.

2. **Run linting**:
   - All files: `uv run pre-commit run --all-files`
   - Specific file: `uv run pre-commit run --files <path>`

If the user doesn't specify a path, run on all files.
