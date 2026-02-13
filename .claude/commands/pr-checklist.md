Pre-PR checklist - verify the branch is ready for pull request.

## Checklist

1. **No merge conflicts**: Fetch latest and check for conflicts with target branch (usually main)
   - `git fetch origin main`
   - `git merge-base --is-ancestor origin/main HEAD || git merge origin/main --no-commit --no-ff`

2. **Tests pass**: Run `uv run pytest` and verify all tests pass

3. **Linting passes**: Run `pre-commit run --all-files` and verify no issues

4. **Documentation updated**: If adding new features, check that docs are updated

5. **Commits are logical**: Review commit history for clean, logical commits

## Instructions

Run through each checklist item in order and report status. If merge conflicts exist, help the user resolve them. If tests or linting fail, help fix issues before proceeding.
