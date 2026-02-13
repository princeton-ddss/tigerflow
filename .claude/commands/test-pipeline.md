Run a manual test pipeline for testing background mode, status, and stop commands.

## Location
The test scripts are in `tests/user/`:
- `run_pipeline.py` - Main script to start a test pipeline
- `slow_task.py` - LocalTask that does CPU work
- `slow_task_async.py` - LocalAsyncTask variant
- `slow_task_slurm.py` - SlurmTask variant

## Usage

```bash
# Start a test pipeline (runs in background by default)
uv run python tests/user/run_pipeline.py --kind local --num-files 10 --delay 2.0

# With failures
uv run python tests/user/run_pipeline.py --kind local --num-files 20 --delay 1.0 --fail-rate 0.1

# For async or slurm
uv run python tests/user/run_pipeline.py --kind local_async --num-files 20
uv run python tests/user/run_pipeline.py --kind slurm --num-files 50
```

## Options
- `--kind`: local | local_async | slurm (default: local)
- `--num-files`: Number of input files (default: 10)
- `--delay`: Seconds of CPU work per file (default: 1.0)
- `--fail-rate`: Probability of failure 0-1 (default: 0.0)
- `--idle-timeout`: Minutes before auto-shutdown (default: 5)

## Managing the Pipeline

After starting, the script prints commands to check status and stop:
```bash
tigerflow status <output_dir>
tigerflow stop <output_dir>
```

## What to Do

If the user wants to test the pipeline, help them run the script with appropriate options. Check status periodically if they want to monitor progress.
