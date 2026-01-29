# Configuration

TigerFlow provides several configurable settings that control internal timing, polling intervals, and Slurm task behavior.
These settings can be customized via environment variables or a `.env` file.

## Environment Variables

All settings use the `TIGERFLOW_` prefix. For example, to set the pipeline polling interval:

```bash
export TIGERFLOW_PIPELINE_POLL_INTERVAL=30
```

## Using a `.env` File

You can also define settings in a `.env` file in your working directory:

```bash title=".env"
TIGERFLOW_PIPELINE_POLL_INTERVAL=30
TIGERFLOW_TASK_POLL_INTERVAL=5
TIGERFLOW_SLURM_TASK_CLIENT_HOURS=12
```

To use a custom `.env` file location, set the `TIGERFLOW_ENV_FILE` environment variable:

```bash
export TIGERFLOW_ENV_FILE=/path/to/custom.env
```

## Available Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `TIGERFLOW_TASK_VALIDATION_TIMEOUT` | `60` | Timeout in seconds for validating task modules |
| `TIGERFLOW_PIPELINE_POLL_INTERVAL` | `10` | Pipeline polling interval in seconds |
| `TIGERFLOW_TASK_POLL_INTERVAL` | `3` | Task polling interval in seconds |
| `TIGERFLOW_SLURM_TASK_CLIENT_HOURS` | `24` | Time limit in hours for each Slurm task client job (respawns when expired) |
| `TIGERFLOW_SLURM_TASK_SCALE_INTERVAL` | `15` | Interval in seconds between Slurm task scaling checks |
| `TIGERFLOW_SLURM_TASK_SCALE_WAIT_COUNT` | `8` | Number of consecutive idle checks before removing a worker |
| `TIGERFLOW_SLURM_TASK_WORKER_STARTUP_TIMEOUT` | `600` | Timeout in seconds for each Slurm task worker to initialize |

## Example: Tuning Slurm Task Behavior

For workflows with bursty workloads, you may want workers to remain available longer before scaling down:

```bash title=".env"
# Check scaling less frequently (every 30 seconds instead of 15)
TIGERFLOW_SLURM_TASK_SCALE_INTERVAL=30

# Require more idle checks before removing a worker (16 instead of 8)
TIGERFLOW_SLURM_TASK_SCALE_WAIT_COUNT=16
```

For environments with slow Slurm queues or large model loading times, you may need to increase the worker startup timeout:

```bash title=".env"
# Allow longer time for workers to initialize (20 minutes instead of 10)
TIGERFLOW_SLURM_TASK_WORKER_STARTUP_TIMEOUT=1200
```
