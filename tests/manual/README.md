# Manual Pipeline Testing

Scripts for manually testing long-running pipelines.

## Usage

```bash
# Local task (sequential processing)
python tests/manual/run_pipeline.py --kind local --num-files 10 --delay 2.0

# Local async task (concurrent processing)
python tests/manual/run_pipeline.py --kind local_async --num-files 20 --delay 1.0 --concurrency-limit 4

# Slurm task (distributed processing, use uv run on the cluster)
# Use --tmp-dir to place files on a shared filesystem accessible to compute nodes
uv run python tests/manual/run_pipeline.py --kind slurm --num-files 50 --delay 5.0 --max-workers 4 --tmp-dir /scratch/$USER

# With random failures (10% failure rate)
python tests/manual/run_pipeline.py --kind local --num-files 20 --fail-rate 0.1

# With delay variation (±30% random variation in processing time)
python tests/manual/run_pipeline.py --kind local --num-files 10 --delay 2.0 --delay-variation 0.3

# Multi-task pipeline (comma-separated values)
python tests/manual/run_pipeline.py --kind local,local_async --delay 1.0,2.0 --fail-rate 0.1,0.2
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--kind`, `-k` | local | Task kind(s): local, local_async, slurm |
| `--num-files`, `-n` | 10 | Number of input files to generate |
| `--delay`, `-d` | 1.0 | Seconds of CPU work per file |
| `--delay-variation`, `-v` | 0.0 | Random variation in delay (0-1, as fraction of delay) |
| `--fail-rate`, `-f` | 0.0 | Probability of random failure (0-1) |
| `--idle-timeout` | 5 | Pipeline idle timeout in minutes |
| `--concurrency-limit` | 4 | Concurrency limit (local_async only) |
| `--max-workers` | 4 | Max workers (slurm only) |
| `--tmp-dir`, `-t` | None | Base directory for temp files |

Multi-task pipelines accept comma-separated values for `--kind`, `--delay`,
`--delay-variation`, and `--fail-rate`. Singleton values are expanded to match
the longest list.

## Output

The script creates a temp directory and starts the pipeline in background mode:

```
Pipeline started (pid 12345)
Output directory: /tmp/tigerflow-test-abc123/output
Tasks: 1

Commands:
  tigerflow report /tmp/tigerflow-test-abc123/output
  tigerflow report /tmp/tigerflow-test-abc123/output --watch
  tigerflow stop /tmp/tigerflow-test-abc123/output
```
