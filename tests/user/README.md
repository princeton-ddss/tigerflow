# Manual Pipeline Testing

Scripts for manually testing long-running pipelines.

## Usage

```bash
# Local task (sequential processing)
python tests/user/run_pipeline.py --kind local --num-files 10 --delay 2.0

# Local async task (concurrent processing)
python tests/user/run_pipeline.py --kind local_async --num-files 20 --delay 1.0 --concurrency-limit 4

# Slurm task (distributed processing)
python tests/user/run_pipeline.py --kind slurm --num-files 50 --delay 5.0 --max-workers 4

# With random failures (10% failure rate)
python tests/user/run_pipeline.py --kind local --num-files 20 --fail-rate 0.1
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--kind` | local | Task kind: local, local_async, slurm |
| `--num-files` | 10 | Number of input files to generate |
| `--delay` | 1.0 | Seconds of CPU work per file |
| `--fail-rate` | 0.0 | Probability of random failure (0-1) |
| `--idle-timeout` | 5 | Pipeline idle timeout in minutes |
| `--concurrency-limit` | 4 | Concurrency limit (local_async only) |
| `--max-workers` | 4 | Max workers (slurm only) |

## Output

The script creates a temp directory and starts the pipeline in background mode:

```
Pipeline started (pid 12345)
Output directory: /tmp/tigerflow-test-abc123/output

Commands:
  tigerflow status /tmp/tigerflow-test-abc123/output
  tigerflow stop /tmp/tigerflow-test-abc123/output
  kill -TERM 12345
```
