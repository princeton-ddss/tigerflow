# Pipeline in TigerFlow

!!! note

    Before proceeding, please review how to create and use [tasks](task.md) in TigerFlow.

In TigerFlow, tasks are organized into a pipeline by creating a configuration file.

Let's build on the [example](task.md#examples) from the *Task in TigerFlow* section,
where we created a sequence of tasks to:

1. Transcribe videos using an open-source model (Whisper)
2. Embed the transcriptions using an external API service (Voyage AI)
3. Ingest the embeddings into a single-writer database (DuckDB)

## Defining a pipeline

A pipeline is configured using a [YAML](https://circleci.com/blog/what-is-yaml-a-beginner-s-guide/) file.
For example, the tasks above can be structured into a pipeline as follows:

```yaml title="config.yaml"
tasks:
  - name: transcribe
    kind: slurm
    module: ./transcribe.py
    input_ext: .mp4
    output_ext: .txt
    resources:
      cpus: 1
      gpus: 1
      memory: "8G"
      time: "02:00:00"
      max_workers: 3
    setup_commands: |
      module purge
      module load anaconda3/2024.6
      conda activate tiktok
  - name: embed
    depends_on: transcribe
    kind: local_async
    module: ./embed.py
    input_ext: .txt
    output_ext: .json
    keep_output: false
    concurrency_limit: 10
    setup_commands: |
      module purge
      module load anaconda3/2024.6
      conda activate tiktok
  - name: ingest
    depends_on: embed
    kind: local
    module: ./ingest.py
    input_ext: .json
    keep_output: false
    setup_commands: |
      module purge
      module load anaconda3/2024.6
      conda activate tiktok
```

where:

- `kind` specifies the task type (one of: `local`, `local_async`, or `slurm`).
- `module` specifies the Python script defining task logic. Care should be taken when using a relative file path as it may resolve incorrectly when running the pipeline.
- `depends_on` specifies the name of the parent task whose output is used as input for the current task.
- `keep_output` specifies whether to retain output files from the current task. If unspecified, it defaults to `true`.
- `setup_commands` specifies Bash commands to run before starting the task. This can be used to activate a virtual environment required for the task logic.
- `resources` is a section applicable only to Slurm tasks. It specifies compute, memory, and other resources to allocate for running the current task. `max_workers` specifies the maximum number of parallel workers used for auto-scaling.
- `concurrency_limit` is a field applicable only to local asynchronous tasks. It specifies the maximum number of coroutines (e.g., API requests) that may run concurrently at any given time (excess coroutines are queued until capacity becomes available).

!!! note

    TigerFlow supports pipelines where the task dependency graph forms a *rooted tree*.
    That is, there must be a single root task, and every other task must have exactly one parent.

## Running a pipeline

Assuming the configuration file and task scripts are in the current directory,
we can run the pipeline as follows:

=== "Command"

    ```bash
    tigerflow run config.yaml path/to/data/ path/to/results/
    ```

=== "Output"

    ```log
    2025-09-22 09:20:10 | INFO     | Starting pipeline execution
    2025-09-22 09:20:10 | INFO     | [transcribe] Starting as a SLURM task
    2025-09-22 09:20:10 | INFO     | [transcribe] Submitted with Slurm job ID 847632
    2025-09-22 09:20:10 | INFO     | [embed] Starting as a LOCAL_ASYNC task
    2025-09-22 09:20:10 | INFO     | [embed] Started with PID 3007442
    2025-09-22 09:20:10 | INFO     | [ingest] Starting as a LOCAL task
    2025-09-22 09:20:10 | INFO     | [ingest] Started with PID 3007443
    2025-09-22 09:20:10 | INFO     | All tasks started, beginning pipeline tracking loop
    2025-09-22 09:20:10 | INFO     | [transcribe] Status changed: INACTIVE -> PENDING (Reason: (None))
    2025-09-22 09:20:10 | INFO     | [embed] Status changed: INACTIVE -> ACTIVE
    2025-09-22 09:20:10 | INFO     | [ingest] Status changed: INACTIVE -> ACTIVE
    2025-09-22 09:20:11 | INFO     | Staged 91 new file(s) for processing
    2025-09-22 09:20:31 | INFO     | [transcribe] Status changed: PENDING (Reason: (None)) -> ACTIVE (0 workers)
    2025-09-22 09:21:01 | INFO     | [transcribe] Status changed: ACTIVE (0 workers) -> ACTIVE (3 workers)
    2025-09-22 09:21:54 | ERROR    | [embed] 4 failed file(s)
    2025-09-22 09:21:55 | INFO     | Completed processing 25 file(s)
    2025-09-22 09:22:05 | ERROR    | [embed] 1 failed file(s)
    2025-09-22 09:22:05 | INFO     | Completed processing 7 file(s)
    2025-09-22 09:22:15 | ERROR    | [embed] 1 failed file(s)
    2025-09-22 09:22:15 | INFO     | Completed processing 13 file(s)
    2025-09-22 09:22:25 | INFO     | Completed processing 11 file(s)
    2025-09-22 09:22:35 | INFO     | Completed processing 3 file(s)
    2025-09-22 09:22:45 | INFO     | Completed processing 5 file(s)
    2025-09-22 09:22:55 | ERROR    | [embed] 1 failed file(s)
    2025-09-22 09:22:55 | INFO     | Completed processing 8 file(s)
    2025-09-22 09:23:05 | ERROR    | [embed] 1 failed file(s)
    2025-09-22 09:23:05 | INFO     | Completed processing 4 file(s)
    2025-09-22 09:23:15 | INFO     | Completed processing 6 file(s)
    2025-09-22 09:23:55 | INFO     | Completed processing 1 file(s)
    2025-09-22 09:25:06 | INFO     | [transcribe] Status changed: ACTIVE (3 workers) -> ACTIVE (1 workers)
    2025-09-22 09:25:46 | INFO     | [transcribe] Status changed: ACTIVE (1 workers) -> ACTIVE (0 workers)
    2025-09-22 09:33:48 | WARNING  | Idle timeout reached, initiating shutdown
    2025-09-22 09:33:48 | INFO     | Shutting down pipeline
    2025-09-22 09:33:48 | INFO     | [embed] Terminating...
    2025-09-22 09:33:48 | INFO     | [ingest] Terminating...
    2025-09-22 09:33:48 | INFO     | [transcribe] Terminating...
    2025-09-22 09:33:49 | ERROR    | [transcribe] Status changed: ACTIVE (0 workers) -> INACTIVE (Reason: CANCELLED+)
    2025-09-22 09:33:50 | ERROR    | [embed] Status changed: ACTIVE -> INACTIVE (Exit Code: 143)
    2025-09-22 09:33:50 | ERROR    | [ingest] Status changed: ACTIVE -> INACTIVE (Exit Code: 143)
    2025-09-22 09:33:51 | INFO     | Pipeline shutdown complete
    ```

!!! tip

    Run each task individually (see [examples](task.md#examples)) to ensure
    they work correctly before executing the entire pipeline.

The console output shows that the pipeline:

- Runs like a server, "listening" for and staging new files for processing
- Acts as a central orchestrator that launches, monitors, and manages the lifecycle of tasks
- Optimizes resource usage through autoscaling and idle timeout

By default, pipelines time out after 10 minutes of inactivity (i.e., when there are no more files
left to process). We can override this behavior using the `--idle-timeout` option, like so:

```bash
# Time out after 30 days of inactivity
tigerflow run config.yaml path/to/data/ path/to/results/ --idle-timeout 43200
```

Before the timeout threshold is reached, the pipeline will remain active with a minimal
resource footprint, ready to stage and process any new files placed in the input directory.
This behavior is useful for streaming-like workflows where data may arrive sporadically.

!!! info

    To see all available options for the `run` subcommand, run `tigerflow run --help`.

Since the pipeline has been configured to retain output files only for the transcription task,
the output directory (i.e., `path/to/results/`) will look as follows:

```
path/to/results/
├── .tigerflow/
└── transcribe/
    ├── 1.txt
    ├── 2.txt
    └── ...
```

where `.tigerflow/` is an internal directory storing the pipeline's operational state and related metadata.

!!! warning

    `.tigerflow/` is what enables resuming a previous pipeline run, so it should not be deleted or modified.

## Checking progress

We can check the pipeline's progress at any point by running:

=== "Command"

    ```bash
    tigerflow report progress path/to/results/
    ```

=== "Output"

    ```log
    ┏━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━┓
    ┃ Task       ┃ Processed ┃ Ongoing ┃ Failed ┃
    ┡━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━┩
    │ transcribe │        91 │       0 │      0 │
    │ embed      │        83 │       0 │      8 │
    │ ingest     │        83 │       0 │      0 │
    └────────────┴───────────┴─────────┴────────┘
    COMPLETED: ███████████████████████████░░░ 83/91 ( 91.2%)
    ```

where `path/to/results/` must be a valid output directory containing `.tigerflow/`.

## Checking errors

If the progress reports any failed files, we can identify them by running:

=== "Command"

    ```bash
    tigerflow report errors path/to/results/
    ```

=== "Output"

    ```log
    [embed] 8 failed files (open to view errors):
      results/.tigerflow/embed/7501863358941940997.err
      results/.tigerflow/embed/7501867598829702430.err
      results/.tigerflow/embed/7501869468910423326.err
      results/.tigerflow/embed/7501869707121757470.err
      results/.tigerflow/embed/7501870655906860306.err
      results/.tigerflow/embed/7501870694288985390.err
      results/.tigerflow/embed/7501870878855154987.err
      results/.tigerflow/embed/7501870943883545899.err
    ```

Each error file contains specific error messages that help identify and resolve issues
in the code or data.

!!! example

    In this case, all error files contain the same message:

    ```log
    Traceback (most recent call last):
    File "/home/sp8538/.conda/envs/tiktok/lib/python3.12/site-packages/tigerflow/tasks/local_async.py", line 47, in task
        await self.run(self._context, input_file, temp_file)
    File "/home/sp8538/tiktok/pipeline/tigerflow/demo/code/embed.py", line 35, in run
        resp.raise_for_status()  # Raise error if unsuccessful
        ^^^^^^^^^^^^^^^^^^^^^^^
    File "/home/sp8538/.conda/envs/tiktok/lib/python3.12/site-packages/aiohttp/client_reqrep.py", line 629, in raise_for_status
        raise ClientResponseError(
    aiohttp.client_exceptions.ClientResponseError: 400, message='Bad Request', url='https://api.voyageai.com/v1/embeddings'
    ```

    which suggests an issue with the embedding API request. However, since the same request was
    successful for other files, the issue likely lies in the input data (i.e., transcription).

    Upon inspection, we find the failed files have empty transcriptions, which explains the API
    request failure. Furthermore, we can confirm that the corresponding videos contain no audio,
    which led to the empty transcriptions in the first place.

    We may then exclude such videos from the pipeline to prevent future errors.
