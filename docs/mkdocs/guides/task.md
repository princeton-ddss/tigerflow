# Task in TigerFlow

## Overview

TigerFlow supports three types of tasks:

- `LocalTask`: Runs *synchronous* operations on a login/head node
- `LocalAsyncTask`: Runs *asynchronous* operations on a login/head node
- `SlurmTask`: Runs *parallel* operations across compute nodes via Slurm

To define a task, subclass one of these types and implement the following methods:

| Method     | Required  | Description                                                                             |
|------------|:---------:|-----------------------------------------------------------------------------------------|
| `setup`    | No        | Initializes shared context used across multiple files (e.g., loading a model)           |
| `run`      | Yes       | Contains the processing logic applied to each file                                      |
| `teardown` | No        | Performs cleanup operations for graceful shutdown (e.g., closing a database connection) |

Then, simply call the inherited `cli()` method to turn the module into a runnable CLI application.

For instance, we can define a simple local task that converts text files to uppercase as follows:

```py title="upper.py"
from tigerflow.tasks import LocalTask


class Upper(LocalTask):
    @staticmethod
    def setup(context):
        context.common_data = "Common Data from Setup"
        print("Setup executed successfully!")

    @staticmethod
    def run(context, input_file, output_file):
        with open(input_file, "r") as fi:
            content = fi.read()

        new_content = context.common_data + "\n" + content.upper()

        with open(output_file, "w") as fo:
            fo.write(new_content)

    @staticmethod
    def teardown(context):
        print("Teardown executed successfully!")


Upper.cli()
```

where:

- `context` is a namespace to store and access any common, reusable data/objects (e.g., DB connection)
- `input_file` is a path to the input file to be processed
- `output_file` is a path to the output file to be generated

With `Upper.cli()`, this module becomes a runnable CLI application and we can check its details by running:

=== "Command"

    ```bash
    python upper.py --help
    ```

=== "Output"

    ```console
    Usage: test.py [OPTIONS]

    Run the task as a CLI application

    ╭─ Options ───────────────────────────────────────────────────────────────────╮
    │ *  --input-dir         PATH  Input directory to read data [required]        │
    │ *  --input-ext         TEXT  Input file extension [required]                │
    │ *  --output-dir        PATH  Output directory to store results [required]   │
    │ *  --output-ext        TEXT  Output file extension [required]               │
    │    --help                    Show this message and exit.                    │
    ╰─────────────────────────────────────────────────────────────────────────────╯
    ```

We can then run the task as follows:

=== "Command"

    ```bash
    python test.py --input-dir path/to/data/ --input-ext .txt --output-dir path/to/results/ --output-ext .txt
    ```

=== "Output"

    ```log
    2025-09-11 10:54:23 | INFO     | Setting up task
    Setup executed successfully!
    2025-09-11 10:54:23 | INFO     | Task setup complete
    2025-09-11 10:54:23 | INFO     | Starting processing: 5.txt
    2025-09-11 10:54:23 | INFO     | Successfully processed: 5.txt
    2025-09-11 10:54:23 | INFO     | Starting processing: 4.txt
    2025-09-11 10:54:23 | INFO     | Successfully processed: 4.txt
    ...
    2025-09-11 10:54:23 | INFO     | Starting processing: 1.txt
    2025-09-11 10:54:23 | INFO     | Successfully processed: 1.txt
    ^C2025-09-11 10:54:30 | WARNING  | Received signal 2, initiating shutdown
    2025-09-11 10:54:30 | INFO     | Shutting down task
    Teardown executed successfully!
    2025-09-11 10:54:30 | INFO     | Task shutdown complete
    ```

!!! info

    If a file is not processed successfully, an error output file will be generated (e.g., `4.err`
    instead of `4.txt`). This file contains specific error messages to assist with debugging.

## Examples

Say we want to implement the following workflow:

1. Transcribe video files using an open-source model (e.g., [Whisper](https://github.com/openai/whisper))
2. Embed the transcription files using an external API service (e.g., [Voyage AI](https://docs.voyageai.com/docs/embeddings))
3. Ingest the embeddings into a single-writer database (e.g., [DuckDB](https://duckdb.org/docs/stable/clients/python/overview.html))

We can create and test each task as shown below.

### Transcribing Video Files (`SlurmTask`)

We implement the transcription step as a Slurm task because it involves
compute-intensive work and we want to process files in parallel.

```py title="transcribe.py"
import whisper

from tigerflow.tasks import SlurmTask


class Transcribe(SlurmTask):
    @staticmethod
    def setup(context):
        context.model = whisper.load_model(
            "medium",
            download_root="/home/sp8538/.cache/whisper",
        )
        print("Model loaded successfully")

    @staticmethod
    def run(context, input_file, output_file):
        result = context.model.transcribe(str(input_file))
        print(f"Transcription ran successfully for {input_file}")

        with open(output_file, "w") as f:
            f.write(result["text"])


Transcribe.cli()
```

As shown, the task is defined such that:

- The model is loaded once during setup and stored in `context`
- This pre-loaded model is then accessed from `context` to transcribe each file

!!! warning

    With

    ```py
    context.model = whisper.load_model(
        "medium",
        download_root="/home/sp8538/.cache/whisper",
    )
    ```

    Whisper will attempt to load the model from `download_root` if the model file (`medium.pt`)
    is already present. If the file is missing, it will try to download it, which would fail
    in this case because `SlurmTask` runs on compute nodes without internet access.

    To avoid this issue, we can update the code to explicitly load the model from a local path:

    ```py
    local_model_path = "/home/sp8538/.cache/whisper/medium.pt"
    model = torch.load(local_model_path)
    context.model = whisper.Whisper(model)
    ```

Calling `Transcribe.cli()` turns this module into a runnable CLI application:

=== "Command"

    ```bash
    python transcribe.py --help
    ```

=== "Output"

    ```console
    Usage: transcribe.py [OPTIONS]

    Run the task as a CLI application

    ╭─ Options ──────────────────────────────────────────────────────────────────────────────╮
    │ *  --input-dir             PATH     Input directory to read data [required]            │
    │ *  --input-ext             TEXT     Input file extension [required]                    │
    │ *  --output-dir            PATH     Output directory to store results [required]       │
    │ *  --output-ext            TEXT     Output file extension [required]                   │
    │ *  --cpus                  INTEGER  Number of CPUs per worker [required]               │
    │ *  --memory                TEXT     Memory per worker [required]                       │
    │ *  --time                  TEXT     Wall time per worker [required]                    │
    │ *  --max-workers           INTEGER  Max number of workers for autoscaling [required]   │
    │    --gpus                  INTEGER  Number of GPUs per worker                          │
    │    --setup-commands        TEXT     Shell commands to run before the task starts       │
    │                                     (separate commands with a semicolon)               │
    │    --task-name             TEXT     Task name [default: Transcribe]                    │
    │    --help                           Show this message and exit.                        │
    ╰────────────────────────────────────────────────────────────────────────────────────────╯
    ```

We can then run the task as follows:

=== "Command"

    ```bash
    python transcribe.py \
    --input-dir path/to/data/ \
    --input-ext .mp4 \
    --output-dir path/to/results/ \
    --output-ext .txt \
    --cpus 1 \
    --memory "12G" \
    --time "02:00:00" \
    --max-workers 3 \
    --gpus 1 \
    --setup-commands "module purge; module load anaconda3/2024.6; conda activate tiktok"
    ```

=== "Output"

    ```log
    2025-09-16 10:53:44 | INFO     | Submitted task with Slurm job ID 690468
    2025-09-16 10:53:44 | INFO     | Status changed: INACTIVE -> PENDING (Reason: (None))
    2025-09-16 10:54:04 | INFO     | Status changed: PENDING (Reason: (None)) -> ACTIVE (0 workers)
    2025-09-16 10:59:55 | INFO     | Status changed: ACTIVE (0 workers) -> ACTIVE (1 workers)
    2025-09-16 11:00:45 | INFO     | 4 processed files
    2025-09-16 11:00:55 | INFO     | Status changed: ACTIVE (1 workers) -> ACTIVE (3 workers)
    2025-09-16 11:00:55 | INFO     | 1 processed files
    2025-09-16 11:01:05 | INFO     | 6 processed files
    ...
    2025-09-16 11:03:08 | INFO     | 2 processed files
    2025-09-16 11:04:08 | INFO     | 1 processed files
    2025-09-16 11:04:58 | INFO     | Status changed: ACTIVE (3 workers) -> ACTIVE (1 workers)
    2025-09-16 11:05:58 | INFO     | Status changed: ACTIVE (1 workers) -> ACTIVE (0 workers)
    ^C2025-09-16 11:06:40 | WARNING  | Received signal 2, initiating shutdown
    2025-09-16 11:06:40 | INFO     | Shutting down task
    2025-09-16 11:06:40 | ERROR    | Status changed: ACTIVE (0 workers) -> INACTIVE (Reason: CANCELLED+)
    2025-09-16 11:06:41 | INFO     | Task shutdown complete
    ```

!!! note

    The resources specified here, including `time`, apply to each individual worker.
    Workers can be spun up and down dynamically in response to incoming workloads,
    so it is beneficial to allocate only the minimal necessary resources per worker.

    For example, setting the worker `time` to a reasonable value like 2 hours (instead
    of 12 hours) can reduce scheduling delays, as longer Slurm job requests often result
    in longer queue times. Of course, the definition of "reasonable" depends on the nature
    of the work the worker performs. For instance, if processing each file takes around
    3 hours, setting the worker `time` to 12 hours may be appropriate.

### Embedding Text Files (`LocalAsyncTask`)

We implement the embedding step as a local *asynchronous* task because it involves
I/O-bound work (i.e., making external API requests) and we want to process multiple
files concurrently.

```py title="embed.py"
import asyncio
import os

import aiofiles
import aiohttp

from tigerflow.tasks import LocalAsyncTask


class Embed(LocalAsyncTask):
    @staticmethod
    async def setup(context):
        context.url = "https://api.voyageai.com/v1/embeddings"
        context.headers = {
            "Authorization": f"Bearer {os.environ['VOYAGE_API_KEY']}",
            "Content-Type": "application/json",
        }
        context.session = aiohttp.ClientSession()
        print("Session created successfully!")

    @staticmethod
    async def run(context, input_file, output_file):
        async with aiofiles.open(input_file, "r") as f:
            text = await f.read()

        async with context.session.post(
            context.url,
            headers=context.headers,
            json={
                "input": text.strip(),
                "model": "voyage-3.5",
                "input_type": "document",
            },
        ) as resp:
            resp.raise_for_status()  # Raise error if unsuccessful
            result = await resp.text()  # Raw JSON
            await asyncio.sleep(1)  # For API rate limit

        async with aiofiles.open(output_file, "w") as f:
            await f.write(result)

    @staticmethod
    async def teardown(context):
        await context.session.close()
        print("Session closed successfully!")


Embed.cli()
```

As shown, the task is defined such that it:

- Initializes reusable resources (e.g., HTTP session) and stores them in `context`
- Utilizes these resources from `context` to send a request to the external API for each input file
- Cleans up resources (e.g., HTTP session) at the end to ensure a graceful shutdown

!!! info

    `LocalAsyncTask` requires all operations to adhere to Python's `async`/`await` syntax.
    For example, as shown above, file reading and writing are performed using `aiofiles`,
    since standard file I/O would block the event loop and prevent concurrent file processing.
    Similarly, `LocalAsyncTask` should not include compute-intensive logic, as this would
    also block the event loop and goes against its intended use. For compute-heavy tasks,
    consider using `SlurmTask` instead.

??? tip "API Rate Limits"

    API services often enforce rate limits (e.g., 2000 requests per minute).
    To comply with these limits, we can use `asyncio.sleep()` within the `run`
    logic (as shown above), in combination with the `--concurrency-limit` option
    (see below), which controls the maximum number of files processed concurrently.

    For example, if each API request takes less than a second and the service allows
    up to 2000 requests per minute, we can:

    - Use `asyncio.sleep(1)` in the `run` logic to ensure each request takes at least one second
    - Set `--concurrency-limit` to 30 to ensure no more than 30 requests are processed concurrently

    Together, these measures effectively cap the request rate at 1800 requests per minute,
    keeping it safely within the limit.

Calling `Embed.cli()` turns this module into a runnable CLI application:

=== "Command"

    ```bash
    python embed.py --help
    ```

=== "Output"

    ```console
    Usage: embed.py [OPTIONS]

    Run the task as a CLI application

    ╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────────────╮
    │ *  --input-dir                PATH     Input directory to read data [required]                                 │
    │ *  --input-ext                TEXT     Input file extension [required]                                         │
    │ *  --output-dir               PATH     Output directory to store results [required]                            │
    │ *  --output-ext               TEXT     Output file extension [required]                                        │
    │ *  --concurrency-limit        INTEGER  Maximum number of coroutines that may run concurrently at any given     │
    │                                        time (excess coroutines are queued until capacity becomes available)    │
    │                                        [required]                                                              │
    │    --task-name                TEXT     Task name [default: Embed]                                              │
    │    --help                              Show this message and exit.                                             │
    ╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
    ```

We can then run the task as follows:

=== "Command"

    ```bash
    python embed.py \
    --input-dir path/to/data/ \
    --input-ext .txt \
    --output-dir path/to/results/ \
    --output-ext .json \
    --concurrency-limit 30
    ```

=== "Output"

    ```log
    2025-09-19 10:28:32 | INFO     | Setting up task
    Session created successfully!
    2025-09-19 10:28:32 | INFO     | Task setup complete
    2025-09-19 10:28:32 | INFO     | Starting processing: 7501870786337115434.txt
    2025-09-19 10:28:32 | INFO     | Starting processing: 7501870786941127967.txt
    2025-09-19 10:28:32 | INFO     | Starting processing: 7501870783862541576.txt
    ...
    2025-09-19 10:28:34 | INFO     | Starting processing: 7501869901028592927.txt
    2025-09-19 10:28:34 | INFO     | Successfully processed: 7501871089715268906.txt
    2025-09-19 10:28:34 | INFO     | Starting processing: 7501870443775692078.txt
    2025-09-19 10:28:34 | INFO     | Successfully processed: 7501863546456771870.txt
    2025-09-19 10:28:34 | INFO     | Starting processing: 7501869899782901022.txt
    2025-09-19 10:28:34 | INFO     | Successfully processed: 7501870775461317906.txt
    ...
    2025-09-19 10:28:36 | INFO     | Successfully processed: 7501870542656474398.txt
    2025-09-19 10:28:36 | INFO     | Successfully processed: 7501870861306318126.txt
    2025-09-19 10:28:36 | INFO     | Successfully processed: 7501870700089707807.txt
    ^C2025-09-19 10:28:40 | WARNING  | Received signal 2, initiating shutdown
    2025-09-19 10:28:40 | INFO     | Shutting down task
    Session closed successfully!
    2025-09-19 10:28:40 | INFO     | Task shutdown complete
    ```

### Ingesting Text Embeddings (`LocalTask`)

We implement the ingestion step as a local *synchronous* task because our target
database ([DuckDB](https://duckdb.org/docs/stable/connect/concurrency.html))
only supports writes from a single process.

```py title="ingest.py"
import json

import duckdb

from tigerflow.tasks import LocalTask


class Ingest(LocalTask):
    @staticmethod
    def setup(context):
        db_path = "/home/sp8538/tiktok/pipeline/tigerflow/demo/results/test.db"

        conn = duckdb.connect(db_path)  # Creates file if not existing
        print(f"Successfully connected to {db_path}")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS embeddings (
                id UBIGINT,
                embedding FLOAT[1024],
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        context.conn = conn

    @staticmethod
    def run(context, input_file, output_file):
        with open(input_file, "r") as f:
            content = json.load(f)

        embedding = content["data"][0]["embedding"]

        context.conn.execute(
            "INSERT INTO embeddings (id, embedding) VALUES (?, ?)",
            (input_file.stem, embedding),
        )

    @staticmethod
    def teardown(context):
        context.conn.close()
        print("DB connection closed")


Ingest.cli()
```

As shown, the task is defined such that:

- A database connection is created once during setup and stored in `context`
- This database connection is then accessed from `context` to ingest each file
- The database connection is closed at the end to ensure a graceful shutdown

Calling `Ingest.cli()` turns this module into a runnable CLI application:

=== "Command"

    ```bash
    python ingest.py --help
    ```

=== "Output"

    ```console
    Usage: ingest.py [OPTIONS]

    Run the task as a CLI application

    ╭─ Options ────────────────────────────────────────────────────────────────────╮
    │ *  --input-dir         PATH  Input directory to read data [required]         │
    │ *  --input-ext         TEXT  Input file extension [required]                 │
    │ *  --output-dir        PATH  Output directory to store results [required]    │
    │ *  --output-ext        TEXT  Output file extension [required]                │
    │    --task-name         TEXT  Task name [default: Ingest]                     │
    │    --help                    Show this message and exit.                     │
    ╰──────────────────────────────────────────────────────────────────────────────╯
    ```

We can then run the task as follows:

=== "Command"

    ```bash
    python ingest.py \
    --input-dir path/to/data/ \
    --input-ext .json \
    --output-dir path/to/results/ \
    --output-ext .out
    ```

=== "Output"

    ```log
    2025-09-19 13:02:16 | INFO     | Task setup complete
    2025-09-19 13:02:16 | INFO     | Starting processing: 7501869531975929119.json
    2025-09-19 13:02:17 | INFO     | Successfully processed: 7501869531975929119.json
    2025-09-19 13:02:17 | INFO     | Starting processing: 7501869470705782062.json
    2025-09-19 13:02:17 | INFO     | Successfully processed: 7501869470705782062.json
    2025-09-19 13:02:17 | INFO     | Starting processing: 7501871439457373470.json
    ...
    2025-09-19 13:02:18 | INFO     | Successfully processed: 7501870861306318126.json
    2025-09-19 13:02:18 | INFO     | Starting processing: 7501870700089707807.json
    2025-09-19 13:02:18 | INFO     | Successfully processed: 7501870700089707807.json
    ^C2025-09-19 13:02:22 | WARNING  | Received signal 2, initiating shutdown
    2025-09-19 13:02:22 | INFO     | Shutting down task
    DB connection closed
    2025-09-19 13:02:22 | INFO     | Task shutdown complete
    ```

!!! info

    Note that we specify `--output-dir` and `--output-ext` even though the task’s `run` logic
    does not write to `output_file`. This is necessary because TigerFlow creates an empty
    "placeholder" file even when no content is written. This placeholder indicates that the file
    was processed successfully according to the user-provided `run` logic, even if no concrete
    output was produced. If processing fails, however, TigerFlow generates a separate error file
    containing the relevant error message instead of the placeholder.


