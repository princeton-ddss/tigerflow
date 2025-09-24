# Pipeline for Audio Feature Extraction

This example demonstrates a pipeline with the following steps:

1. Transcribe audio [files](https://dare.wisc.edu/audio/) using an open-source model (e.g., [Whisper](https://github.com/openai/whisper))
2. Embed the transcription files using an external API service (e.g., [Voyage AI](https://docs.voyageai.com/docs/embeddings))
3. Ingest the embeddings into a single-writer database (e.g., [DuckDB](https://duckdb.org/docs/stable/clients/python/overview.html))

Because the pipeline involves Slurm jobs (Step 1) and external API requests (Step 2),
it should be run on a login/head node of a Slurm-managed HPC cluster.

## Prerequisites

- [ ] Install the package with the additional dependencies required to run the examples:

    ```bash
    pip install tigerflow[examples]
    ```

- [ ] Update `setup_commands` in `code/config.yaml` to correctly activate the virtual environment where TigerFlow is installed.

- [ ] Download the model for transcription (Step 1):

    ```py
    python models/whisper/download.py
    ```

- [ ] Obtain an API key from [Voyage AI](https://docs.voyageai.com/docs/api-key-and-installation#authentication-with-api-keys) and set it as an environment variable for the pipeline:

    ```bash
    export VOYAGE_API_KEY=<your-secret-key>
    ```

## Running the Pipeline

To run the pipeline, execute:

```bash
cd code/
tigerflow run config.yaml ../data/ ../results/
```

Explore more commands and features in the user
[guides](https://princeton-ddss.github.io/tigerflow/latest/guides/task/).
