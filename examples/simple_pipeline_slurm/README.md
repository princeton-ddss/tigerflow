# Simple Pipeline with Slurm and Local Tasks

This example demonstrates a simple pipeline with the following steps:

1. Download books from [Project Gutenberg](https://www.gutenberg.org/)
2. Count the unique words in each book
3. Ingest the word counts into a single-writer database (SQLite)

Because the pipeline involves external API requests (Step 1) and Slurm jobs (Step 2),
it should be run on a login/head node of a Slurm-managed HPC cluster.

## Prerequisites

- [ ] Install the package with the additional dependencies required to run the examples:

    ```bash
    pip install tigerflow[examples]
    ```

- [ ] Update `setup_commands` in `code/config.yaml` to correctly activate the virtual environment where TigerFlow is installed.

## Running the Pipeline

To run the pipeline, execute:

```bash
cd code/
tigerflow run config.yaml ../data/ ../results/
```

Explore more commands and features in the user
[guides](https://princeton-ddss.github.io/tigerflow/latest/guides/task/).
