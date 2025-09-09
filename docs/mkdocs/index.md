# Welcome to TigerFlow

TigerFlow is a Python framework that simplifies the creation and execution of data pipelines on Slurm-managed HPC clusters. Designed as a continuously running service with dynamic scaling, it minimizes the need for users to manually plan and allocate resources in advance.

## Why TigerFlow Matters

HPC clusters are an invaluable asset for researchers who require significant computational resources. For example, computational social scientists may need to extract features (e.g., transcription embeddings) from a large volume of TikTok videos and store them in databases for downstream analysis and modeling. However, the architecture of HPC clusters can present challenges for such workflows:

- ***Compute nodes often lack internet access.*** This prevents direct access to external APIs (e.g., LLM services provided by Google) or remote data sources (e.g., Amazon S3), requiring such tasks to be executed on a login or head node instead.

- ***Compute nodes often have restricted access to file systems.*** Certain file systems (e.g., cold storage) may not be mounted on compute nodes. This necessitates moving or copying data to accessible locations (e.g., scratch space) before processing can occur on compute nodes.

These constraints make it difficult to design and implement end-to-end data pipelines, especially when some steps require external API calls (restricted to login/head nodes) while others depend on high-performance compute resources (available only on compute nodes). TigerFlow addresses these challenges by offering a simple, unified framework for defining and running data pipelines across different types of cluster nodes.

Note that TigerFlow is *not* a fully generalized pipeline framework. Instead, it is a convenient and reliable tool tailored for specific types of data pipelines and their execution in HPC environments. Specifically, it supports data pipelines where:

- ***Each task performs embarrassingly parallel file processing.*** That is, files are processed independently of one another.
- ***The task dependency graph forms a rooted tree.*** That is, the graph has a single root task, and every other task has exactly one parent.

## How to Use TigerFlow

TigerFlow can be run on any HPC cluster managed by Slurm. Since it is written in Python, the system must have Python (version 3.10 or higher) installed.

### Installation

TigerFlow can be installed using `pip` or other package managers such as [`uv`](https://docs.astral.sh/uv/) and [`poetry`](https://python-poetry.org/docs/).

=== "pip"

    ```bash
    pip install tigerflow
    ```

=== "uv"

    ```bash
    uv add tigerflow
    ```

=== "poetry"

    ```bash
    poetry add tigerflow
    ```

### Quick Start

Once the package is installed, `tigerflow` command will be available, like so:

```bash
tigerflow --help
```

Running the above will display an overview of the tool, including supported subcommands.

For instance, `run` is a subcommand for running a user-defined pipeline, and its details can be viewed by running:

```bash
tigerflow run --help
```

### What Next

Please check out user [guides](guides/task.md) for more detailed instructions and examples.
