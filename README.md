# TigerFlow

[![Python](https://img.shields.io/badge/Python-3.10%20%7C%203.11%20%7C%203.12-3776AB.svg?style=flat&logo=python&logoColor=white)](https://www.python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

<p align="center">
  <img alt="tigerflow-run-screenshot" src="https://raw.githubusercontent.com/princeton-ddss/tigerflow/refs/heads/main/.github/assets/screenshot.png" width="750" />
</p>

TigerFlow is a Python framework that simplifies the creation and execution of ***data pipelines on Slurm-managed HPC clusters***. It supports data pipelines where:

- *Each task performs embarrassingly parallel file processing.* That is, files are processed independently of one another.
- *The task dependency graph forms a rooted tree.* That is, the graph has a single root task, and every other task has exactly one parent.

Designed as a ***continuously running service with dynamic scaling***, TigerFlow minimizes the need for users to manually plan and allocate resources in advance.

## Why TigerFlow Matters

HPC clusters are an invaluable asset for researchers who require significant computational resources. For example, computational social scientists may need to extract features (e.g., transcription embeddings) from a large volume of TikTok videos and store them in databases for downstream analysis and modeling. However, the architecture of HPC clusters can present challenges for such workflows:

- ***Compute nodes often lack internet access.*** This prevents direct access to external APIs (e.g., LLM services provided by Google) or remote data sources (e.g., Amazon S3), requiring such tasks to be executed on a login or head node instead.

- ***Compute nodes often have restricted access to file systems.*** Certain file systems (e.g., cold storage) may not be mounted on compute nodes. This necessitates moving or copying data to accessible locations (e.g., scratch space) before processing can occur on compute nodes.

These constraints make it difficult to design and implement end-to-end data pipelines, especially when some steps require external API calls (restricted to login/head nodes) while others depend on high-performance compute resources (available only on compute nodes). TigerFlow addresses these challenges by offering a simple, unified framework for defining and running data pipelines across different types of cluster nodes.

### Additional Advantages

TigerFlow further streamlines HPC workflows by addressing common inefficiencies in traditional Slurm-based job scheduling:

- ***No need to pre-batch workloads.*** Each Slurm task in TigerFlow runs a dynamically scalable worker cluster that automatically adapts to the incoming workload, eliminating the need for manual batch planning and tuning.
- ***No need to start a new Slurm job for each file.*** In TigerFlow, a single Slurm job runs as a long-lived worker process that handles multiple files. It performs common operations (e.g., setup and teardown) only once, while applying the actual file-processing logic individually to each file. This reduces idle time and resource waste from launching a separate Slurm job for every file.
- ***No need to wait for all files to complete a pipeline step.*** In TigerFlow, files are processed individually as they arrive, supporting more flexible and dynamic workflows.

These features make TigerFlow especially well-suited for running large-scale or real-time data pipelines on HPC systems.

## How to Use TigerFlow

TigerFlow can be run on any HPC cluster managed by Slurm. Since it is written in Python, the system must have Python (version 3.10 or higher) installed.

### Installation

TigerFlow can be installed using `pip`:

```bash
pip install tigerflow
```

It can also be installed using other package managers such as [`uv`](https://docs.astral.sh/uv/) and [`poetry`](https://python-poetry.org/docs/).

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

Please check out user [guides](https://princeton-ddss.github.io/tigerflow/latest/guides/task/) for more detailed instructions and examples.
