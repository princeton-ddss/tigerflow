# TigerFlow

[![Python](https://img.shields.io/badge/Python-3.10%20%7C%203.11%20%7C%203.12-3776AB.svg?style=flat&logo=python&logoColor=white)](https://www.python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/princeton-ddss/tigerflow/actions/workflows/ci.yml/badge.svg)](https://github.com/princeton-ddss/tigerflow/actions/workflows/ci.yml)
[![CD](https://github.com/princeton-ddss/tigerflow/actions/workflows/cd.yml/badge.svg)](https://github.com/princeton-ddss/tigerflow/actions/workflows/cd.yml)

<p align="center">
  <img alt="tigerflow-logo" src="https://raw.githubusercontent.com/princeton-ddss/tigerflow/refs/heads/main/docs/mkdocs/assets/img/logo.png" width="350" />
</p>

**TigerFlow** is a Python framework that simplifies the creation and execution of data pipelines on Slurm-managed HPC clusters. It supports data pipelines where:

- *Each task performs embarrassingly parallel, one-to-one file processing.* That is, each input file is transformed into a single output file independently of all other input files.
- *The graph of task input/output files forms an [arborescence](https://en.wikipedia.org/wiki/Arborescence_(graph_theory)).* That is, there is a single root file, and every other file depends on exactly one parent.

Designed as a continuously running service with dynamic scaling, TigerFlow minimizes the need for users to manually plan and allocate resources in advance.

## Why TigerFlow?

HPC clusters are an invaluable asset for researchers who require significant computational resources. For example, computational social scientists may need to extract features (e.g., transcription embeddings) from a large volume of TikTok videos and store them in databases for downstream analysis and modeling. However, the architecture of HPC clusters can present challenges for such workflows:

- **Compute nodes often lack internet access.** This prevents direct access to external APIs (e.g., LLM services provided by Google) or remote data sources (e.g., Amazon S3), requiring such tasks to be executed on a login or head node instead.

- **Compute nodes often have restricted access to file systems.** Certain file systems (e.g., cold storage) may not be mounted on compute nodes. This necessitates moving or copying data to accessible locations (e.g., scratch space) before processing can occur on compute nodes.

These constraints make it difficult to design and implement end-to-end data pipelines when some steps require external API call&mdash;restricted to login/head nodes&mdash;while others depend on high-performance compute resources available only on compute nodes. TigerFlow addresses these challenges by offering a simple, unified framework for defining and running data pipelines across different types of cluster nodes.

## Key Features

TigerFlow further streamlines HPC workflows by addressing common inefficiencies in traditional Slurm-based job scheduling:

- **No need to pre-batch workloads.** Each Slurm task in TigerFlow runs a dynamically scalable worker cluster that automatically adapts to the incoming workload, eliminating the need for manual batch planning and tuning.
- **No need to start a new Slurm job for each file.** In TigerFlow, a single Slurm job runs as a long-lived worker process that handles multiple files. It performs shared operations (e.g., setup and teardown) *once*, while applying file-processing logic individually to each file. This reduces idle time and resource waste from launching a separate Slurm job for every file.
- **No need to wait for all files to complete a pipeline step.** In TigerFlow, files are processed individually as they arrive, supporting more flexible and dynamic workflows.

These features make TigerFlow especially well-suited for running large-scale or real-time data pipelines on HPC systems.

## Quickstart

TigerFlow can be run on any HPC cluster managed by Slurm. Since it is written in Python, the system must have Python (version 3.10 or higher) installed.

### Installation

TigerFlow can be installed using `pip`:

```bash
pip install tigerflow
```

It can also be installed using other package managers such as [`uv`](https://docs.astral.sh/uv/) and [`poetry`](https://python-poetry.org/docs/).

### Usage

Once the package is installed, `tigerflow` command will be available, like so:

```bash
tigerflow --help
```

Running the above will display an overview of the tool, including supported subcommands.

For instance, `run` is a subcommand for running a user-defined pipeline, and its details can be viewed by running:

```bash
tigerflow run --help
```

Try running the [examples](https://github.com/princeton-ddss/tigerflow/tree/main/examples),
starting with a simple pipeline consisting of two local tasks.

### Next Steps

Check out the user [guide](https://princeton-ddss.github.io/tigerflow/latest/guides/task/) for more details.
