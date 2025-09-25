# Simple Pipeline with Local Tasks

This example demonstrates a minimal pipeline consisting of two tasks:

1. Download books from [Project Gutenberg](https://www.gutenberg.org/)
2. Count the unique words in each book

Since both tasks are defined to run locally&mdash;one asynchronously and the other
synchronously&mdash;the pipeline can run in any environment with internet access,
including a personal laptop.

## Prerequisites

- [ ] Install the package with the additional dependencies required to run the examples:

    ```bash
    pip install tigerflow[examples]
    ```

## Running the Pipeline

To run the pipeline, execute:

```bash
cd code/
tigerflow run config.yaml ../data/ ../results/
```

Explore more commands and features in the user
[guides](https://princeton-ddss.github.io/tigerflow/latest/guides/task/).
