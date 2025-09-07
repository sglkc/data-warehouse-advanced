# Data Warehouse Pipeline

This directory contains dlt and SQLMesh project.

## Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/)
- [Moonrepo](https://moonrepo.dev/docs/getting-started/installation)

## Setup

- Create virtual env with uv

  ```sh
  uv venv
  ```

- Activate virtual env

  ```sh
  source .venv/bin/activate
  ```

- Sync Python packages

  ```sh
  uv sync
  ```

## dlt

dlt (data load tool) is a Python library to easily move data from various sources into a
well-structured datasets.

The sources for this project, inside `dlt_project/sources` is a bunch of csv files. Where each file is its own
dataset (table).

Every sources will be extracted into `staging` schema of the configure PostgreSQL database.
The staging layer contains 1:1 copy of source data and the rest of the process will use these
datasets as the raw primary data.

To load the source data into staging database:

```sh
moon :dlt-run -- load_csv_sources.py
```

## SQLMesh

SQLMesh is a Python framework for data transformation in TL (transform, load)

To transform source data from staging layer to intermediate layer:

```sh
moon :sqlmesh-plan
```
