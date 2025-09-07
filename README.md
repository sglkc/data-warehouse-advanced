# Data Warehouse Monorepo

This project is a data warehouse mock project about a hardware company sales. The final output will
be a data warehouse with layered medallion architecture-like that will be presented in a human-friendly way.

## TODO

The complete architecture will be included inside `docs` directory. Stay tuned!

## Quick Intro

The project is bootstrapped from [Zero One Group's monorepo](https://github.com/zero-one-group/monorepo/)
which is used as a standardized boilerplate for company projects.

Most of the tasks will be done inside `data-pipeline` directory, where the ETL process of the data
warehouse happen.

## Prerequisites

- [pnpm](https://pnpm.io/installation) to run commands
- [Docker](https://docs.docker.com/engine/install/) for containerization
- [uv](https://docs.astral.sh/uv/getting-started/installation/) for Python
- [Moonrepo](https://moonrepo.dev/docs/getting-started/installation) for monorepo management

## Containerization

The project uses Docker for development, the services used are:

- PostgreSQL, as database for data warehouse
- Metabase, as front-end for data warehouse
- pgweb, for web-based database browser

To run these services inside Docker, run:

```sh
pnpm compose:up
```

To stop the services, run:

```sh
pnpm compose:down
```

To clean up everything, including container data, run:

```sh
pnpm compose:cleanup
```

Once everything is running, you may use each services below.

## Database

To log in to the PostgreSQL data warehouse inside Docker, use the following credentials:

- Host: localhost
- Port: 5432
- User: postgres
- Password: securedb
- Database: myorg

## Front-end

The project uses [Metabase](https://www.metabase.com/) for data presentation. Metabase is an
open-source web-based business inteligence and analytics tool to query and visualize data in a
straightforward manner.

Metabase uses its own database and separated from the data warehouse. This way, the front-end can be
synced by export and import.

After running `pnpm compose:up`, you must do a first setup for Metabase. Please read
[SETUP-METABASE.md](./docs/SETUP-METABASE.md) for a walkthrough.

## Data Pipeline

The data pipeline uses `dlt` and `sqlmesh` for the ETL process.

For technical details, please head to `data-pipeline` directory and open [README.md](./data-pipeline/README.md).
