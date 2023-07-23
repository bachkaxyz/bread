# BREAD: Blockchain Read, Extract, Analyze, Display

BREAD is a data pipeline for blockchain data. Currently built for tendermint-based blockchains, it reads raw data from nodes, parses it into an easily consumable format, loaded into a database, and then can be served to a frontend or API (both coming soon).

## Core Components
- **extract.py**: A script that reads tx and block data.
- **parse.py**: A script to parse the JSON data into a relational format and save as Parquet files.
- **[DuckDB](https://duckdb.org/)**: An in-memory analytical database that is used as the engine for data processing and analysis.
- **[dbt](https://www.getdbt.com/)**: Data Build Tool (dbt) is used for transforming the data models, making it easier to understand and analyze the data.
- **[Dagster](https://dagster.io/)**: Orchestration tool that is used to schedule and manage the data pipeline.
- **[duckdbt](https://github.com/jwills/duckdbt)**: A package that enables DuckDB to run in a concurrent manner via a PostgreSQL proxy server called Buena Vista.

## Getting Started

1. **Clone the Repository**: Clone this repository to your local machine.
2. **Set up Environment Variables**: Set up the necessary environment variables in a `.env` file. This includes the network for the blockchain data.
3. **Build and Run the Docker Container**: Use the provided Makefile command, `make up`, to build and run the Docker container.
4. **Do Things**: run `make bash` to enter the container. You can also access a query interface at `http://localhost:8080/#`. 