# Secret Network Monorepo

For more information structure look at this [example](https://github.com/python-poetry/poetry/issues/936#issuecomment-734504568)

## How is this organized?

There is a root `pyproject.toml` file that contains the dev dependencies for the entire project.

Each project has its own `pyproject.toml` file that contains the dependencies for that project.

So for example, the `dash` dependency is only in the dashboard project's `pyproject.toml` file.

Each project has their own distinct configuration files, like `.gitignore`, to keep things separate while also letting them share common things between them.

## Services

### `packages/airflow`

We are using airflow to schedule different tasks that need to be run periodically. For example:

- updating our token prices from coingecko
- validator distribution calculations
- bridge monitoring
- ibc monitoring

### `packages/api`

This is the api that serves the data to the dashboard. It is a rest api that is built with [fastapi](https://fastapi.tiangolo.com/)

### `packages/dashboard`

This is the dashboard that is built with [dash](https://dash.plotly.com/) and deployed here [https:/secretanalytics.xyz](https:/secretanalytics.xyz)

## Current Development Status

We are currently in the process of migrating from multiple old repos to this new one:

Here are a list of tasks that need to be done:
Work Plan

- [ ] Migrate from Heroku to GCP (2 weeks)
  - [ ] Redis (cut until ended)
  - [x] Postgres
  - [x] Dashboard app via Cloud Run [(link)](https://dashboard-server-2sizcg3ipa-uc.a.run.app/)

- [ ] Data modeling (6 weeks)
  - [ ] Airflow via Cloud Compose to execute Python scripts and cronjobs (RQ tasks)
    - [ ] CG Prices
    - [x] Validator
    - [ ] Bridges
      - [ ] Old ETH and BSC bridges
      - [ ] New ETH bridge
      - [ ] IBC
    - [ ] Production Deployment
  - [ ] DBT for rollup tables and SQL transformations
  - [ ] Indexer for data ingestion
    - [ ] Blocks
    - [ ] Transactions
    - [ ] Messages
    - [ ] Events
    - [ ] Logs
    - [ ] Contracts

- [ ] API updates
  - [ ] Update API to come from new repo
  - [ ] Integrate the API into secret analytics; as opposed to DB queries, all data will be managed from the API.
  - [ ] Eliminate task queue in favor of API

- [ ] Fixes to existing website
  - [ ] Overview page: Refactor cumulative addresses
  - [ ] SNIP/Contracts pages (maybe best to just remove)
  - [ ] Gas Station / Mempool (maybe best to just remove)
  - [ ] Improve server side caching

## Setup

### Create new virtual environment

```bash
conda create -n "sn-mono" python=3.11    
```

### Activate virtual environment

```bash
conda activate "sn-mono"
```

### Install poetry

```bash
pip install poetry
```

### Install dependencies

From root:

```bash
poetry install
```

This will install all the dependencies for all the projects

## Running

### Start Core Services

```bash
docker compose up -d
```

**Output:**

```bash
❯ docker compose up -d
[+] Running 3/3
 ⠿ Network sn-mono-network              Created                                                           0.0s
 ⠿ Volume "sn-mono_postgres-db-volume"  Created                                                           0.0s
 ⠿ Container sn-mono-workhorse          Started 
```

This network (`sn-mono-network`) is used to connect  all of the projects together (and all of the services in each project)

Each sub-project connects to this network so that they can communicate with each other.

note: core service postgres runs on port 5431, so db url is either:

- postgresql://postgres:postgres@127.0.0.1:5431/postgres
- postgresql://postgres:postgres@sn-mono-workhorse:5431/postgres (if in another docker container)

### Start Sub Project

```bash
cd <project-name>
docker compose up -d
```

## To add a new project

### Create a new poetry project

This scaffolds the project in the correct way.

Run from root:

```bash
poetry new ./packages/<project-name>
```

### Link the new project to the root `pyproject.toml` file

Then in the the root `pyproject.toml` file, add the new project to the `packages` section like so:

```toml
project-name = { path = "./packages/<project-name>", develop = true, extras = ["dev"] }}`
```

extras = ["dev"] is optional, but it will install the dev dependencies for that project, so for airflow we do this because we need the dev dependencies for the `airflow` package for IDE support.
