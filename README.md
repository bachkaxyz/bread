# Secret Network Monorepo

I based the structure of off of this [example](https://github.com/python-poetry/poetry/issues/936#issuecomment-734504568)

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
 ⠿ Container sn-mono-postgres-1         Started
```

This network (`sn-mono-network`) is used to connect  all of the projects together (and all of the services in each project)

Each sub-project connects to this network so that they can communicate with each other.

### Start Sub Project

```bash
cd <project-name>
docker compose up -d
```

## How is this organized?

There is a root `pyproject.toml` file that contains the dev dependencies for the entire project.

Each project has its own `pyproject.toml` file that contains the dependencies for that project.

So for example, the `dash` dependency is only in the dashboard project's `pyproject.toml` file.

Each project has their own distinct configuration files, like `.gitignore`, to keep things separate.

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
