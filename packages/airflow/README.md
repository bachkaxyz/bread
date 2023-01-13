# Secret Network Airflow

We are using docker for local development.

## Running

### Start

```bash
docker-compose up
```

### Add Postgres and Redis services as Airflow connections

```bash
```

### To remove everything

```bash
docker-compose down --volumes --remove-orphans
```

## To use airflow cli

```bash
docker compose run airflow-cli <command>
```

or [install airflow cli wrapper](https://airflow.apache.org/docs/apache-airflow/stable/cli-and-env-variables-ref.html#installing-the-cli)
