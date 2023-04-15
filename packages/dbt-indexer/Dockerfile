FROM python:3.10 AS dbt

RUN pip install poetry

COPY poetry.lock pyproject.toml ./dbt-indexer/

WORKDIR /dbt-indexer

COPY ./indexer ./indexer

RUN POETRY_VIRTUALENVS_CREATE=false poetry install