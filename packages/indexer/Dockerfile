FROM python:3.10 AS indexer-base

RUN pip install poetry

WORKDIR /indexer

COPY poetry.lock pyproject.toml ./


ENV PYTHONPATH=/indexer
ENV POETRY_VIRTUALENVS_CREATE=false
FROM indexer-base AS indexer-prod

RUN poetry install -vv --no-cache

COPY . ./

FROM indexer-base AS indexer-dev

RUN poetry install --with dev -vv --no-cache

COPY . ./
