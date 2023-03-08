FROM python:3.10 as indexer-base

RUN pip install poetry

WORKDIR /indexer

COPY poetry.lock pyproject.toml ./

COPY . ./

ENV PYTHONPATH=/indexer

FROM indexer-base as indexer-prod

RUN POETRY_VIRTUALENVS_CREATE=false poetry install -vv

FROM indexer-base as indexer-dev

RUN POETRY_VIRTUALENVS_CREATE=false poetry install --with dev -vv