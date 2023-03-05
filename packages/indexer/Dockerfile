FROM python:3.10 as indexer-base

RUN pip install poetry

WORKDIR /indexer

COPY poetry.lock pyproject.toml ./

FROM indexer-base as indexer-prod

COPY . ./indexer

ENV PYTHONPATH=/indexer

RUN POETRY_VIRTUALENVS_CREATE=false poetry install -vv

FROM indexer-prod as indexer-dev

RUN POETRY_VIRTUALENVS_CREATE=false poetry install --with dev -vv