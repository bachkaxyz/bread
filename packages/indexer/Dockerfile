FROM python:3.10

RUN pip install poetry

WORKDIR /indexer

COPY poetry.lock pyproject.toml ./

RUN POETRY_VIRTUALENVS_CREATE=false poetry install

COPY . ./indexer

ENV PYTHONPATH=/indexer