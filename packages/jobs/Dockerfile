FROM python:3.10 AS dagster-base

RUN pip install poetry

WORKDIR /jobs

COPY poetry.lock pyproject.toml ./

COPY . ./

ENV PYTHONPATH=/jobs

FROM dagster-base AS indexer-dev


ENV POETRY_VIRTUALENVS_CREATE=false

RUN pip wheel --use-pep517 "grpclib (==0.4.3)"
RUN pip wheel --use-pep517 "parsedatetime (==2.4)"

RUN poetry install --with dev -vv