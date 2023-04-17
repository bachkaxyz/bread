FROM python:3.10 AS dagster-base

RUN pip install poetry==1.4.1

WORKDIR /dags

COPY poetry.lock pyproject.toml ./

ENV PYTHONPATH=/dags

ENV POETRY_VIRTUALENVS_CREATE=false

RUN pip wheel --use-pep517 "grpclib (==0.4.3)"
RUN pip wheel --use-pep517 "parsedatetime (==2.4)"
RUN pip wheel --use-pep517 "pendulum (==2.1.2)"

RUN poetry install --with dev -vv

COPY ./workspace.yaml ./workspace.yaml
COPY ./dagster_home/dagster.yaml ./dagster_home/dagster.yaml
COPY ./dbt_project ./dbt_project
COPY ./dags ./dags
