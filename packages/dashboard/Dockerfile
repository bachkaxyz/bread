FROM python:3.10 AS dashboard-base

RUN pip install poetry

COPY poetry.lock pyproject.toml ./

WORKDIR /dashboard

COPY ./dashboard ./dashboard

FROM dashboard-base AS dashboard-prod

RUN POETRY_VIRTUALENVS_CREATE=false poetry install

CMD gunicorn --bind :8050 --log-level info --workers 1 --threads 8 --timeout 0 dashboard.index:server

FROM dashboard-base AS dashboard-dev

RUN POETRY_VIRTUALENVS_CREATE=false poetry install --with dev

CMD python3 dashboard/index.py