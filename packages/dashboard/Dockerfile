FROM python:3.10 AS dashboard-base

RUN pip install poetry

WORKDIR /dashboard

COPY poetry.lock pyproject.toml ./

ENV PYTHONPATH=/dashboard
ENV POETRY_VIRTUALENVS_CREATE=false

FROM dashboard-base AS dashboard-prod

RUN poetry install

COPY . ./

CMD gunicorn --bind :8050 --log-level info --workers 1 --threads 8 --timeout 0 dashboard.index:server

FROM dashboard-base AS dashboard-dev

RUN poetry install --with dev

COPY . ./

CMD python3 dashboard/index.py