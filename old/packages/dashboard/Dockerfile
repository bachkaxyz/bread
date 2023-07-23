FROM python:3.10 AS dashboard

RUN pip install poetry

WORKDIR /dashboard

COPY poetry.lock pyproject.toml ./

ENV PYTHONPATH=/dashboard
ENV POETRY_VIRTUALENVS_CREATE=false

RUN poetry install

COPY . ./

CMD gunicorn --bind :${DASHBOARD_PORT} --log-level info --workers 1 --threads 8 --timeout 0 dashboard.index:server
