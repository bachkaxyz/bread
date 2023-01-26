FROM python:3.10

RUN pip install poetry

COPY poetry.lock pyproject.toml ./

RUN POETRY_VIRTUALENVS_CREATE=false poetry install

COPY ./dashboard ./dashboard

WORKDIR /dashboard

CMD exec gunicorn --bind :8050 --log-level info --workers 1 --threads 8 --timeout 0 index:server