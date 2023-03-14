FROM python:3.10 as api-base

RUN pip install poetry

COPY poetry.lock pyproject.toml ./

WORKDIR /api

COPY ./api ./api

FROM api-base as api-prod

RUN POETRY_VIRTUALENVS_CREATE=false poetry install

CMD exec gunicorn --bind :4000 --workers 1 --worker-class uvicorn.workers.UvicornWorker  --threads 8 api.main:app

FROM api-base as api-dev

RUN POETRY_VIRTUALENVS_CREATE=false poetry install --with dev

CMD uvicorn api.main:app --host 0.0.0.0 --port 4000 --reload