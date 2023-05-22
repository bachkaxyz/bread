FROM python:3.10 AS api-base

RUN pip install poetry

COPY poetry.lock pyproject.toml ./

WORKDIR /api

RUN POETRY_VIRTUALENVS_CREATE=false poetry install

COPY . ./

CMD gunicorn --bind :4000 --workers 1 --worker-class uvicorn.workers.UvicornWorker  --threads 8 api.main:app