FROM python:3.9

RUN pip install poetry

COPY poetry.lock pyproject.toml ./

RUN POETRY_VIRTUALENVS_CREATE=false poetry install

COPY ./api ./api

WORKDIR /api

CMD exec gunicorn --bind :4000 --workers 1 --worker-class uvicorn.workers.UvicornWorker  --threads 8 main:app