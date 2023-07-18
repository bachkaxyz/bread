FROM python:3.10 AS api

RUN pip install poetry

WORKDIR /api

COPY poetry.lock pyproject.toml ./

ENV PYTHONPATH=/api
ENV POETRY_VIRTUALENVS_CREATE=false
RUN poetry install

COPY . ./

CMD gunicorn --bind :4000 --workers 1 --worker-class uvicorn.workers.UvicornWorker  --threads 8 api.main:app