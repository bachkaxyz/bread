FROM python:3.10 AS parse

RUN pip install poetry

WORKDIR /parse

COPY poetry.lock pyproject.toml ./

ENV PYTHONPATH=/parse
ENV POETRY_VIRTUALENVS_CREATE=false

RUN poetry install

COPY . ./