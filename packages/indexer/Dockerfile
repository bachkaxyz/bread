FROM python:3.10 AS indexer

RUN pip install poetry

WORKDIR /indexer

COPY poetry.lock pyproject.toml ./

COPY dist/*.whl /tmp/
COPY info/requirements.txt /tmp/

ENV PYTHONPATH=/indexer

RUN python3 -m pip install --pre --no-cache-dir --find-links=/tmp/ -r /tmp/requirements.txt
