#!/bin/sh

pytest --junitxml=./reports/pytest.xml --cov="indexer" --cov-report xml --cov-report term-missing --cov-fail-under=90 -vvv > ./reports/pytest-coverage.txt