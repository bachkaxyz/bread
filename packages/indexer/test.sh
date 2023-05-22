#!/bin/sh

pytest --junitxml=./reports/pytest.xml --cov="indexer" --cov-report xml --cov-report term-missing --cov-fail-under=85 -vvv > ./reports/pytest-coverage.txt