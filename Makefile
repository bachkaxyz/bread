# import config.
cnf ?= .env
-include $(cnf)
export $(shell sed 's/=.*//' $(cnf))

# HELP
# This will output the help for each task
# thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help

help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.DEFAULT_GOAL := help

up:
	docker-compose up -d 
	
down:
	docker-compose down

bash:
	docker-compose exec bread bash
	
build:
	docker build -t bread -f Dockerfile.bread .

build-no-cache:
	docker build  --no-cache -t bread -f Dockerfile.bread .

server:
	cd dbt && python3 -m duckdbt.server

dbt-run:
	cd dbt && dbt run && cd

dbt-build:
	cd dbt && dbt build && cd

make pipeline:
	python pipelines/pipeline.py
