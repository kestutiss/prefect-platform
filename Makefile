.PHONY: help up down build logs logs-worker deploy setup test clean restart-worker shell \
        mongo-up mongo-down mongo-logs mongo-shell migrate seed

.DEFAULT_GOAL := help

ifeq ($(OS),Windows_NT)
VENV_PYTHON := .venv/Scripts/python.exe
else
VENV_PYTHON := .venv/bin/python
endif

##@ Core

## Show this help message (default when no target is given)
help:
	@python scripts/help.py

## Start all services (postgres + server + worker)
up:
	docker compose up -d

## Stop all services
down:
	docker compose down

## Rebuild image after code or requirements changes, then restart worker
build:
	docker compose up -d --build prefect-worker

## Tail all service logs
logs:
	docker compose logs -f

## Tail worker logs only
logs-worker:
	docker compose logs -f prefect-worker

## Redeploy all flows from prefect.yaml
deploy:
	docker compose exec prefect-worker prefect deploy --all

## Create venv and install requirements (run once before `make test`)
setup:
	python -m venv .venv
	$(VENV_PYTHON) -m pip install -U pip
	$(VENV_PYTHON) -m pip install -r requirements.txt

## Run tests locally; pass TEST=<path|node-id|pytest args> to filter (default: all)
test:
	$(VENV_PYTHON) -m pytest $(if $(TEST),$(TEST),tests/) -v

## Open a shell inside the worker container
shell:
	docker compose exec prefect-worker bash

## Restart worker (picks up code changes when volumes are used)
restart-worker:
	docker compose restart prefect-worker

## Destroy all containers and volumes (full reset)
clean:
	docker compose down -v

##@ MongoDB dev environment

## Start MongoDB (joins prefect-network; flow containers reach it at mongo:27017)
mongo-up:
	docker compose up -d mongo

## Stop MongoDB (data volume is preserved)
mongo-down:
	docker compose stop mongo

## Tail MongoDB logs
mongo-logs:
	docker compose logs -f mongo

## Open a mongosh shell on the account database
mongo-shell:
	docker compose exec mongo mongosh account

## Apply pending migrations (requires: make setup && make mongo-up)
migrate:
	$(VENV_PYTHON) -m scripts.migrate

## Seed coupons collection with test data (requires: make migrate first)
seed:
	$(VENV_PYTHON) -m scripts.seed
