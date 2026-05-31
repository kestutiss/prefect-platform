.PHONY: up down build logs logs-worker deploy setup test clean restart-worker shell

ifeq ($(OS),Windows_NT)
VENV_PYTHON := .venv/Scripts/python.exe
else
VENV_PYTHON := .venv/bin/python
endif

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

## Run tests locally (no Docker needed; requires `make setup` first)
test:
	$(VENV_PYTHON) -m pytest tests/ -v

## Open a shell inside the worker container
shell:
	docker compose exec prefect-worker bash

## Restart worker (picks up code changes when volumes are used)
restart-worker:
	docker compose restart prefect-worker

## Destroy all containers and volumes (full reset)
clean:
	docker compose down -v
