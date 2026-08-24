.PHONY: help setup-dev lock env-init test test-fast test-unit test-integration airflow-build airflow-check airflow-dirs airflow-init airflow-up airflow-down airflow-start

UV ?= uv
UV_RUN := $(UV) run
COMPOSE := docker compose --env-file ../.env

help:
	@echo "Available targets:"
	@echo "  make setup-dev         - Sync the local project environment with uv"
	@echo "  make lock              - Refresh uv.lock from pyproject.toml"
	@echo "  make test              - Run the full test suite"
	@echo "  make test-fast         - Run tests without integration"
	@echo "  make test-unit         - Run unit tests only"
	@echo "  make test-integration  - Run integration tests only"
	@echo ""
	@echo "Airflow:"
	@echo "  make env-init          - Create .env from .env.example if missing"
	@echo "  make airflow-build     - Build Airflow Docker image"
	@echo "  make airflow-check     - Check Python dependencies inside container"
	@echo "  make airflow-init      - Initialize Airflow database and admin user"
	@echo "  make airflow-up        - Start all Airflow services"
	@echo "  make airflow-down      - Stop Airflow services"
	@echo "  make airflow-start     - Full startup (build + check + init + up)"

setup-dev:
	$(UV) sync --locked

lock:
	$(UV) lock

env-init:
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		sed -i.bak 's/^AIRFLOW_UID=.*/AIRFLOW_UID=$(shell id -u)/' .env && rm -f .env.bak; \
		echo "Created .env from .env.example (AIRFLOW_UID=$(shell id -u))"; \
	else \
		echo ".env already exists"; \
	fi

test:
	$(UV_RUN) python -m pytest -q

test-fast:
	$(UV_RUN) python -m pytest -q -m "not integration"

test-unit:
	$(UV_RUN) python -m pytest -q tests/unit

test-integration:
	$(UV_RUN) python -m pytest -q -m integration

# -----------------------------
# Airflow / Docker
# -----------------------------

airflow-build:
	cd docker && $(COMPOSE) build

airflow-check: airflow-dirs
	cd docker && $(COMPOSE) run --rm --no-deps --entrypoint python airflow-webserver -c "import pandas, duckdb, requests; print('ok')"

# Docker creates missing bind-mount sources as root, which locks Airflow out of
# its own log directory. Creating them here keeps them owned by the host user.
airflow-dirs:
	@mkdir -p logs/airflow

airflow-init: airflow-dirs
	cd docker && $(COMPOSE) up airflow-init

airflow-up: airflow-dirs
	cd docker && $(COMPOSE) up -d --no-build

airflow-down:
	cd docker && $(COMPOSE) down

airflow-start: airflow-build airflow-check airflow-init airflow-up
