SHELL := /bin/bash
COMPOSE := docker compose

.PHONY: help env up down reset logs ps init-es test lint airflow-shell spark-gold

help: ## Liste des commandes
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

env: ## Crée le .env à partir de .env.example s'il n'existe pas
	@test -f .env || cp .env.example .env

up: env ## Démarre toute la stack de zéro
	$(COMPOSE) up -d --build
	@echo "Airflow  : http://localhost:8081"
	@echo "Kibana   : http://localhost:5601"
	@echo "MinIO    : http://localhost:9001"
	@echo "Spark UI : http://localhost:8080"

down: ## Arrête la stack
	$(COMPOSE) down

reset: ## Arrête et supprime les volumes (repart de zéro)
	$(COMPOSE) down -v

logs: ## Suit les logs
	$(COMPOSE) logs -f --tail=100

ps: ## État des services
	$(COMPOSE) ps

init-es: ## Crée les index Elasticsearch avec leurs mappings
	$(COMPOSE) exec airflow-scheduler python /opt/airflow/scripts/init_es.py

airflow-shell: ## Shell dans le scheduler Airflow
	$(COMPOSE) exec airflow-scheduler bash

spark-gold: ## Lance le job Spark Gold manuellement
	$(COMPOSE) exec airflow-scheduler python /opt/airflow/src/scipulse/spark/gold_job.py

test: ## Tests unitaires
	PYTHONPATH=src pytest -q

lint: ## Lint
	ruff check src dags tests

