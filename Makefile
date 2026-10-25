# Live Momentum Engine - developer shortcuts
# Usage: make <target>   (run `make help` for the list)

-include .env
KAFKA_TOPIC ?= live-match-events
POSTGRES_USER ?= momentum
POSTGRES_DB ?= momentum_dev

KAFKA_BIN := /opt/kafka/bin
COMPOSE := docker compose

.DEFAULT_GOAL := help
.PHONY: help up down ps logs topics describe consume psql reset lint

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

up: ## Start Kafka + Postgres (and create the topic)
	$(COMPOSE) up -d --wait kafka postgres
	$(COMPOSE) up kafka-init

down: ## Stop containers (keeps data volumes)
	$(COMPOSE) down

ps: ## Show container status
	$(COMPOSE) ps -a

logs: ## Follow logs for all services
	$(COMPOSE) logs -f

topics: ## List Kafka topics
	$(COMPOSE) exec kafka $(KAFKA_BIN)/kafka-topics.sh --bootstrap-server kafka:9092 --list

describe: ## Describe the events topic (partitions, leaders)
	$(COMPOSE) exec kafka $(KAFKA_BIN)/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic $(KAFKA_TOPIC)

consume: ## Tail the events topic from the beginning (Ctrl+C to stop)
	$(COMPOSE) exec kafka $(KAFKA_BIN)/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
		--topic $(KAFKA_TOPIC) --from-beginning --property print.key=true

psql: ## Open a psql shell in the Postgres container
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB)

reset: ## Stop containers AND delete all data volumes (destructive)
	$(COMPOSE) down -v

lint: ## Run ruff lint + format check
	uv run ruff check . && uv run ruff format --check .

