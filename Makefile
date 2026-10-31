# Live Momentum Engine - developer shortcuts
# Usage: make <target>   (run `make help` for the list)

-include .env
KAFKA_TOPIC ?= live-match-events
POSTGRES_USER ?= momentum
POSTGRES_DB ?= momentum_dev

KAFKA_BIN := /opt/kafka/bin
COMPOSE := docker compose

.DEFAULT_GOAL := help
.PHONY: help up down ps logs topics describe consume psql reset lint test matches produce-local spark-build spark-test spark-run spark-logs query-latest query-momentum

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

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

test: ## Run unit tests
	uv run pytest

matches: ## List World Cup 2022 matches (ids for MATCH_ID)
	uv run python -m producer.producer --list-matches

produce-local: ## Replay a match into Kafka (ARGS="--speed 60 --max-events 500")
	uv run python -m producer.producer --bootstrap localhost:29092 $(ARGS)

spark-build: ## Build the Spark Docker image with pre-bundled connectors
	$(COMPOSE) build spark

spark-test: ## Run unit tests inside the Spark container (with Java & Spark)
	$(COMPOSE) run --rm spark-tests

spark-run: ## Run the Spark streaming container
	$(COMPOSE) up spark

spark-logs: ## Follow Spark container logs
	$(COMPOSE) logs -f spark

query-latest: ## Query the latest momentum score view in PostgreSQL
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "SELECT * FROM v_latest_momentum;"

query-momentum: ## Query the latest 10 momentum windows in PostgreSQL
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c "SELECT window_start, window_end, home_possession_pct, away_possession_pct, home_shots, away_shots, home_momentum, away_momentum, momentum_diff FROM match_momentum ORDER BY window_start DESC LIMIT 10;"

