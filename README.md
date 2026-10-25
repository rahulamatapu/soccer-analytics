# Live Momentum Engine ⚽

A real-time streaming pipeline that replays StatsBomb World Cup match events into **Kafka**,
calculates rolling momentum metrics with **PySpark Structured Streaming**, and upserts them
into **PostgreSQL**. Everything runs with **Docker Compose**.

> 🚧 Work in progress — built incrementally, phase by phase.

## Tech stack

| Layer | Tool |
|---|---|
| Data source | [statsbombpy](https://github.com/statsbomb/statsbombpy) (open data) |
| Message broker | Apache Kafka (KRaft mode) |
| Stream processing | Apache Spark 3.5 (PySpark Structured Streaming) |
| Sink | PostgreSQL 16 |
| Infra | Docker Compose |
| Tooling | uv, ruff, pytest, GitHub Actions |

## Local development setup

Prerequisites: [Docker Desktop](https://www.docker.com/products/docker-desktop/) and [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env
uv sync            # creates .venv with Python 3.11 + dev dependencies
```

## Infrastructure

```bash
make up        # start Kafka (KRaft) + Postgres, create the `live-match-events` topic
make ps        # container status
make describe  # topic partitions / leaders
make consume   # tail the topic from the beginning
make psql      # psql shell into Postgres
make down      # stop (keeps data)
make reset     # stop and DELETE all data volumes
```

| Service | From containers | From your Mac |
|---|---|---|
| Kafka | `kafka:9092` | `localhost:29092` |
| Postgres | `postgres:5432` | `localhost:5432` |

