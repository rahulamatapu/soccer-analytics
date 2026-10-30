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

## Producer: simulated live feed

[`producer/producer.py`](producer/producer.py) replays a StatsBomb match into Kafka as if it
were live (default: 2022 World Cup Final, Argentina v France, 4,407 events).

```bash
make matches                                  # list match ids
make produce-local                            # replay at 30x speed (~3-4 min)
make produce-local ARGS="--speed 0"           # no sleeping, dump the whole match
make produce-local ARGS="--max-events 500 --late-prob 0"
make consume                                  # watch messages arrive
```

How it works:
- **Envelope:** each raw, nested StatsBomb event is wrapped with `match_id`, teams and
  `kickoff_ts`, then serialized as JSON. The Kafka key is `match_id`, so one match's events
  stay in order within a single partition.
- **Match clock:** StatsBomb timestamps restart every period, so each period gets a fixed
  offset (1st half 0', 2nd half 60', ET 120'/150', penalties 180'). This keeps 1st-half
  stoppage time from overlapping the 2nd half.
- **Live pacing:** sleeps `Δmatch-time / SPEED` between events. Single sleeps are capped
  (`--max-sleep`) so half-time doesn't stall the replay.
- **Out-of-order events:** `--late-prob` (default 3%) holds events back by up to
  `--late-max-seconds` of match time, so Spark's watermarking gets exercised. Held events are
  always flushed before the next period starts.
- **Delivery guarantees:** `acks=all` + `enable.idempotence=true`, so producer retries don't
  create duplicates.

```bash
make test      # unit tests (no Kafka needed)
```

