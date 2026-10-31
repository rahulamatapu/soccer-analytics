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

| Service | From containers | Local |
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

## Stream processing: PySpark environment

The Spark streaming environment is containerized via `apache/spark:3.5.3-python3` using a standalone **Python 3.11** virtual environment and pre-bundled JAR connectors:
- `spark-sql-kafka-0-10_2.12:3.5.3` (Kafka Structured Streaming source)
- `postgresql:42.7.4` (PostgreSQL JDBC sink)

```bash
make spark-build  # build the Spark container image with pre-bundled connectors
make spark-test   # run the test suite inside the container (with Java & Spark runtime)
make spark-run    # run the Spark job entrypoint (streams to PostgreSQL with checkpointing)
make spark-logs   # follow Spark streaming logs
```

### Running the End-to-End Pipeline

Open two terminal windows:

**Terminal 1 (Start infra & streaming processor):**
```bash
make up                 # starts Kafka and Postgres
make spark-run          # starts PySpark Structured Streaming sinking to Postgres
```

**Terminal 2 (Replay events):**
```bash
make produce-local      # replays live match events at 30x speed (~3-4 min)
# or for an instant replay of 500 events:
make produce-local ARGS="--speed 0 --max-events 500"
```

### Querying Real-Time Momentum in PostgreSQL

Query live rolling metrics stored in the relational database:

```bash
make query-latest    # inspect the most recent momentum window from v_latest_momentum
make query-momentum  # inspect the 10 most recent rolling momentum windows
make psql            # interactive psql shell
```

Example query in PostgreSQL:
```sql
SELECT 
    window_start, 
    home_possession_pct, 
    away_possession_pct, 
    home_shots, 
    away_shots, 
    home_momentum, 
    away_momentum, 
    momentum_diff 
FROM match_momentum 
ORDER BY window_start DESC 
LIMIT 5;
```

### Idempotency & Fault Tolerance

1. **Idempotency**: Replaying the match event stream will never duplicate rows. The table uses `(match_id, window_start)` as its natural primary key. Micro-batches bulk-insert into `stg_match_momentum` and execute an atomic `INSERT ... ON CONFLICT (match_id, window_start) DO UPDATE`.
2. **Fault Tolerance**: Spark offsets and window states are saved to the persistent volume `spark-checkpoints:/checkpoints/dev/momentum`. If the Spark container stops or crashes, restarting it resumes exactly from the last committed micro-batch.




