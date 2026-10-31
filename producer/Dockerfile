# Lightweight Python 3.11 container for the live event producer
FROM python:3.11-slim

# Install uv for fast, deterministic package installation
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Install producer dependencies
RUN uv pip install --system --no-cache \
    confluent-kafka==2.8.2 \
    statsbombpy==1.14.0

COPY producer/ /app/producer/

ENV PYTHONPATH=/app
ENV PRODUCER_BOOTSTRAP=kafka:9092

ENTRYPOINT ["python", "-m", "producer.producer"]
CMD ["--bootstrap", "kafka:9092", "--speed", "30"]

