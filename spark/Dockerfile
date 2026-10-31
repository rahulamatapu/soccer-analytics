# Containerized Spark 3.5 runtime with pre-bundled Kafka & Postgres connectors
FROM apache/spark:3.5.3-python3

USER 0

# Install uv for fast, deterministic Python 3.11 installation
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Install Python 3.11 into a globally accessible directory (/opt/python)
ENV UV_PYTHON_INSTALL_DIR=/opt/python
RUN uv python install 3.11 && \
    uv venv /opt/venv --python 3.11

ENV SPARK_HOME=/opt/spark
ENV PATH=/opt/venv/bin:/opt/spark/bin:/opt/spark/sbin:$PATH
ENV VIRTUAL_ENV=/opt/venv
ENV PYSPARK_PYTHON=/opt/venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python
ENV PYTHONPATH=/app:$PYTHONPATH

# Download connectors directly into Spark's JAR directory to eliminate runtime downloads:
# 1. Spark SQL Kafka 0-10 (v3.5.3)
# 2. Spark Token Provider Kafka 0-10 (v3.5.3)
# 3. Kafka Clients (v3.4.1)
# 4. Commons Pool 2 (v2.11.1)
# 5. PostgreSQL JDBC driver (v42.7.4)
RUN curl -fsSL -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSL -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -fsSL -o /opt/spark/jars/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \
    curl -fsSL -o /opt/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -fsSL -o /opt/spark/jars/postgresql-42.7.4.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar

# Install Python packages inside the Python 3.11 venv
RUN uv pip install --no-cache \
    psycopg2-binary==2.9.10 \
    confluent-kafka==2.8.2 \
    statsbombpy==1.14.0 \
    pytest>=8.0 \
    pytest-cov>=5.0

# Prepare application directory and checkpoint directory with spark permissions
WORKDIR /app
RUN mkdir -p /checkpoints /app/.pytest_cache && \
    chmod -R 755 /opt/python && \
    chown -R 185:185 /app /checkpoints /opt/venv

COPY --chown=185:185 producer/ /app/producer/
COPY --chown=185:185 spark/ /app/spark/
COPY --chown=185:185 tests/ /app/tests/

USER 185
