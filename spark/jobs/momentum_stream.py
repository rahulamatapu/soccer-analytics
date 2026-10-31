"""Spark Structured Streaming entrypoint for the Live Momentum Engine."""

from __future__ import annotations

import logging
import sys

from pyspark.sql import SparkSession

from spark.momentum.config import Settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("momentum_stream")


def create_spark_session(app_name: str = "LiveMomentumEngine") -> SparkSession:
    """Initialize SparkSession with local settings and bundled connectors."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


def main() -> None:
    cfg = Settings.from_env()
    log.info(
        "Starting Live Momentum Engine [env=%s, topic=%s, kafka=%s, postgres=%s]",
        cfg.app_env,
        cfg.kafka_topic,
        cfg.kafka_bootstrap,
        cfg.jdbc_url,
    )

    spark = create_spark_session()
    log.info(
        "SparkSession initialized successfully: version=%s, python=%s",
        spark.version,
        sys.version.split()[0],
    )
    log.info(
        "Momentum configuration: window=%s, slide=%s, watermark=%s",
        cfg.window_duration,
        cfg.window_slide,
        cfg.watermark,
    )


if __name__ == "__main__":
    main()
