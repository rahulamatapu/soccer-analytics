"""Spark Structured Streaming entrypoint for the Live Momentum Engine."""

from __future__ import annotations

import logging
import signal
import sys
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from spark.momentum.config import Settings
from spark.momentum.transforms import (
    add_event_time,
    calculate_momentum_window,
    extract_features,
    parse_kafka_stream,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("momentum_stream")


def create_spark_session(app_name: str = "LiveMomentumEngine") -> SparkSession:
    """Initialize SparkSession with local execution settings."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )


def build_pipeline(
    raw_kafka_df: DataFrame, cfg: Settings, apply_watermark: bool = True
) -> DataFrame:
    """Apply transformation pipeline to raw Kafka DataFrame."""
    parsed = parse_kafka_stream(raw_kafka_df)
    timed = add_event_time(parsed)
    featured = extract_features(timed)
    return calculate_momentum_window(featured, cfg, apply_watermark=apply_watermark)


def main() -> None:
    cfg = Settings.from_env()
    log.info(
        "Starting Live Momentum Engine [env=%s, topic=%s, kafka=%s]",
        cfg.app_env,
        cfg.kafka_topic,
        cfg.kafka_bootstrap,
    )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    log.info(
        "SparkSession initialized (version=%s, python=%s)",
        spark.version,
        sys.version.split()[0],
    )
    log.info(
        "Pipeline configuration: window=%s, slide=%s, watermark=%s, trigger=%s",
        cfg.window_duration,
        cfg.window_slide,
        cfg.watermark,
        cfg.trigger_interval,
    )

    # 1. Read Kafka Stream
    raw_kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("subscribe", cfg.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Apply Transformations
    momentum_df = build_pipeline(raw_kafka_df, cfg)

    # 3. Stream to Console Sink (Phase 2 milestone)
    query = (
        momentum_df.writeStream.format("console")
        .outputMode("update")
        .trigger(processingTime=cfg.trigger_interval)
        .option("truncate", "false")
        .option("numRows", "20")
        .start()
    )

    log.info("Streaming query started [id=%s]. Waiting for micro-batches...", query.id)

    def handle_shutdown(signum: int, frame: Any) -> None:
        log.info("Shutdown signal received (%s). Stopping streaming query...", signum)
        query.stop()
        spark.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    query.awaitTermination()


if __name__ == "__main__":
    main()
