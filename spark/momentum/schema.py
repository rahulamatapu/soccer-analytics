"""Strict PySpark schemas for parsing raw Kafka JSON payloads."""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Schema for the nested StatsBomb event body
EVENT_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("index", IntegerType(), nullable=True),
        StructField("period", IntegerType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),  # 'HH:MM:SS.fff'
        StructField("minute", IntegerType(), nullable=True),
        StructField("second", IntegerType(), nullable=True),
        StructField("type", StructType([StructField("name", StringType(), True)]), nullable=True),
        StructField("team", StructType([StructField("name", StringType(), True)]), nullable=True),
        StructField("location", ArrayType(DoubleType()), nullable=True),
        StructField(
            "pass",
            StructType(
                [
                    StructField("end_location", ArrayType(DoubleType()), nullable=True),
                    StructField(
                        "outcome",
                        StructType([StructField("name", StringType(), True)]),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
        StructField(
            "shot",
            StructType(
                [
                    StructField("statsbomb_xg", DoubleType(), nullable=True),
                    StructField(
                        "outcome",
                        StructType([StructField("name", StringType(), True)]),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
        StructField(
            "duel",
            StructType(
                [
                    StructField(
                        "type",
                        StructType([StructField("name", StringType(), True)]),
                        nullable=True,
                    ),
                ]
            ),
            nullable=True,
        ),
    ]
)

# Schema for the outer message envelope published by the producer
ENVELOPE_SCHEMA = StructType(
    [
        StructField("match_id", IntegerType(), nullable=False),
        StructField("home_team", StringType(), nullable=False),
        StructField("away_team", StringType(), nullable=False),
        StructField("kickoff_ts", TimestampType(), nullable=False),
        StructField("produced_at", StringType(), nullable=True),
        StructField("simulated_delay_s", DoubleType(), nullable=True),
        StructField("event", EVENT_SCHEMA, nullable=False),
    ]
)
