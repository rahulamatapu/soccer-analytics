"""Pure transformation functions for the Live Momentum Engine.

Transforms raw Kafka payloads into structured event streams, derives
deterministic match-clock event times, extracts momentum action flags,
and computes rolling window aggregations.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark.momentum.config import Settings
from spark.momentum.schema import ENVELOPE_SCHEMA

# Match-clock start offset (in seconds) for each match period.
# Keeps first-half stoppage time from bleeding into the second half.
PERIOD_OFFSETS_S: dict[int, int] = {
    1: 0,  # 1st half: starts at 0'
    2: 60 * 60,  # 2nd half: starts at 60' (15 min gap for stoppage time)
    3: 120 * 60,  # Extra time 1: starts at 120'
    4: 150 * 60,  # Extra time 2: starts at 150'
    5: 180 * 60,  # Penalties: starts at 180'
}


def parse_kafka_stream(raw_df: DataFrame) -> DataFrame:
    """Parse raw binary Kafka values into flattened event records.

    Filters out null or malformed JSON payloads.
    """
    parsed = raw_df.select(
        F.from_json(F.col("value").cast("string"), ENVELOPE_SCHEMA).alias("data")
    ).filter(F.col("data.match_id").isNotNull())

    return parsed.select(
        F.col("data.match_id").alias("match_id"),
        F.col("data.home_team").alias("home_team"),
        F.col("data.away_team").alias("away_team"),
        F.col("data.kickoff_ts").alias("kickoff_ts"),
        F.col("data.event.id").alias("event_id"),
        F.col("data.event.index").alias("event_index"),
        F.col("data.event.period").alias("period"),
        F.col("data.event.timestamp").alias("timestamp_str"),
        F.col("data.event.minute").alias("minute"),
        F.col("data.event.second").alias("second"),
        F.col("data.event.type.name").alias("event_type"),
        F.col("data.event.team.name").alias("team_name"),
        F.col("data.event.location").alias("location"),
        F.col("data.event.pass.end_location").alias("pass_end_location"),
        F.col("data.event.pass.outcome.name").alias("pass_outcome"),
        F.col("data.event.shot.statsbomb_xg").alias("shot_xg"),
        F.col("data.event.shot.outcome.name").alias("shot_outcome"),
        F.col("data.event.duel.type.name").alias("duel_type"),
    )


def add_event_time(df: DataFrame) -> DataFrame:
    """Derive continuous event_time on the match clock.

    event_time = kickoff_ts + period_offset + in_period_seconds.
    Using match-clock timestamps guarantees deterministic, idempotent
    window assignments upon stream replays.
    """
    hours = F.substring_index(F.col("timestamp_str"), ":", 1).cast("int")
    minutes = F.substring(F.col("timestamp_str"), 4, 2).cast("int")
    seconds = F.substring(F.col("timestamp_str"), 7, 6).cast("double")
    in_period_seconds = (hours * 3600) + (minutes * 60) + seconds

    period_offset = (
        F.when(F.col("period") == 1, PERIOD_OFFSETS_S[1])
        .when(F.col("period") == 2, PERIOD_OFFSETS_S[2])
        .when(F.col("period") == 3, PERIOD_OFFSETS_S[3])
        .when(F.col("period") == 4, PERIOD_OFFSETS_S[4])
        .when(F.col("period") == 5, PERIOD_OFFSETS_S[5])
        .otherwise(0)
    )

    total_offset = period_offset + in_period_seconds
    event_time_epoch = F.col("kickoff_ts").cast("long") + total_offset.cast("long")

    return df.withColumn("event_time", F.timestamp_seconds(event_time_epoch))


def extract_features(df: DataFrame) -> DataFrame:
    """Extract boolean action flags and metrics for momentum scoring."""
    is_home = F.col("team_name") == F.col("home_team")
    is_away = F.col("team_name") == F.col("away_team")

    # Pass logic: in StatsBomb, null pass_outcome indicates a successfully completed pass
    is_pass = F.col("event_type") == "Pass"
    is_completed_pass = is_pass & F.col("pass_outcome").isNull()

    # Final-third pass: completed pass ending at X >= 80 (pitch dimensions: 120 x 80)
    # StatsBomb coordinates always orient in the attacking direction
    pass_end_x = F.col("pass_end_location").getItem(0)
    is_final_third_pass = is_completed_pass & (pass_end_x >= 80.0)

    # Shots & Expected Goals (xG)
    is_shot = F.col("event_type") == "Shot"
    xg = F.coalesce(F.col("shot_xg"), F.lit(0.0))

    # Defensive tackles: Duels categorized specifically as Tackles
    is_tackle = (F.col("event_type") == "Duel") & (F.col("duel_type") == "Tackle")

    return (
        df.withColumn("is_home", is_home)
        .withColumn("is_away", is_away)
        .withColumn("is_completed_pass", is_completed_pass)
        .withColumn("is_final_third_pass", is_final_third_pass)
        .withColumn("is_shot", is_shot)
        .withColumn("xg", xg)
        .withColumn("is_tackle", is_tackle)
    )


def calculate_momentum_window(
    df: DataFrame,
    settings: Settings,
    apply_watermark: bool = True,
) -> DataFrame:
    """Compute 5-minute rolling window momentum aggregations sliding every 1 minute.

    Applies a watermark to prune state and drop late data beyond the threshold.
    """
    stream_df = df.withWatermark("event_time", settings.watermark) if apply_watermark else df

    window_spec = F.window(
        timeColumn="event_time",
        windowDuration=settings.window_duration,
        slideDuration=settings.window_slide,
    )

    is_home = F.col("is_home")
    is_away = F.col("is_away")

    aggregated = stream_df.groupBy(
        F.col("match_id"),
        F.col("home_team"),
        F.col("away_team"),
        window_spec,
    ).agg(
        # Passes
        F.sum(F.when(is_home & F.col("is_completed_pass"), 1).otherwise(0)).alias(
            "home_completed_passes"
        ),
        F.sum(F.when(is_away & F.col("is_completed_pass"), 1).otherwise(0)).alias(
            "away_completed_passes"
        ),
        # Final Third Passes
        F.sum(F.when(is_home & F.col("is_final_third_pass"), 1).otherwise(0)).alias(
            "home_final_third_passes"
        ),
        F.sum(F.when(is_away & F.col("is_final_third_pass"), 1).otherwise(0)).alias(
            "away_final_third_passes"
        ),
        # Shots
        F.sum(F.when(is_home & F.col("is_shot"), 1).otherwise(0)).alias("home_shots"),
        F.sum(F.when(is_away & F.col("is_shot"), 1).otherwise(0)).alias("away_shots"),
        # Expected Goals
        F.coalesce(F.sum(F.when(is_home & F.col("is_shot"), F.col("xg"))), F.lit(0.0)).alias(
            "home_total_xg"
        ),
        F.coalesce(F.sum(F.when(is_away & F.col("is_shot"), F.col("xg"))), F.lit(0.0)).alias(
            "away_total_xg"
        ),
        # Tackles
        F.sum(F.when(is_home & F.col("is_tackle"), 1).otherwise(0)).alias("home_tackles"),
        F.sum(F.when(is_away & F.col("is_tackle"), 1).otherwise(0)).alias("away_tackles"),
    )

    # Derived metrics
    total_passes = F.col("home_completed_passes") + F.col("away_completed_passes")
    home_possession_pct = F.when(
        total_passes > 0,
        F.round((F.col("home_completed_passes") / total_passes) * 100.0, 2),
    ).otherwise(50.0)
    away_possession_pct = F.when(
        total_passes > 0,
        F.round((F.col("away_completed_passes") / total_passes) * 100.0, 2),
    ).otherwise(50.0)

    home_avg_xg = F.when(
        F.col("home_shots") > 0,
        F.round(F.col("home_total_xg") / F.col("home_shots"), 4),
    ).otherwise(0.0)
    away_avg_xg = F.when(
        F.col("away_shots") > 0,
        F.round(F.col("away_total_xg") / F.col("away_shots"), 4),
    ).otherwise(0.0)

    w = settings.weights
    home_momentum = F.round(
        (F.lit(w.final_third) * F.col("home_final_third_passes"))
        + (F.lit(w.shot) * F.col("home_shots"))
        + (F.lit(w.xg) * F.col("home_total_xg"))
        + (F.lit(w.tackle) * F.col("home_tackles")),
        3,
    )
    away_momentum = F.round(
        (F.lit(w.final_third) * F.col("away_final_third_passes"))
        + (F.lit(w.shot) * F.col("away_shots"))
        + (F.lit(w.xg) * F.col("away_total_xg"))
        + (F.lit(w.tackle) * F.col("away_tackles")),
        3,
    )
    momentum_diff = F.round(home_momentum - away_momentum, 3)

    return (
        aggregated.withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("home_possession_pct", home_possession_pct)
        .withColumn("away_possession_pct", away_possession_pct)
        .withColumn("home_avg_xg", home_avg_xg)
        .withColumn("away_avg_xg", away_avg_xg)
        .withColumn("home_momentum", home_momentum)
        .withColumn("away_momentum", away_momentum)
        .withColumn("momentum_diff", momentum_diff)
        .drop("window")
    )
