"""PostgreSQL JDBC and SQL upsert sink for PySpark Structured Streaming."""

from __future__ import annotations

import logging
from typing import Any

import psycopg2
from pyspark.sql import DataFrame

from spark.momentum.config import Settings

log = logging.getLogger("momentum_sink")

UPSERT_SQL = """
INSERT INTO match_momentum (
    match_id, window_start, window_end, home_team, away_team,
    home_completed_passes, away_completed_passes, home_possession_pct, away_possession_pct,
    home_shots, away_shots, home_total_xg, away_total_xg, home_avg_xg, away_avg_xg,
    home_final_third_passes, away_final_third_passes, home_tackles, away_tackles,
    home_momentum, away_momentum, momentum_diff, updated_at
)
SELECT
    match_id, window_start, window_end, home_team, away_team,
    home_completed_passes, away_completed_passes, home_possession_pct, away_possession_pct,
    home_shots, away_shots, home_total_xg, away_total_xg, home_avg_xg, away_avg_xg,
    home_final_third_passes, away_final_third_passes, home_tackles, away_tackles,
    home_momentum, away_momentum, momentum_diff, CURRENT_TIMESTAMP
FROM stg_match_momentum
ON CONFLICT (match_id, window_start) DO UPDATE SET
    window_end = EXCLUDED.window_end,
    home_completed_passes = EXCLUDED.home_completed_passes,
    away_completed_passes = EXCLUDED.away_completed_passes,
    home_possession_pct = EXCLUDED.home_possession_pct,
    away_possession_pct = EXCLUDED.away_possession_pct,
    home_shots = EXCLUDED.home_shots,
    away_shots = EXCLUDED.away_shots,
    home_total_xg = EXCLUDED.home_total_xg,
    away_total_xg = EXCLUDED.away_total_xg,
    home_avg_xg = EXCLUDED.home_avg_xg,
    away_avg_xg = EXCLUDED.away_avg_xg,
    home_final_third_passes = EXCLUDED.home_final_third_passes,
    away_final_third_passes = EXCLUDED.away_final_third_passes,
    home_tackles = EXCLUDED.home_tackles,
    away_tackles = EXCLUDED.away_tackles,
    home_momentum = EXCLUDED.home_momentum,
    away_momentum = EXCLUDED.away_momentum,
    momentum_diff = EXCLUDED.momentum_diff,
    updated_at = CURRENT_TIMESTAMP;
"""


class PostgresSink:
    """Manages foreachBatch sink writes to PostgreSQL with idempotent upserts."""

    def __init__(self, cfg: Settings) -> None:
        self.cfg = cfg
        self._jdbc_properties = {
            "user": cfg.postgres_user,
            "password": cfg.postgres_password,
            "driver": "org.postgresql.Driver",
            "truncate": "true",  # Truncates existing rows in staging without modifying table DDL
        }

    def _get_pg_connection(self) -> Any:
        return psycopg2.connect(
            host=self.cfg.postgres_host,
            port=self.cfg.postgres_port,
            dbname=self.cfg.postgres_db,
            user=self.cfg.postgres_user,
            password=self.cfg.postgres_password,
        )

    def write_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        """Process a single streaming micro-batch.

        1. Ingests updated window records into unlogged staging table via JDBC.
        2. Executes atomic ON CONFLICT DO UPDATE upsert into production table.
        """
        row_count = batch_df.count()
        if row_count == 0:
            log.info("Batch %d: empty batch, skipping database write", batch_id)
            return

        log.info(
            "Batch %d: staging %d records via JDBC to %s",
            batch_id,
            row_count,
            self.cfg.jdbc_url,
        )

        # 1. Bulk write batch to staging table using PySpark JDBC
        batch_df.write.jdbc(
            url=self.cfg.jdbc_url,
            table="stg_match_momentum",
            mode="overwrite",
            properties=self._jdbc_properties,
        )

        # 2. Execute atomic idempotent upsert transaction
        conn = self._get_pg_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(UPSERT_SQL)
            conn.commit()
            log.info(
                "Batch %d: successfully upserted %d records into %s.match_momentum",
                batch_id,
                row_count,
                self.cfg.postgres_db,
            )
        except Exception:
            conn.rollback()
            log.exception("Batch %d: failed to execute SQL upsert transaction", batch_id)
            raise
        finally:
            conn.close()
