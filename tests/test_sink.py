"""Unit tests for PostgreSQL sink and idempotent upsert logic."""

from unittest import mock

import pytest

from spark.momentum.config import Settings
from spark.momentum.sink import UPSERT_SQL, PostgresSink


def test_sink_empty_batch_skipped():
    cfg = Settings.from_env()
    sink = PostgresSink(cfg)

    mock_df = mock.MagicMock()
    mock_df.count.return_value = 0

    with mock.patch.object(sink, "_get_pg_connection") as mock_conn_fn:
        sink.write_batch(mock_df, batch_id=1)
        mock_df.write.jdbc.assert_not_called()
        mock_conn_fn.assert_not_called()


def test_sink_writes_to_staging_and_executes_upsert():
    cfg = Settings.from_env()
    sink = PostgresSink(cfg)

    mock_df = mock.MagicMock()
    mock_df.count.return_value = 10

    mock_conn = mock.MagicMock()
    mock_cur = mock.MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    with mock.patch.object(sink, "_get_pg_connection", return_value=mock_conn):
        sink.write_batch(mock_df, batch_id=5)

        # 1. JDBC bulk write to staging with truncate=true
        mock_df.write.jdbc.assert_called_once_with(
            url=cfg.jdbc_url,
            table="stg_match_momentum",
            mode="overwrite",
            properties=sink._jdbc_properties,
        )
        assert sink._jdbc_properties["truncate"] == "true"

        # 2. Atomic SQL upsert execution and commit
        mock_cur.execute.assert_called_once_with(UPSERT_SQL)
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()


def test_sink_rolls_back_on_upsert_failure():
    cfg = Settings.from_env()
    sink = PostgresSink(cfg)

    mock_df = mock.MagicMock()
    mock_df.count.return_value = 5

    mock_conn = mock.MagicMock()
    mock_cur = mock.MagicMock()
    mock_cur.execute.side_effect = RuntimeError("Database connection lost")
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur

    with mock.patch.object(sink, "_get_pg_connection", return_value=mock_conn):
        with pytest.raises(RuntimeError, match="Database connection lost"):
            sink.write_batch(mock_df, batch_id=9)

        mock_conn.rollback.assert_called_once()
        mock_conn.close.assert_called_once()
