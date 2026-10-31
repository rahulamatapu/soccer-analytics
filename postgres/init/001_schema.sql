-- PostgreSQL Schema for Live Momentum Engine
-- Executed on container first-boot via /docker-entrypoint-initdb.d

-- Create test database for test suite isolation if not exists
SELECT 'CREATE DATABASE momentum_test'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'momentum_test')\gexec

-- Connect to primary development database
\c momentum_dev;

-- -----------------------------------------------------------------------------
-- Production match momentum table
-- Primary Key: (match_id, window_start) guarantees deterministic idempotency
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS match_momentum (
    match_id INT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    home_completed_passes INT NOT NULL,
    away_completed_passes INT NOT NULL,
    home_possession_pct NUMERIC(5, 2) NOT NULL,
    away_possession_pct NUMERIC(5, 2) NOT NULL,
    home_shots INT NOT NULL,
    away_shots INT NOT NULL,
    home_total_xg NUMERIC(6, 4) NOT NULL,
    away_total_xg NUMERIC(6, 4) NOT NULL,
    home_avg_xg NUMERIC(6, 4) NOT NULL,
    away_avg_xg NUMERIC(6, 4) NOT NULL,
    home_final_third_passes INT NOT NULL,
    away_final_third_passes INT NOT NULL,
    home_tackles INT NOT NULL,
    away_tackles INT NOT NULL,
    home_momentum NUMERIC(8, 3) NOT NULL,
    away_momentum NUMERIC(8, 3) NOT NULL,
    momentum_diff NUMERIC(8, 3) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (match_id, window_start)
);

CREATE INDEX IF NOT EXISTS idx_momentum_match_window 
ON match_momentum (match_id, window_start DESC);

-- -----------------------------------------------------------------------------
-- Unlogged staging table for high-throughput micro-batch bulk ingestion
-- Truncated and overwritten per Spark micro-batch, then upserted to target
-- -----------------------------------------------------------------------------
CREATE UNLOGGED TABLE IF NOT EXISTS stg_match_momentum (
    LIKE match_momentum INCLUDING DEFAULTS
);

-- -----------------------------------------------------------------------------
-- Analytical View: Latest calculated rolling momentum window per match
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_latest_momentum AS
SELECT DISTINCT ON (match_id)
    match_id,
    window_start,
    window_end,
    home_team,
    away_team,
    home_possession_pct,
    away_possession_pct,
    home_shots,
    away_shots,
    home_total_xg,
    away_total_xg,
    home_momentum,
    away_momentum,
    momentum_diff,
    updated_at
FROM match_momentum
ORDER BY match_id, window_start DESC;

-- Repeat table definitions in momentum_test database for test isolation
\c momentum_test;

CREATE TABLE IF NOT EXISTS match_momentum (
    match_id INT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    home_team VARCHAR(100) NOT NULL,
    away_team VARCHAR(100) NOT NULL,
    home_completed_passes INT NOT NULL,
    away_completed_passes INT NOT NULL,
    home_possession_pct NUMERIC(5, 2) NOT NULL,
    away_possession_pct NUMERIC(5, 2) NOT NULL,
    home_shots INT NOT NULL,
    away_shots INT NOT NULL,
    home_total_xg NUMERIC(6, 4) NOT NULL,
    away_total_xg NUMERIC(6, 4) NOT NULL,
    home_avg_xg NUMERIC(6, 4) NOT NULL,
    away_avg_xg NUMERIC(6, 4) NOT NULL,
    home_final_third_passes INT NOT NULL,
    away_final_third_passes INT NOT NULL,
    home_tackles INT NOT NULL,
    away_tackles INT NOT NULL,
    home_momentum NUMERIC(8, 3) NOT NULL,
    away_momentum NUMERIC(8, 3) NOT NULL,
    momentum_diff NUMERIC(8, 3) NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (match_id, window_start)
);

CREATE UNLOGGED TABLE IF NOT EXISTS stg_match_momentum (
    LIKE match_momentum INCLUDING DEFAULTS
);
