"""Typed configuration for the Spark Structured Streaming pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class MomentumWeights:
    final_third: float = 1.0
    shot: float = 3.0
    xg: float = 5.0
    tackle: float = 0.5


@dataclass(frozen=True)
class Settings:
    app_env: str
    kafka_bootstrap: str
    kafka_topic: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    checkpoint_dir: str
    window_duration: str
    window_slide: str
    watermark: str
    trigger_interval: str
    weights: MomentumWeights

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ.get("APP_ENV", "dev").strip().lower()
        if env not in {"dev", "test", "demo"}:
            raise ValueError(f"Invalid APP_ENV='{env}'. Expected one of: dev, test, demo")

        topic = os.environ.get("KAFKA_TOPIC", "live-match-events")
        checkpoint_root = os.environ.get("CHECKPOINT_ROOT", "/checkpoints").rstrip("/")

        return cls(
            app_env=env,
            kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
            kafka_topic=f"test.{topic}"
            if env == "test" and not topic.startswith("test.")
            else topic,
            postgres_host=os.environ.get("POSTGRES_HOST", "postgres"),
            postgres_port=int(os.environ.get("POSTGRES_PORT", "5432")),
            postgres_db=os.environ.get("POSTGRES_DB", f"momentum_{env}"),
            postgres_user=os.environ.get("POSTGRES_USER", "momentum"),
            postgres_password=os.environ.get("POSTGRES_PASSWORD", "change-me"),
            checkpoint_dir=f"{checkpoint_root}/{env}/momentum",
            window_duration=os.environ.get("WINDOW_DURATION", "5 minutes"),
            window_slide=os.environ.get("WINDOW_SLIDE", "1 minute"),
            watermark=os.environ.get("WATERMARK", "2 minutes"),
            trigger_interval=os.environ.get("TRIGGER_INTERVAL", "10 seconds"),
            weights=MomentumWeights(
                final_third=float(os.environ.get("W_FINAL_THIRD", "1.0")),
                shot=float(os.environ.get("W_SHOT", "3.0")),
                xg=float(os.environ.get("W_XG", "5.0")),
                tackle=float(os.environ.get("W_TACKLE", "0.5")),
            ),
        )
