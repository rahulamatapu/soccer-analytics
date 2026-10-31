import os
from unittest import mock

import pytest

from spark.momentum.config import MomentumWeights, Settings


def test_config_defaults():
    with mock.patch.dict(os.environ, {}, clear=True):
        cfg = Settings.from_env()
        assert cfg.app_env == "dev"
        assert cfg.kafka_bootstrap == "kafka:9092"
        assert cfg.kafka_topic == "live-match-events"
        assert cfg.postgres_host == "postgres"
        assert cfg.postgres_port == 5432
        assert cfg.postgres_db == "momentum_dev"
        assert cfg.checkpoint_dir == "/checkpoints/dev/momentum"
        assert cfg.jdbc_url == "jdbc:postgresql://postgres:5432/momentum_dev"
        assert cfg.weights == MomentumWeights(final_third=1.0, shot=3.0, xg=5.0, tackle=0.5)


def test_config_test_env_isolation():
    with mock.patch.dict(
        os.environ, {"APP_ENV": "test", "KAFKA_TOPIC": "live-match-events"}, clear=True
    ):
        cfg = Settings.from_env()
        assert cfg.app_env == "test"
        assert cfg.kafka_topic == "test.live-match-events"
        assert cfg.postgres_db == "momentum_test"
        assert cfg.checkpoint_dir == "/checkpoints/test/momentum"


def test_config_invalid_env_raises():
    with (
        mock.patch.dict(os.environ, {"APP_ENV": "production"}, clear=True),
        pytest.raises(ValueError, match="Invalid APP_ENV"),
    ):
        Settings.from_env()


def test_config_custom_weights():
    custom_env = {
        "W_FINAL_THIRD": "2.5",
        "W_SHOT": "4.0",
        "W_XG": "10.0",
        "W_TACKLE": "1.0",
    }
    with mock.patch.dict(os.environ, custom_env, clear=True):
        cfg = Settings.from_env()
        assert cfg.weights.final_third == 2.5
        assert cfg.weights.shot == 4.0
        assert cfg.weights.xg == 10.0
        assert cfg.weights.tackle == 1.0
