"""Pytest configuration and shared SparkSession fixtures."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Shared local SparkSession for unit testing transformations."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("MomentumUnitTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
