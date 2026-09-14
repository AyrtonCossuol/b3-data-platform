import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("b3-unit-tests")
        .config(
            "spark.ui.enabled",
            "false",
        )
        .config(
            "spark.driver.host",
            "127.0.0.1",
        )
        .config(
            "spark.driver.bindAddress",
            "127.0.0.1",
        )
        .config(
            "spark.sql.shuffle.partitions",
            "1",
        )
        .config(
            "spark.default.parallelism",
            "1",
        )
        .config(
            "spark.sql.session.timeZone",
            "UTC",
        )
        .config(
            "spark.python.worker.reuse",
            "false",
        )
        .config(
            "spark.python.worker.faulthandler.enabled",
            "true",
        )
        .getOrCreate()
    )

    session.sparkContext.setLogLevel(
        "ERROR"
    )

    yield session

    session.stop()