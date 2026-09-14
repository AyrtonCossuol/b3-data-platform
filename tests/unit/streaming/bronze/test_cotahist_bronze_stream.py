import json
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import streaming.bronze.cotahist_bronze_stream as bronze_module

from streaming.bronze.cotahist_bronze_stream import (
    EVENT_SCHEMA,
    build_bronze_dataframe,
    create_spark_session,
    get_required_env,
    read_kafka_stream,
    write_bronze_stream,
)


def create_kafka_dataframe(
    spark,
    *,
    key: str,
    value: str,
    topic: str = "b3.cotahist.raw.v1",
    partition: int = 0,
    offset: int = 0,
    timestamp: datetime = datetime(
        2026,
        9,
        14,
        18,
        0,
        0,
    ),
):
    escaped_key = key.replace(
        "'",
        "''",
    )

    escaped_value = value.replace(
        "'",
        "''",
    )

    escaped_topic = topic.replace(
        "'",
        "''",
    )

    timestamp_value = timestamp.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    return spark.sql(
        f"""
        SELECT
            encode(
                '{escaped_key}',
                'UTF-8'
            ) AS key,

            encode(
                '{escaped_value}',
                'UTF-8'
            ) AS value,

            '{escaped_topic}' AS topic,

            CAST(
                {partition}
                AS INT
            ) AS partition,

            CAST(
                {offset}
                AS BIGINT
            ) AS offset,

            TIMESTAMP '{timestamp_value}'
                AS timestamp
        """
    )


def test_event_schema_should_match_producer_contract():
    assert EVENT_SCHEMA.fieldNames() == [
        "schema_version",
        "event_id",
        "source",
        "dataset",
        "file_name",
        "file_date",
        "line_number",
        "record_type",
        "raw_record",
        "ingested_at",
    ]


def test_event_schema_should_have_expected_types():
    fields = {
        field.name: field.dataType.simpleString()
        for field in EVENT_SCHEMA.fields
    }

    assert fields == {
        "schema_version": "int",
        "event_id": "string",
        "source": "string",
        "dataset": "string",
        "file_name": "string",
        "file_date": "string",
        "line_number": "bigint",
        "record_type": "string",
        "raw_record": "string",
        "ingested_at": "string",
    }


def test_get_required_env_should_return_value(
    monkeypatch,
):
    monkeypatch.setenv(
        "TEST_ENV_VARIABLE",
        "expected-value",
    )

    result = get_required_env(
        "TEST_ENV_VARIABLE"
    )

    assert result == "expected-value"


def test_get_required_env_should_raise_when_missing(
    monkeypatch,
):
    monkeypatch.delenv(
        "TEST_ENV_VARIABLE",
        raising=False,
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "Required environment variable "
            "is missing: TEST_ENV_VARIABLE"
        ),
    ):
        get_required_env(
            "TEST_ENV_VARIABLE"
        )


def test_should_build_valid_bronze_record(
    spark,
):
    raw_record = (
        "012026091102PNVL3"
        "       "
    )

    event = {
        "schema_version": 1,
        "event_id": (
            "COTAHIST_D11092026.TXT:2"
        ),
        "source": "B3",
        "dataset": "COTAHIST",
        "file_name": (
            "COTAHIST_D11092026.TXT"
        ),
        "file_date": "2026-09-11",
        "line_number": 2,
        "record_type": "01",
        "raw_record": raw_record,
        "ingested_at": (
            "2026-09-14T18:00:00Z"
        ),
    }

    kafka_value = json.dumps(
        event,
        separators=(",", ":"),
    )

    kafka_df = create_kafka_dataframe(
        spark,
        key="COTAHIST_D11092026.TXT",
        value=kafka_value,
        partition=1,
        offset=42,
        timestamp=datetime(
            2026,
            9,
            14,
            18,
            30,
            0,
        ),
    )

    result = (
        build_bronze_dataframe(
            kafka_df
        )
        .collect()[0]
    )

    assert result.kafka_key == (
        "COTAHIST_D11092026.TXT"
    )

    assert result.kafka_value == (
        kafka_value
    )

    assert result.kafka_topic == (
        "b3.cotahist.raw.v1"
    )

    assert result.kafka_partition == 1

    assert result.kafka_offset == 42

    assert result.schema_version == 1

    assert result.event_id == (
        "COTAHIST_D11092026.TXT:2"
    )

    assert result.source == "B3"

    assert result.dataset == "COTAHIST"

    assert result.file_name == (
        "COTAHIST_D11092026.TXT"
    )

    assert result.file_date == (
        "2026-09-11"
    )

    assert result.line_number == 2

    assert result.record_type == "01"

    assert result.raw_record == (
        raw_record
    )

    assert result.ingested_at == (
        "2026-09-14T18:00:00Z"
    )

    assert result.parse_status == "OK"

    assert result.ingestion_date == date(
        2026,
        9,
        14,
    )


def test_should_preserve_fixed_width_spaces(
    spark,
):
    raw_record = (
        "01DIMED              "
    )

    event = {
        "schema_version": 1,
        "event_id": "file.txt:1",
        "source": "B3",
        "dataset": "COTAHIST",
        "file_name": "file.txt",
        "file_date": "2026-09-11",
        "line_number": 1,
        "record_type": "01",
        "raw_record": raw_record,
        "ingested_at": (
            "2026-09-14T18:00:00Z"
        ),
    }

    kafka_df = create_kafka_dataframe(
        spark,
        key="file.txt",
        value=json.dumps(event),
        partition=0,
        offset=10,
    )

    result = (
        build_bronze_dataframe(
            kafka_df
        )
        .collect()[0]
    )

    assert result.raw_record == raw_record

    assert result.raw_record.endswith(
        "              "
    )


@pytest.mark.parametrize(
    "record_type",
    [
        "00",
        "01",
        "99",
    ],
)
def test_should_preserve_record_type(
    spark,
    record_type,
):
    event = {
        "schema_version": 1,
        "event_id": "file.txt:1",
        "source": "B3",
        "dataset": "COTAHIST",
        "file_name": "file.txt",
        "file_date": "2026-09-11",
        "line_number": 1,
        "record_type": record_type,
        "raw_record": (
            f"{record_type}TEST"
        ),
        "ingested_at": (
            "2026-09-14T18:00:00Z"
        ),
    }

    kafka_df = create_kafka_dataframe(
        spark,
        key="file.txt",
        value=json.dumps(event),
        partition=0,
        offset=1,
    )

    result = (
        build_bronze_dataframe(
            kafka_df
        )
        .collect()[0]
    )

    assert (
        result.record_type
        == record_type
    )


def test_should_mark_invalid_json(
    spark,
):
    invalid_json = (
        '{"schema_version":'
    )

    kafka_df = create_kafka_dataframe(
        spark,
        key="file.txt",
        value=invalid_json,
        partition=2,
        offset=100,
    )

    result = (
        build_bronze_dataframe(
            kafka_df
        )
        .collect()[0]
    )

    assert result.kafka_value == (
        invalid_json
    )

    assert result.parse_status == (
        "INVALID_JSON"
    )

    assert result.event_id is None
    assert result.raw_record is None

    assert result.kafka_partition == 2
    assert result.kafka_offset == 100


class FakeReadStream:
    def __init__(self):
        self.format_name = None
        self.options = {}
        self.load_called = False

    def format(
        self,
        format_name,
    ):
        self.format_name = format_name
        return self

    def option(
        self,
        key,
        value,
    ):
        self.options[key] = value
        return self

    def load(self):
        self.load_called = True
        return "kafka-dataframe"


def test_read_kafka_stream_should_use_expected_options(
    monkeypatch,
):
    monkeypatch.setenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:29092",
    )

    monkeypatch.setenv(
        "KAFKA_COTAHIST_TOPIC",
        "b3.cotahist.raw.v1",
    )

    read_stream = FakeReadStream()

    fake_spark = SimpleNamespace(
        readStream=read_stream,
    )

    result = read_kafka_stream(
        fake_spark
    )

    assert result == "kafka-dataframe"

    assert (
        read_stream.format_name
        == "kafka"
    )

    assert read_stream.options[
        "kafka.bootstrap.servers"
    ] == "kafka:29092"

    assert read_stream.options[
        "subscribe"
    ] == "b3.cotahist.raw.v1"

    assert read_stream.options[
        "startingOffsets"
    ] == "earliest"

    assert read_stream.options[
        "failOnDataLoss"
    ] == "true"

    assert (
        read_stream.load_called
        is True
    )


class FakeWriteStream:
    def __init__(self):
        self.query_name = None
        self.format_name = None
        self.output_mode = None
        self.options = {}
        self.partition_columns = None
        self.trigger_options = None

        self.query = object()

    def queryName(
        self,
        name,
    ):
        self.query_name = name
        return self

    def format(
        self,
        name,
    ):
        self.format_name = name
        return self

    def outputMode(
        self,
        mode,
    ):
        self.output_mode = mode
        return self

    def option(
        self,
        key,
        value,
    ):
        self.options[key] = value
        return self

    def partitionBy(
        self,
        *columns,
    ):
        self.partition_columns = columns
        return self

    def trigger(
        self,
        **kwargs,
    ):
        self.trigger_options = kwargs
        return self

    def start(self):
        return self.query


def test_write_bronze_stream_should_use_expected_configuration(
    monkeypatch,
):
    monkeypatch.setenv(
        "BRONZE_PATH",
        "s3a://b3-bronze/b3/cotahist",
    )

    monkeypatch.setenv(
        "CHECKPOINT_PATH",
        (
            "s3a://b3-checkpoints/"
            "b3/cotahist"
        ),
    )

    monkeypatch.setenv(
        "STREAM_TRIGGER_INTERVAL",
        "30 seconds",
    )

    writer = FakeWriteStream()

    bronze_df = SimpleNamespace(
        writeStream=writer,
    )

    query = write_bronze_stream(
        bronze_df
    )

    assert query is writer.query

    assert writer.query_name == (
        "b3-cotahist-bronze"
    )

    assert writer.format_name == (
        "parquet"
    )

    assert writer.output_mode == (
        "append"
    )

    assert writer.options[
        "path"
    ] == (
        "s3a://b3-bronze/b3/cotahist"
    )

    assert writer.options[
        "checkpointLocation"
    ] == (
        "s3a://b3-checkpoints/"
        "b3/cotahist"
    )

    assert writer.partition_columns == (
        "ingestion_date",
    )

    assert writer.trigger_options == {
        "processingTime": "30 seconds",
    }


class FakeSparkContext:
    def __init__(self):
        self.log_level = None

    def setLogLevel(
        self,
        level,
    ):
        self.log_level = level


class FakeSpark:
    def __init__(self):
        self.sparkContext = (
            FakeSparkContext()
        )


class FakeSparkBuilder:
    def __init__(
        self,
        spark,
    ):
        self.spark = spark
        self.app_name = None
        self.configs = {}

    def appName(
        self,
        name,
    ):
        self.app_name = name
        return self

    def config(
        self,
        key,
        value,
    ):
        self.configs[key] = value
        return self

    def getOrCreate(self):
        return self.spark


def test_create_spark_session_should_configure_minio(
    monkeypatch,
):
    monkeypatch.setenv(
        "MINIO_ENDPOINT",
        "http://minio:9000",
    )

    monkeypatch.setenv(
        "MINIO_REGION",
        "us-east-1",
    )

    monkeypatch.setenv(
        "MINIO_ACCESS_KEY",
        "spark-b3",
    )

    monkeypatch.setenv(
        "MINIO_SECRET_KEY",
        "test-secret",
    )

    fake_spark = FakeSpark()

    builder = FakeSparkBuilder(
        fake_spark
    )

    fake_spark_session = SimpleNamespace(
        builder=builder,
    )

    monkeypatch.setattr(
        bronze_module,
        "SparkSession",
        fake_spark_session,
    )

    result = create_spark_session()

    assert result is fake_spark

    assert builder.app_name == (
        "b3-cotahist-bronze"
    )

    assert builder.configs[
        "spark.hadoop.fs.s3a.impl"
    ] == (
        "org.apache.hadoop.fs.s3a."
        "S3AFileSystem"
    )

    assert builder.configs[
        "spark.hadoop.fs.s3a.endpoint"
    ] == "http://minio:9000"

    assert builder.configs[
        (
            "spark.hadoop.fs.s3a."
            "endpoint.region"
        )
    ] == "us-east-1"

    assert builder.configs[
        (
            "spark.hadoop.fs.s3a."
            "path.style.access"
        )
    ] == "true"

    assert builder.configs[
        (
            "spark.hadoop.fs.s3a."
            "connection.ssl.enabled"
        )
    ] == "false"

    assert builder.configs[
        (
            "spark.hadoop.fs.s3a."
            "access.key"
        )
    ] == "spark-b3"

    assert builder.configs[
        (
            "spark.hadoop.fs.s3a."
            "secret.key"
        )
    ] == "test-secret"

    assert (
        fake_spark
        .sparkContext
        .log_level
        == "WARN"
    )


def test_main_should_start_pipeline_and_wait(
    monkeypatch,
):
    fake_spark = MagicMock()
    kafka_df = MagicMock()
    bronze_df = MagicMock()
    query = MagicMock()

    create_spark_session_mock = MagicMock(
        return_value=fake_spark
    )

    read_kafka_stream_mock = MagicMock(
        return_value=kafka_df
    )

    build_bronze_dataframe_mock = MagicMock(
        return_value=bronze_df
    )

    write_bronze_stream_mock = MagicMock(
        return_value=query
    )

    monkeypatch.setattr(
        bronze_module,
        "create_spark_session",
        create_spark_session_mock,
    )

    monkeypatch.setattr(
        bronze_module,
        "read_kafka_stream",
        read_kafka_stream_mock,
    )

    monkeypatch.setattr(
        bronze_module,
        "build_bronze_dataframe",
        build_bronze_dataframe_mock,
    )

    monkeypatch.setattr(
        bronze_module,
        "write_bronze_stream",
        write_bronze_stream_mock,
    )

    bronze_module.main()

    create_spark_session_mock.assert_called_once_with()

    read_kafka_stream_mock.assert_called_once_with(
        fake_spark
    )

    build_bronze_dataframe_mock.assert_called_once_with(
        kafka_df
    )

    write_bronze_stream_mock.assert_called_once_with(
        bronze_df
    )

    query.awaitTermination.assert_called_once_with()