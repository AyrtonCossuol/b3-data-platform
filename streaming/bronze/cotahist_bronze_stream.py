import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    lit,
    to_date,
    when,
)
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


EVENT_SCHEMA = StructType(
    [
        StructField(
            "schema_version",
            IntegerType(),
            True,
        ),
        StructField(
            "event_id",
            StringType(),
            True,
        ),
        StructField(
            "source",
            StringType(),
            True,
        ),
        StructField(
            "dataset",
            StringType(),
            True,
        ),
        StructField(
            "file_name",
            StringType(),
            True,
        ),
        StructField(
            "file_date",
            StringType(),
            True,
        ),
        StructField(
            "line_number",
            LongType(),
            True,
        ),
        StructField(
            "record_type",
            StringType(),
            True,
        ),
        StructField(
            "raw_record",
            StringType(),
            True,
        ),
        StructField(
            "ingested_at",
            StringType(),
            True,
        ),
    ]
)


def get_required_env(
    name: str,
) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Required environment variable is missing: {name}"
        )

    return value


def create_spark_session() -> SparkSession:
    minio_endpoint = get_required_env(
        "MINIO_ENDPOINT"
    )

    minio_region = get_required_env(
        "MINIO_REGION"
    )

    minio_access_key = get_required_env(
        "MINIO_ACCESS_KEY"
    )

    minio_secret_key = get_required_env(
        "MINIO_SECRET_KEY"
    )

    spark = (
        SparkSession.builder
        .appName("b3-cotahist-bronze")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            minio_endpoint,
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            minio_region,
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            minio_access_key,
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            minio_secret_key,
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            (
                "org.apache.hadoop.fs.s3a."
                "SimpleAWSCredentialsProvider"
            ),
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


def read_kafka_stream(
    spark: SparkSession,
) -> DataFrame:
    bootstrap_servers = get_required_env(
        "KAFKA_BOOTSTRAP_SERVERS"
    )

    topic = get_required_env(
        "KAFKA_COTAHIST_TOPIC"
    )

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            bootstrap_servers,
        )
        .option(
            "subscribe",
            topic,
        )
        .option(
            "startingOffsets",
            "earliest",
        )
        .option(
            "failOnDataLoss",
            "true",
        )
        .load()
    )


def build_bronze_dataframe(
    kafka_df: DataFrame,
) -> DataFrame:
    kafka_records = kafka_df.select(
        col("key")
        .cast("string")
        .alias("kafka_key"),

        col("value")
        .cast("string")
        .alias("kafka_value"),

        col("topic")
        .alias("kafka_topic"),

        col("partition")
        .alias("kafka_partition"),

        col("offset")
        .alias("kafka_offset"),

        col("timestamp")
        .alias("kafka_timestamp"),
    )

    parsed = kafka_records.withColumn(
        "payload",
        from_json(
            col("kafka_value"),
            EVENT_SCHEMA,
        ),
    )

    bronze = parsed.select(
        "kafka_key",
        "kafka_value",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",

        col("payload.schema_version")
        .alias("schema_version"),

        col("payload.event_id")
        .alias("event_id"),

        col("payload.source")
        .alias("source"),

        col("payload.dataset")
        .alias("dataset"),

        col("payload.file_name")
        .alias("file_name"),

        col("payload.file_date")
        .alias("file_date"),

        col("payload.line_number")
        .alias("line_number"),

        col("payload.record_type")
        .alias("record_type"),

        col("payload.raw_record")
        .alias("raw_record"),

        col("payload.ingested_at")
        .alias("ingested_at"),

        when(
            col("payload").isNull(),
            lit("INVALID_JSON"),
        )
        .otherwise(
            lit("OK")
        )
        .alias("parse_status"),
    )

    return bronze.withColumn(
        "ingestion_date",
        to_date(
            col("kafka_timestamp")
        ),
    )


def write_bronze_stream(
    bronze_df: DataFrame,
):
    bronze_path = get_required_env(
        "BRONZE_PATH"
    )

    checkpoint_path = get_required_env(
        "CHECKPOINT_PATH"
    )

    trigger_interval = os.getenv(
        "STREAM_TRIGGER_INTERVAL",
        "30 seconds",
    )

    return (
        bronze_df.writeStream
        .queryName(
            "b3-cotahist-bronze"
        )
        .format(
            "parquet"
        )
        .outputMode(
            "append"
        )
        .option(
            "path",
            bronze_path,
        )
        .option(
            "checkpointLocation",
            checkpoint_path,
        )
        .partitionBy(
            "ingestion_date"
        )
        .trigger(
            processingTime=trigger_interval
        )
        .start()
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        ),
    )

    logger = logging.getLogger(
        "b3-cotahist-bronze"
    )

    logger.info(
        "Starting B3 COTAHIST Bronze stream."
    )

    spark = create_spark_session()

    kafka_df = read_kafka_stream(
        spark
    )

    bronze_df = build_bronze_dataframe(
        kafka_df
    )

    query = write_bronze_stream(
        bronze_df
    )

    logger.info(
        "B3 COTAHIST Bronze stream started."
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()