import os
import sys
import threading
import time
import warnings
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import pyspark.sql.functions as F
from feast import FeatureStore
from feast.data_source import PushMode
from feast.infra.contrib.spark_kafka_processor import (
    SparkKafkaProcessor,
    SparkProcessorConfig,
)
from loguru import logger
from offline_write_batch import PostgreSQLOfflineWriter
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Ignore all warnings
warnings.filterwarnings("ignore")

# Set up logging
logger.remove()
logger.add(sys.stdout, level="DEBUG")
logger.add("logs/ingest_stream.log", level="DEBUG")

# Kafka environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "tracking.user_behavior.validated")

# PostgreSQL environment variables
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "dwh")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "feature_store")
POSTGRES_USER = os.getenv("POSTGRES_USER", "dwh")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "dwh")

# Schema for the nested "payload"
PAYLOAD_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_session", StringType(), True),
    ]
)

# Top-level event schema
EVENT_SCHEMA = StructType(
    [
        StructField(
            "schema",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField(
                        "fields",
                        ArrayType(
                            StructType(
                                [
                                    StructField("name", StringType(), True),
                                    StructField("type", StringType(), True),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField("payload", PAYLOAD_SCHEMA, True),
        StructField(
            "metadata",
            StructType(
                [
                    StructField("processed_at", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("valid", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("error_type", StringType(), True),
    ]
)

# Set Spark packages using the config instead of environment variables
SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.spark:spark-avro_2.12:3.5.0,"
    "org.apache.kafka:kafka-clients:3.4.0"
)

# Create SparkSession
spark = (
    SparkSession.builder.master("local[*]")
    .appName("feast-feature-ingestion")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .config("spark.sql.shuffle.partitions", 8)
    .config("spark.streaming.kafka.maxRatePerPartition", 100)
    .config("spark.streaming.backpressure.enabled", True)
    .config("spark.kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .config("spark.kafka.subscribe", KAFKA_INPUT_TOPIC)
    .config("spark.kafka.startingOffsets", "latest")
    .config("spark.kafka.failOnDataLoss", "false")
    .getOrCreate()
)

# Initialize PostgreSQL writer after SparkSession initialization
postgres_writer = PostgreSQLOfflineWriter(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    database=POSTGRES_DB,
    db_schema=POSTGRES_SCHEMA,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
)

# Initialize the FeatureStore
store = FeatureStore(repo_path=".")

# Session data tracking (optional usage)
session_activity_counts = defaultdict(int)
session_last_seen = defaultdict(datetime.now)
SESSION_TIMEOUT = timedelta(hours=24)


def initialize_feature_tables():
    """Initialize feature tables in PostgreSQL if they don't exist."""
    try:
        # Define feature schema based on your feature view
        feature_names = [
            "price",
            "brand",
            "category_code_level1",
            "category_code_level2",
            "event_weekday",
            "activity_count",
            "is_purchased",
        ]

        feature_types = [
            "float64",  # price
            "string",  # brand
            "string",  # category_code_level1
            "string",  # category_code_level2
            "int32",  # event_weekday
            "int64",  # activity_count
            "int32",  # is_purchased
        ]

        entity_names = ["user_id", "product_id", "user_session", "event_timestamp"]
        entity_types = ["int64", "int64", "string", "datetime64[ns]"]

        # Create table for streaming features
        postgres_writer.create_table_from_features(
            table_name="streaming_features",
            feature_names=feature_names,
            feature_types=feature_types,
            entity_names=entity_names,
            entity_types=entity_types,
        )
        logger.info("Successfully initialized feature tables in PostgreSQL")

    except Exception as e:
        logger.error(f"Failed to initialize feature tables: {str(e)}")
        raise


def preprocess_fn(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform validated events and write to both online and offline stores.
    """
    try:
        if df.empty:
            logger.info("Received empty batch, returning empty DataFrame with schema.")
            return pd.DataFrame(
                columns=[
                    "event_timestamp",
                    "user_id",
                    "product_id",
                    "user_session",
                    "price",
                    "brand",
                    "category_code_level1",
                    "category_code_level2",
                    "event_weekday",
                    "activity_count",
                    "is_purchased",
                ]
            )

        # Only keep valid events but don't include valid column in output
        df = df[df["valid"] == "VALID"].copy()
        if df.empty:
            logger.info("No valid records in this batch.")
            return pd.DataFrame(columns=df.columns)

        # Convert to Spark DataFrame
        spark_df = spark.createDataFrame(df)

        # Ensure we have a proper event_timestamp column
        if "event_time" in spark_df.columns:
            spark_df = spark_df.withColumnRenamed("event_time", "event_timestamp")

        # Convert event_timestamp to timestamp and handle invalid timestamps
        spark_df = spark_df.withColumn(
            "event_timestamp",
            F.when(
                F.col("event_timestamp").contains("UTC"),
                F.to_timestamp(
                    F.regexp_replace(F.col("event_timestamp"), " UTC$", ""),
                    "yyyy-MM-dd HH:mm:ss",
                ),
            ).otherwise(
                F.to_timestamp(F.col("event_timestamp"), "yyyy-MM-dd HH:mm:ss")
            ),
        )

        # Drop rows with invalid timestamps
        spark_df = spark_df.filter(F.col("event_timestamp").isNotNull())

        # Window: activity_count by user_session
        window_spec = Window.partitionBy("user_session")
        spark_df = spark_df.withColumn("activity_count", F.count("*").over(window_spec))

        # Fill missing fields with defaults
        spark_df = spark_df.fillna(
            {"price": 0.0, "brand": "", "category_code": "", "event_type": ""}
        )

        # Split category_code into two levels using split and regexp_replace
        spark_df = spark_df.withColumn(
            "category_code_level1",
            F.regexp_replace(
                F.split(F.col("category_code"), "\\.", 2).getItem(0), "null", ""
            ),
        ).withColumn(
            "category_code_level2",
            F.regexp_replace(
                F.split(F.col("category_code"), "\\.", 2).getItem(1), "null", ""
            ),
        )

        # Calculate weekday from timestamp
        spark_df = spark_df.withColumn(
            "event_weekday", F.dayofweek(F.col("event_timestamp")) - 1
        )

        # Derived feature: is_purchased
        spark_df = spark_df.withColumn(
            "is_purchased",
            F.when(F.lower(F.col("event_type")) == "purchase", 1).otherwise(0),
        )

        # Cast columns to correct types
        spark_df = (
            spark_df.withColumn("user_id", F.col("user_id").cast("long"))
            .withColumn("product_id", F.col("product_id").cast("long"))
            .withColumn("price", F.col("price").cast("double"))
            .withColumn("event_weekday", F.col("event_weekday").cast("integer"))
            .withColumn("activity_count", F.col("activity_count").cast("long"))
            .withColumn("is_purchased", F.col("is_purchased").cast("integer"))
        )

        # Filter out rows with null or invalid values
        spark_df = spark_df.filter(
            # Remove rows with null values in any column
            F.col("user_id").isNotNull()
            & F.col("product_id").isNotNull()
            & F.col("price").isNotNull()
            & F.col("brand").isNotNull()
            & F.col("category_code_level1").isNotNull()
            & F.col("category_code_level2").isNotNull()
            & F.col("user_session").isNotNull()
            &
            # Remove rows with empty strings in text columns
            (F.trim(F.col("brand")) != "")
            & (F.trim(F.col("category_code_level1")) != "")
            & (F.trim(F.col("category_code_level2")) != "")
            & (F.trim(F.col("user_session")) != "")
            &
            # Additional data quality checks
            (F.col("user_id") > 0)
            & (F.col("product_id") > 0)
            & (F.col("price") >= 0)
            & (F.col("activity_count") > 0)
            & (F.col("event_weekday").between(0, 6))
        )

        # Select final columns
        feature_cols = [
            "event_timestamp",
            "user_id",
            "product_id",
            "user_session",
            "price",
            "brand",
            "category_code_level1",
            "category_code_level2",
            "event_weekday",
            "activity_count",
            "is_purchased",
        ]
        spark_df = spark_df.select(feature_cols)

        # Convert back to pandas
        df = spark_df.toPandas()

        logger.info(f"Preprocessing produced {len(df)} rows")

        # After all preprocessing is done, write to offline store
        if not df.empty:
            postgres_writer.write_batch(
                df=df,
                table_name="streaming_features",
                if_exists="append",
                chunk_size=10000,
            )
            logger.info(f"Wrote {len(df)} rows to offline store")

        return df

    except Exception as e:
        logger.exception(f"Error in preprocess_fn: {str(e)}")
        return pd.DataFrame(columns=df.columns)


class ValidatedEventsProcessor(SparkKafkaProcessor):
    """
    Custom Kafka processor that reads from validated events,
    applies 'preprocess_fn', and pushes data to the Feast stream feature view.
    """

    def _ingest_stream_data(self):
        from pyspark.sql.functions import col, from_json

        logger.debug("Starting stream ingestion from validated events topic...")
        stream_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_INPUT_TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # Parse JSON
        parsed_df = stream_df.select(
            from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")
        ).select(
            F.col("data.payload.event_time").alias("event_timestamp"),
            F.col("data.payload.event_type"),
            F.col("data.payload.product_id"),
            F.col("data.payload.category_id"),
            F.col("data.payload.category_code"),
            F.col("data.payload.brand"),
            F.col("data.payload.price"),
            F.col("data.payload.user_id"),
            F.col("data.payload.user_session"),
            F.col("data.valid"),
            F.col("data.metadata.processed_at"),
        )

        return parsed_df


# SparkProcessorConfig
ingestion_config = SparkProcessorConfig(
    mode="spark",
    source="kafka",
    spark_session=spark,
    processing_time="4 seconds",
    query_timeout=30,
)


def run_materialization():
    """
    Runs Feast materialize_incremental every hour, to sync
    data from offline to online store for historical completeness.
    """
    while True:
        try:
            logger.info("Starting materialization job...")
            store.materialize_incremental(end_date=datetime.utcnow())
            logger.info("Materialization completed successfully.")
        except Exception as e:
            logger.error(f"Materialization failed: {e}")
        # Sleep 1 hour
        time.sleep(3600)


if __name__ == "__main__":
    try:
        # Initialize feature tables in PostgreSQL
        initialize_feature_tables()

        # Retrieve your streaming Feature View definition from the repo
        sfv = store.get_stream_feature_view("streaming_features")

        # Start a background thread to materialize every hour
        materialization_thread = threading.Thread(
            target=run_materialization, daemon=True
        )
        materialization_thread.start()
        logger.info("Background materialization thread started")

        # Initialize the processor
        processor = ValidatedEventsProcessor(
            config=ingestion_config,
            fs=store,
            sfv=sfv,  # Stream Feature View
            preprocess_fn=preprocess_fn,
        )

        # Start ingestion in ONLINE push mode
        logger.info(f"Starting feature ingestion from topic: {KAFKA_INPUT_TOPIC}")
        query = processor.ingest_stream_feature_view(PushMode.ONLINE)
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Failed to start feature ingestion: {str(e)}")
        raise
