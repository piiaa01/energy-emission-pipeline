import os
from typing import Dict, Any, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pymongo import MongoClient


def get_env(name: str, default: str) -> str:
    value = os.getenv(name, default)
    print(f"[streaming_job] {name} = {value}")
    return value


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("EnergyMetricsRawStreamingJob")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[streaming_job] SparkSession created")
    return spark


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    kafka_bootstrap = get_env("KAFKA_BOOTSTRAP_SERVERS", "kafka-svc:9092")
    kafka_topic = get_env("KAFKA_METRICS_TOPIC", "training.metrics")

    print(f"[streaming_job] Reading from Kafka topic '{kafka_topic}' at '{kafka_bootstrap}'")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    # Define a permissive schema for the metrics payload.
    # Missing fields will simply be null.
    metrics_schema = T.StructType(
        [
            T.StructField("timestamp", T.StringType(), True),
            T.StructField("run_id", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("model_name", T.StringType(), True),
            T.StructField("dataset_name", T.StringType(), True),
            T.StructField("region_iso", T.StringType(), True),
            T.StructField("framework", T.StringType(), True),
            T.StructField("environment", T.StringType(), True),
            T.StructField("epoch", T.IntegerType(), True),
            T.StructField("step", T.IntegerType(), True),
            T.StructField("loss", T.DoubleType(), True),
            T.StructField("accuracy", T.DoubleType(), True),
            T.StructField("cpu_utilization_pct", T.DoubleType(), True),
            T.StructField("energy_kwh", T.DoubleType(), True),
            T.StructField("emissions_kg", T.DoubleType(), True),
            T.StructField("cumulative_energy_kwh", T.DoubleType(), True),
            T.StructField("cumulative_emissions_kg", T.DoubleType(), True),
        ]
    )

    df = raw.select(F.col("value").cast("string").alias("json"))

    # Parse JSON into a struct, then expand struct fields
    df = df.select(F.from_json(F.col("json"), metrics_schema).alias("data")).select("data.*")

    # Optional: keep a Spark timestamp column for potential time filtering later
    # Supports timestamps like "2025-12-16T03:19:27.123Z" or without millis.
    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.regexp_replace(F.col("timestamp"), "Z$", ""), "yyyy-MM-dd'T'HH:mm:ss.SSS")
    )

    return df


def write_raw_to_mongo(batch_df: DataFrame, batch_id: int) -> None:
    # Convert to JSON-compatible dicts and drop Spark internal columns if any
    rows = batch_df.drop("event_time").collect()
    records: List[Dict[str, Any]] = [row.asDict(recursive=True) for row in rows]

    if not records:
        print(f"[streaming_job] Batch {batch_id}: no records")
        return

    mongo_uri = get_env("MONGODB_URI", "mongodb://mongodb:27017")
    mongo_db = get_env("MONGODB_DB", "energy_emissions")
    mongo_coll = get_env("MONGODB_METRICS_COLLECTION", "training_metrics")

    print(
        f"[streaming_job] Batch {batch_id}: inserting {len(records)} "
        f"records into MongoDB collection {mongo_db}.{mongo_coll}"
    )

    client = MongoClient(mongo_uri)
    try:
        db = client[mongo_db]
        coll = db[mongo_coll]
        coll.insert_many(records)
    finally:
        client.close()


def main() -> None:
    spark = build_spark()
    kafka_df = read_kafka_stream(spark)

    checkpoint_location = get_env("SPARK_CHECKPOINT_DIR", "/tmp/checkpoints/energy-metrics-raw")
    print(f"[streaming_job] Using checkpointLocation={checkpoint_location}")

    query = (
        kafka_df.writeStream
        .foreachBatch(write_raw_to_mongo)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
