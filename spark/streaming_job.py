import os
from typing import List, Dict, Any

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
        .appName("EnergyMetricsStreamingJob")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[streaming_job] SparkSession created")
    return spark


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    kafka_bootstrap = get_env("KAFKA_BOOTSTRAP_SERVERS", "kafka-svc:9092")
    kafka_topic = get_env("KAFKA_METRICS_TOPIC", "training.metrics")

    print(f"[streaming_job] Reading from Kafka topic '{kafka_topic}' at '{kafka_bootstrap}'")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    value_str = raw_stream.select(F.col("value").cast("string").alias("value"))

    metrics_schema = T.StructType([
        T.StructField("timestamp", T.StringType(), True),
        T.StructField("project_id", T.StringType(), True),
        T.StructField("user_id", T.StringType(), True),
        T.StructField("run_id", T.StringType(), True),
        T.StructField("energy_kwh", T.DoubleType(), True),
        T.StructField("emissions_kg", T.DoubleType(), True),
        T.StructField("model_name", T.StringType(), True),
        T.StructField("dataset_name", T.StringType(), True),
        T.StructField("region", T.StringType(), True),
    ])

    json_parsed = value_str.select(
        F.from_json(F.col("value"), metrics_schema).alias("data")
    ).select("data.*")

    # IMPORTANT: parse ISO timestamps with 'T' correctly
    json_parsed = json_parsed.withColumn(
        "event_time",
        F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss")
    )

    print("[streaming_job] Kafka stream schema:")
    json_parsed.printSchema()

    return json_parsed


def aggregate_metrics(df: DataFrame) -> DataFrame:
    windowed = (
        df
        .withWatermark("event_time", "1 minute")
        .groupBy(
            F.window("event_time", "1 minute").alias("time_window"),
            F.col("user_id"),
            F.col("project_id"),
            F.col("run_id"),
        )
        .agg(
            F.sum("energy_kwh").alias("total_energy_kwh"),
            F.sum("emissions_kg").alias("total_emissions_kg"),
        )
    )

    return windowed


def write_batch_to_mongo(batch_df: DataFrame, batch_id: int) -> None:
    records: List[Dict[str, Any]] = [row.asDict() for row in batch_df.collect()]
    if not records:
        print(f"[streaming_job] Batch {batch_id}: no records, skipping.")
        return

    mongo_uri = get_env("MONGODB_URI", "mongodb://mongodb:27017")
    mongo_db = get_env("MONGODB_DB", "energy_metrics")
    mongo_coll = get_env("MONGODB_AGG_COLLECTION", "aggregated_metrics")

    print(f"[streaming_job] Batch {batch_id}: inserting {len(records)} records into MongoDB collection {mongo_db}.{mongo_coll}")

    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    coll = db[mongo_coll]

    docs: List[Dict[str, Any]] = []
    for rec in records:
        window = rec.pop("time_window", None)
        if window is not None:
            rec["window_start"] = window.start.isoformat()
            rec["window_end"] = window.end.isoformat()
        docs.append(rec)

    if docs:
        coll.insert_many(docs)

    client.close()


def main() -> None:
    spark = build_spark()
    kafka_df = read_kafka_stream(spark)
    aggregated_df = aggregate_metrics(kafka_df)

    checkpoint_location = get_env("SPARK_CHECKPOINT_DIR", "/checkpoints/energy-metrics")

    print("[streaming_job] Starting streaming query with foreachBatch -> MongoDB")

    query = (
        aggregated_df.writeStream
        .foreachBatch(write_batch_to_mongo)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_location)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
