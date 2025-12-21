import os
import shutil
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

from pymongo import MongoClient, UpdateOne


# ----------------------------
# Env helpers
# ----------------------------
def get_env(name: str, default: str) -> str:
    value = os.getenv(name, default)
    print(f"[streaming_job] {name} = {value}")
    return value


def get_env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default


def get_env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


# ----------------------------
# Mongo safe conversion
# ----------------------------
def mongo_safe(obj: Any) -> Any:
    """
    Convert objects not BSON-serializable (e.g., datetime.date) into JSON/Mongo-safe types.
    - datetime/date -> ISO string
    - nested dict/list -> recurse
    """
    if obj is None:
        return None

    # datetime.date / datetime.datetime both have isoformat()
    if hasattr(obj, "isoformat") and callable(getattr(obj, "isoformat")):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)

    if isinstance(obj, dict):
        return {k: mongo_safe(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [mongo_safe(v) for v in obj]

    return obj


def mongo_safe_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    return mongo_safe(d)


# ----------------------------
# Spark session
# ----------------------------
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("EnergyEventsStreamingJob")
        .getOrCreate()
    )

    # Streaming-friendly config
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", str(get_env_int("SPARK_SHUFFLE_PARTITIONS", 32)))

    # Make join strategy explicit (we use broadcast() ourselves)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    spark.sparkContext.setLogLevel(get_env("SPARK_LOG_LEVEL", "WARN"))
    print("[streaming_job] SparkSession created")
    return spark


# ----------------------------
# Timestamp parsing (microseconds + optional Z)
# ----------------------------
def parse_event_time(ts_col: F.Column) -> F.Column:
    ts_clean = F.regexp_replace(ts_col, "Z$", "")
    with_us = F.to_timestamp(ts_clean, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    with_ms = F.to_timestamp(ts_clean, "yyyy-MM-dd'T'HH:mm:ss.SSS")
    without_frac = F.to_timestamp(ts_clean, "yyyy-MM-dd'T'HH:mm:ss")
    return F.coalesce(with_us, with_ms, without_frac)


# ----------------------------
# Schema (permissive)
# ----------------------------
def build_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("event_type", T.StringType(), True),  # metric | run_summary

            T.StructField("timestamp", T.StringType(), True),
            T.StructField("run_id", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("model_name", T.StringType(), True),
            T.StructField("dataset_name", T.StringType(), True),
            T.StructField("region_iso", T.StringType(), True),
            T.StructField("framework", T.StringType(), True),
            T.StructField("environment", T.StringType(), True),

            # keep flexible (producer may send dict; keeping string avoids schema break)
            T.StructField("hyperparameters", T.StringType(), True),

            # metric fields
            T.StructField("epoch", T.IntegerType(), True),
            T.StructField("step", T.IntegerType(), True),
            T.StructField("loss", T.DoubleType(), True),
            T.StructField("accuracy", T.DoubleType(), True),
            T.StructField("cpu_utilization_pct", T.DoubleType(), True),
            T.StructField("energy_kwh", T.DoubleType(), True),
            T.StructField("emissions_kg", T.DoubleType(), True),
            T.StructField("cumulative_energy_kwh", T.DoubleType(), True),
            T.StructField("cumulative_emissions_kg", T.DoubleType(), True),

            # run_summary fields
            T.StructField("status", T.StringType(), True),
            T.StructField("start_timestamp", T.StringType(), True),
            T.StructField("end_timestamp", T.StringType(), True),
            T.StructField("total_duration_s", T.DoubleType(), True),
            T.StructField("n_metric_events", T.IntegerType(), True),
            T.StructField("total_energy_kwh", T.DoubleType(), True),
            T.StructField("total_emissions_kg", T.DoubleType(), True),
        ]
    )


# ----------------------------
# Static dimensions used by pipeline (joins)
# ----------------------------
def build_region_factor_dim(spark: SparkSession) -> DataFrame:
    rows = [
        ("DE", 0.40),
        ("FR", 0.06),
        ("ES", 0.20),
        ("US", 0.42),
        ("GB", 0.23),
        ("NL", 0.30),
        ("BE", 0.18),
        ("UNK", 0.40),
    ]
    return spark.createDataFrame(rows, schema=["region_iso", "grid_intensity_kg_per_kwh"])


def build_model_meta_dim(spark: SparkSession) -> DataFrame:
    """
    Larger-ish dim to justify a non-broadcast join (sort-merge hint).
    """
    base = [
        ("logistic_regression", "linear", "small"),
        ("random_forest", "tree", "medium"),
        ("xgboost", "tree", "medium"),
        ("bert_base", "transformer", "large"),
        ("dummy_model", "baseline", "small"),
    ]
    replicated = []
    for i in range(2000):
        for (m, fam, b) in base:
            replicated.append((m, fam, b, i))
    return spark.createDataFrame(replicated, schema=["model_name", "model_family", "param_bucket", "replica_id"])


def ensure_partitioned_model_store(spark: SparkSession) -> DataFrame:
    """
    Optional: if ENABLE_MODEL_META_STORE=1, we materialize model meta as partitioned parquet
    so Spark can show partition pruning when filtering on model_family.
    If disabled, we just return the in-memory model meta DF.
    """
    enabled = get_env_bool("ENABLE_MODEL_META_STORE", False)
    if not enabled:
        return build_model_meta_dim(spark)

    path = get_env("MODEL_META_STORE_PATH", "/tmp/model_meta_partitioned")

    # Avoid the "Unable to infer schema for Parquet" by ensuring we overwrite cleanly
    try:
        if os.path.exists(path):
            shutil.rmtree(path)
    except Exception:
        pass

    df = build_model_meta_dim(spark).select("model_name", "model_family", "param_bucket").dropDuplicates()
    df.write.mode("overwrite").partitionBy("model_family").parquet(path)
    return spark.read.parquet(path)


# ----------------------------
# UDF (business logic)
# ----------------------------
@F.udf(returnType=T.StringType())
def classify_run_health(loss: Optional[float], accuracy: Optional[float], cpu: Optional[float]) -> str:
    try:
        l = float(loss) if loss is not None else None
        a = float(accuracy) if accuracy is not None else None
        c = float(cpu) if cpu is not None else None

        if a is not None and a >= 0.9 and (l is None or l <= 0.3):
            return "good"
        if c is not None and c >= 90.0:
            return "cpu_hot"
        if l is not None and l >= 1.5:
            return "diverging"
        return "ok"
    except Exception:
        return "unknown"


# ----------------------------
# Kafka stream read
# ----------------------------
def read_kafka_stream(spark: SparkSession) -> DataFrame:
    kafka_bootstrap = get_env("KAFKA_BOOTSTRAP_SERVERS", "kafka-svc:9092")
    kafka_topic = get_env("KAFKA_TOPIC", get_env("KAFKA_METRICS_TOPIC", "training.events"))
    starting_offsets = get_env("KAFKA_STARTING_OFFSETS", "latest")

    print(f"[streaming_job] Reading from Kafka topic '{kafka_topic}' at '{kafka_bootstrap}'")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    schema = build_schema()
    df = raw.select(F.col("value").cast("string").alias("json"))
    df = df.select(F.from_json(F.col("json"), schema).alias("data")).select("data.*")

    # infer event_type if missing
    df = df.withColumn(
        "event_type",
        F.when(F.col("event_type").isNotNull(), F.col("event_type"))
         .when(F.col("end_timestamp").isNotNull() | F.col("start_timestamp").isNotNull(), F.lit("run_summary"))
         .otherwise(F.lit("metric"))
    )

    df = df.withColumn("event_time", parse_event_time(F.col("timestamp")))
    # Note: event_date becomes a Python datetime.date after collect(). We keep it because it's useful,
    # but we convert it to ISO string before writing to Mongo (mongo_safe_dict()).
    df = df.withColumn("event_date", F.to_date(F.col("event_time")))

    # deterministic id (idempotent upsert => effectively exactly-once)
    df = df.withColumn(
        "event_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("run_id"), F.lit("")),
                F.coalesce(F.col("timestamp"), F.lit("")),
                F.coalesce(F.col("event_type"), F.lit("")),
                F.coalesce(F.col("epoch").cast("string"), F.lit("")),
                F.coalesce(F.col("step").cast("string"), F.lit("")),
            ),
            256
        )
    )

    # custom UDF
    df = df.withColumn("run_health", classify_run_health(F.col("loss"), F.col("accuracy"), F.col("cpu_utilization_pct")))

    return df


# ----------------------------
# Mongo config
# ----------------------------
def mongo_config() -> Tuple[str, str, str, str, str]:
    mongo_uri = get_env("MONGODB_URI", "mongodb://mongodb:27017")
    mongo_db = get_env("MONGODB_DB", "energy_emissions")

    metrics_coll = get_env("MONGODB_METRICS_COLLECTION", "training_metrics")
    summary_coll = get_env("MONGODB_RUN_SUMMARY_COLLECTION", "training_run_summary")
    agg_coll = get_env("MONGODB_AGG_COLLECTION", "aggregated_metrics")

    return mongo_uri, mongo_db, metrics_coll, summary_coll, agg_coll


# ----------------------------
# Sink 1: Raw + run summary (idempotent upsert)
# ----------------------------
def upsert_raw_and_summary_to_mongo(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        print(f"[streaming_job] Batch {batch_id}: empty")
        return

    mongo_uri, mongo_db, metrics_coll_name, summary_coll_name, _ = mongo_config()

    batch_df.persist(StorageLevel.MEMORY_AND_DISK)

    metrics_df = batch_df.filter(F.col("event_type") == F.lit("metric"))
    summary_df = batch_df.filter(F.col("event_type") == F.lit("run_summary"))

    metrics_count = metrics_df.count()
    summary_count = summary_df.count()
    print(f"[streaming_job] Batch {batch_id}: metrics_count={metrics_count}, summary_count={summary_count}")

    client = MongoClient(mongo_uri)
    try:
        db = client[mongo_db]
        metrics_coll = db[metrics_coll_name]
        summary_coll = db[summary_coll_name]

        metric_records = [
            mongo_safe_dict(r.asDict(recursive=True))
            for r in metrics_df.collect()
        ]
        if metric_records:
            ops = []
            for rec in metric_records:
                event_id = rec.get("event_id")
                if event_id:
                    ops.append(UpdateOne({"event_id": event_id}, {"$set": rec}, upsert=True))
            if ops:
                metrics_coll.bulk_write(ops, ordered=False)
                print(f"[streaming_job] Batch {batch_id}: upserted {len(ops)} raw metric docs")

        summary_records = [
            mongo_safe_dict(r.asDict(recursive=True))
            for r in summary_df.collect()
        ]
        if summary_records:
            ops = []
            for rec in summary_records:
                run_id = rec.get("run_id")
                if run_id:
                    ops.append(UpdateOne({"run_id": run_id}, {"$set": rec}, upsert=True))
            if ops:
                summary_coll.bulk_write(ops, ordered=False)
                print(f"[streaming_job] Batch {batch_id}: upserted {len(ops)} run_summary docs")

    finally:
        batch_df.unpersist()
        client.close()


# ----------------------------
# Sink 2: Aggregates (joins + watermark + state + window funcs + pivot/unpivot)
# ----------------------------
def upsert_aggregates_to_mongo(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    mongo_uri, mongo_db, _, _, agg_coll_name = mongo_config()

    metrics = batch_df.filter(F.col("event_type") == F.lit("metric"))

    # Performance: repartition on key to reduce skew for aggregations
    metrics = metrics.repartition(get_env_int("SPARK_METRICS_REPARTITIONS", 16), F.col("run_id"))
    metrics.persist(StorageLevel.MEMORY_AND_DISK)

    # Join 1: broadcast (small dim)
    region_dim = build_region_factor_dim(metrics.sparkSession)
    enriched = metrics.join(F.broadcast(region_dim), on="region_iso", how="left")

    # Derived metric (business logic)
    enriched = enriched.withColumn(
        "emissions_estimated_kg",
        F.col("energy_kwh") * F.coalesce(F.col("grid_intensity_kg_per_kwh"), F.lit(0.40))
    )

    # Join 2: larger dim (sort-merge hint)
    model_dim = ensure_partitioned_model_store(metrics.sparkSession)
    enriched = enriched.join(model_dim.hint("merge"), on="model_name", how="left")

    # Watermark + late data handling
    watermark_delay = get_env("SPARK_WATERMARK_DELAY", "10 minutes")
    window_size = get_env("SPARK_WINDOW_SIZE", "1 minute")

    # Stateful window aggregation
    windowed = (
        enriched
        .withWatermark("event_time", watermark_delay)
        .groupBy(
            F.col("run_id"),
            F.col("model_name"),
            F.col("dataset_name"),
            F.col("model_family"),
            F.window(F.col("event_time"), window_size).alias("w")
        )
        .agg(
            F.count(F.lit(1)).alias("n_events"),
            F.sum("energy_kwh").alias("energy_kwh_sum"),
            F.sum("emissions_kg").alias("emissions_kg_sum"),
            F.sum("emissions_estimated_kg").alias("emissions_estimated_kg_sum"),
            F.avg("cpu_utilization_pct").alias("cpu_avg"),
            F.stddev("cpu_utilization_pct").alias("cpu_stddev"),
            F.avg("loss").alias("loss_avg"),
            F.avg("accuracy").alias("accuracy_avg"),
            F.expr("approx_percentile(energy_kwh, array(0.5, 0.9, 0.99))").alias("energy_kwh_pctl"),
            F.corr(F.col("energy_kwh"), F.col("cpu_utilization_pct")).alias("corr_energy_cpu"),
        )
        .withColumn("window_start", F.col("w.start"))
        .withColumn("window_end", F.col("w.end"))
        .drop("w")
    )

    # Time series analysis with window functions (lag)
    ts_w = Window.partitionBy("run_id").orderBy(F.col("window_start").asc())
    windowed = (
        windowed
        .withColumn("prev_energy_kwh_sum", F.lag("energy_kwh_sum", 1).over(ts_w))
        .withColumn("energy_kwh_sum_delta", F.col("energy_kwh_sum") - F.col("prev_energy_kwh_sum"))
        .withColumn("prev_cpu_avg", F.lag("cpu_avg", 1).over(ts_w))
        .withColumn("cpu_avg_delta", F.col("cpu_avg") - F.col("prev_cpu_avg"))
    )

    # Unpivot (stack) -> long format  (FIXED)
    long_metrics = (
        enriched
        .select(
            "run_id",
            "event_time",
            F.expr(
                "stack(4, "
                "'loss', cast(loss as double), "
                "'accuracy', cast(accuracy as double), "
                "'cpu', cast(cpu_utilization_pct as double), "
                "'energy_kwh', cast(energy_kwh as double)"
                ") as (metric_name, metric_value)"
            )
        )
    )

    # Pivot back: per run_id + window => avg for each metric
    pivoted = (
        long_metrics
        .withWatermark("event_time", watermark_delay)
        .groupBy("run_id", F.window("event_time", window_size).alias("w"))
        .pivot("metric_name", ["loss", "accuracy", "cpu", "energy_kwh"])
        .agg(F.avg("metric_value"))
        .withColumn("window_start", F.col("w.start"))
        .withColumn("window_end", F.col("w.end"))
        .drop("w")
    )

    # Performance: cache aggregates reused for join + write
    windowed.persist(StorageLevel.MEMORY_AND_DISK)
    pivoted.persist(StorageLevel.MEMORY_AND_DISK)

    # Join aggregates together (multiple joins optimization / hints)
    joined_aggs = (
        windowed.alias("w")
        .join(
            pivoted.alias("p").hint("merge"),
            on=[
                F.col("w.run_id") == F.col("p.run_id"),
                F.col("w.window_start") == F.col("p.window_start"),
                F.col("w.window_end") == F.col("p.window_end"),
            ],
            how="left"
        )
        .drop(F.col("p.run_id"))
        .drop(F.col("p.window_start"))
        .drop(F.col("p.window_end"))
    )

    # Deterministic aggregate id for idempotent upsert
    joined_aggs = joined_aggs.withColumn(
        "agg_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("run_id"), F.lit("")),
                F.coalesce(F.col("window_start").cast("string"), F.lit("")),
                F.coalesce(F.col("window_end").cast("string"), F.lit("")),
            ),
            256
        )
    )

    agg_records = [
        mongo_safe_dict(r.asDict(recursive=True))
        for r in joined_aggs.collect()
    ]

    client = MongoClient(mongo_uri)
    try:
        db = client[mongo_db]
        coll = db[agg_coll_name]
        ops = []
        for rec in agg_records:
            agg_id = rec.get("agg_id")
            if agg_id:
                ops.append(UpdateOne({"agg_id": agg_id}, {"$set": rec}, upsert=True))
        if ops:
            coll.bulk_write(ops, ordered=False)
            print(f"[streaming_job] Batch {batch_id}: upserted {len(ops)} aggregate docs")
    finally:
        windowed.unpersist()
        pivoted.unpersist()
        metrics.unpersist()
        client.close()


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    spark = build_spark()

    events = read_kafka_stream(spark)

    checkpoint_base = get_env("SPARK_CHECKPOINT_DIR", "/tmp/checkpoints/energy-events")
    checkpoint_raw = checkpoint_base.rstrip("/") + "/raw"
    checkpoint_agg = checkpoint_base.rstrip("/") + "/agg"

    # Output mode: append for raw, update for stateful aggregates
    _q1 = (
        events.writeStream
        .foreachBatch(upsert_raw_and_summary_to_mongo)
        .outputMode("append")
        .option("checkpointLocation", checkpoint_raw)
        .start()
    )

    _q2 = (
        events.writeStream
        .foreachBatch(upsert_aggregates_to_mongo)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_agg)
        .start()
    )

    print("[streaming_job] Streaming queries started. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
