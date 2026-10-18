"""SparkSession construction and Kafka stream helpers shared by Stages 1-3."""
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField,
                               StructType)

RAW_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("dropoff_time", StringType()),
    StructField("pickup_zone", IntegerType()),
    StructField("dropoff_zone", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("fare_amount", DoubleType()),
    StructField("passenger_count", IntegerType()),
    StructField("payment_type", IntegerType()),
    StructField("producer_ts_ms", LongType()),
    StructField("control", StringType()),
    StructField("num_partitions", IntegerType()),
    StructField("producer_sent", LongType()),
])

CLEAN_SCHEMA = StructType(RAW_SCHEMA.fields + [
    StructField("route_key", StringType()),
    StructField("trip_duration_min", DoubleType()),
    StructField("slow_trip_bit", IntegerType()),
    StructField("bloom_seen_before", IntegerType()),
    StructField("gt_seen_before", IntegerType()),
    StructField("stage1_ts_ms", LongType()),
    StructField("s1_accepted", LongType()),
    StructField("s1_rejected", LongType()),
])


def setup_env(cfg):
    """Environment Spark needs on Windows: winutils (HADOOP_HOME) and the venv python."""
    hadoop = cfg["hadoop_home"]
    os.environ["HADOOP_HOME"] = hadoop
    os.environ["PATH"] = str(Path(hadoop) / "bin") + os.pathsep + os.environ["PATH"]
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    if "JAVA_HOME" not in os.environ:
        for base in (Path("C:/Program Files/Eclipse Adoptium"),):
            jdks = sorted(base.glob("jdk-17*")) if base.exists() else []
            if jdks:
                os.environ["JAVA_HOME"] = str(jdks[-1])
                os.environ["PATH"] = str(jdks[-1] / "bin") + os.pathsep + os.environ["PATH"]


def get_spark(cfg, app_name):
    setup_env(cfg)
    slots = int(cfg["parallelism"])
    spark = (SparkSession.builder
             .appName(f"{app_name}-{cfg['run_id']}")
             .master(f"local[{slots}]")
             .config("spark.jars.packages", cfg["spark_kafka_package"])
             .config("spark.sql.shuffle.partitions", str(slots))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.driver.memory", "1g")
             # local mode: a heartbeat that reaches the driver before its block manager registered
             # triggers a NullPointerException loop (BlockManagerMasterEndpoint) on slow start-ups
             .config("spark.executor.heartbeatInterval", "60s")
             .config("spark.network.timeout", "600s")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
             .config("spark.local.dir", str(Path(cfg["work_dir"]) / "spark-tmp"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_kafka(spark, cfg, topic, schema):
    """Kafka source -> parsed columns + Kafka partition/offset."""
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", cfg["bootstrap_servers"])
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .load())
    return (raw.select(F.col("partition").alias("kafka_partition"),
                       F.col("offset").alias("kafka_offset"),
                       F.col("timestamp").alias("kafka_ts"),
                       F.from_json(F.col("value").cast("string"), schema).alias("e"))
            .select("kafka_partition", "kafka_offset", "kafka_ts", "e.*"))


def mark_ready(run_dir, stage):
    (Path(run_dir) / f"{stage}.ready").write_text("ok")
