"""Shared pipeline code used by every stage: configuration loading, Kafka helpers
(topic (re)creation, lag measurement, offset bookkeeping) and the SparkSession /
Kafka-source helpers of Stages 1-3.

The orchestrator (final_consumer/run_experiment.py) writes the effective
configuration of a run to <run_dir>/config.json and passes its path in the
BDA_CONFIG environment variable, so all processes of one run see exactly the
same parameters.
"""
import json
import os
import sys
import time
from pathlib import Path

import yaml
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField,
                               StructType)


# =========================================================================== configuration

ROOT = Path(__file__).resolve().parent.parent

TOPIC_RAW = "taxi.raw"
TOPIC_CLEAN = "taxi.clean"
TOPIC_METRICS = "taxi.metrics"
TOPIC_LSH = "taxi.lsh"


def _parse_value(text):
    """Parse a --set override value with YAML rules (numbers, lists, booleans)."""
    return yaml.safe_load(text)


def load_config(path=None, overrides=None):
    path = path or os.environ.get("BDA_CONFIG") or ROOT / "config.yaml"
    path = Path(path)
    with open(path, encoding="utf-8") as f:
        cfg = json.load(f) if path.suffix == ".json" else yaml.safe_load(f)
    for item in overrides or []:
        key, _, value = item.partition("=")
        cfg[key.strip()] = _parse_value(value)
    for key in ("kafka_home", "hadoop_home", "work_dir"):
        cfg[key] = str(Path(os.path.expanduser(cfg[key])))
    if not Path(cfg["data_file"]).is_absolute():
        cfg["data_file"] = str(ROOT / cfg["data_file"])
    assert cfg["lsh_bands"] * cfg["lsh_rows_per_band"] == cfg["lsh_num_hashes"], \
        "lsh_bands * lsh_rows_per_band must equal lsh_num_hashes"
    return cfg


def run_dir(cfg):
    d = ROOT / "results" / "runs" / cfg["run_id"]
    d.mkdir(parents=True, exist_ok=True)
    return d


def checkpoint_dir(cfg, stage):
    return Path(cfg["work_dir"]) / "checkpoints" / cfg["run_id"] / stage


# =========================================================================== Kafka helpers

def admin(cfg, quiet=False):
    conf = {"bootstrap.servers": cfg["bootstrap_servers"]}
    if quiet:
        conf["log_level"] = 0  # broker may still be starting: don't print connect errors
    return AdminClient(conf)


def wait_for_broker(cfg, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            admin(cfg, quiet=True).list_topics(timeout=3)
            return True
        except Exception:
            time.sleep(1)
    return False


def recreate_topics(cfg):
    """Delete and recreate the four pipeline topics so each run starts from offset 0."""
    a = admin(cfg)
    topics = [TOPIC_RAW, TOPIC_CLEAN, TOPIC_METRICS, TOPIC_LSH]
    existing = set(a.list_topics(timeout=10).topics)
    to_delete = [t for t in topics if t in existing]
    if to_delete:
        for fut in a.delete_topics(to_delete, operation_timeout=30).values():
            fut.result()
        # deletion is asynchronous on the broker; wait until the names are gone
        deadline = time.time() + 60
        while time.time() < deadline and set(to_delete) & set(a.list_topics(timeout=10).topics):
            time.sleep(0.5)
    p = int(cfg["partitions"])
    mp = int(cfg.get("metrics_partitions", 1))
    new = [NewTopic(TOPIC_RAW, p, 1), NewTopic(TOPIC_CLEAN, p, 1),
           NewTopic(TOPIC_METRICS, mp, 1), NewTopic(TOPIC_LSH, mp, 1)]
    for attempt in range(30):
        try:
            for fut in a.create_topics(new, operation_timeout=30).values():
                fut.result()
            break
        except Exception as e:  # topic still marked for deletion -> retry
            if "already exists" in str(e).lower() or "marked for deletion" in str(e).lower():
                time.sleep(1)
                new = [t for t in new if t.topic not in a.list_topics(timeout=10).topics]
                if not new:
                    break
            else:
                raise
    # metadata propagation
    deadline = time.time() + 30
    while time.time() < deadline:
        md = a.list_topics(timeout=10).topics
        if all(t in md and len(md[t].partitions) > 0 for t in topics):
            return
        time.sleep(0.5)


class OffsetTracker:
    """Measures lag of a topic and commits a stage's processed offsets to a
    consumer group, so external tools (kafka-consumer-groups, the orchestrator's
    lag monitor) can see how far a Spark stage has progressed.

    Spark's Kafka source keeps its offsets in the checkpoint, not in Kafka; these
    commits are for observability only."""

    def __init__(self, cfg, topic, group):
        self.topic = topic
        self.consumer = Consumer({
            "bootstrap.servers": cfg["bootstrap_servers"],
            "group.id": group,
            "enable.auto.commit": False,
        })

    def high_watermarks(self):
        md = self.consumer.list_topics(self.topic, timeout=10)
        out = {}
        for p in md.topics[self.topic].partitions:
            _, high = self.consumer.get_watermark_offsets(TopicPartition(self.topic, p), timeout=10)
            out[p] = high
        return out

    def lag(self, next_offsets):
        """next_offsets: {partition: next offset to read}. Returns total lag."""
        hw = self.high_watermarks()
        return int(sum(max(0, high - next_offsets.get(p, 0)) for p, high in hw.items()))

    def commit(self, next_offsets):
        if next_offsets:
            tps = [TopicPartition(self.topic, p, o) for p, o in next_offsets.items()]
            self.consumer.commit(offsets=tps, asynchronous=False)

    def close(self):
        self.consumer.close()


def group_lag(cfg, topic, group):
    """Lag of a consumer group on a topic from committed offsets (-1 if group unknown)."""
    c = Consumer({"bootstrap.servers": cfg["bootstrap_servers"], "group.id": group,
                  "enable.auto.commit": False})
    try:
        md = c.list_topics(topic, timeout=10)
        if topic not in md.topics or md.topics[topic].error is not None:
            return None
        parts = [TopicPartition(topic, p) for p in md.topics[topic].partitions]
        committed = c.committed(parts, timeout=10)
        total = 0
        per = {}
        for tp in committed:
            _, high = c.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=10)
            pos = tp.offset if tp.offset >= 0 else 0
            per[tp.partition] = high
            total += max(0, high - pos)
        return total, per
    finally:
        c.close()


def partition_counts(cfg, topic):
    c = Consumer({"bootstrap.servers": cfg["bootstrap_servers"], "group.id": "bda-inspect"})
    try:
        md = c.list_topics(topic, timeout=10)
        out = {}
        for p in md.topics[topic].partitions:
            low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            out[p] = high - low
        return out
    finally:
        c.close()


# =========================================================================== Spark helpers

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
