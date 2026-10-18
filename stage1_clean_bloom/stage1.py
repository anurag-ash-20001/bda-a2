"""Stage 1 - Clean + Enrich + Bloom   (taxi.raw -> taxi.clean)

Spark Structured Streaming reads taxi.raw; parsing, validation and the derived
fields (route_key, trip_duration_min, slow_trip_bit) are computed by Spark in
parallel over the Kafka partitions (local[parallelism] task slots).

Inside foreachBatch the driver runs the Bloom-filter membership test on
event_id in arrival order (one global filter -> one consistent answer for the
whole stream) and publishes one enriched event per accepted input to
taxi.clean. Events flagged by the Bloom filter are NOT dropped: they are
marked (bloom_seen_before) and evaluated against an exact set of event_ids
(gt_seen_before), which exists only as the evaluation baseline.
"""
import json
import math
import sys
import time
from pathlib import Path

from confluent_kafka import Producer
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.config import TOPIC_CLEAN, TOPIC_RAW, checkpoint_dir, load_config, run_dir  # noqa: E402
from common.kafka_utils import OffsetTracker  # noqa: E402
from common.sketches import BloomFilter  # noqa: E402
from common.spark_utils import RAW_SCHEMA, get_spark, mark_ready, read_kafka  # noqa: E402

OUT_FIELDS = ["event_id", "event_time", "dropoff_time", "pickup_zone", "dropoff_zone", "trip_distance",
              "fare_amount", "passenger_count", "payment_type", "producer_ts_ms", "route_key",
              "trip_duration_min", "slow_trip_bit"]
INT_FIELDS = {"pickup_zone", "dropoff_zone", "passenger_count", "payment_type", "producer_ts_ms", "slow_trip_bit"}


def build_query(spark, cfg):
    df = read_kafka(spark, cfg, TOPIC_RAW, RAW_SCHEMA)
    pickup = F.to_timestamp("event_time")
    dropoff = F.to_timestamp("dropoff_time")
    duration = (F.unix_timestamp(dropoff) - F.unix_timestamp(pickup)) / 60.0
    reason = (F.when(F.col("control").isNotNull(), F.lit(None))
              .when(F.col("pickup_zone").isNull() | F.col("dropoff_zone").isNull(), "missing_zone")
              .when(~F.col("pickup_zone").between(1, 265) | ~F.col("dropoff_zone").between(1, 265), "bad_zone")
              .when(pickup.isNull() | dropoff.isNull(), "bad_timestamp")
              .when((duration <= 0) | (duration > cfg["max_trip_minutes"]), "bad_duration")
              .when(F.col("fare_amount").isNull() | (F.col("fare_amount") < 0)
                    | (F.col("fare_amount") > cfg["max_fare"]), "bad_fare")
              .when(F.col("trip_distance").isNull() | (F.col("trip_distance") < 0)
                    | (F.col("trip_distance") > cfg["max_trip_miles"]), "bad_distance"))
    return (df.withColumn("reject_reason", reason)
              .withColumn("route_key", F.concat_ws("-", "pickup_zone", "dropoff_zone"))
              .withColumn("trip_duration_min", F.round(duration, 3))
              .withColumn("slow_trip_bit", (duration > cfg["slow_trip_minutes"]).cast("int")))


class Stage1:
    def __init__(self, cfg):
        self.cfg = cfg
        self.out = run_dir(cfg)
        self.bloom = BloomFilter(cfg["bloom_expected_items"], cfg["bloom_false_positive_target"])
        self.exact_seen = set()          # evaluation baseline only
        self.keyed = cfg["partition_key"] == "pickup_zone"
        self.producer = Producer({"bootstrap.servers": cfg["bootstrap_servers"], "linger.ms": 5,
                                  "queue.buffering.max.messages": 1000000, "compression.type": "lz4"})
        self.tracker = OffsetTracker(cfg, TOPIC_RAW, "stage1")
        self.stats = {"accepted": 0, "rejected": 0, "reject_reasons": {}, "bloom_tp": 0, "bloom_fp": 0,
                      "bloom_fn": 0, "bloom_tn": 0, "fp_examples": [], "batches": 0, "max_raw_lag": 0,
                      "lag_samples": []}
        self.eos_seen = set()
        self.eos_expected = None
        self.eos_sent = False
        self.first_ts = None
        self.busy_s = 0.0
        self.last_stats_write = 0
        self.processed_total = 0
        self.timeline = open(self.out / "stage1_batches.csv", "w")
        self.timeline.write("batch_id,t_start,t_end,rows,cumulative,lag_raw\n")

    def process(self, batch_df, batch_id):
        t0 = time.time()
        pdf = batch_df.toPandas()
        t_pd = time.time()
        if pdf.empty:
            return
        self.first_ts = self.first_ts or t0
        pdf = pdf.sort_values(["kafka_ts", "kafka_partition", "kafka_offset"], kind="stable")
        st = self.stats
        data_rows = 0
        for row in pdf.itertuples(index=False):
            if row.control == "EOS":
                self.eos_seen.add(row.kafka_partition)
                self.eos_expected = int(row.num_partitions)
                continue
            if row.control == "WARMUP":  # forward so Stage 2/3 warm up too; not counted anywhere
                self._produce(json.dumps({"control": "WARMUP", "event_id": row.event_id,
                                          "event_time": row.event_time,
                                          "producer_ts_ms": int(row.producer_ts_ms)}).encode())
                continue
            data_rows += 1
            if row.reject_reason is not None:
                st["rejected"] += 1
                st["reject_reasons"][row.reject_reason] = st["reject_reasons"].get(row.reject_reason, 0) + 1
                continue
            bloom_seen = self.bloom.test_and_add(row.event_id)
            truly_seen = row.event_id in self.exact_seen
            self.exact_seen.add(row.event_id)
            if bloom_seen and truly_seen:
                st["bloom_tp"] += 1
            elif bloom_seen:
                st["bloom_fp"] += 1
                if len(st["fp_examples"]) < 10:
                    st["fp_examples"].append({"event_id": row.event_id, "event_time": row.event_time,
                                              "inserted_before": self.bloom.inserted - 1})
            elif truly_seen:
                st["bloom_fn"] += 1
            else:
                st["bloom_tn"] += 1
            ev = {f: _clean(getattr(row, f), f in INT_FIELDS) for f in OUT_FIELDS}
            ev["bloom_seen_before"] = int(bloom_seen)
            ev["gt_seen_before"] = int(truly_seen)
            ev["stage1_ts_ms"] = int(time.time() * 1000)
            key = str(ev["pickup_zone"]).encode() if self.keyed else None
            self._produce(json.dumps(ev).encode(), key)
            st["accepted"] += 1

        t_loop = time.time()
        self.producer.flush(30)
        t_flush = time.time()
        next_offsets = (pdf.groupby("kafka_partition")["kafka_offset"].max() + 1).to_dict()
        self.tracker.commit({int(p): int(o) for p, o in next_offsets.items()})
        lag = self.tracker.lag(self.committed_positions(next_offsets))
        st["max_raw_lag"] = max(st["max_raw_lag"], lag)
        st["lag_samples"].append([round(time.time(), 1), lag])
        st["batches"] += 1
        self.busy_s += time.time() - t0
        if data_rows:
            self.processed_total += data_rows
            self.timeline.write(f"{batch_id},{t0:.3f},{time.time():.3f},{data_rows},{self.processed_total},{lag}\n")
            self.timeline.flush()
        print(f"[stage1] batch {batch_id}: rows={len(pdf)} spark+toPandas={t_pd - t0:.2f}s "
              f"bloom+produce={t_loop - t_pd:.2f}s flush={t_flush - t_loop:.2f}s lag_raw={lag}", flush=True)

        if self.eos_expected and len(self.eos_seen) >= self.eos_expected and not self.eos_sent:
            self.send_eos(pdf)
        if time.time() - self.last_stats_write > 5 or self.eos_sent:
            self.write_stats()

    def committed_positions(self, latest):
        self._pos = getattr(self, "_pos", {})
        for p, o in latest.items():
            self._pos[int(p)] = max(self._pos.get(int(p), 0), int(o))
        return self._pos

    def _produce(self, value, key=None, partition=None):
        while True:
            try:
                if partition is None:
                    self.producer.produce(TOPIC_CLEAN, value=value, key=key)
                else:
                    self.producer.produce(TOPIC_CLEAN, value=value, partition=partition)
                return
            except BufferError:
                self.producer.poll(0.05)

    def send_eos(self, pdf):
        eos_rows = pdf[pdf["control"] == "EOS"]
        eos_time = eos_rows["event_time"].iloc[0] if len(eos_rows) else None
        md = self.producer.list_topics(TOPIC_CLEAN, timeout=10)
        parts = sorted(md.topics[TOPIC_CLEAN].partitions)
        for p in parts:
            self._produce(json.dumps({
                "control": "EOS", "event_id": f"EOS-{p}", "event_time": eos_time,
                "producer_ts_ms": int(time.time() * 1000), "stage1_ts_ms": int(time.time() * 1000),
                "num_partitions": len(parts), "s1_accepted": self.stats["accepted"],
                "s1_rejected": self.stats["rejected"]}).encode(), partition=p)
        self.producer.flush(30)
        self.eos_sent = True
        print(f"[stage1] EOS forwarded. accepted={self.stats['accepted']} rejected={self.stats['rejected']}",
              flush=True)

    def write_stats(self):
        st = dict(self.stats)
        processed = st["accepted"] + st["rejected"]
        wall = time.time() - self.first_ts if self.first_ts else 0
        negatives = st["bloom_fp"] + st["bloom_tn"]
        positives = st["bloom_tp"] + st["bloom_fn"]
        st.update({
            "processed": processed,
            "stage1_eps_wall": round(processed / wall, 1) if wall else None,
            "stage1_busy_s": round(self.busy_s, 2),
            "bloom": {**self.bloom.params(),
                      "fp_rate": round(st["bloom_fp"] / negatives, 6) if negatives else 0.0,
                      "fn_rate": round(st["bloom_fn"] / positives, 6) if positives else 0.0},
            "exact_set_ids": len(self.exact_seen),
            "eos_forwarded": self.eos_sent,
        })
        (self.out / "stage1_stats.json").write_text(json.dumps(st, indent=1))
        self.last_stats_write = time.time()


def _clean(v, as_int=False):
    """pandas -> JSON-safe value (NaN -> null; nullable ints arrive as floats)."""
    if v is None:
        return None
    if hasattr(v, "item"):  # numpy scalar
        v = v.item()
    if isinstance(v, float) and math.isnan(v):
        return None
    return int(v) if as_int else v


def main():
    cfg = load_config()
    spark = get_spark(cfg, "stage1")
    stage = Stage1(cfg)
    query = (build_query(spark, cfg).writeStream
             .foreachBatch(stage.process)
             .option("checkpointLocation", str(checkpoint_dir(cfg, "stage1")))
             .start())
    mark_ready(run_dir(cfg), "stage1")
    print(f"[stage1] running: bloom m={stage.bloom.m} bits k={stage.bloom.k} slots={cfg['parallelism']}",
          flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
