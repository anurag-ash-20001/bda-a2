"""Stage 2 - Stream analytics   (taxi.clean -> taxi.metrics)

Three Spark Structured Streaming queries share one SparkSession and all read
taxi.clean:

  A. per-event (foreachBatch, arrival order)
       reservoir sampling (k = 100/1000/10000) vs exact streaming mean
       DGIM over slow_trip_bit (N = dgim_max_window) vs exact last-N bits
       Bloom evaluation (FP/FN rates from Stage 1's flags vs ground truth)
       latency p50/p95, throughput, taxi.clean consumer lag, late-event classes
       -> periodic {"type": "periodic"} summary every report_interval_seconds
  B. event-time windows per (window, pickup_zone) with watermark
       exact / event-sampled / entity-sampled destination sets
       -> {"type": "diversity"} per closed window
  C. event-time windows per window with watermark
       HyperLogLog++ (approx_count_distinct, precision p) vs exact distinct routes
       -> {"type": "window"} per closed window

Restart behaviour: Spark restores Kafka offsets and the state of B/C from the
checkpoint. Query A's Python state is pickled after every batch together with
the batch id; a batch replayed after a crash (at-least-once foreachBatch) is
detected and skipped, so the sketches never count an event twice.
"""
import json
import math
import os
import pickle
import sys
import threading
import time
from collections import deque
from pathlib import Path

import mmh3
import numpy as np
import pandas as pd
from confluent_kafka import Producer
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from producer.pipeline_common import (TOPIC_CLEAN, TOPIC_METRICS, checkpoint_dir, load_config,
                                      run_dir, OffsetTracker, CLEAN_SCHEMA, get_spark, mark_ready,
                                      read_kafka)  # noqa: E402
from stage1_clean_bloom.bloom import BloomFilter  # noqa: E402
from stage2_stream_analytics.dgim import DGIM  # noqa: E402
from stage2_stream_analytics.reservoir import Reservoir  # noqa: E402


def pct_err(est, exact):
    return round(abs(est - exact) / exact * 100, 3) if exact else (0.0 if est == 0 else None)


def hll_rsd(p):
    """Spark's HLL++ picks p = ceil(2 log2(1.106 / rsd)); invert it (slightly above the boundary)."""
    return 1.106 / 2 ** (p / 2) * 1.001


# =========================================================================== query A
class EventState:
    """Everything query A keeps between batches (pickled for restart)."""

    def __init__(self, cfg):
        self.last_batch_id = -1
        self.events = 0
        self.first_wall = None
        self.prod_ts_min = self.prod_ts_max = None
        self.s1_ts_min = self.s1_ts_max = None
        seed = cfg["random_seed"]
        self.reservoirs = {int(k): Reservoir(k, 2, seed=seed + i) for i, k in enumerate(cfg["reservoir_sizes"])}
        self.sum_fare = self.sum_dist = 0.0
        self.dgim = DGIM(cfg["dgim_max_window"])
        self.exact_bits = deque(maxlen=int(cfg["dgim_max_window"]))
        self.bloom = {"tp": 0, "fp": 0, "fn": 0, "tn": 0, "inserted": 0}
        self.latencies = []           # per-event end-to-end latency (ms)
        self.interval_latencies = []
        self.max_event_time = None    # mirrors Spark's watermark input (max event time seen)
        self.late = {"on_time": 0, "late_included": 0, "late_dropped": 0,
                     "lateness_buckets_included": {"<1min": 0, "1-10min": 0, "10-30min": 0, ">=30min": 0},
                     "examples": []}
        self.max_lag = 0
        self.lag = 0
        self.positions = {}
        self.eos_partitions = set()
        self.eos_expected = None
        self.s1_counts = {}
        self.reports = 0
        self.replayed_batches_skipped = 0


class Stage2:
    def __init__(self, cfg, spark):
        self.cfg = cfg
        self.spark = spark
        self.out = run_dir(cfg)
        self.state_file = Path(cfg["work_dir"]) / "state" / cfg["run_id"] / "stage2_eventstate.pkl"
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        if not (self.out / "stage2_batches.csv").exists():
            (self.out / "stage2_batches.csv").write_text("batch_id,t_start,t_end,rows,cumulative,lag_clean\n")
        if self.state_file.exists():
            with open(self.state_file, "rb") as f:
                self.st = pickle.load(f)
            print(f"[stage2] restored state after batch {self.st.last_batch_id}: "
                  f"{self.st.events} events", flush=True)
            self.restored = True
        else:
            self.st = EventState(cfg)
            self.restored = False
        self.window_batches = self._load_json("window_batches.json", {"B": -1, "C": -1})
        self.bloom_params = BloomFilter(cfg["bloom_expected_items"], cfg["bloom_false_positive_target"])
        self.producer = Producer({"bootstrap.servers": cfg["bootstrap_servers"], "linger.ms": 5})
        self.tracker = OffsetTracker(cfg, TOPIC_CLEAN, "stage2")
        self.last_report = time.time()
        self.window_ms = int(cfg["window_minutes"]) * 60_000
        self.wm_ms = int(cfg["watermark_minutes"]) * 60_000
        self.dropped_by_spark = {}     # query name -> {batchId: numRowsDroppedByWatermark}
        self.windows_reported = {"diversity": 0, "hll": 0}
        self.lock = threading.Lock()

    # ---------------------------------------------------------------- persistence
    def _load_json(self, name, default):
        p = self.state_file.parent / name
        return json.loads(p.read_text()) if p.exists() else default

    def _save_json(self, name, obj):
        p = self.state_file.parent / name
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(obj))
        os.replace(tmp, p)

    def save_state(self):
        tmp = self.state_file.with_suffix(".tmp")
        with open(tmp, "wb") as f:
            pickle.dump(self.st, f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(tmp, self.state_file)

    def publish(self, msg):
        msg["run_id"] = self.cfg["run_id"]
        msg["report_ts_ms"] = int(time.time() * 1000)
        self.producer.produce(TOPIC_METRICS, value=json.dumps(msg).encode())
        self.producer.flush(10)

    # ---------------------------------------------------------------- query A
    def process_events(self, batch_df, batch_id):
        with self.lock:
            self._process_events(batch_df, batch_id)

    def _process_events(self, batch_df, batch_id):
        st = self.st
        if batch_id <= st.last_batch_id:
            st.replayed_batches_skipped += 1
            print(f"[stage2] batch {batch_id} already applied before restart -> skipped (no double counting)",
                  flush=True)
            return
        pdf = batch_df.select("kafka_partition", "kafka_offset", "kafka_ts", "control", "event_id",
                              "event_time", "fare_amount", "trip_distance", "slow_trip_bit",
                              "bloom_seen_before", "gt_seen_before", "producer_ts_ms", "stage1_ts_ms",
                              "num_partitions", "s1_accepted", "s1_rejected").toPandas()
        now_ms = int(time.time() * 1000)
        if pdf.empty:
            st.last_batch_id = batch_id
            self.maybe_report()
            return
        st.first_wall = st.first_wall or time.time()
        pdf = pdf.sort_values(["kafka_ts", "kafka_partition", "kafka_offset"], kind="stable")

        ctrl = pdf[pdf["control"] == "EOS"]
        for row in ctrl.itertuples(index=False):
            st.eos_partitions.add(int(row.kafka_partition))
            st.eos_expected = int(row.num_partitions)
            st.s1_counts = {"accepted": int(row.s1_accepted), "rejected": int(row.s1_rejected)}
        ev = pdf[pdf["control"].isna()]  # drop EOS / WARMUP control records
        n = len(ev)

        if n:
            st.events += n
            prod = ev["producer_ts_ms"].to_numpy(dtype=np.int64)
            s1 = ev["stage1_ts_ms"].to_numpy(dtype=np.int64)
            st.prod_ts_min = int(prod.min()) if st.prod_ts_min is None else min(st.prod_ts_min, int(prod.min()))
            st.prod_ts_max = int(prod.max()) if st.prod_ts_max is None else max(st.prod_ts_max, int(prod.max()))
            st.s1_ts_min = int(s1.min()) if st.s1_ts_min is None else min(st.s1_ts_min, int(s1.min()))
            st.s1_ts_max = int(s1.max()) if st.s1_ts_max is None else max(st.s1_ts_max, int(s1.max()))
            lat = (now_ms - prod).tolist()
            st.latencies.extend(lat)
            st.interval_latencies.extend(lat)

            # Bloom evaluation (flags computed by Stage 1)
            b = ev["bloom_seen_before"].to_numpy(dtype=np.int64)
            g = ev["gt_seen_before"].to_numpy(dtype=np.int64)
            st.bloom["tp"] += int(((b == 1) & (g == 1)).sum())
            st.bloom["fp"] += int(((b == 1) & (g == 0)).sum())
            st.bloom["fn"] += int(((b == 0) & (g == 1)).sum())
            st.bloom["tn"] += int(((b == 0) & (g == 0)).sum())
            st.bloom["inserted"] += int((b == 0).sum())

            # exact streaming means + reservoirs + DGIM, in arrival order
            fare = ev["fare_amount"].to_numpy(dtype=np.float64)
            dist = ev["trip_distance"].to_numpy(dtype=np.float64)
            bits = ev["slow_trip_bit"].to_numpy(dtype=np.int64)
            st.sum_fare += float(fare.sum())
            st.sum_dist += float(dist.sum())
            reservoirs = list(st.reservoirs.values())
            for f_, d_, bit in zip(fare, dist, bits):
                row = (f_, d_)
                for r in reservoirs:
                    r.add(row)
                st.dgim.add(int(bit))
                st.exact_bits.append(int(bit))

            self.classify_late(ev)

        next_offsets = (pdf.groupby("kafka_partition")["kafka_offset"].max() + 1).to_dict()
        for p, o in next_offsets.items():
            st.positions[int(p)] = max(st.positions.get(int(p), 0), int(o))
        self.tracker.commit(st.positions)
        st.lag = self.tracker.lag(st.positions)
        st.max_lag = max(st.max_lag, st.lag)
        st.last_batch_id = batch_id

        eos = st.eos_expected is not None and len(st.eos_partitions) >= st.eos_expected
        self.maybe_report(final=eos)
        self.save_state()
        print(f"[stage2] batch {batch_id}: rows={len(pdf)} took={time.time() - now_ms / 1000:.2f}s "
              f"lag_clean={st.lag}", flush=True)
        if n:
            with open(self.out / "stage2_batches.csv", "a") as f:
                f.write(f"{batch_id},{now_ms / 1000:.3f},{time.time():.3f},{n},{st.events},{st.lag}\n")

    def classify_late(self, ev):
        """Classify each event against the watermark Spark uses in queries B/C.

        Spark computes the watermark at the end of a batch as
            max(event_time seen) - watermark_minutes
        and drops a row from a window aggregation when its window end <= watermark.
        on_time       : event_time >= max event time seen so far
        late_included : out of order, but its window is still open -> counted
        late_dropped  : its window was already closed -> discarded by Spark"""
        st = self.st
        t = pd.to_datetime(ev["event_time"]).astype("int64").to_numpy() // 1_000_000  # ms
        prev_max = st.max_event_time if st.max_event_time is not None else int(t[0])
        watermark = prev_max - self.wm_ms  # fixed for the whole batch, like Spark
        running_max = np.maximum.accumulate(np.concatenate([[prev_max], t]))[:-1]
        win_end = (t // self.window_ms + 1) * self.window_ms
        on_time = t >= running_max
        dropped = (~on_time) & (win_end <= watermark)
        included = (~on_time) & ~dropped
        st.late["on_time"] += int(on_time.sum())
        st.late["late_included"] += int(included.sum())
        st.late["late_dropped"] += int(dropped.sum())
        lateness_min = (running_max - t)[included] / 60000
        buckets = st.late["lateness_buckets_included"]
        for name, lo, hi in (("<1min", 0, 1), ("1-10min", 1, 10), ("10-30min", 10, 30), (">=30min", 30, 1e18)):
            buckets[name] += int(((lateness_min >= lo) & (lateness_min < hi)).sum())
        ids = ev["event_id"].to_numpy()
        ids_all = ev["event_id"].to_numpy()
        if dropped.any():  # evaluation: joined with producer_delayed.csv by final_consumer/analyze_results.py
            with open(self.out / "late_dropped_ids.csv", "a") as f:
                for i in np.flatnonzero(dropped):
                    f.write(f"{ids_all[i]},{pd.Timestamp(int(t[i]), unit='ms').isoformat()},"
                            f"{(running_max[i] - t[i]) / 60000:.2f}\n")
        notable = included & ((running_max - t) >= 10 * 60000)  # examples: >= 10 min late but still counted
        for kind, mask in (("late_included", notable), ("late_dropped", dropped)):
            have = sum(1 for e in st.late["examples"] if e["class"] == kind)
            for i in np.flatnonzero(mask)[: max(0, 5 - have)]:
                st.late["examples"].append({
                    "class": kind, "event_id": ids[i],
                    "event_time": pd.Timestamp(int(t[i]), unit="ms").isoformat(),
                    "max_event_time_seen": pd.Timestamp(int(running_max[i]), unit="ms").isoformat(),
                    "lateness_min": round((running_max[i] - t[i]) / 60000, 2),
                    "window_end": pd.Timestamp(int(win_end[i]), unit="ms").isoformat(),
                    "watermark": pd.Timestamp(int(watermark), unit="ms").isoformat()})
        st.max_event_time = int(max(prev_max, t.max()))

    def maybe_report(self, final=False):
        if not final and time.time() - self.last_report < self.cfg["report_interval_seconds"]:
            return
        if self.st.events == 0:
            return
        self.publish(self.periodic_report(final))
        self.st.interval_latencies = []
        self.last_report = time.time()
        self.write_stats()
        if final:
            print(f"[stage2] EOS reached: final report published ({self.st.events} events)", flush=True)

    def periodic_report(self, final):
        st, cfg = self.st, self.cfg
        exact_fare = st.sum_fare / st.events
        exact_dist = st.sum_dist / st.events
        by_k = []
        for k, r in st.reservoirs.items():
            sf, sd = r.mean(0), r.mean(1)
            by_k.append({"k": k, "sample_size": min(k, r.n), "exact_mean_fare": round(exact_fare, 4),
                         "sample_mean_fare": round(sf, 4), "fare_error_pct": pct_err(sf, exact_fare),
                         "exact_mean_distance": round(exact_dist, 4), "sample_mean_distance": round(sd, 4),
                         "distance_error_pct": pct_err(sd, exact_dist)})
        rk = next((x for x in by_k if x["k"] == int(cfg["reservoir_report_k"])), by_k[0])

        bits = list(st.exact_bits)
        dgim_by_k = []
        for k in cfg["dgim_queries"]:
            exact = sum(bits[-int(k):])
            est = st.dgim.query(int(k))
            dgim_by_k.append({"k": int(k), "exact_ones": exact, "estimate": est,
                              "abs_error": abs(est - exact), "error_pct": pct_err(est, exact),
                              "window_filled": len(bits) >= int(k)})
        dk = next((x for x in dgim_by_k if x["k"] == int(cfg["dgim_report_k"])), dgim_by_k[0])

        b = st.bloom
        negatives, positives = b["fp"] + b["tn"], b["tp"] + b["fn"]
        lat = np.asarray(st.latencies)
        ilat = np.asarray(st.interval_latencies) if st.interval_latencies else lat
        wall = time.time() - st.first_wall
        prod_span = (st.prod_ts_max - st.prod_ts_min) / 1000 if st.prod_ts_max != st.prod_ts_min else None
        s1_span = (st.s1_ts_max - st.s1_ts_min) / 1000 if st.s1_ts_max != st.s1_ts_min else None
        return {
            "type": "periodic",
            "final": bool(final),
            "events_processed": st.events,
            "producer_eps": round(st.events / prod_span, 1) if prod_span else None,
            "stage1_eps": round(st.events / s1_span, 1) if s1_span else None,
            "stage2_eps": round(st.events / wall, 1) if wall > 0 else None,
            "consumer_lag": st.lag,
            "max_consumer_lag": st.max_lag,
            "latency_p50_ms": int(np.percentile(lat, 50)),
            "latency_p95_ms": int(np.percentile(lat, 95)),
            "latency_interval_p50_ms": int(np.percentile(ilat, 50)),
            "latency_interval_p95_ms": int(np.percentile(ilat, 95)),
            "max_event_time": pd.Timestamp(st.max_event_time, unit="ms").isoformat() if st.max_event_time else None,
            "bloom": {"fp_rate": round(b["fp"] / negatives, 6) if negatives else 0.0,
                      "fn_rate": round(b["fn"] / positives, 6) if positives else 0.0,
                      "false_positives": b["fp"], "false_negatives": b["fn"],
                      "true_duplicates_flagged": b["tp"], "true_duplicates": positives,
                      "inserted_ids": b["inserted"], "bit_array_size": self.bloom_params.m,
                      "num_hashes": self.bloom_params.k, "memory_bytes": len(self.bloom_params.bits)},
            "sampling": {"exact_mean_fare": rk["exact_mean_fare"], "sample_mean_fare": rk["sample_mean_fare"],
                         "error_pct": rk["fare_error_pct"], "k": rk["k"], "by_k": by_k},
            "dgim": {"k": dk["k"], "exact_ones": dk["exact_ones"], "estimate": dk["estimate"],
                     "error_pct": dk["error_pct"], "buckets": st.dgim.num_buckets(), "N": st.dgim.N,
                     "by_k": dgim_by_k},
            "late_events": {k: v for k, v in st.late.items() if k != "examples"},
            "spark_rows_dropped_by_watermark": {q: sum(v.values()) for q, v in self.dropped_by_spark.items()},
            "replayed_batches_skipped": st.replayed_batches_skipped,
            "stage1_counts": st.s1_counts,
        }

    def write_stats(self):  # called with self.lock held (from query A)
        stats = {"events": self.st.events, "late": self.st.late, "max_lag": self.st.max_lag,
                 "replayed_batches_skipped": self.st.replayed_batches_skipped,
                 "restored_from_snapshot": self.restored,
                 "windows_reported": self.windows_reported,
                 "spark_rows_dropped_by_watermark": {q: sum(v.values()) for q, v in self.dropped_by_spark.items()}}
        (self.out / "stage2_stats.json").write_text(json.dumps(stats, indent=1, default=str))

    # ---------------------------------------------------------------- query B
    def process_diversity(self, batch_df, batch_id):
        if batch_id <= self.window_batches["B"]:
            return
        pdf = batch_df.toPandas()
        pdf = pdf[pdf["pickup_zone"].notna()]
        cfg = self.cfg
        thr = int(cfg["diversity_min_destinations"])
        rate = float(cfg["sampling_rate"])
        for (ws, we), g in pdf.groupby(["window_start", "window_end"]):
            g = g.copy()
            g["n_dst"] = g["dsts"].apply(len)
            g["n_dst_sampled"] = g["sampled_dsts"].apply(len)
            exact = float((g["n_dst"] >= thr).mean())

            # event sampling: Bernoulli(rate) on event_id (deterministic hash), property from sampled events only
            es = g[g["n_sampled"] > 0]
            ev_est = float((es["n_dst_sampled"] >= thr).mean()) if len(es) else 0.0
            budget = int(g["n_sampled"].sum())

            # entity sampling with the same storage budget: zones in hash order until the
            # events kept (all events of each selected zone) would exceed the budget
            g["h"] = g["pickup_zone"].apply(lambda z: mmh3.hash(str(int(z)), cfg["random_seed"], signed=False))
            order = g.sort_values("h")
            cum = order["n"].cumsum()
            chosen = order[cum <= budget] if (cum <= budget).any() else order.head(1)
            en_est = float((chosen["n_dst"] >= thr).mean())

            # entity sampling at the same *rate* (fixed hash threshold), for reference
            fixed = g[g["h"] < rate * 2 ** 32]
            fx_est = float((fixed["n_dst"] >= thr).mean()) if len(fixed) else None

            self.publish({
                "type": "diversity", "window_start": _iso(ws), "window_end": _iso(we),
                "active_zones": int(len(g)), "events_in_window": int(g["n"].sum()),
                "exact_fraction_diverse": round(exact, 4),
                "event_sampling": {"estimate": round(ev_est, 4), "abs_error": round(abs(ev_est - exact), 4),
                                   "stored_events": budget, "zones_observed": int(len(es)),
                                   "sampling_rate": rate},
                "entity_sampling": {"estimate": round(en_est, 4), "abs_error": round(abs(en_est - exact), 4),
                                    "stored_events": int(chosen["n"].sum()), "zones_selected": int(len(chosen))},
                "entity_sampling_fixed_rate": {
                    "estimate": None if fx_est is None else round(fx_est, 4),
                    "abs_error": None if fx_est is None else round(abs(fx_est - exact), 4),
                    "stored_events": int(fixed["n"].sum()), "zones_selected": int(len(fixed))},
            })
            self.windows_reported["diversity"] += 1
        self.window_batches["B"] = batch_id
        self._save_json("window_batches.json", self.window_batches)

    # ---------------------------------------------------------------- query C
    def process_hll(self, batch_df, batch_id):
        if batch_id <= self.window_batches["C"]:
            return
        p = int(self.cfg["hll_precision"])
        m = 2 ** p
        # toPandas (Arrow) honours spark.sql.session.timeZone=UTC; collect() would convert
        # window timestamps to the machine's local time zone
        for r in batch_df.toPandas().to_dict("records"):
            if int(r["events"]) == 0:  # window holding only the end-of-stream marker
                continue
            exact = int(r["exact_routes"])
            est = int(r["hll_estimate"])
            self.publish({
                "type": "window", "window_start": _iso(r["window_start"]), "window_end": _iso(r["window_end"]),
                "events_in_window": int(r["events"]),
                "hll": {"exact_routes": exact, "estimate": est, "error_pct": pct_err(est, exact),
                        "precision": p, "registers": m, "relative_sd": round(1.04 / math.sqrt(m), 5),
                        # Spark HLL++ packs 6-bit registers into 64-bit words
                        "sketch_bytes": int(math.ceil(m / 10) * 8),
                        # exact set of route strings: ~ (string object ~56 B + hash-set slot ~16 B) per key
                        "exact_set_bytes_est": exact * 72},
            })
            self.windows_reported["hll"] += 1
        self.window_batches["C"] = batch_id
        self._save_json("window_batches.json", self.window_batches)

    def track_progress(self, name, query):
        progress = query.recentProgress
        with self.lock:  # dropped_by_spark is read by query A's report
            for prog in progress:
                ops = prog.get("stateOperators") or []
                if ops:
                    self.dropped_by_spark.setdefault(name, {})[prog["batchId"]] = \
                        int(ops[0].get("numRowsDroppedByWatermark", 0))


def _iso(ts):
    return pd.Timestamp(ts).isoformat()


def main():
    cfg = load_config()
    spark = get_spark(cfg, "stage2")
    stage = Stage2(cfg, spark)
    clean = read_kafka(spark, cfg, TOPIC_CLEAN, CLEAN_SCHEMA)
    ckpt = checkpoint_dir(cfg, "stage2")

    q_a = (clean.writeStream.foreachBatch(stage.process_events)
           .option("checkpointLocation", str(ckpt / "events")).queryName("events").start())

    timed = (clean.withColumn("ts", F.to_timestamp("event_time"))
             .withWatermark("ts", f"{int(cfg['watermark_minutes'])} minutes"))
    win = F.window("ts", f"{int(cfg['window_minutes'])} minutes")
    rate_ppm = int(float(cfg["sampling_rate"]) * 1_000_000)
    sampled = F.pmod(F.xxhash64("event_id"), F.lit(1_000_000)) < rate_ppm

    diversity = (timed.withColumn("ev_sampled", sampled)
                 .groupBy(win.alias("w"), "pickup_zone")
                 .agg(F.count("*").alias("n"),
                      F.collect_set("dropoff_zone").alias("dsts"),
                      F.sum(F.col("ev_sampled").cast("int")).alias("n_sampled"),
                      F.collect_set(F.when(F.col("ev_sampled"), F.col("dropoff_zone"))).alias("sampled_dsts"))
                 .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                         "pickup_zone", "n", "dsts", "n_sampled", "sampled_dsts"))
    q_b = (diversity.writeStream.outputMode("append").foreachBatch(stage.process_diversity)
           .option("checkpointLocation", str(ckpt / "diversity")).queryName("diversity").start())

    # No filter before the aggregation: Spark pushes filters below the watermark operator,
    # which would hide the end-of-stream marker from it and leave the last windows open.
    # The marker row has route_key = null, so count/approx_count_distinct/collect_set ignore it.
    hll = (timed.groupBy(win.alias("w"))
           .agg(F.count("route_key").alias("events"),
                F.approx_count_distinct("route_key", hll_rsd(int(cfg["hll_precision"]))).alias("hll_estimate"),
                F.size(F.collect_set("route_key")).alias("exact_routes"))
           .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                   "events", "hll_estimate", "exact_routes"))
    q_c = (hll.writeStream.outputMode("append").foreachBatch(stage.process_hll)
           .option("checkpointLocation", str(ckpt / "hll")).queryName("hll").start())

    mark_ready(run_dir(cfg), "stage2")
    print(f"[stage2] running 3 queries, slots={cfg['parallelism']} watermark={cfg['watermark_minutes']}min "
          f"window={cfg['window_minutes']}min restored={stage.restored}", flush=True)
    while all(q.isActive for q in (q_a, q_b, q_c)):
        stage.track_progress("diversity", q_b)
        stage.track_progress("hll", q_c)
        time.sleep(2)
    for q in (q_a, q_b, q_c):
        if q.exception():
            print(f"[stage2] query {q.name} failed: {q.exception()}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
