"""Stage 3 - MinHash-LSH on pickup-zone destination sets   (taxi.clean -> taxi.lsh)

Spark builds, for every 60-minute event-time window (with the same watermark
as Stage 2), the destination set of each pickup zone:

    S(P) = { dropoff_zone observed for pickup zone P during the window }

When a window closes (append mode), foreachBatch on the driver:
  1. keeps zones with >= lsh_min_trips trips
  2. MinHash signature (lsh_num_hashes values) per destination set
  3. banding (bands x rows) -> candidate pairs from shared buckets
  4. exact Jaccard only for candidates, keep pairs with J >= jaccard_threshold
  5. evaluation only: exact all-pairs Jaccard -> true similar pairs
and publishes recall / candidate precision / candidate reduction / latencies
for the default setting and every extra (bands, rows) setting.
"""
import csv
import json
import sys
import time
from itertools import combinations
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from producer.pipeline_common import (ROOT, TOPIC_CLEAN, TOPIC_LSH, checkpoint_dir, load_config,
                                      run_dir, CLEAN_SCHEMA, get_spark, mark_ready, read_kafka)  # noqa: E402
from stage3_lsh.minhash_lsh import MinHasher, jaccard, lsh_candidates, lsh_threshold  # noqa: E402


def zone_names():
    path = Path(__file__).resolve().parent / "taxi_zone_lookup.csv"
    if not path.exists():
        return {}
    with open(path, newline="") as f:
        return {int(r["LocationID"]): f'{r["Zone"]} ({r["Borough"]})' for r in csv.DictReader(f)}


class Stage3:
    def __init__(self, cfg):
        self.cfg = cfg
        self.out = run_dir(cfg)
        self.minhash = MinHasher(int(cfg["lsh_num_hashes"]), seed=int(cfg["random_seed"]))
        self.settings = [(int(cfg["lsh_bands"]), int(cfg["lsh_rows_per_band"]))]
        for b, r in cfg.get("lsh_extra_settings") or []:
            assert int(b) * int(r) == int(cfg["lsh_num_hashes"]), f"{b}x{r} must use all hash values"
            self.settings.append((int(b), int(r)))
        self.threshold = float(cfg["jaccard_threshold"])
        self.min_trips = int(cfg["lsh_min_trips"])
        self.names = zone_names()
        self.producer = Producer({"bootstrap.servers": cfg["bootstrap_servers"], "linger.ms": 5})
        self.windows = 0
        self.last_batch = -1

    def process(self, batch_df, batch_id):
        if batch_id <= self.last_batch:
            return
        pdf = batch_df.toPandas()
        pdf = pdf[pdf["pickup_zone"].notna()]  # end-of-stream marker rows (kept until here so they move the watermark)
        for (ws, we), g in pdf.groupby(["window_start", "window_end"]):
            self.process_window(ws, we, g)
        self.last_batch = batch_id

    def process_window(self, ws, we, g):
        active = g[g["n"] >= self.min_trips]
        sets = {int(r.pickup_zone): set(int(x) for x in r.dsts) for r in active.itertuples(index=False)}
        zones = sorted(sets)
        n = len(zones)
        all_pairs = n * (n - 1) // 2

        # ---- ground truth: exact all-pairs Jaccard (evaluation only)
        t0 = time.perf_counter()
        exact = {}
        for p, q in combinations(zones, 2):
            j = jaccard(sets[p], sets[q])
            if j >= self.threshold:
                exact[(p, q)] = j
        exact_ms = (time.perf_counter() - t0) * 1000
        true_pairs = set(exact)

        results = []
        default_pairs = []
        for bands, rows in self.settings:
            t0 = time.perf_counter()
            sigs = {z: self.minhash.signature(sets[z]) for z in zones}
            t_sig = time.perf_counter()
            cands = lsh_candidates(sigs, bands, rows)
            t_band = time.perf_counter()
            verified = {}
            for p, q in cands:
                verified[(p, q)] = jaccard(sets[p], sets[q])
            returned = {pq for pq, j in verified.items() if j >= self.threshold}
            t_end = time.perf_counter()
            tp = len(returned & true_pairs)
            res = {
                "bands": bands, "rows": rows, "approx_threshold": round(lsh_threshold(bands, rows), 3),
                "candidates": len(cands), "returned_pairs": len(returned), "true_pairs": len(true_pairs),
                "true_in_candidates": tp,
                "recall": round(tp / len(true_pairs), 4) if true_pairs else None,
                "candidate_precision": round(tp / len(cands), 4) if cands else None,
                "candidate_reduction": round(1 - len(cands) / all_pairs, 4) if all_pairs else None,
                "lsh_latency_ms": round((t_end - t0) * 1000, 3),
                "signature_ms": round((t_sig - t0) * 1000, 3),
                "banding_ms": round((t_band - t_sig) * 1000, 3),
                "verify_ms": round((t_end - t_band) * 1000, 3),
            }
            results.append(res)
            if (bands, rows) == self.settings[0]:
                default_pairs = sorted(
                    ({"p": p, "q": q, "p_name": self.names.get(p), "q_name": self.names.get(q),
                      "jaccard": round(j, 4), "similar": j >= self.threshold, "true_similar": (p, q) in true_pairs}
                     for (p, q), j in verified.items()), key=lambda x: -x["jaccard"])
        missed = sorted(true_pairs - {(d["p"], d["q"]) for d in default_pairs if d["similar"]})

        d = results[0]
        msg = {
            "type": "lsh", "run_id": self.cfg["run_id"], "report_ts_ms": int(time.time() * 1000),
            "window_start": pd.Timestamp(ws).isoformat(), "window_end": pd.Timestamp(we).isoformat(),
            "zones_in_window": int(len(g)), "active_zones": n, "all_pairs": all_pairs,
            "exact_latency_ms": round(exact_ms, 3), "jaccard_threshold": self.threshold,
            "candidates": d["candidates"], "true_pairs": d["true_pairs"], "recall": d["recall"],
            "candidate_precision": d["candidate_precision"], "candidate_reduction": d["candidate_reduction"],
            "lsh_latency_ms": d["lsh_latency_ms"],
            "settings": results,
            "candidate_pairs": default_pairs,
            "missed_true_pairs": [{"p": p, "q": q, "jaccard": round(exact[(p, q)], 4)} for p, q in missed],
        }
        self.producer.produce(TOPIC_LSH, value=json.dumps(msg).encode())
        self.producer.flush(10)
        self.windows += 1
        print(f"[stage3] window {msg['window_start']} zones={n} pairs={all_pairs} true={d['true_pairs']} "
              + " | ".join(f"{r['bands']}x{r['rows']}: cand={r['candidates']} recall={r['recall']}"
                           for r in results), flush=True)


def main():
    cfg = load_config()
    spark = get_spark(cfg, "stage3")
    stage = Stage3(cfg)
    clean = read_kafka(spark, cfg, TOPIC_CLEAN, CLEAN_SCHEMA)
    sets = (clean.withColumn("ts", F.to_timestamp("event_time"))
            .withWatermark("ts", f"{int(cfg['watermark_minutes'])} minutes")
            .groupBy(F.window("ts", f"{int(cfg['window_minutes'])} minutes").alias("w"), "pickup_zone")
            .agg(F.count("*").alias("n"), F.collect_set("dropoff_zone").alias("dsts"))
            .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                    "pickup_zone", "n", "dsts"))
    query = (sets.writeStream.outputMode("append").foreachBatch(stage.process)
             .option("checkpointLocation", str(checkpoint_dir(cfg, "stage3"))).start())
    mark_ready(run_dir(cfg), "stage3")
    print(f"[stage3] running: settings={stage.settings} threshold={stage.threshold} "
          f"min_trips={stage.min_trips}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
