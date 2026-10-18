"""Final Kafka consumer: taxi.metrics + taxi.lsh -> console + results.jsonl

One JSON object per event-time window is appended to results/results.jsonl
(all runs) and results/runs/<run_id>/results.jsonl. Each record joins:
  * the window's HLL result          (taxi.metrics, type=window)
  * the window's diversity sampling  (taxi.metrics, type=diversity)
  * the window's LSH result          (taxi.lsh)
  * the most recent periodic summary (taxi.metrics, type=periodic): throughput,
    lag, latency, Bloom, reservoir sampling, DGIM
When Stage 2 reports end-of-stream and no message arrives for
final_idle_timeout_seconds, incomplete windows are flushed, a run summary is
written (final_summary.json) and the consumer exits.
"""
import json
import sys
import time
from pathlib import Path

import numpy as np
from confluent_kafka import Consumer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.config import ROOT, TOPIC_LSH, TOPIC_METRICS, load_config, run_dir  # noqa: E402

PERIODIC_FIELDS = ["events_processed", "producer_eps", "stage1_eps", "stage2_eps", "consumer_lag",
                   "latency_p50_ms", "latency_p95_ms"]


class FinalConsumer:
    def __init__(self, cfg):
        self.cfg = cfg
        self.out_run = run_dir(cfg) / "results.jsonl"
        self.out_all = ROOT / "results" / "results.jsonl"
        self.out_run.write_text("")
        self.periodic = None
        self.final_periodic = None
        self.windows = {}
        self.emitted = set()
        self.records = []

    def handle(self, msg):
        t = msg.get("type")
        if t == "periodic":
            self.periodic = msg
            if msg.get("final"):
                self.final_periodic = msg
            return
        if t == "lsh":  # keep full candidate-pair detail (topics are wiped between runs)
            with open(run_dir(self.cfg) / "lsh_windows.jsonl", "a") as f:
                f.write(json.dumps(msg) + "\n")
        ws = msg["window_start"]
        slot = self.windows.setdefault(ws, {})
        slot[{"window": "hll", "diversity": "diversity", "lsh": "lsh"}[t]] = msg  # replay -> latest wins
        if ws not in self.emitted and all(k in slot for k in ("hll", "diversity", "lsh")):
            self.emit(ws)

    def emit(self, ws):
        slot = self.windows[ws]
        p = self.periodic or {}
        any_msg = slot.get("hll") or slot.get("lsh") or slot.get("diversity")
        rec = {"run_id": self.cfg["run_id"], "window_start": ws, "window_end": any_msg["window_end"]}
        rec.update({k: p.get(k) for k in PERIODIC_FIELDS})
        rec["bloom"] = p.get("bloom")
        rec["sampling"] = p.get("sampling")
        rec["hll"] = (slot.get("hll") or {}).get("hll")
        rec["dgim"] = p.get("dgim")
        lsh = slot.get("lsh")
        rec["lsh"] = None if lsh is None else {
            k: lsh[k] for k in ("candidates", "true_pairs", "recall", "candidate_precision",
                                "candidate_reduction", "lsh_latency_ms", "exact_latency_ms", "active_zones",
                                "all_pairs", "settings")}
        div = slot.get("diversity")
        rec["diversity"] = None if div is None else {
            k: div[k] for k in ("exact_fraction_diverse", "event_sampling", "entity_sampling",
                                "entity_sampling_fixed_rate", "active_zones")}
        rec["events_in_window"] = (slot.get("hll") or {}).get("events_in_window")
        rec["late_events"] = p.get("late_events")
        rec["final_report_ts_ms"] = int(time.time() * 1000)
        self.write(rec)
        self.emitted.add(ws)
        self.records.append(rec)
        h, l, d = rec["hll"] or {}, rec["lsh"] or {}, rec["diversity"] or {}
        print(f"[final] {ws[:16]} ev={rec['events_processed']} s2_eps={rec['stage2_eps']} "
              f"lag={rec['consumer_lag']} p50/p95={rec['latency_p50_ms']}/{rec['latency_p95_ms']}ms | "
              f"HLL {h.get('estimate')}/{h.get('exact_routes')} ({h.get('error_pct')}%) | "
              f"LSH cand={l.get('candidates')} true={l.get('true_pairs')} recall={l.get('recall')} | "
              f"diverse exact={d.get('exact_fraction_diverse')} "
              f"ev={(d.get('event_sampling') or {}).get('estimate')} "
              f"ent={(d.get('entity_sampling') or {}).get('estimate')}", flush=True)

    def write(self, rec):
        line = json.dumps(rec) + "\n"
        with open(self.out_run, "a") as f:
            f.write(line)
        with open(self.out_all, "a") as f:
            f.write(line)

    def finish(self):
        for ws in sorted(self.windows):
            if ws not in self.emitted:
                self.emit(ws)
        fp = self.final_periodic or self.periodic or {}
        recs = self.records

        def mean(vals):
            vals = [v for v in vals if v is not None]
            return round(float(np.mean(vals)), 4) if vals else None

        lsh_settings = {}
        for r in recs:
            for s in (r["lsh"] or {}).get("settings", []):
                lsh_settings.setdefault(f"{s['bands']}x{s['rows']}", []).append(s)
        summary = {
            "run_id": self.cfg["run_id"],
            "type": "run_summary",
            "window_start": recs[0]["window_start"] if recs else None,
            "window_end": recs[-1]["window_end"] if recs else None,
            "windows": len(recs),
            **{k: fp.get(k) for k in PERIODIC_FIELDS},
            "max_consumer_lag_clean": fp.get("max_consumer_lag"),
            "bloom": fp.get("bloom"),
            "sampling": fp.get("sampling"),
            "dgim": fp.get("dgim"),
            "late_events": fp.get("late_events"),
            "spark_rows_dropped_by_watermark": fp.get("spark_rows_dropped_by_watermark"),
            "replayed_batches_skipped": fp.get("replayed_batches_skipped"),
            "hll_mean_error_pct": mean([(r["hll"] or {}).get("error_pct") for r in recs]),
            "hll_max_error_pct": max([(r["hll"] or {}).get("error_pct") or 0 for r in recs], default=None),
            "diversity_mean_abs_error": {
                kind: mean([((r["diversity"] or {}).get(kind) or {}).get("abs_error") for r in recs])
                for kind in ("event_sampling", "entity_sampling", "entity_sampling_fixed_rate")},
            "lsh_by_setting": {
                name: {"mean_recall": mean([s["recall"] for s in ss]),
                       "mean_candidate_precision": mean([s["candidate_precision"] for s in ss]),
                       "mean_candidate_reduction": mean([s["candidate_reduction"] for s in ss]),
                       "total_candidates": int(sum(s["candidates"] for s in ss)),
                       "total_true_pairs": int(sum(s["true_pairs"] for s in ss)),
                       "mean_lsh_latency_ms": mean([s["lsh_latency_ms"] for s in ss])}
                for name, ss in lsh_settings.items()},
            "mean_exact_latency_ms": mean([(r["lsh"] or {}).get("exact_latency_ms") for r in recs]),
            "eos_received": self.final_periodic is not None,
        }
        (run_dir(self.cfg) / "final_summary.json").write_text(json.dumps(summary, indent=1))
        self.write(summary)
        print(f"[final] run {summary['run_id']} complete: {len(recs)} windows, "
              f"events={summary['events_processed']} p50/p95={summary['latency_p50_ms']}/"
              f"{summary['latency_p95_ms']}ms HLL err={summary['hll_mean_error_pct']}% "
              f"LSH={ {k: v['mean_recall'] for k, v in summary['lsh_by_setting'].items()} }", flush=True)


def main():
    cfg = load_config()
    fc = FinalConsumer(cfg)
    consumer = Consumer({"bootstrap.servers": cfg["bootstrap_servers"],
                         "group.id": f"final-consumer-{cfg['run_id']}-{int(time.time())}",
                         "auto.offset.reset": "earliest"})
    consumer.subscribe([TOPIC_METRICS, TOPIC_LSH])
    (run_dir(cfg) / "final_consumer.ready").write_text("ok")
    print(f"[final] consuming {TOPIC_METRICS} + {TOPIC_LSH}", flush=True)
    last_msg = time.time()
    try:
        while True:
            m = consumer.poll(1.0)
            if m is None or m.error():
                if fc.final_periodic and time.time() - last_msg > cfg["final_idle_timeout_seconds"]:
                    break
                continue
            last_msg = time.time()
            fc.handle(json.loads(m.value()))
    finally:
        fc.finish()
        consumer.close()


if __name__ == "__main__":
    main()
