"""Display LSH candidate pairs and their exact Jaccard for one window (TA check 6).

    python stage3_lsh/show_lsh_window.py                       # busiest window of the default run
    python stage3_lsh/show_lsh_window.py --run default --window 2026-01-05T08:00:00

Reads taxi.lsh-derived records from results/runs/<run>/results.jsonl is not
enough (pairs are only in the topic), so this consumes taxi.lsh directly if the
run's data is still in Kafka, else falls back to results/runs/<run>/lsh_windows.jsonl.
"""
import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from producer.pipeline_common import ROOT, TOPIC_LSH, load_config  # noqa: E402


def from_kafka(cfg):
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": cfg["bootstrap_servers"], "group.id": f"show-lsh-{time.time()}",
                  "auto.offset.reset": "earliest", "log_level": 0})
    c.subscribe([TOPIC_LSH])
    out, idle = [], 0
    while idle < 4:
        m = c.poll(1.0)
        if m is None:
            idle += 1
            continue
        if not m.error():
            out.append(json.loads(m.value()))
    c.close()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run", default="default")
    ap.add_argument("--window", help="window_start, e.g. 2026-01-05T08:00:00")
    ap.add_argument("--top", type=int, default=25)
    a = ap.parse_args()
    path = ROOT / "results" / "runs" / a.run / "lsh_windows.jsonl"
    if path.exists():
        windows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    else:
        windows = [w for w in from_kafka(load_config()) if w.get("run_id") == a.run]
    if not windows:
        sys.exit(f"no LSH windows found for run {a.run}")
    w = (next((x for x in windows if x["window_start"].startswith(a.window)), None) if a.window
         else max(windows, key=lambda x: x["true_pairs"]))
    if w is None:
        sys.exit(f"window {a.window} not found; available: {[x['window_start'] for x in windows]}")
    print(f"run={a.run} window {w['window_start']} .. {w['window_end']}")
    print(f"active zones (>= min trips) = {w['active_zones']}, all pairs = {w['all_pairs']}, "
          f"true similar pairs (J >= {w['jaccard_threshold']}) = {w['true_pairs']}")
    for s in w["settings"]:
        print(f"  {s['bands']:>2}x{s['rows']:<2} candidates={s['candidates']:>4} recall={s['recall']} "
              f"cand.precision={s['candidate_precision']} reduction={s['candidate_reduction']} "
              f"lsh={s['lsh_latency_ms']}ms (exact all-pairs {w['exact_latency_ms']}ms)")
    print(f"\nCandidate pairs from the default {w['settings'][0]['bands']}x{w['settings'][0]['rows']} banding "
          f"(exact Jaccard computed only for these):")
    print(f"{'P':>4} {'Q':>4} {'Jaccard':>8}  similar  zones")
    for p in w["candidate_pairs"][:a.top]:
        print(f"{p['p']:>4} {p['q']:>4} {p['jaccard']:>8.3f}  {'yes' if p['similar'] else 'no ':>7}  "
              f"{p['p_name']}  <->  {p['q_name']}")
    if len(w["candidate_pairs"]) > a.top:
        print(f"... {len(w['candidate_pairs']) - a.top} more")
    if w["missed_true_pairs"]:
        print(f"\nTrue similar pairs missed by LSH (false negatives): "
              + ", ".join(f"{m['p']}-{m['q']} (J={m['jaccard']})" for m in w["missed_true_pairs"]))


if __name__ == "__main__":
    main()
