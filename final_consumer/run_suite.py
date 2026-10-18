"""Run every required experiment (brief sections 7 and 8) and build the summary table.

    python final_consumer/run_suite.py                 # all experiments
    python final_consumer/run_suite.py --only P1,P2    # a subset

Rates were calibrated on the reference machine (see README): with 4 partitions
and 4 slots the pipeline sustains ~7-8k events/s, so 1000 = low,
6000 = medium (close to capacity), 15000 = high (lag builds up).
Every run replays the same segment (config: segment_start, max_events) on
fresh topics.
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from final_consumer.run_experiment import run  # noqa: E402

MEDIUM = 6000
EXPERIMENTS = [
    # run_id, overrides, restart (after_s, down_s), notes
    ("default", [], None, "default config: 4 partitions, 4 slots, key=pickup_zone, 1000 ev/s"),
    ("P1", ["partitions=1", "parallelism=1", f"producer_rate={MEDIUM}"], None, "7.2 P1: 1 partition, 1 slot"),
    ("P2", ["partitions=4", "parallelism=1", f"producer_rate={MEDIUM}"], None, "7.2 P2: 4 partitions, 1 slot"),
    ("P3", ["partitions=4", "parallelism=4", f"producer_rate={MEDIUM}"], None, "7.2 P3: 4 partitions, 4 slots"),
    ("P4", ["partitions=4", "parallelism=6", f"producer_rate={MEDIUM}"], None, "7.2 P4: 4 partitions, 6 slots"),
    ("K1", ["partitions=4", "parallelism=4", f"producer_rate={MEDIUM}", "partition_key=pickup_zone"], None,
     "7.3 K1: key=pickup_zone"),
    ("K2", ["partitions=4", "parallelism=4", f"producer_rate={MEDIUM}", "partition_key=none"], None,
     "7.3 K2: no key (sticky round-robin)"),
    ("R-low", ["partitions=4", "parallelism=4", "producer_rate=1000"], None, "7.4 low rate"),
    ("R-medium", ["partitions=4", "parallelism=4", f"producer_rate={MEDIUM}"], None, "7.4 medium rate"),
    ("R-high", ["partitions=4", "parallelism=4", "producer_rate=15000"], None, "7.4 high rate"),
    ("restart", ["producer_rate=1000"], (40, 20), "8.2 Stage 2 killed 40 s into the run, down 20 s"),
    ("bloom-small", ["bloom_expected_items=2000", "producer_rate=3000"], None,
     "5.1 undersized Bloom filter (2000 expected items) to demonstrate false positives"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="comma-separated run ids")
    a = ap.parse_args()
    only = set(a.only.split(",")) if a.only else None
    t0 = time.time()
    failures = []
    for run_id, overrides, restart, notes in EXPERIMENTS:
        if only and run_id not in only:
            continue
        try:
            run(run_id, overrides, restart, notes)
        except Exception as e:  # keep going; the failure is reported at the end
            print(f"!!! {run_id} failed: {e}", flush=True)
            failures.append(run_id)
    print(f"suite finished in {(time.time() - t0) / 60:.1f} min; failures: {failures or 'none'}", flush=True)


if __name__ == "__main__":
    main()
