"""Taxi Data Producer: replays the parquet file into taxi.raw at a controlled rate.

  * records are read in pickup-time order (stable on the original row index)
  * key = pickup_zone (partition_key: pickup_zone) or no key (partition_key: none)
  * ~duplicate_rate of the events are re-sent later with the same event_id
  * ~delayed_event_rate of the events are held back 1..max_delay_seconds (wall clock)
  * actual produced events/second is measured, not just the requested rate
  * after the last event an end-of-stream control record is written to every
    partition so downstream windows can be flushed

Ground truth for the evaluation is written to results/runs/<run_id>/:
producer_stats.json, producer_duplicates.csv, producer_delayed.csv
"""
import csv
import heapq
import json
import math
import random
import sys
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from producer.pipeline_common import TOPIC_RAW, load_config, run_dir  # noqa: E402

COLUMNS = {  # source column -> pipeline name (brief section 2.1)
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "PULocationID": "PULocationID",
    "DOLocationID": "DOLocationID",
    "trip_distance": "trip_distance",
    "fare_amount": "fare_amount",
    "passenger_count": "passenger_count",
    "payment_type": "payment_type",
}


def load_segment(cfg):
    df = pd.read_parquet(cfg["data_file"], columns=list(COLUMNS)).rename(columns=COLUMNS)
    df["row"] = df.index  # original row number -> stable event_id
    df = df[df["pickup_datetime"] >= pd.Timestamp(cfg["segment_start"])]
    df = df.sort_values(["pickup_datetime", "row"], kind="stable").head(int(cfg["max_events"]))
    return df.reset_index(drop=True)


def _num(v, cast):
    return None if v is None or (isinstance(v, float) and math.isnan(v)) or pd.isna(v) else cast(v)


def to_event(r):
    return {
        "event_id": f"2026-01-row-{int(r.row):08d}",
        "event_time": r.pickup_datetime.isoformat(timespec="seconds") if pd.notna(r.pickup_datetime) else None,
        "dropoff_time": r.dropoff_datetime.isoformat(timespec="seconds") if pd.notna(r.dropoff_datetime) else None,
        "pickup_zone": _num(r.PULocationID, int),
        "dropoff_zone": _num(r.DOLocationID, int),
        "trip_distance": _num(r.trip_distance, float),
        "fare_amount": _num(r.fare_amount, float),
        "passenger_count": _num(r.passenger_count, int),
        "payment_type": _num(r.payment_type, int),
    }


def main():
    cfg = load_config()
    out = run_dir(cfg)
    rng = random.Random(cfg["random_seed"])
    rate = float(cfg["producer_rate"])
    keyed = cfg["partition_key"] == "pickup_zone"

    df = load_segment(cfg)
    events = [to_event(r) for r in df.itertuples(index=False)]
    print(f"[producer] loaded {len(events)} events {events[0]['event_time']} .. {events[-1]['event_time']}",
          flush=True)

    producer = Producer({
        "bootstrap.servers": cfg["bootstrap_servers"],
        "linger.ms": 5,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 1000000,
        "compression.type": "lz4",
        "acks": "1",
    })

    sent = 0
    per_second = []
    sec_start = time.time()
    sec_count = 0

    def send(ev):
        nonlocal sent, sec_count
        ev = dict(ev)
        ev["producer_ts_ms"] = int(time.time() * 1000)
        key = str(ev["pickup_zone"]).encode() if keyed and ev["pickup_zone"] is not None else None
        while True:
            try:
                producer.produce(TOPIC_RAW, value=json.dumps(ev).encode(), key=key)
                break
            except BufferError:
                producer.poll(0.01)
        sent += 1
        sec_count += 1

    # warm-up: control records that exercise every Spark query (code generation, JIT,
    # Kafka connections) before measurement starts; all stages ignore them for metrics
    warm_n = int(cfg.get("warmup_events", 0))
    if warm_n:
        warm_time = (pd.Timestamp(cfg["segment_start"]) - pd.Timedelta(days=1)).isoformat()
        for w in range(warm_n):
            producer.produce(TOPIC_RAW, value=json.dumps({
                "control": "WARMUP", "event_id": f"WARMUP-{w}", "event_time": warm_time,
                "producer_ts_ms": int(time.time() * 1000)}).encode())
            if w % 100 == 99:
                producer.poll(0)
                time.sleep(0.05)
        producer.flush(30)
        print(f"[producer] sent {warm_n} warm-up records, waiting {cfg['warmup_seconds']}s", flush=True)
        time.sleep(float(cfg["warmup_seconds"]))

    delayed_heap = []     # (release_wall_time, seq, event)
    dup_pending = []      # (source_index_at_which_to_resend, seq, event)
    dup_log, delay_log = [], []
    seq = 0

    start = time.time()
    last_print = start
    for i, ev in enumerate(events):
        # pace: the i-th source event is due at start + i / rate
        due = start + i / rate
        while True:
            now = time.time()
            while delayed_heap and delayed_heap[0][0] <= now:
                _, _, dev = heapq.heappop(delayed_heap)
                send(dev)
            if now >= due:
                break
            producer.poll(0)
            time.sleep(min(0.002, due - now))
        while dup_pending and dup_pending[0][0] <= i:
            _, _, dev = heapq.heappop(dup_pending)
            send(dev)

        if rng.random() < cfg["delayed_event_rate"]:
            delay = rng.uniform(1, cfg["max_delay_seconds"])
            seq += 1
            heapq.heappush(delayed_heap, (time.time() + delay, seq, ev))
            delay_log.append((ev["event_id"], ev["event_time"], round(delay, 3)))
        else:
            send(ev)
        if rng.random() < cfg["duplicate_rate"]:
            seq += 1
            heapq.heappush(dup_pending, (i + rng.randint(1, 50), seq, ev))
            dup_log.append((ev["event_id"], ev["event_time"]))

        now = time.time()
        if now - sec_start >= 1.0:
            per_second.append(round(sec_count / (now - sec_start), 1))
            sec_start, sec_count = now, 0
            producer.poll(0)
        if now - last_print >= 5:
            print(f"[producer] sent={sent} source={i + 1}/{len(events)} "
                  f"actual_eps={sent / (now - start):.0f} pending_delayed={len(delayed_heap)}", flush=True)
            last_print = now

    for _, _, dev in sorted(dup_pending):
        send(dev)
    replay_end, replay_sent = time.time(), sent
    while delayed_heap:  # flush delayed events at their release times
        t, _, dev = heapq.heappop(delayed_heap)
        time.sleep(max(0.0, t - time.time()))
        send(dev)
    producer.flush(60)
    end = time.time()
    data_sent = sent

    # end-of-stream marker on every partition (control record, not a trip)
    md = producer.list_topics(TOPIC_RAW, timeout=10)
    partitions = sorted(md.topics[TOPIC_RAW].partitions)
    eos_time = (pd.Timestamp(events[-1]["event_time"]) + pd.Timedelta(days=1)).isoformat()
    for p in partitions:
        producer.produce(TOPIC_RAW, partition=p, value=json.dumps({
            "control": "EOS", "event_id": f"EOS-{p}", "event_time": eos_time,
            "producer_ts_ms": int(time.time() * 1000), "num_partitions": len(partitions),
            "producer_sent": data_sent}).encode())
    producer.flush(30)

    elapsed = end - start
    stats = {
        "run_id": cfg["run_id"],
        "requested_rate": rate,
        "source_events": len(events),
        "messages_sent": data_sent,
        "duplicates_sent": len(dup_log),
        "delayed_events": len(delay_log),
        "elapsed_s": round(elapsed, 3),
        # rate while replaying the segment (what producer_rate controls) ...
        "actual_eps": round(replay_sent / (replay_end - start), 1),
        # ... and including the tail spent waiting to release the last delayed events
        "actual_eps_incl_delay_tail": round(data_sent / elapsed, 1),
        "replay_s": round(replay_end - start, 3),
        "per_second_eps": per_second,
        "first_event_time": events[0]["event_time"],
        "last_event_time": events[-1]["event_time"],
        "partition_key": cfg["partition_key"],
        "partitions": len(partitions),
    }
    (out / "producer_stats.json").write_text(json.dumps(stats, indent=1))
    with open(out / "producer_duplicates.csv", "w", newline="") as f:
        csv.writer(f).writerows([("event_id", "event_time"), *dup_log])
    with open(out / "producer_delayed.csv", "w", newline="") as f:
        csv.writer(f).writerows([("event_id", "event_time", "delay_s"), *delay_log])
    print(f"[producer] done: sent={data_sent} in {elapsed:.1f}s actual_eps={stats['actual_eps']} "
          f"(requested {rate}) dups={len(dup_log)} delayed={len(delay_log)}", flush=True)


if __name__ == "__main__":
    main()
