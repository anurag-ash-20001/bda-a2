"""Run one end-to-end experiment.

    python final_consumer/run_experiment.py --run-id P3 --set partitions=4 --set parallelism=4 --set producer_rate=4000
    python final_consumer/run_experiment.py --run-id restart --restart-stage2 20:15   # kill Stage 2 20 s after start, 15 s down

Steps: fresh topics (clean offsets) -> start Stage 1/2/3 + final consumer ->
start producer -> sample consumer lag every second -> wait for the final
consumer to see end-of-stream -> stop stages -> append a row to
results/experiment_summary.csv.
"""
import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from producer.pipeline_common import (TOPIC_CLEAN, TOPIC_RAW, load_config, group_lag,
                                      partition_counts, wait_for_broker)  # noqa: E402
from final_consumer.kafka_ctl import fresh_topics, start as kafka_start  # noqa: E402

STAGES = {
    "stage1": "stage1_clean_bloom.stage1",
    "stage2": "stage2_stream_analytics.stage2",
    "stage3": "stage3_lsh.stage3",
    "final_consumer": "final_consumer.final_consumer",
}
SUMMARY_COLUMNS = [
    "run_id", "partitions", "parallelism", "partition_key", "requested_rate", "produced_eps",
    "stage1_eps", "stage2_eps", "latency_p50_ms", "latency_p95_ms", "max_lag_raw", "max_lag_clean",
    "skew_raw", "skew_clean", "events_processed", "windows", "bloom_fp_rate", "bloom_fn_rate",
    "bloom_false_positives", "reservoir_k", "sampling_error_pct", "dgim_k", "dgim_error_pct", "hll_mean_error_pct",
    "lsh_recall_default", "lsh_reduction_default", "lsh_candidates_default", "lsh_recall_extra",
    "lsh_reduction_extra", "lsh_candidates_extra", "lsh_recall_by_setting", "diversity_err_event", "diversity_err_entity",
    "late_included", "late_dropped", "spark_dropped_by_watermark", "replayed_batches_skipped", "duration_s",
    "notes",
]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def child_env(config_path):
    env = os.environ.copy()
    env["BDA_CONFIG"] = str(config_path)
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONPATH"] = str(ROOT)
    for base in (Path("C:/Program Files/Eclipse Adoptium"),):
        jdks = sorted(base.glob("jdk-17*")) if base.exists() else []
        if jdks and "JAVA_HOME" not in env:
            env["JAVA_HOME"] = str(jdks[-1])
            env["PATH"] = str(jdks[-1] / "bin") + os.pathsep + env["PATH"]
    return env


def launch(name, module, rdir, env):
    logf = open(rdir / f"{name}.log", "a", encoding="utf-8")
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    return subprocess.Popen([sys.executable, "-m", module], cwd=ROOT, env=env, stdout=logf,
                            stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, creationflags=flags,
                            start_new_session=(os.name != "nt"))


def kill(proc):
    if proc is None or proc.poll() is not None:
        return
    if os.name == "nt":
        subprocess.run(["taskkill", "/T", "/F", "/PID", str(proc.pid)], capture_output=True)
    else:
        os.killpg(os.getpgid(proc.pid), 9)
    try:
        proc.wait(timeout=20)
    except subprocess.TimeoutExpired:
        pass


def wait_ready(rdir, names, procs, env, timeout=120, attempts=3, wipe_checkpoint=True):
    """Wait until every stage wrote its .ready file. Spark local mode on Windows occasionally
    fails to register the driver's block manager at start-up (NullPointerException in
    BlockManagerMasterEndpoint) and never recovers; such a stage is killed and relaunched."""
    for attempt in range(1, attempts + 1):
        deadline = time.time() + timeout
        while time.time() < deadline:
            for n in names:
                if procs[n].poll() is not None:
                    raise RuntimeError(f"{n} exited during start-up, see {rdir / (n + '.log')}")
            if all((rdir / f"{n}.ready").exists() for n in names):
                return
            time.sleep(1)
        stuck = [n for n in names if not (rdir / f"{n}.ready").exists()]
        if attempt == attempts:
            raise TimeoutError(f"stages not ready after {attempts} attempts: {stuck}")
        for n in stuck:
            log(f"{n} not ready after {timeout}s (attempt {attempt}) -> relaunching")
            kill(procs[n])
            if wipe_checkpoint:  # a stuck first start never processed anything
                shutil.rmtree(Path(json.loads(Path(env["BDA_CONFIG"]).read_text())["work_dir"]) / "checkpoints"
                              / rdir.name / n, ignore_errors=True)
            procs[n] = launch(n, STAGES[n], rdir, env)


class LagMonitor(threading.Thread):
    """Samples committed-offset lag of stage1 on taxi.raw and stage2 on taxi.clean every second."""

    def __init__(self, cfg, path):
        super().__init__(daemon=True)
        self.cfg, self.path = cfg, path
        self.stop_flag = threading.Event()
        self.max = {"raw": 0, "clean": 0}
        self.t0 = time.time()

    def run(self):
        with open(self.path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["t_s", "lag_raw_stage1", "lag_clean_stage2", "raw_end_offset", "clean_end_offset"])
            while not self.stop_flag.is_set():
                try:
                    raw = group_lag(self.cfg, TOPIC_RAW, "stage1")
                    clean = group_lag(self.cfg, TOPIC_CLEAN, "stage2")
                    lr, hr = raw if raw else (None, {})
                    lc, hc = clean if clean else (None, {})
                    self.max["raw"] = max(self.max["raw"], lr or 0)
                    self.max["clean"] = max(self.max["clean"], lc or 0)
                    w.writerow([round(time.time() - self.t0, 1), lr, lc, sum(hr.values()), sum(hc.values())])
                    f.flush()
                except Exception as e:  # broker hiccup: keep sampling
                    w.writerow([round(time.time() - self.t0, 1), None, None, None, f"error {e}"])
                self.stop_flag.wait(1.0)


def skew(counts):
    total = sum(counts.values())
    return round(max(counts.values()) / total, 4) if total else None


def timeline_eps(path, fraction=0.95):
    """Throughput of a stage over the first `fraction` of its events: events / seconds from the
    start of the first batch to the end of the batch that reached the fraction. Excludes the
    tail where only the last delayed events trickle in."""
    if not path.exists():
        return None
    with open(path) as f:
        rows = [r for r in csv.DictReader(f)]
    if not rows:
        return None
    total = int(rows[-1]["cumulative"])
    t0 = float(rows[0]["t_start"])
    for r in rows:
        if int(r["cumulative"]) >= fraction * total:
            span = float(r["t_end"]) - t0
            return round(int(r["cumulative"]) / span, 1) if span > 0 else None
    return None


def summarize(cfg, rdir, lagmon, duration, notes):
    def load(name):
        p = rdir / name
        return json.loads(p.read_text()) if p.exists() else {}

    prod, s1, fin = load("producer_stats.json"), load("stage1_stats.json"), load("final_summary.json")
    raw_counts = partition_counts(cfg, TOPIC_RAW)
    clean_counts = partition_counts(cfg, TOPIC_CLEAN)
    (rdir / "partition_counts.json").write_text(json.dumps({"taxi.raw": raw_counts, "taxi.clean": clean_counts}))
    lsh = fin.get("lsh_by_setting") or {}
    default_name = f"{cfg['lsh_bands']}x{cfg['lsh_rows_per_band']}"
    extra = next((v for k, v in lsh.items() if k != default_name), {})
    d = lsh.get(default_name, {})
    late = fin.get("late_events") or {}
    sampling = fin.get("sampling") or {}
    dgim = fin.get("dgim") or {}
    bloom = fin.get("bloom") or {}
    div = fin.get("diversity_mean_abs_error") or {}
    row = {
        "run_id": cfg["run_id"], "partitions": cfg["partitions"], "parallelism": cfg["parallelism"],
        "partition_key": cfg["partition_key"], "requested_rate": cfg["producer_rate"],
        "produced_eps": prod.get("actual_eps"), "stage1_eps": timeline_eps(rdir / "stage1_batches.csv"),
        "stage2_eps": timeline_eps(rdir / "stage2_batches.csv"), "latency_p50_ms": fin.get("latency_p50_ms"),
        "latency_p95_ms": fin.get("latency_p95_ms"),
        "max_lag_raw": max(lagmon.max["raw"], s1.get("max_raw_lag", 0)),
        "max_lag_clean": max(lagmon.max["clean"], fin.get("max_consumer_lag_clean") or 0),
        "skew_raw": skew(raw_counts), "skew_clean": skew(clean_counts),
        "events_processed": fin.get("events_processed"), "windows": fin.get("windows"),
        "bloom_fp_rate": bloom.get("fp_rate"), "bloom_fn_rate": bloom.get("fn_rate"),
        "bloom_false_positives": bloom.get("false_positives"),
        "reservoir_k": sampling.get("k"), "sampling_error_pct": sampling.get("error_pct"),
        "dgim_k": dgim.get("k"), "dgim_error_pct": dgim.get("error_pct"),
        "hll_mean_error_pct": fin.get("hll_mean_error_pct"),
        "lsh_recall_default": d.get("mean_recall"), "lsh_reduction_default": d.get("mean_candidate_reduction"),
        "lsh_candidates_default": d.get("total_candidates"),
        "lsh_recall_extra": extra.get("mean_recall"), "lsh_reduction_extra": extra.get("mean_candidate_reduction"),
        "lsh_candidates_extra": extra.get("total_candidates"),
        "lsh_recall_by_setting": json.dumps({k: v.get("mean_recall") for k, v in lsh.items()}),
        "diversity_err_event": div.get("event_sampling"), "diversity_err_entity": div.get("entity_sampling"),
        "late_included": late.get("late_included"), "late_dropped": late.get("late_dropped"),
        "spark_dropped_by_watermark": json.dumps(fin.get("spark_rows_dropped_by_watermark")),
        "replayed_batches_skipped": fin.get("replayed_batches_skipped"),
        "duration_s": round(duration, 1), "notes": notes,
    }
    path = ROOT / "results" / "experiment_summary.csv"
    rows = []
    if path.exists():
        with open(path, newline="") as f:
            rows = [r for r in csv.DictReader(f) if r["run_id"] != cfg["run_id"]]
    rows.append(row)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=SUMMARY_COLUMNS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    return row


def run(run_id, overrides, restart=None, notes="", timeout=1800):
    cfg = load_config(overrides=list(overrides) + [f"run_id={run_id}"])
    rdir = ROOT / "results" / "runs" / run_id
    shutil.rmtree(rdir, ignore_errors=True)
    rdir.mkdir(parents=True)
    for sub in ("checkpoints", "state"):
        shutil.rmtree(Path(cfg["work_dir"]) / sub / run_id, ignore_errors=True)
    config_path = rdir / "config.json"
    config_path.write_text(json.dumps(cfg, indent=1))
    env = child_env(config_path)
    os.environ.update({k: env[k] for k in ("JAVA_HOME", "PATH") if k in env})

    log(f"=== run {run_id}: partitions={cfg['partitions']} parallelism={cfg['parallelism']} "
        f"key={cfg['partition_key']} rate={cfg['producer_rate']} events={cfg['max_events']}")
    if cfg["kafka_mode"] == "native":
        kafka_start(cfg)
    if not wait_for_broker(cfg, 30):
        raise RuntimeError("Kafka broker not reachable")
    fresh_topics(cfg)
    log("topics ready (fresh offsets)")

    procs = {}
    t_start = time.time()
    lagmon = LagMonitor(cfg, rdir / "lag_timeline.csv")
    try:
        for name in ("stage1", "stage2", "stage3", "final_consumer"):
            procs[name] = launch(name, STAGES[name], rdir, env)
            time.sleep(4)  # stagger JVM start-ups
        wait_ready(rdir, list(STAGES), procs, env)
        log("all stages running; starting producer")
        lagmon.start()
        procs["producer"] = launch("producer", "producer.producer", rdir, env)
        t_prod = time.time()

        restart_log = []
        restarted = False
        deadline = time.time() + timeout
        while procs["final_consumer"].poll() is None:
            if time.time() > deadline:
                raise TimeoutError("run did not finish in time")
            if restart and not restarted and time.time() - t_prod >= restart[0]:
                log(f"RESTART TEST: killing stage2 for {restart[1]}s")
                restart_log.append({"t_s": round(time.time() - lagmon.t0, 1), "event": "stage2 killed"})
                kill(procs["stage2"])
                (rdir / "stage2.ready").unlink(missing_ok=True)
                time.sleep(restart[1])
                procs["stage2"] = launch("stage2", STAGES["stage2"], rdir, env)
                restart_log.append({"t_s": round(time.time() - lagmon.t0, 1), "event": "stage2 restarted"})
                wait_ready(rdir, ["stage2"], procs, env, wipe_checkpoint=False)
                restart_log.append({"t_s": round(time.time() - lagmon.t0, 1), "event": "stage2 queries running"})
                restarted = True
                (rdir / "restart_events.json").write_text(json.dumps(restart_log, indent=1))
            for n in ("stage1", "stage2", "stage3"):
                if procs[n].poll() is not None:
                    raise RuntimeError(f"{n} died, see {rdir / (n + '.log')}")
            time.sleep(1)
        duration = time.time() - t_prod
    finally:
        lagmon.stop_flag.set()
        for n in ("producer", "stage1", "stage2", "stage3", "final_consumer"):
            kill(procs.get(n))
    row = summarize(cfg, rdir, lagmon, duration, notes)
    log(f"=== run {run_id} done in {duration:.0f}s: produced={row['produced_eps']}/s "
        f"stage2={row['stage2_eps']}/s p50/p95={row['latency_p50_ms']}/{row['latency_p95_ms']}ms "
        f"lag raw/clean={row['max_lag_raw']}/{row['max_lag_clean']} skew={row['skew_raw']} "
        f"HLL={row['hll_mean_error_pct']}% LSH recall={row['lsh_recall_default']}")
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", default="default")
    ap.add_argument("--set", action="append", default=[], help="config override key=value")
    ap.add_argument("--restart-stage2", help="AFTER_S:DOWN_S kill Stage 2 AFTER_S seconds into the run")
    ap.add_argument("--notes", default="")
    a = ap.parse_args()
    restart = tuple(float(x) for x in a.restart_stage2.split(":")) if a.restart_stage2 else None
    run(a.run_id, a.set, restart, a.notes)


if __name__ == "__main__":
    main()
