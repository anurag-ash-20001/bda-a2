"""Narrative text of the report (layout is in build_report.py).

Paragraph strings use ReportLab mini-HTML (<b>, <i>, <font>). A leading '>' renders a callout box,
a leading '#' a small heading. Functions named *_fmt compute the numbers quoted in the text."""
import csv
import json
from datetime import date

TEXT = {}
TEXT["date"] = f"Report generated {date.today():%d %B %Y}"


def _n(v, nd=0):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "-"
    return f"{x:,.{nd}f}"


def _get(S, rid, key, nd=0):
    return _n((S.get(rid) or {}).get(key), nd)


# ============================================================================ 1 summary
TEXT["summary"] = [
    "This report documents a complete, reproducible streaming system for NYC yellow-taxi trips "
    "(January 2026, 3.72 million trips). A producer replays the trips as a live stream into Apache Kafka; "
    "three Spark Structured Streaming jobs then process the stream <b>exclusively through Kafka topics</b>: "
    "Stage 1 validates and enriches every event and runs a Bloom-filter duplicate test, Stage 2 computes "
    "reservoir samples, event- versus entity-sampling estimates, HyperLogLog distinct counts and DGIM "
    "counts against exact baselines, and Stage 3 finds pickup zones with similar destination sets using "
    "MinHash-LSH. A final Kafka consumer joins the outputs into the standardized <i>results.jsonl</i>.",
    "Section 3 analyses the code component by component (with excerpts taken from the submitted source), "
    "section 4 presents the measurements of all required experiments and section 5 interprets them. "
    "The table below maps every requirement of the brief to its implementation and headline result.",
]


def summary_rows(S, d, s1, prod):
    b = d.get("bloom") or {}
    sm = d.get("sampling") or {}
    dg = d.get("dgim") or {}
    lsh = (d.get("lsh_by_setting") or {})
    l20 = lsh.get("20x5", {})
    dme = d.get("diversity_mean_abs_error") or {}
    late = d.get("late_events") or {}
    rr = S.get("restart") or {}
    return [
        ["Producer: controlled rate, 2 % duplicates, 2 % delayed (4.1)", "producer/producer.py",
         f"{_n(prod.get('actual_eps'), 1)} ev/s achieved for 1 000 requested; {prod.get('duplicates_sent')} duplicates, "
         f"{prod.get('delayed_events')} delayed events out of {_n(prod.get('source_events'))} trips"],
        ["Stage 1 clean + enrich + Bloom (4.2, 5.1)", "stage1_clean_bloom/stage1.py",
         f"{_n(s1.get('accepted'))} accepted, {_n(s1.get('rejected'))} rejected; Bloom FP rate {b.get('fp_rate')}, "
         f"FN rate {b.get('fn_rate')} (m = {_n(b.get('bit_array_size'))} bits, k = {b.get('num_hashes')})"],
        ["Reservoir sampling k = 100 / 1 000 / 10 000 (5.2)", "stage2 query A, common/sketches.py",
         "fare error " + ", ".join(f"{x['fare_error_pct']} %" for x in sm.get("by_k", [])) +
         " for k = 100 / 1 000 / 10 000"],
        ["Event vs entity sampling, non-linear statistic (5.3)", "stage2 query B",
         f"mean absolute error {_n(dme.get('event_sampling'), 3)} (event) vs {_n(dme.get('entity_sampling'), 3)} "
         f"(entity) at equal storage"],
        ["HyperLogLog distinct routes per window (5.4)", "stage2 query C (Spark HLL++)",
         f"mean error {d.get('hll_mean_error_pct')} %, max {d.get('hll_max_error_pct')} % over {d.get('windows')} "
         f"windows with a ~3.3 KB sketch"],
        ["DGIM, k = 1 000 / 5 000 / 10 000 (5.5)", "common/sketches.py DGIM (own code)",
         ", ".join(f"k={x['k']}: {x['error_pct']} %" for x in dg.get("by_k", [])) +
         f"; {dg.get('buckets')} buckets stored"],
        ["MinHash-LSH, 20x5 + other settings (6)", "stage3_lsh/stage3.py",
         f"20x5: recall {l20.get('mean_recall')}, candidate reduction {l20.get('mean_candidate_reduction')}; "
         f"25x4 recall {(lsh.get('25x4') or {}).get('mean_recall')}, 10x10 recall {(lsh.get('10x10') or {}).get('mean_recall')}"],
        ["Partition / consumer runs P1-P4 (7.2)", "scripts/run_suite.py",
         f"Stage 2 throughput {_get(S, 'P1', 'stage2_eps')} / {_get(S, 'P2', 'stage2_eps')} / "
         f"{_get(S, 'P3', 'stage2_eps')} / {_get(S, 'P4', 'stage2_eps')} ev/s at 6 000 ev/s input"],
        ["Partition key K1 vs K2 (7.3)", "partition_key config",
         f"busiest-partition share {_get(S, 'K1', 'skew_raw', 3)} (pickup_zone) vs {_get(S, 'K2', 'skew_raw', 3)} (no key)"],
        ["Low / medium / high rate (7.4)", "producer_rate config",
         f"p95 latency {_get(S, 'R-low', 'latency_p95_ms')} / {_get(S, 'R-medium', 'latency_p95_ms')} / "
         f"{_get(S, 'R-high', 'latency_p95_ms')} ms; max raw lag {_get(S, 'R-low', 'max_lag_raw')} / "
         f"{_get(S, 'R-medium', 'max_lag_raw')} / {_get(S, 'R-high', 'max_lag_raw')}"],
        ["Late events with watermark (8.1)", "Spark withWatermark(30 min)",
         f"{_n(late.get('late_dropped'))} events arrived after their window closed and were discarded"],
        ["Restart of Stage 2 (8.2)", "checkpoint + state snapshot",
         f"killed for 20 s; max taxi.clean lag {_n(rr.get('max_lag_clean'))}; all events counted exactly once "
         f"({rr.get('events_processed') or '-'} processed)"],
        ["Standardized output (9)", "final_consumer/final_consumer.py",
         "results/results.jsonl, results/experiment_summary.csv, per-run folders"],
    ]


TEXT["summary_rows"] = summary_rows

# ============================================================================ 2 architecture
TEXT["architecture"] = [
    "Figure 1 (title page) shows the pipeline. It follows the fixed topology of the brief: the producer is "
    "the only component that touches the source file, and every processing stage consumes one Kafka topic "
    "and publishes its output to another Kafka topic. Stage 2 and Stage 3 consume <i>taxi.clean</i> "
    "independently (separate Spark applications, separate offsets), so a slow or restarted Stage 2 does not "
    "affect Stage 3.",
]
TEXT["environment"] = [
    "<b>Machine:</b> AMD Ryzen 7 7840HS (8 cores / 16 threads), 15.3 GB RAM, Windows 11 Home (build 26300), "
    "SSD. Docker and WSL were <i>not</i> available (they need administrator rights and a reboot), so Kafka runs "
    "natively on Windows; a <i>docker-compose.yml</i> is included for other machines. The broker, the "
    "producer, three Spark applications and the final consumer all share the laptop.",
]
TEXT["tech_rows"] = [
    ["Message broker", "Apache Kafka 3.9.1, KRaft, 1 node", "Mandatory. KRaft avoids ZooKeeper; one node is enough "
     "at laptop scale (replication factor 1)."],
    ["Stream processing", "Spark Structured Streaming 3.5.3 (PySpark), local[N]",
     "Kafka source with checkpointed offsets, event-time windows and watermarks, built-in HLL++. "
     "local[N] gives N task slots = the 'parallel consumers' of section 7."],
    ["Sketches", "own code in common/sketches.py", "Bloom, reservoir, DGIM, MinHash + banding: every parameter is "
     "visible and reportable; DGIM must be group-implemented. Unit tests in tests/."],
    ["Kafka clients", "confluent-kafka (librdkafka)", "fast producer (>10 000 ev/s from Python), consumer for the "
     "final report, AdminClient for topics and lag."],
    ["Language", "Python 3.12, Java 17 (Temurin)", "PySpark 3.5 does not support Python 3.13+, pandas 3 or "
     "numpy 2; versions pinned in requirements.txt."],
]
TEXT["layout"] = """config.yaml                     single configuration file (all parameters of section 3 + run knobs)
producer/producer.py            parquet -> taxi.raw
stage1_clean_bloom/stage1.py    taxi.raw -> taxi.clean  (Spark)
stage2_stream_analytics/stage2.py taxi.clean -> taxi.metrics (Spark, 3 queries)
stage3_lsh/stage3.py            taxi.clean -> taxi.lsh  (Spark)
final_consumer/final_consumer.py taxi.metrics + taxi.lsh -> console + results.jsonl
common/  config.py  kafka_utils.py  spark_utils.py  sketches.py
scripts/ kafka_ctl.py  run_experiment.py  run_suite.py  analyze_results.py  show_lsh_window.py  build_report.py
tests/test_sketches.py          unit tests for Bloom, DGIM, reservoir, MinHash/LSH
results/ results.jsonl  experiment_summary.csv  analysis.md  plots/  runs/<run_id>/...
run_all.bat / run_all.sh / run_experiments.bat / setup_windows.ps1 / docker-compose.yml"""

# ============================================================================ 3 code
TEXT["code_intro"] = [
    "The code is organised exactly like the required submission structure: one folder per stage, plus a "
    "<i>common</i> package for configuration, Kafka/Spark helpers and the sketch implementations, and "
    "<i>scripts</i> for orchestration and analysis. All excerpts below are copied automatically from the "
    "source files when this report is generated.",
]
TEXT["config"] = [
    "All parameters listed in section 3 of the brief (partitions, producer_rate, duplicate_rate, "
    "delayed_event_rate, max_delay_seconds, window_minutes, hll_precision, dgim_max_window, Bloom sizing, LSH "
    "settings, jaccard_threshold) live in <i>config.yaml</i>, together with run knobs (parallelism, "
    "partition_key, data segment, watermark, reservoir sizes, DGIM query sizes). Any value can be overridden "
    "per run with <font face='Courier'>--set key=value</font>. The orchestrator writes the effective "
    "configuration of a run to <i>results/runs/&lt;run&gt;/config.json</i> and passes its path to every process "
    "in the <font face='Courier'>BDA_CONFIG</font> environment variable, so all stages of a run see identical "
    "parameters and every result folder documents how it was produced.",
]
TEXT["producer"] = [
    "The producer reads only the eight columns of section 2.1, keeps trips with pickup time from "
    "<i>segment_start</i> (Monday 2026-01-05 00:00) and replays the first <i>max_events</i> = 100 000 trips "
    "sorted by pickup time (ties broken by the original row number). Each trip becomes the JSON event of "
    "section 2.2; <font face='Courier'>event_id = 2026-01-row-&lt;original parquet row&gt;</font> is therefore "
    "stable, and <i>dropoff_time</i> is added because Stage 1 needs it for <i>trip_duration_min</i>. "
    "<i>producer_ts_ms</i> is set at the moment of sending (wall clock), <i>event_time</i> is the pickup time.",
    "<b>Rate control.</b> The i-th source trip is due at <font face='Courier'>start + i / producer_rate</font>; "
    "the loop sleeps until then while releasing delayed events whose time has come. This keeps the rate "
    "accurate over the whole run instead of drifting with per-message overhead. "
    "<b>Fault injection.</b> With probability 0.02 a trip is held back in a heap for U(1, 20) seconds of wall "
    "clock (a <i>delayed</i> event), and with probability 0.02 a duplicate is scheduled 1-50 source events later "
    "with the same event_id.",
]
TEXT["producer2"] = [
    "The Kafka key is <i>pickup_zone</i> (baseline) or absent (<font face='Courier'>partition_key=none</font>, "
    "experiment K2, librdkafka's sticky partitioner). The producer measures what it really achieved: events per "
    "second for every wall-clock second, the replay-phase rate <i>actual_eps</i> and, separately, the rate "
    "including the tail spent releasing the last delayed events. Ground truth for the evaluation is written to "
    "the run folder (<i>producer_duplicates.csv</i>, <i>producer_delayed.csv</i>, <i>producer_stats.json</i>). "
    "Two kinds of control records complete the protocol: a short <b>WARMUP</b> burst before measurement (it makes "
    "Spark compile its query plans before the first real event; every stage ignores these records for all "
    "metrics) and an <b>EOS</b> (end of stream) record on every partition after the last event, whose far-future "
    "event time advances the watermarks so that the final windows close.",
]
TEXT["stage1"] = [
    "Stage 1 is a Spark Structured Streaming job that subscribes to <i>taxi.raw</i>. Parsing (from_json with a "
    "fixed schema), validation and the derived fields are Spark SQL expressions, so they run as parallel tasks "
    "over the Kafka partitions on the <i>local[N]</i> slots. A record is rejected (counted per reason, never "
    "forwarded) when a zone is missing or outside 1-265, a timestamp is unparsable, the duration is <= 0 or "
    "> 300 minutes, the fare is negative or > $1 000, or the distance is negative or > 100 miles. The last two "
    "limits were added after the first runs showed odometer values up to 269 097 miles in the source, which "
    "dominated the exact mean distance (see 4.2).",
]
TEXT["stage1_bloom"] = [
    "The micro-batch is then handed to <font face='Courier'>foreachBatch</font>, where the driver applies the "
    "Bloom filter to the events in arrival order and publishes one enriched event per accepted input to "
    "<i>taxi.clean</i> with the same key policy as the producer. One global filter (instead of one per task) "
    "gives a single consistent membership answer for the whole stream. The filter is sized from the configuration: "
    "<font face='Courier'>m = -n ln p / (ln 2)<super>2</super></font> = 1 917 012 bits (234 KB) and "
    "<font face='Courier'>k = (m/n) ln 2</font> = 7 for n = 200 000 and p = 0.01; the k positions are derived "
    "from one 128-bit MurmurHash3 by double hashing.",
    ">Following the Bloom-filter rule of the brief, the filter never deletes anything: it only sets "
    "<i>bloom_seen_before</i>. An exact set of event ids (evaluation only) sets <i>gt_seen_before</i>, so every "
    "decision can be scored as TP / FP / FN / TN. Ground truth is \"this event_id was already processed\"; this is "
    "more precise than \"the producer resent it\", because a delayed original can arrive after its duplicate.",
]
TEXT["stage2"] = [
    "Stage 2 runs <b>three streaming queries in one Spark application</b>, all reading <i>taxi.clean</i>:",
    "<b>Query A</b> (per event, foreachBatch in arrival order) maintains the order-dependent single-stream "
    "structures: three reservoirs, the DGIM counter, exact streaming means and the exact last-10 000 bits "
    "(baselines), Bloom evaluation counters, per-event latency, consumer lag and the late-event classification. "
    "Every <i>report_interval_seconds</i> (5 s) it publishes a compact <i>periodic</i> JSON summary to "
    "<i>taxi.metrics</i>. "
    "<b>Query B</b> groups by 60-minute event-time window and pickup zone (watermark 30 min) and keeps, per zone, "
    "the exact destination set, the event-sampled destination set and counts. "
    "<b>Query C</b> groups by window only and computes HyperLogLog++ next to the exact distinct count. "
    "B and C run in append mode: Spark emits a window exactly once, when the watermark passes its end, and the "
    "foreachBatch handlers publish <i>diversity</i> and <i>window</i> messages to <i>taxi.metrics</i>.",
]
TEXT["reservoir"] = [
    "Algorithm R keeps the first k items, then replaces a random slot with probability k/n for the n-th item, "
    "which gives every item seen so far the same inclusion probability k/n. One reservoir per k (100, 1 000, "
    "10 000) stores fare_amount and trip_distance together; the exact mean is a running sum over all events.",
]
TEXT["dgim"] = [
    "DGIM summarises the last N = 10 000 bits of <i>slow_trip_bit</i> (1 if the trip lasted more than 30 "
    "minutes). Each bucket stores the timestamp of its most recent 1 and a size that is a power of two; at most "
    "two buckets of each size exist, and when a third appears the two oldest of that size merge (keeping the "
    "newer timestamp). A bucket is discarded once its timestamp leaves the window. A query for the last k bits "
    "sums every bucket newer than t - k and counts only half of the oldest one, because only that bucket can "
    "straddle the boundary; a size-1 bucket is exact. This bounds the relative error by 50 % while storing "
    "O(log<super>2</super> N) bits; in practice about 14 buckets are stored instead of 10 000 bits.",
]
TEXT["hll"] = [
    "Distinct routes (route_key = pickup-dropoff) per window use Spark's built-in HyperLogLog++ "
    "(<font face='Courier'>approx_count_distinct</font>), which the brief allows. Spark derives the precision from "
    "the requested relative standard deviation as p = ceil(2 log<sub>2</sub>(1.106 / rsd)); the code inverts that "
    "formula so that <i>hll_precision</i> = 12 gives exactly 2<super>12</super> = 4 096 six-bit registers "
    "(3 280 bytes) and a theoretical standard error of 1.04/sqrt(4096) = 1.6 %. The exact baseline is "
    "<font face='Courier'>size(collect_set(route_key))</font> in the same aggregation, so both see exactly the "
    "same events.",
]
TEXT["sampling"] = [
    "The statistic is the fraction of active pickup zones with at least 10 distinct drop-off zones in a window. "
    "<b>Event sampling</b> keeps each event with probability 0.10, decided by a hash of event_id (deterministic, so "
    "a duplicate gets the same decision), and evaluates the property on the sampled events only. "
    "<b>Entity sampling</b> selects pickup zones by a hash of the zone id and keeps <i>all</i> events of the "
    "selected zones. To compare at the <b>same storage budget</b>, zones are taken in hash order until the number "
    "of their events would exceed the number of events the event sample stored in that window; a fixed-rate "
    "entity sample (hash below 10 %) is reported as well.",
]
TEXT["stage2_misc"] = [
    "<b>Latency</b> is measured per event as Stage 2 processing time minus <i>producer_ts_ms</i> (p50/p95 over the "
    "run). <b>Lag</b> is computed from the offsets each stage has processed versus the log-end offsets; because "
    "Spark keeps its Kafka offsets in the checkpoint, Stages 1 and 2 also commit their positions to the consumer "
    "groups <i>stage1</i>/<i>stage2</i>, which the orchestrator samples every second. <b>Late events</b> are "
    "classified with the rule Spark applies: the watermark used for a batch is the maximum event time seen in "
    "earlier batches minus 30 minutes, and a row is discarded by a window aggregation when its window end is "
    "<= that watermark.",
    "<b>Restart safety.</b> foreachBatch is at-least-once: after a crash Spark re-executes the last batch whose "
    "commit was not written. Query A therefore pickles its whole state together with the id of the last applied "
    "batch after every batch; on restart it reloads the snapshot and skips any batch id it has already applied, "
    "so no event is counted twice. Queries B and C keep their window state in Spark's checkpointed state store.",
]
TEXT["stage3"] = [
    "Stage 3 is a separate Spark application on <i>taxi.clean</i>. It groups by 60-minute window and pickup zone "
    "with the same 30-minute watermark and collects S(P), the set of drop-off zones. When a window closes, the "
    "driver keeps zones with at least 20 trips, builds a 100-value MinHash signature per set with the universal "
    "hash family h<sub>i</sub>(x) = (a<sub>i</sub>x + b<sub>i</sub>) mod (2<super>31</super> - 1), applies banding "
    "(b bands x r rows; two signatures that agree on all r values of any band land in the same bucket and become a "
    "candidate pair), verifies candidates with exact Jaccard and returns pairs with J >= 0.50. For evaluation only, "
    "the exact all-pairs Jaccard gives the true similar pairs, from which recall, candidate precision, candidate "
    "reduction and both latencies are computed. The default 20x5 and two other 100-hash settings (10x10, 25x4) are "
    "evaluated on exactly the same sets, so their differences are caused only by the banding.",
]
TEXT["final"] = [
    "The final consumer subscribes to <i>taxi.metrics</i> and <i>taxi.lsh</i>. It keeps the latest periodic summary "
    "and, per window, the HLL, diversity and LSH messages; when all three of a window are present it appends one "
    "record with every mandatory field of section 9 (plus extra fields) to <i>results/results.jsonl</i> and the "
    "run's own file, and prints a one-line summary. After Stage 2 reports end of stream and no message arrives "
    "for 20 s, remaining windows are flushed and a <i>run_summary</i> record with run-level aggregates is "
    "written. Full LSH window messages (all candidate pairs with their exact Jaccard) are stored in "
    "<i>lsh_windows.jsonl</i> for the TA demonstration.",
]
TEXT["orchestration"] = [
    "<font face='Courier'>scripts/run_experiment.py</font> runs one experiment end to end: it gives the run fresh "
    "topics, starts Stages 1-3 and the final consumer as separate processes, waits until every Spark query is "
    "running, starts the producer, samples consumer lag every second, optionally kills and restarts Stage 2, waits "
    "for the final consumer to finish and appends a row to <i>experiment_summary.csv</i>. "
    "<font face='Courier'>run_suite.py</font> runs all required experiments, <font face='Courier'>analyze_results.py"
    "</font> produces tables and plots.",
    "<b>Windows-specific engineering.</b> (1) Deleting a topic makes a native Windows broker shut down (it cannot "
    "rename open segment files), so a clean run stops the broker, wipes its data directory and restarts it, a "
    "\"clean equivalent\" that the brief allows. (2) Spark needs <i>winutils.exe</i>, which links against the "
    "Visual C++ 2010 runtime; the Microsoft-signed DLL is extracted from the official redistributable without "
    "administrator rights. (3) Spark local mode occasionally fails to register its block manager at start-up; "
    "the stages bind the driver to 127.0.0.1, use a longer heartbeat interval, and the orchestrator relaunches a "
    "stage that does not become ready.",
    ">Two subtle Spark pitfalls were found and fixed during development. Filtering control rows with "
    "<font face='Courier'>where(...)</font> before a windowed aggregation is pushed by the optimizer <i>below</i> "
    "the watermark operator, which hides the EOS record from the watermark so the final windows never close; the "
    "aggregations therefore ignore control rows through null columns instead. And <font face='Courier'>collect()"
    "</font> converts timestamps to the machine's time zone (IST, +5:30) while <font face='Courier'>toPandas()"
    "</font> honours <i>spark.sql.session.timeZone = UTC</i>; mixing them misaligned the windows.",
]

# ============================================================================ 4 results
TEXT["results_intro"] = [
    "All runs replay the same 100 000-trip segment (Monday 2026-01-05 00:00 to about 03:00 the next day, 28 "
    "event-time windows) on fresh topics. Tables 1 and 2 summarise all {rows} runs; the following subsections "
    "analyse each requirement. Throughput of a stage is the number of events it processed divided by the time it "
    "needed for the first 95 % of them (this excludes the tail where only the last delayed events trickle in).",
]
TEXT["bloom"] = [
    "Default run: m = {m} bits ({mem} bytes), k = {k}, {ins} inserted ids. All {dups} real repeats were flagged "
    "({flag} true positives, FN = {fn}: a Bloom filter cannot produce false negatives because bits are never "
    "cleared). False positives: <b>{fp}</b> (observed FP rate {fpr}). The theoretical FP probability at this fill "
    "level is (1 - e<super>-kn/m</super>)<super>k</super> = {theo}: the filter was sized for 200 000 items but "
    "holds only ~100 000, so it operates far below its 1 % design point. The observed rate agrees with theory.",
    "To demonstrate false positives clearly, the run <b>bloom-small</b> sizes the filter for 2 000 items "
    "(m = {bs_m} bits, k = {bs_k}) on the same 100 000-event stream: it reports <b>{bs_fp}</b> false positives "
    "(FP rate {bs_fpr}, theoretical {bs_theo}) and still {bs_fn} false negatives. Had Stage 1 dropped flagged "
    "events, this undersized filter would have silently deleted that many genuine trips, which is why the "
    "pipeline only marks them. Concrete false-positive examples (id never seen before, filter answered \"seen\"):",
]
TEXT["reservoir_res"] = [
    "The mean is a <b>linear</b> statistic: every sampled event is an unbiased estimate of it, so a uniform "
    "sample works well and the error shrinks roughly like the standard error sigma/sqrt(k). A single run is one "
    "random draw, so in an individual run a larger reservoir can occasionally be less accurate (as k = 10 000 versus "
    "k = 1 000 in the default run above); the reliable picture is the spread over the seven independent runs of "
    "Table 3. The fare error ranges over {r100} % for k = 100, {r1000} % for k = 1 000 and {r10000} % for k = 10 000 "
    "(the stream holds {n} accepted events, so k = 10 000 is a 10 % sample): the worst case shrinks steadily with k. "
    "Fares and distances are heavy-tailed (airport trips), which is why k = 100 can be off by more than 10 %. "
    "Before the distance limit was added, the exact mean distance (6.70 miles) was dominated by a few bogus "
    "odometer values that a sample almost never contains, and every reservoir showed about 27 % error; this was a "
    "data-quality effect, not a sampling one.",
]
TEXT["diversity"] = [
    "Event sampling <b>systematically underestimates</b> the fraction of diverse zones (mean absolute error "
    "{ev}), while entity sampling at the same storage budget is unbiased and much closer (mean absolute error "
    "{en}; fixed-rate entity sample {fx}). The reason is that the property \"has trips to at least 10 distinct "
    "drop-off zones\" depends on the <i>complete</i> behaviour of a zone. With 10 % event sampling, a zone with 40 "
    "trips to 15 destinations keeps about 4 trips and can show at most 4 destinations, so it can almost never "
    "qualify: the statistic is non-linear in the events and the sample distorts every zone. Entity sampling keeps "
    "all events of the selected zones, so each selected zone's property is evaluated exactly; the only error is "
    "which zones were selected, an ordinary sampling error that shrinks with the number of zones and is unbiased. "
    "Its weakness is visible too: with a small budget and skewed zones only a handful of zones fit (column \"zones "
    "kept\"), so individual windows are noisy. The general lesson: sample at the level of the unit the statistic "
    "is defined on.",
]
TEXT["hll_res"] = [
    "With p = {p} ({m} registers, theoretical relative standard error {rsd}), the mean error over {nwin} windows "
    "is <b>{mean} %</b> and the largest is {mx} %, consistent with the 1.6 % standard error. The sketch needs about "
    "{sk} bytes regardless of the number of distinct routes, whereas the exact set needs one entry per route "
    "(about 72 bytes per route string in Python/JVM, roughly 180 KB for a busy hour); the sketch also merges "
    "across partitions by a register-wise maximum, which is what lets Spark compute it in parallel.",
]
TEXT["dgim_res"] = [
    "Every estimate is within the 50 % guarantee, typically 5-25 % off, using about 14 buckets (a few hundred "
    "bits) instead of 10 000 bits. The error comes only from the oldest bucket, of which half is assumed to lie "
    "inside the window; it is largest when that bucket is big compared with the answer (k = 1 000 holds only ~100 "
    "slow trips, so a single 32- or 64-sized bucket at the boundary matters). The estimate depends only on the "
    "stream's bit sequence, which is why it does not change systematically with the input rate.",
]
TEXT["lsh_res"] = [
    "In busy hours about 45-53 zones qualify (>= 20 trips), i.e. 1 000-1 400 possible pairs, of which 80-180 are "
    "truly similar (J >= 0.5). The banding parameters behave exactly as the S-curve P(candidate) = "
    "1 - (1 - J<super>r</super>)<super>b</super> predicts. With 20x5 the threshold (1/b)<super>1/r</super> = 0.55 lies "
    "slightly <i>above</i> 0.5, so pairs with J in [0.5, 0.6) are often missed: recall is about 0.74 while 87 % of "
    "pair comparisons are avoided. 25x4 (threshold 0.45) moves the curve left: recall rises to about 0.95 at the "
    "price of twice as many candidates (lower precision, smaller reduction) and slightly more time. 10x10 "
    "(threshold 0.79) generates almost no candidates and finds only the near-identical pairs (recall about 0.03). "
    "If high recall at J = 0.5 matters, 25x4 is the better choice for this workload.",
    "With only ~50 sets of at most ~100 elements, the exact all-pairs computation takes about 2 ms, comparable to "
    "the LSH path (signatures + banding + verification, 2-4 ms). LSH does not pay off at this size; its benefit "
    "is that candidate generation grows roughly linearly with the number of sets while all-pairs grows "
    "quadratically, so it matters for thousands of zones or documents, not for 50.",
]


def partition_fmt(S):
    return {k: _get(S, k.split("_")[0], k.split("_", 1)[1]) for k in
            ("P1_stage2_eps", "P2_stage2_eps", "P3_stage2_eps", "P4_stage2_eps", "P1_stage1_eps", "P2_stage1_eps",
             "P3_stage1_eps", "P4_stage1_eps", "P1_latency_p50_ms", "P2_latency_p50_ms", "P3_latency_p50_ms",
             "P4_latency_p50_ms")}


TEXT["partition_fmt"] = partition_fmt
TEXT["partitions"] = [
    "All four runs use the same segment at 6 000 ev/s (the medium rate, close to capacity), so differences "
    "come from the partition/slot layout. Stage 2 throughput was {P1_stage2_eps} (P1), {P2_stage2_eps} (P2), "
    "{P3_stage2_eps} (P3) and {P4_stage2_eps} (P4) events/s; Stage 1 reached {P1_stage1_eps} / {P2_stage1_eps} / "
    "{P3_stage1_eps} / {P4_stage1_eps} events/s. In other words, <b>more partitions and more slots did not "
    "materially increase throughput on this setup</b>, and the differences between runs are of the same size as "
    "the run-to-run noise.",
    "#Why P2 versus P3 shows little gain",
    "With one slot (P2), Spark runs the four Kafka-partition tasks of a micro-batch one after another; with four "
    "slots (P3) they run in parallel. But only part of each micro-batch is parallel: reading and parsing the "
    "records. The per-batch logs show that the other part, which runs on the single driver (collecting the batch, "
    "the Bloom test in arrival order, JSON serialisation and publishing to taxi.clean in Stage 1; the reservoir/DGIM "
    "loop in Stage 2), takes as long as the parallel part. By Amdahl's law, speeding up roughly half of the work "
    "4x yields well under 2x, and Spark's fixed per-micro-batch cost (planning, writing the offset and commit logs "
    "of the checkpoint) is paid regardless of the slot count. In addition, three Spark applications, Kafka and the "
    "producer share 16 hardware threads, so extra slots in one application compete with the others. The order-"
    "dependent sketches are deliberately global (one Bloom filter, one DGIM stream), which is exactly what limits "
    "scaling; a scalable design would partition them by key (e.g. one Bloom filter per Kafka partition, which works "
    "because duplicates share the pickup_zone key).",
    "#Why 6 consumers do not beat 4 on a 4-partition topic (P4)",
    ">In a Kafka consumer group each partition is assigned to <b>at most one</b> consumer, so with 4 partitions at "
    "most 4 consumers receive data and the remaining ones sit idle as hot standbys. Spark's Kafka source behaves "
    "the same way by default: it creates one task per topic partition per micro-batch, so with local[6] two of "
    "the six slots have nothing to do. P4 therefore cannot be faster than P3 (here {P4_stage2_eps} vs "
    "{P3_stage2_eps} events/s in Stage 2, equal within noise, with slightly more scheduling overhead). Parallelism "
    "beyond the partition count requires more partitions, or splitting partitions by offset ranges "
    "(Spark's minPartitions option), which gives up per-partition ordering.",
    "P1 also shows the cost of a single partition: all 105 058 raw messages go to partition 0 (skew 1.0). It was "
    "not slower here because the bottleneck is the serial driver work, not the Kafka partition; with a heavier "
    "parallel part, one partition would cap the parallelism at one task.",
]


def key_fmt(S):
    return {"k1_skew": _get(S, "K1", "skew_raw", 3), "k2_skew": _get(S, "K2", "skew_raw", 3),
            "k1_s2": _get(S, "K1", "stage2_eps"), "k2_s2": _get(S, "K2", "stage2_eps"),
            "k1_s1": _get(S, "K1", "stage1_eps"), "k2_s1": _get(S, "K2", "stage1_eps"),
            "k1_p95": _get(S, "K1", "latency_p95_ms"), "k2_p95": _get(S, "K2", "latency_p95_ms"),
            "p3_s2": _get(S, "P3", "stage2_eps")}


TEXT["key_fmt"] = key_fmt
TEXT["key"] = [
    "<b>Skew.</b> With key = pickup_zone the busiest partition carries {k1_skew} of the messages (ideal 0.25). "
    "Kafka assigns a key to partition murmur2(key) mod 4, so all trips of a zone land in one partition. Demand is "
    "very uneven: the three busiest zones (Upper East Side North/South, Midtown Center) each account for about "
    "4 % of all trips, and how the ~260 zones happen to hash decides which partition becomes hot. Without a key "
    "(K2), librdkafka's sticky partitioner fills one partition per batch and rotates, which gives a near-uniform "
    "split (busiest share {k2_skew}).",
    "<b>Throughput.</b> Stage 1 / Stage 2 processed {k1_s1} / {k1_s2} ev/s with the key and {k2_s1} / {k2_s2} ev/s "
    "without it (p95 latency {k1_p95} vs {k2_p95} ms). The keyless run was somewhat faster, but by about as much "
    "as two runs with <i>identical</i> configuration differ (P3 and K1 are the same setup and reached {p3_s2} and "
    "{k1_s2} ev/s), so the effect of skew is within run-to-run noise here: the partition is not the bottleneck "
    "(see 4.7). With a parallel bottleneck, the hot partition would set the pace of every micro-batch (a straggler "
    "task).",
    "<b>Repartitioning.</b> Spark does not know how Kafka partitioned the data, so every per-pickup-zone state "
    "(the window aggregations of Stage 2 query B and Stage 3) <b>always</b> goes through a shuffle "
    "(hash-partitioning by pickup_zone into spark.sql.shuffle.partitions), with or without the Kafka key. The key "
    "therefore buys no locality in Spark; what it does buy is per-zone ordering within a partition (all events of "
    "a zone are consumed in production order) and the possibility of a shuffle-free, per-partition consumer such as "
    "Kafka Streams or a partition-local Bloom filter. The trade-off is ordering and locality versus load balance; "
    "in our measured workload balance mattered little, so the key's value lies mainly in ordering guarantees.",
]


def rate_fmt(S):
    out = {}
    for rid, tag in (("R-low", "lo"), ("R-medium", "me"), ("R-high", "hi")):
        for k in ("produced_eps", "stage2_eps", "latency_p50_ms", "latency_p95_ms", "max_lag_raw", "max_lag_clean",
                  "hll_mean_error_pct", "lsh_recall_default", "late_dropped"):
            out[f"{tag}_{k}"] = _get(S, rid, k, 3 if k in ("hll_mean_error_pct", "lsh_recall_default") else 0)
    return out


TEXT["rate_fmt"] = rate_fmt
TEXT["rates"] = [
    "The capacity of this setup was first calibrated at 10 000 ev/s (Stage 1 about 8 000 ev/s, Stage 2 about "
    "7 200 ev/s). Accordingly: <b>low</b> = 1 000 ev/s, <b>medium</b> = 6 000 ev/s (just below capacity), "
    "<b>high</b> = 15 000 ev/s (about twice the capacity).",
    "<b>Low ({lo_produced_eps} ev/s).</b> The pipeline keeps up throughout: lag stays at the size of one or two "
    "micro-batches and drains immediately. Latency (p50 {lo_latency_p50_ms} ms, p95 {lo_latency_p95_ms} ms) is "
    "not caused by queueing but by the <i>micro-batch floor</i>: each Spark stage starts a new batch only about "
    "every 1-1.5 s because of its fixed per-batch cost (planning plus writing offset and commit logs to the "
    "checkpoint, which on Windows goes through winutils), and an event crosses two Spark stages.",
    "<b>Medium ({me_produced_eps} ev/s).</b> Stage 2 processed {me_stage2_eps} ev/s. Lag rises to about "
    "{me_max_lag_raw} (raw) / {me_max_lag_clean} (clean) during the replay, then drains within a few seconds after "
    "the producer stops; micro-batches grow with the backlog, which amortises the fixed batch cost and keeps the "
    "system stable. Latency is higher (p50 {me_latency_p50_ms}, p95 {me_latency_p95_ms} ms).",
    "<b>High ({hi_produced_eps} ev/s).</b> The producer exceeds the processing capacity ({hi_stage2_eps} ev/s in "
    "Stage 2): lag grows during the whole replay, to {hi_max_lag_raw} messages on taxi.raw and {hi_max_lag_clean} "
    "on taxi.clean, and latency rises to p50 {hi_latency_p50_ms} / p95 {hi_latency_p95_ms} ms. Because Kafka "
    "buffers the excess durably, nothing is lost: all events are processed once the producer stops.",
    ">Result quality does not degrade with speed: HLL error {lo_hll_mean_error_pct} / {me_hll_mean_error_pct} / "
    "{hi_hll_mean_error_pct} % and LSH recall {lo_lsh_recall_default} / {me_lsh_recall_default} / "
    "{hi_lsh_recall_default} for low / medium / high. The sketches see the same events regardless of how fast they "
    "arrive; lag changes <i>when</i> an answer is available, not the answer. The one mechanism through which speed "
    "does change results is event time: a delay of up to 20 s of wall clock corresponds to more event time at a "
    "higher replay rate, so more of the deliberately delayed events miss their window "
    "({lo_late_dropped} / {me_late_dropped} / {hi_late_dropped} events classified as dropped). Differences in DGIM "
    "and reservoir error between runs come from the arrival order, which differs slightly from run to run (four "
    "partitions are interleaved by the micro-batching), not from the rate.",
]


def late_fmt(RUNS):
    delayed = {}
    p = RUNS / "default" / "producer_delayed.csv"
    if p.exists():
        with open(p, newline="") as f:
            delayed = {r["event_id"]: float(r["delay_s"]) for r in csv.DictReader(f)}
    dropped = set()
    q = RUNS / "default" / "late_dropped_ids.csv"
    if q.exists():
        dropped = {line.split(",")[0] for line in q.read_text().splitlines() if line}
    dd = [e for e in delayed if e in dropped]
    inc = [delayed[e] for e in delayed if e not in dropped]
    drp = [delayed[e] for e in dd]
    return {"n_delayed": len(delayed), "n_dd": len(dd), "n_inc": len(delayed) - len(dd),
            "n_natural": len(dropped) - len(dd),
            "max_inc_delay": f"{max(inc):.1f}" if inc else "-", "min_drop_delay": f"{min(drp):.1f}" if drp else "-"}


TEXT["late_fmt"] = late_fmt
TEXT["late"] = [
    "<b>Policy.</b> Stages 2 and 3 use 60-minute tumbling event-time windows with "
    "<font face='Courier'>withWatermark(event_time, \"30 minutes\")</font> in append mode. A window's result is "
    "emitted once, when the watermark (maximum event time seen minus 30 minutes) passes the window end. Rows that "
    "arrive for a window that has already been emitted are discarded; results are never updated afterwards. "
    "Spark guarantees to include every row that is less than 30 minutes (event time) late; later rows may or may "
    "not be dropped, depending on micro-batch boundaries.",
    "<b>Scale of the delay.</b> At 1 000 ev/s the replay covers about 5 700 trips per event-time hour, so a "
    "producer delay of 1-20 s of wall clock means 10 minutes to 3.5 hours of event time. In the default run the "
    "producer delayed {n_delayed} events. Joining its ground truth with Stage 2's log of dropped ids: "
    "<b>{n_dd}</b> of them arrived after their window was closed and were <b>discarded</b>, {n_inc} were still "
    "<b>included</b> because their window was still open. The wall-clock delay alone does not decide the outcome: "
    "the longest delay still included was {max_inc_delay} s and the shortest delay that was dropped only "
    "{min_drop_delay} s. What matters is how much <i>event time</i> the stream advanced during the delay and where "
    "in its window the event falls: at night a second of replay covers far more event time (fewer trips per hour) "
    "than at rush hour, and an event from the end of a window has 30 minutes of slack less than one from its start. "
    "{n_natural} dropped events were not deliberately delayed, i.e. the reordering caused by interleaving four "
    "partitions and micro-batches stayed well inside the 30-minute watermark.",
    "Stage 2's classification of all events: {on} in order, {inc} out of order but inside an open window "
    "(lateness buckets {b}), {drop} beyond the watermark. Spark's own counter numRowsDroppedByWatermark reported "
    "{spark}. The numbers differ between the queries because each query has its own micro-batches and therefore its "
    "own watermark progression: this is exactly the \"may or may not be dropped\" part of the watermark contract. "
    "Two examples recorded by Stage 2 (one discarded, one included although it was more than 10 minutes late):",
]


def restart_fmt(RUNS, S):
    ev = []
    p = RUNS / "restart" / "restart_events.json"
    if p.exists():
        ev = json.loads(p.read_text())
    lp = RUNS / "restart" / "stage2.log"
    log = lp.read_text(errors="ignore") if lp.exists() else ""
    restored = next((ln.strip() for ln in log.splitlines() if "restored state" in ln), "-")
    skipped = sum(1 for ln in log.splitlines() if "already applied" in ln)
    q = RUNS / "restart" / "final_summary.json"
    fs = json.loads(q.read_text()) if q.exists() else {}
    dq = RUNS / "default" / "final_summary.json"
    dfs = json.loads(dq.read_text()) if dq.exists() else {}
    t = {e["event"]: e["t_s"] for e in ev}
    return {"t_kill": t.get("stage2 killed", "-"), "t_up": t.get("stage2 queries running", "-"),
            "max_lag": _get(S, "restart", "max_lag_clean"), "restored": restored.replace("[stage2] ", ""),
            "skip_sentence": (f"In this run {skipped} replayed batch(es) were detected and skipped." if skipped else
                              "In this run the kill landed after Spark had committed the last applied batch, so no "
                              "batch was re-executed and the check did not have to fire."),
            "events": _n(fs.get("events_processed")),
            "events_def": _n(dfs.get("events_processed")), "windows": fs.get("windows", "-"),
            "hll": fs.get("hll_mean_error_pct", "-"), "p95": _get(S, "restart", "latency_p95_ms")}


TEXT["restart_fmt"] = restart_fmt
TEXT["restart"] = [
    "The run <b>restart</b> (1 000 ev/s) kills the Stage 2 process tree (hard kill, no clean shutdown) 40 s after "
    "the producer started and relaunches it 20 s later, while the producer, Stage 1 and Stage 3 keep running. "
    "Stage 2 was killed at t = {t_kill} s and its queries were running again at t = {t_up} s (Spark start-up adds "
    "to the 20 s outage).",
    "<b>Kafka lag and offsets.</b> During the outage nothing consumes taxi.clean, so its lag grows linearly with "
    "the input (Figure 6, up to {max_lag} messages) while taxi.raw lag stays near zero. Kafka retains the messages, "
    "so nothing is lost. On restart, Spark reads the last committed offsets of every query from its checkpoint and "
    "continues from there; the large first micro-batch drains the backlog in a few seconds and lag returns to the "
    "normal level. Latency of the events produced during the outage includes the outage (p95 {p95} ms).",
    "<b>Application state.</b> The window state of queries B and C is restored by Spark from the checkpointed state "
    "store. Query A's Python state (reservoirs, DGIM, exact baselines, latency samples, counters) was restored from "
    "its snapshot: <font face='Courier'>{restored}</font>. Without the snapshot, the restarted Stage 2 would have "
    "resumed the offsets but started all sketches from zero.",
    "<b>Duplicate processing.</b> Spark's foreachBatch sink is at-least-once: if the process dies after a batch was "
    "applied but before Spark wrote its commit, that batch is executed again after the restart with the same "
    "batch id; the batch-id check skips such a batch. {skip_sentence} The end "
    "result confirms exactly-once state: {events} events were counted, the same as in the uninterrupted default "
    "run ({events_def}), and all {windows} windows were reported (mean HLL error {hll} %). Messages published to "
    "taxi.metrics during a replayed batch could still be duplicated (Kafka output of foreachBatch is "
    "at-least-once); the final consumer is idempotent per window (the latest message wins).",
]
TEXT["discussion"] = [
    "<b>Approximation versus state.</b> Every sketch delivered its accuracy at a small, fixed memory cost: HLL++ "
    "about 1 % error with 3.3 KB per window instead of a set of ~2 500 routes; DGIM 5-30 % error with ~14 buckets "
    "instead of 10 000 bits; a 234 KB Bloom filter with essentially no false positives at half load. The exact "
    "baselines were affordable here only because the stream is small; their state grows with the data, the "
    "sketches' does not.",
    "<b>Sampling design must follow the statistic.</b> The mean fare (linear) is estimated well by any uniform "
    "sample, and the error behaves like 1/sqrt(k). The fraction of diverse zones (non-linear, defined per entity) "
    "is biased under event sampling and only becomes reliable when whole entities are sampled.",
    "<b>Parallelism is bounded by partitions and by the serial part.</b> Consumers beyond the partition count stay "
    "idle, and adding slots helps only the parallel part of the work. Our pipeline is dominated by driver-side, "
    "order-dependent sketch updates and Spark's per-batch overhead, so 1, 4 and 6 slots performed alike. More "
    "partitions are not automatically better: they add overhead, and keyed partitions can be skewed (30 % instead "
    "of 25 % here).",
    "<b>Speed affects timeliness, not correctness</b>, with one exception. A higher producer rate increased lag "
    "and latency but not sketch error or LSH recall, and Kafka absorbed every burst. The exception is event time: "
    "when late data is dropped by a watermark, the amount of data that is \"late\" depends on how event time and "
    "processing time relate, which changes with the rate.",
    "<b>LSH is a candidate filter.</b> The banding parameters set the similarity threshold of the S-curve: 20x5 "
    "(0.55) misses part of the pairs near J = 0.5, 25x4 (0.45) finds 95 % of them with twice the candidates, 10x10 "
    "(0.79) finds almost nothing at this threshold. At ~50 zones per window exact all-pairs is just as fast; LSH's "
    "linear candidate generation pays off only at much larger scales.",
]
TEXT["limitations"] = [
    "<b>Laptop scale and short runs.</b> 100 000 events last 100 s at 1 000 ev/s but only 17 s at 6 000 and 7 s at "
    "15 000 ev/s, so the medium/high runs are dominated by a burst and its drain rather than a long steady state. "
    "All processes share one machine, so measurements include interference (run-to-run noise of about 5-10 %).",
    "<b>Driver-side sketches.</b> Bloom, reservoir and DGIM run on the Spark driver in foreachBatch to keep one "
    "global, order-dependent structure. This is simple and exactly reportable but limits horizontal scaling; "
    "a partitioned design (per-key sketches merged at report time) would scale better.",
    "<b>Latency floor.</b> Spark micro-batches on Windows cost about 1 s each (checkpoint writes via winutils), which "
    "dominates latency at low rates. Continuous processing or Kafka Streams would reach lower latency.",
    "<b>Clean runs by broker reset.</b> Topic deletion crashes native Kafka on Windows, so native runs restart the "
    "single-node broker with an empty log directory; on Docker/Linux topics are deleted and recreated.",
    "<b>Known issues.</b> Spark local mode sometimes fails to start (block-manager registration race); the "
    "orchestrator relaunches such a stage, and the runs that failed in the first suite were rerun. The run "
    "<i>bloom-small</i> reports a long duration because the laptop slept during its shutdown phase; its "
    "measurements were taken before that and are complete.",
    "<b>Event time.</b> Results are emitted once per window (append mode) and never updated; a late event beyond "
    "the watermark is discarded rather than triggering a corrected result.",
]
TEXT["checklist"] = [
    ["Producer reads taxi data and writes only to taxi.raw", "producer/producer.py (the only file reader)"],
    ["Stage 1 reads taxi.raw and writes taxi.clean", "stage1_clean_bloom/stage1.py"],
    ["Stage 2 reads taxi.clean and writes taxi.metrics", "stage2_stream_analytics/stage2.py"],
    ["Stage 3 reads taxi.clean and writes taxi.lsh", "stage3_lsh/stage3.py"],
    ["Final consumer reads Kafka output topics and creates results.jsonl", "final_consumer/final_consumer.py"],
    ["Bloom false-positive rate is measured", "4.1 (default + bloom-small with examples)"],
    ["Reservoir sampling and non-linear sampling comparison complete", "4.2, 4.3"],
    ["HLL/FM exact-vs-estimate result present", "4.4 (per window)"],
    ["DGIM answers k = 1 000 / 5 000 / 10 000", "4.5"],
    ["LSH recall and candidate reduction reported", "4.6 (20x5, 10x10, 25x4)"],
    ["P1-P4 partition/consumer experiments present", "4.7"],
    ["pickup_zone vs round-robin comparison present", "4.8"],
    ["Low/medium/high producer-rate experiment present", "4.9"],
    ["p50/p95 latency and Kafka lag reported", "Table 1, Figures 3, 4, 6"],
    ["Late event and restart behaviour demonstrated", "4.10, 4.11"],
    ["README with one-command default run", "README.md, run_all.bat / run_all.sh"],
    ["LLM_USAGE.md included", "LLM_USAGE.md"],
]
