# LLM usage

> **Team: review and personalise this file before submitting.** Sections 2 and 4 must reflect what
> *you* asked and what *you* can explain; the drafts below are written from the development log of
> this repository.

## 1. Assistants used

* **Claude (Opus 5.5) via Claude Code** (VS Code extension, agentic mode): project analysis,
  environment setup on Windows, implementation of all stages, debugging, experiment runs, README draft.

## 2. Five prompts that materially helped

1. *"analyse this project"* - with only the assignment PDF, the parquet file and the zone lookup in the
   folder; produced the requirements breakdown (topics, stages, marks), the data profile (3.72 M trips,
   45 k non-positive durations, 39 k negative fares, 7 pickups outside January) and the stack decision.
2. *"install all the things which are not in the system"* - Python 3.12 venv, Java 17, Kafka 3.9.1,
   Hadoop winutils; worked around the missing admin rights / UAC prompts (see section 3).
3. *"make complete project required in the given pdf"* - the implementation itself, iterated through
   smoke runs (20 k and 50 k events) before the full experiment suite.
4. *(team prompt, example)* "Explain why Spark's `numRowsDroppedByWatermark` differs between the HLL
   and the diversity query although both read the same topic with the same watermark."
5. *(team prompt, example)* "Why does DGIM keep at most two buckets of each size, and where does the
   50 % error bound come from?"

## 3. Cases where generated code / advice was wrong or unsuitable, and what changed

| # | What the first version did | Symptom | Fix |
|---|---|---|---|
| 1 | Recreated topics between runs with `AdminClient.delete_topics` (the standard advice). | On native Windows the broker cannot rename open segment files: `AccessDeniedException ... taxi.raw-1 -> taxi.raw-1.<id>-delete`, then *"Shutdown broker because all log dirs have failed"*. | In native mode a clean run = stop broker, wipe its data directory, restart, create topics (`final_consumer/kafka_ctl.py fresh_topics`). Delete/recreate is kept for Docker/Linux. |
| 2 | Stage 2/3 filtered control rows with `.where(route_key IS NOT NULL)` *after* `withWatermark`, before the window aggregation. | The last event-time windows never closed and HLL results were missing. Spark's optimizer pushes the filter *below* the watermark operator, so the end-of-stream marker (far-future event time) never advanced the watermark. | No filter before the aggregation; aggregate functions ignore the null columns of control rows, and the marker's window is skipped in `foreachBatch`. |
| 3 | HLL query read finished windows with `batch_df.collect()`. | HLL windows started at 05:30, 06:30, ... while diversity/LSH windows started at 00:00: `collect()` converts timestamps to the machine's local zone (IST, +5:30), ignoring `spark.sql.session.timeZone=UTC`. | Use `toPandas()` (Arrow path honours the session time zone). |
| 4 | Producer reported `messages / total elapsed time`. | Reported 720 ev/s for a requested 2000 ev/s although the per-second log showed ~2000: elapsed time included up to 20 s spent releasing the last delayed events. | Report the replay-phase rate (`actual_eps`) and the tail-inclusive rate separately; stage throughput is measured over the first 95 % of events from per-batch timelines. |
| 5 | Suggested the latest packages (pandas 3, numpy 2, pyarrow 26) with PySpark 3.5 on Python 3.12/3.14. | `ModuleNotFoundError: distutils` inside `toPandas()`; pyarrow 26 refuses numpy 1.x. | Python 3.12 venv, `setuptools` (distutils shim), pandas 2.2.3, numpy 1.26.4, pyarrow 17 (`requirements.txt`). |
| 6 | Reservoir-sampling error for `trip_distance` stayed ~27 % even at k = 10 000. | Looked like a reservoir bug; actually the data contains odometer garbage up to 269 097 miles that dominates the exact mean. | Stage 1 rejects `trip_distance > 100` miles and `fare_amount > 1000` (`bad_distance`, `bad_fare`). |
| 7 | Spark on Windows: `winutils.exe` exited with `STATUS_DLL_NOT_FOUND`; the VC++ 2010 installer needs admin. | Every streaming query failed when writing its checkpoint. | Extract the Microsoft-signed `msvcr100.dll` from the official redistributable's cab (`final_consumer/setup_windows.ps1`) and place it next to `winutils.exe`. |
| 8 | Three Spark apps started simultaneously with default driver binding. | Intermittent `NullPointerException ... BlockManagerId` at start-up. | `spark.driver.host/bindAddress = 127.0.0.1`, staggered start-up. |
| 9 | Wiped the broker's data directory with `shutil.rmtree(path, ignore_errors=True)`. | After a broker crash every restart failed again (`FileSystemException ... 00000000000000000000.index -> .deleted`), even after `reset`. Kafka marks its metadata snapshots (`*.checkpoint`) read-only; on Windows `rmtree` cannot delete read-only files and `ignore_errors` hid that, so a stale snapshot survived every wipe. | `final_consumer/kafka_ctl.py wipe()` clears the read-only bit before deleting; `start` wipes and retries once if the broker dies during start-up. |

## 4. What the team can explain without the LLM

> *Draft - replace with your own words.* We can explain the topic flow and why every stage reads
> and writes Kafka; why the Bloom filter only marks events (false positives would silently delete
> real trips); Algorithm R and why reservoir error shrinks roughly with 1/sqrt(k); why event sampling
> underestimates the fraction of diverse zones while entity sampling does not (the statistic depends on
> each zone's complete destination set); HLL precision vs memory; DGIM's bucket invariant, merging and
> error bound; MinHash/banding and the S-curve threshold (1/b)^(1/r) that explains the recall/candidate
> trade-off between 20x5, 25x4 and 10x10; event-time windows, watermarks and why late data beyond the
> watermark may or may not be dropped; why consumers beyond the partition count stay idle; and the
> restart behaviour (Spark checkpoint offsets + our idempotent state snapshot).
