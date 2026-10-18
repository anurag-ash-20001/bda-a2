"""Build the assignment report PDF:  python scripts/build_report.py  ->  Assignment2_Report.pdf

Code excerpts are pulled from the source files (by function/class name) and all numbers
from results/, so the report always matches the submitted code and results."""
import ast
import csv
import json
import re
import sys
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (CondPageBreak, Image, KeepTogether, PageBreak, Paragraph, Preformatted, SimpleDocTemplate,
                                Spacer, Table, TableStyle)

ROOT = Path(__file__).resolve().parent.parent
RUNS = ROOT / "results" / "runs"
PLOTS = ROOT / "results" / "plots"
sys.path.insert(0, str(ROOT))
from scripts.report_text import TEXT  # noqa: E402  (narrative, kept separate from layout)

NAVY = colors.HexColor("#1f3b5a")
LIGHT = colors.HexColor("#e8eef5")
ss = getSampleStyleSheet()
H1 = ParagraphStyle("H1", parent=ss["Heading1"], textColor=NAVY, fontSize=16, spaceBefore=6, spaceAfter=8)
H2 = ParagraphStyle("H2", parent=ss["Heading2"], textColor=NAVY, fontSize=12.5, spaceBefore=10, spaceAfter=5)
H3 = ParagraphStyle("H3", parent=ss["Heading3"], textColor=NAVY, fontSize=10.5, spaceBefore=6, spaceAfter=3)
BODY = ParagraphStyle("B", parent=ss["BodyText"], fontSize=9.4, leading=12.6, spaceAfter=5)
SMALL = ParagraphStyle("S", parent=BODY, fontSize=8, leading=10)
CELL = ParagraphStyle("C", parent=BODY, fontSize=7.6, leading=9.2, spaceAfter=0)
CELLB = ParagraphStyle("CB", parent=CELL, fontName="Helvetica-Bold")
CODE = ParagraphStyle("Code", fontName="Courier", fontSize=6.9, leading=8.2, backColor=colors.HexColor("#f5f5f5"),
                      borderColor=colors.HexColor("#dddddd"), borderWidth=0.5, borderPadding=4, spaceBefore=3,
                      spaceAfter=8)
CAPTION = ParagraphStyle("Cap", parent=BODY, fontSize=8, textColor=colors.HexColor("#555555"), alignment=TA_CENTER)
CALLOUT = ParagraphStyle("Call", parent=BODY, backColor=LIGHT, borderPadding=6, borderColor=LIGHT, borderWidth=1,
                         spaceBefore=4, spaceAfter=9)


# ------------------------------------------------------------------ data helpers
def jload(path):
    return json.loads(path.read_text()) if path.exists() else {}


def summary():
    p = ROOT / "results" / "experiment_summary.csv"
    if not p.exists():
        return {}
    with open(p, newline="") as f:
        return {r["run_id"]: r for r in csv.DictReader(f)}


def f(v, nd=1):
    if v in (None, "", "None"):
        return "-"
    try:
        x = float(v)
    except (TypeError, ValueError):
        return str(v)
    if nd == 0 or x.is_integer() and abs(x) >= 100:
        return f"{x:,.0f}"
    return f"{x:,.{nd}f}"


def windows(run):
    p = RUNS / run / "results.jsonl"
    if not p.exists():
        return [], {}
    recs = [json.loads(line) for line in p.read_text().splitlines() if line.strip()]
    wins = sorted((r for r in recs if r.get("type") != "run_summary" and r.get("hll")), key=lambda r: r["window_start"])
    summ = next((r for r in recs if r.get("type") == "run_summary"), {})
    return wins, summ


# ------------------------------------------------------------------ flowable helpers
def P(text, style=BODY):
    return Paragraph(text, style)


def table(rows, widths=None, header=True, font=7.6, zebra=True):
    data = [[c if isinstance(c, Paragraph) else Paragraph(str(c), CELLB if (header and i == 0) else CELL)
             for c in row] for i, row in enumerate(rows)]
    t = Table(data, colWidths=widths, repeatRows=1 if header else 0)
    st = [("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#b8c4d0")),
          ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
          ("TOPPADDING", (0, 0), (-1, -1), 2.2), ("BOTTOMPADDING", (0, 0), (-1, -1), 2.2),
          ("LEFTPADDING", (0, 0), (-1, -1), 3), ("RIGHTPADDING", (0, 0), (-1, -1), 3)]
    if header:
        st.append(("BACKGROUND", (0, 0), (-1, 0), LIGHT))
    if zebra:
        for i in range(2 if header else 1, len(rows), 2):
            st.append(("BACKGROUND", (0, i), (-1, i), colors.HexColor("#f7f9fb")))
    t.setStyle(TableStyle(st))
    return t


def img(path, width_cm, caption=None):
    if not Path(path).exists():
        return [P(f"<i>(missing figure {Path(path).name})</i>", SMALL)]
    from reportlab.lib.utils import ImageReader
    iw, ih = ImageReader(str(path)).getSize()
    w = width_cm * cm
    out = [Image(str(path), width=w, height=w * ih / iw)]
    if caption:
        out.append(P(caption, CAPTION))
    out.append(Spacer(1, 6))
    return out


def code_of(relpath, name, max_lines=60, start_pat=None, end_pat=None):
    """Source of a function/class (`name`, may be 'Class.method') or a regex-delimited block."""
    src = (ROOT / relpath).read_text(encoding="utf-8")
    lines = src.splitlines()
    if name:
        tree = ast.parse(src)
        parts = name.split(".")
        node = tree
        for part in parts:
            node = next(n for n in ast.walk(node) if isinstance(n, (ast.FunctionDef, ast.ClassDef))
                        and n.name == part)
        seg = lines[node.lineno - 1 - len(node.decorator_list):node.end_lineno]
        first = node.lineno
    else:
        s = next(i for i, l in enumerate(lines) if re.search(start_pat, l))
        e = next(i for i, l in enumerate(lines[s + 1:], s + 1) if re.search(end_pat, l))
        seg = lines[s:e + 1]
        first = s + 1
    indent = min((len(l) - len(l.lstrip()) for l in seg if l.strip()), default=0)
    seg = [l[indent:] for l in seg]
    if len(seg) > max_lines:
        seg = seg[:max_lines] + ["    ... (truncated, see source)"]
    return seg, first


def code(relpath, name=None, max_lines=60, title=None, **kw):
    seg, first = code_of(relpath, name, max_lines, **kw)
    hdr = title or f"{relpath}  (line {first})"
    return KeepTogether([P(f"<font size=7.5 color='#555555'><b>{hdr}</b></font>", SMALL),
                         Preformatted("\n".join(seg), CODE)]) if len(seg) < 45 else \
        [CondPageBreak(9 * cm), P(f"<font size=7.5 color='#555555'><b>{hdr}</b></font>", SMALL),
         Preformatted("\n".join(seg), CODE)]


def add(story, x):
    if isinstance(x, list):
        story.extend(x)
    else:
        story.append(x)


def text(story, key, **fmt):
    """Append narrative paragraphs from report_text.TEXT[key] (list of strings; '>' prefix = callout)."""
    for para in TEXT[key]:
        para = para.format(**fmt) if fmt else para
        if para.startswith(">"):
            story.append(P(para[1:].strip(), CALLOUT))
        elif para.startswith("#"):
            story.append(P(para[1:].strip(), H3))
        else:
            story.append(P(para))


# ------------------------------------------------------------------ page decoration
def on_page(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 7.5)
    canvas.setFillColor(colors.HexColor("#777777"))
    canvas.drawString(1.8 * cm, 1.1 * cm, "Big Data Analytics - Assignment 2: Real-Time Taxi Stream Analytics")
    canvas.drawRightString(A4[0] - 1.8 * cm, 1.1 * cm, f"Page {doc.page}")
    canvas.restoreState()


def build():
    S = summary()
    d_wins, d_sum = windows("default")
    s1 = jload(RUNS / "default" / "stage1_stats.json")
    prod = jload(RUNS / "default" / "producer_stats.json")
    story = []

    # ============================================================== title
    story += [Spacer(1, 3.5 * cm),
              P("<font size=11 color='#555555'>BIG DATA ANALYTICS</font>", ParagraphStyle("t0", alignment=TA_CENTER)),
              Spacer(1, 10),
              P("<font size=24 color='#1f3b5a'><b>Assignment 2</b></font>", ParagraphStyle("t1", alignment=TA_CENTER,
                                                                                         leading=30)),
              P("<font size=17 color='#1f3b5a'>Real-Time Taxi Stream Analytics</font>",
                ParagraphStyle("t2", alignment=TA_CENTER, leading=24)),
              P("<font size=11>Apache Kafka + Spark Structured Streaming + Streaming Algorithms + MinHash-LSH</font>",
                ParagraphStyle("t3", alignment=TA_CENTER, leading=16)),
              Spacer(1, 1.2 * cm)]
    add(story, img(ROOT / "architecture.png", 16.5))
    story += [Spacer(1, 0.8 * cm),
              P("Implementation, code analysis and experimental results", ParagraphStyle("t4", alignment=TA_CENTER,
                                                                                         fontSize=11)),
              Spacer(1, 0.4 * cm),
              P("Team: ______________________________ &nbsp;&nbsp; Roll numbers: ______________________",
                ParagraphStyle("t5", alignment=TA_CENTER, fontSize=10)),
              Spacer(1, 0.3 * cm),
              P(TEXT["date"], ParagraphStyle("t6", alignment=TA_CENTER, fontSize=9, textColor=colors.grey)),
              PageBreak()]

    # ============================================================== 1 summary
    story.append(P("1. Executive summary", H1))
    text(story, "summary")
    rows = [["Requirement (brief section)", "Where", "Result (default run unless stated)"]]
    for r in TEXT["summary_rows"](S, d_sum, s1, prod):
        rows.append(r)
    story.append(table(rows, [4.6 * cm, 3.7 * cm, 9.1 * cm]))
    story.append(PageBreak())

    # ============================================================== 2 architecture
    story.append(P("2. System architecture and environment", H1))
    text(story, "architecture")
    rows = [["Topic", "Partitions", "Key", "Producer -> consumer(s)", "Content"],
            ["taxi.raw", "4 (1 in P1)", "pickup_zone (none in K2)", "producer -> Stage 1", "raw trip events + EOS/WARMUP control"],
            ["taxi.clean", "4 (1 in P1)", "pickup_zone (none in K2)", "Stage 1 -> Stage 2, Stage 3",
             "validated + enriched events, Bloom flag"],
            ["taxi.metrics", "1", "-", "Stage 2 -> final consumer", "periodic summaries, per-window HLL and diversity"],
            ["taxi.lsh", "1", "-", "Stage 3 -> final consumer", "per-window LSH metrics + candidate pairs"]]
    story.append(table(rows, [2.2 * cm, 2.0 * cm, 3.2 * cm, 4.2 * cm, 5.8 * cm]))
    story.append(Spacer(1, 6))
    text(story, "environment")
    rows = [["Component", "Choice", "Reason"]] + TEXT["tech_rows"]
    story.append(table(rows, [3.0 * cm, 4.6 * cm, 9.8 * cm]))
    story.append(P("Repository layout", H2))
    story.append(Preformatted(TEXT["layout"], CODE))

    # ============================================================== 3 code
    story.append(PageBreak())
    story.append(P("3. Code analysis: how each component works", H1))
    text(story, "code_intro")

    story.append(P("3.1 Configuration and the single source of parameters", H2))
    text(story, "config")
    add(story, code("common/config.py", "load_config", title="common/config.py - load_config()"))

    story.append(P("3.2 Producer (producer/producer.py)", H2))
    text(story, "producer")
    add(story, code("producer/producer.py", None, 48, title="producer/producer.py - pacing, delay and duplicate "
                                                            "injection (main loop)",
                    start_pat=r"^    start = time.time\(\)", end_pat=r"dup_log.append"))
    text(story, "producer2")

    story.append(P("3.3 Stage 1: clean, enrich and Bloom filter (stage1_clean_bloom/stage1.py)", H2))
    text(story, "stage1")
    add(story, code("stage1_clean_bloom/stage1.py", "build_query", title="Stage 1 - Spark transformation (runs in "
                                                                         "parallel over Kafka partitions)"))
    text(story, "stage1_bloom")
    add(story, code("common/sketches.py", "BloomFilter", 52, title="common/sketches.py - BloomFilter"))
    add(story, code("stage1_clean_bloom/stage1.py", None, 30, title="Stage 1 foreachBatch - Bloom test vs exact "
                                                                    "ground truth (mark, never drop)",
                    start_pat=r"bloom_seen = self.bloom.test_and_add", end_pat=r"st\[\"accepted\"\] \+= 1"))

    story.append(P("3.4 Stage 2: stream analytics (stage2_stream_analytics/stage2.py)", H2))
    text(story, "stage2")
    add(story, code("stage2_stream_analytics/stage2.py", None, 40, title="Stage 2 - the three streaming queries "
                                                                         "(main)",
                    start_pat=r"q_a = \(clean.writeStream", end_pat=r"queryName\(\"hll\"\).start"))
    story.append(P("Reservoir sampling (Algorithm R)", H3))
    text(story, "reservoir")
    add(story, code("common/sketches.py", "Reservoir", title="common/sketches.py - Reservoir"))
    story.append(P("DGIM (implemented by the group)", H3))
    text(story, "dgim")
    add(story, code("common/sketches.py", "DGIM", 70, title="common/sketches.py - DGIM"))
    story.append(P("HyperLogLog (Spark HLL++)", H3))
    text(story, "hll")
    story.append(P("Event sampling versus entity sampling", H3))
    text(story, "sampling")
    add(story, code("stage2_stream_analytics/stage2.py", None, 26, title="Stage 2 query B - event vs entity sampling "
                                                                         "with the same storage budget",
                    start_pat=r"# event sampling: Bernoulli", end_pat=r"fx_est = float"))
    story.append(P("Late-event classification, latency, lag and restart safety", H3))
    text(story, "stage2_misc")
    add(story, code("stage2_stream_analytics/stage2.py", None, 12, title="Stage 2 query A - idempotent replay check "
                                                                         "and state snapshot",
                    start_pat=r"if batch_id <= st.last_batch_id", end_pat=r"return"))

    story.append(P("3.5 Stage 3: MinHash-LSH (stage3_lsh/stage3.py)", H2))
    text(story, "stage3")
    add(story, code("common/sketches.py", None, 30, title="common/sketches.py - MinHash and banding",
                    start_pat=r"^class MinHasher", end_pat=r"return candidates"))
    add(story, code("stage3_lsh/stage3.py", None, 40, title="stage3_lsh/stage3.py - per-window evaluation",
                    start_pat=r"# ---- ground truth: exact all-pairs", end_pat=r"\"verify_ms\""))

    story.append(P("3.6 Final consumer and standardized output", H2))
    text(story, "final")
    if d_wins:
        rec = dict(d_wins[min(8, len(d_wins) - 1)])
        rec["sampling"] = {k: v for k, v in rec["sampling"].items() if k != "by_k"}
        rec["dgim"] = {k: v for k, v in rec["dgim"].items() if k != "by_k"}
        rec["lsh"] = {k: v for k, v in rec["lsh"].items() if k != "settings"}
        rec["bloom"] = {k: rec["bloom"][k] for k in ("fp_rate", "fn_rate", "false_positives", "inserted_ids")}
        for k in ("diversity", "late_events"):
            rec.pop(k, None)
        story.append(P("<font size=7.5 color='#555555'><b>One record of results/runs/default/results.jsonl "
                       "(nested by_k / settings lists shortened)</b></font>", SMALL))
        story.append(Preformatted(json.dumps(rec, indent=1)[:2600], CODE))

    story.append(P("3.7 Orchestration, Kafka control and Windows specifics", H2))
    text(story, "orchestration")

    # ============================================================== 4 results
    story.append(PageBreak())
    story.append(P("4. Results", H1))
    text(story, "results_intro", rows=len(S))
    rows = [["run", "part.", "slots", "key", "req./s", "produced/s", "stage1/s", "stage2/s", "p50 ms", "p95 ms",
             "max lag raw", "max lag clean", "skew raw"]]
    for rid in ["default", "P1", "P2", "P3", "P4", "K1", "K2", "R-low", "R-medium", "R-high", "restart", "bloom-small"]:
        r = S.get(rid)
        if r:
            rows.append([rid, r["partitions"], r["parallelism"], r["partition_key"].replace("pickup_zone", "zone"),
                         f(r["requested_rate"], 0), f(r["produced_eps"], 0), f(r["stage1_eps"], 0),
                         f(r["stage2_eps"], 0), f(r["latency_p50_ms"], 0), f(r["latency_p95_ms"], 0),
                         f(r["max_lag_raw"], 0), f(r["max_lag_clean"], 0), f(r["skew_raw"], 3)])
    story.append(table(rows, [1.6 * cm, 0.9 * cm, 0.9 * cm, 1.0 * cm, 1.2 * cm, 1.5 * cm, 1.3 * cm, 1.3 * cm,
                              1.2 * cm, 1.2 * cm, 1.6 * cm, 1.6 * cm, 1.2 * cm]))
    story.append(P("Table 1 - performance of every run (results/experiment_summary.csv).", CAPTION))
    story.append(Spacer(1, 6))
    rows = [["run", "Bloom FP rate", "FN", "reservoir err % (k=1000)", "DGIM err % (k=5000)", "HLL mean err %",
             "LSH recall 20x5", "recall 10x10 / 25x4", "diversity err event / entity"]]
    for rid in ["default", "P1", "P2", "P3", "P4", "K1", "K2", "R-low", "R-medium", "R-high", "restart", "bloom-small"]:
        r = S.get(rid)
        if r:
            rs = json.loads(r["lsh_recall_by_setting"] or "{}")
            rows.append([rid, f(r["bloom_fp_rate"], 6), f(r["bloom_fn_rate"], 1), f(r["sampling_error_pct"], 2),
                         f(r["dgim_error_pct"], 2), f(r["hll_mean_error_pct"], 3), f(r["lsh_recall_default"], 3),
                         f"{f(rs.get('10x10'), 3)} / {f(rs.get('25x4'), 3)}",
                         f"{f(r['diversity_err_event'], 3)} / {f(r['diversity_err_entity'], 3)}"])
    story.append(table(rows, [1.7 * cm, 1.8 * cm, 0.8 * cm, 2.2 * cm, 2.0 * cm, 1.8 * cm, 1.8 * cm, 2.5 * cm,
                              2.9 * cm]))
    story.append(P("Table 2 - result quality of every run. Quality is essentially independent of the input rate "
                   "and partitioning (see 4.6).", CAPTION))

    # ---- 4.1 bloom
    story.append(P("4.1 Bloom filter (5.1)", H2))
    bs = jload(RUNS / "bloom-small" / "stage1_stats.json")
    db = d_sum.get("bloom") or {}
    text(story, "bloom", m=f(db.get("bit_array_size"), 0), k=db.get("num_hashes"),
         ins=f(db.get("inserted_ids"), 0), dups=f(db.get("true_duplicates"), 0),
         flag=f(db.get("true_duplicates_flagged"), 0), fp=db.get("false_positives"), fpr=db.get("fp_rate"),
         fn=db.get("false_negatives"), mem=f(db.get("memory_bytes"), 0),
         theo=(s1.get("bloom") or {}).get("theoretical_fp_now"),
         bs_m=f((bs.get("bloom") or {}).get("bit_array_size"), 0), bs_k=(bs.get("bloom") or {}).get("num_hashes"),
         bs_fp=f(bs.get("bloom_fp"), 0), bs_fpr=(bs.get("bloom") or {}).get("fp_rate"),
         bs_fn=bs.get("bloom_fn"), bs_theo=(bs.get("bloom") or {}).get("theoretical_fp_now"))
    ex = (s1.get("fp_examples") or [])[:2] + (bs.get("fp_examples") or [])[:1]
    if ex:
        story.append(Preformatted(json.dumps(ex, indent=1), CODE))

    # ---- 4.2 reservoir
    story.append(P("4.2 Reservoir sampling - a linear statistic (5.2)", H2))
    samp = (d_sum.get("sampling") or {}).get("by_k", [])
    rows = [["k", "sample size", "exact mean fare", "sample mean fare", "error %", "exact mean distance",
             "sample mean distance", "error %"]]
    for x in samp:
        rows.append([f(x["k"], 0), f(x["sample_size"], 0), f(x["exact_mean_fare"], 3), f(x["sample_mean_fare"], 3),
                     f(x["fare_error_pct"], 2), f(x["exact_mean_distance"], 3), f(x["sample_mean_distance"], 3),
                     f(x["distance_error_pct"], 2)])
    story.append(table(rows, [1.4 * cm, 1.7 * cm, 2.3 * cm, 2.4 * cm, 1.6 * cm, 2.6 * cm, 2.7 * cm, 1.6 * cm]))
    story.append(Spacer(1, 4))
    rk = []
    for rid in ("default", "R-low", "R-medium", "R-high", "P1", "P3", "K2"):
        sj = jload(RUNS / rid / "final_summary.json")
        for x in (sj.get("sampling") or {}).get("by_k", []):
            rk.append((rid, x))
    ranges = {}
    for kk in (100, 1000, 10000):
        errs = [x["fare_error_pct"] for _, x in rk if x["k"] == kk]
        ranges[f"r{kk}"] = f"{min(errs):.2f}-{max(errs):.2f}" if errs else "-"
    text(story, "reservoir_res", n=f(d_sum.get("events_processed"), 0), **ranges)
    if rk:
        rows = [["run"] + [f"k={x['k']} fare / dist err %" for _, x in rk if _ == rk[0][0]]]
        for rid in dict.fromkeys(r for r, _ in rk):
            rows.append([rid] + [f"{f(x['fare_error_pct'], 2)} / {f(x['distance_error_pct'], 2)}" for r, x in rk
                                 if r == rid])
        story.append(table(rows, [2.5 * cm, 4.9 * cm, 4.9 * cm, 4.9 * cm]))
        story.append(P("Table 3 - reservoir error over independent runs: the variation between runs is the sampling "
                       "noise itself (the reservoir RNG is seeded, but the arrival order differs between runs).",
                       CAPTION))

    # ---- 4.3 diversity
    story.append(P("4.3 Event sampling versus entity sampling - a non-linear statistic (5.3)", H2))
    rows = [["window", "active zones", "exact", "event sample", "stored events", "entity sample", "stored events",
             "zones kept"]]
    for w in d_wins:
        dv = w.get("diversity") or {}
        es, en = dv.get("event_sampling") or {}, dv.get("entity_sampling") or {}
        rows.append([w["window_start"][5:16].replace("T", " "), dv.get("active_zones"),
                     f(dv.get("exact_fraction_diverse"), 3), f(es.get("estimate"), 3), es.get("stored_events"),
                     f(en.get("estimate"), 3), en.get("stored_events"), en.get("zones_selected")])
    story.append(table(rows, [2.4 * cm, 1.8 * cm, 1.5 * cm, 2.0 * cm, 2.0 * cm, 2.0 * cm, 2.0 * cm, 1.8 * cm],
                       font=7))
    story.append(P("Table 4 - fraction of active pickup zones with >= 10 distinct drop-off zones per 60-minute "
                   "window, default run (10 % event sample; entity sample uses the same number of stored events).",
                   CAPTION))
    add(story, img(PLOTS / "diversity.png", 14.5, "Figure 2 - exact fraction of diverse zones and the two "
                                                   "sampling estimates per window."))
    dme = d_sum.get("diversity_mean_abs_error") or {}
    text(story, "diversity", ev=f(dme.get("event_sampling"), 3), en=f(dme.get("entity_sampling"), 3),
         fx=f(dme.get("entity_sampling_fixed_rate"), 3))

    # ---- 4.4 HLL
    story.append(P("4.4 HyperLogLog - distinct routes per window (5.4)", H2))
    rows = [["window", "events", "exact routes", "HLL++ estimate", "error %", "exact set ~bytes", "sketch bytes"]]
    for w in d_wins:
        h = w["hll"]
        rows.append([w["window_start"][5:16].replace("T", " "), f(w.get("events_in_window"), 0),
                     f(h["exact_routes"], 0), f(h["estimate"], 0), f(h["error_pct"], 2),
                     f(h["exact_set_bytes_est"], 0), f(h["sketch_bytes"], 0)])
    story.append(table(rows, [2.6 * cm, 1.8 * cm, 2.2 * cm, 2.4 * cm, 1.8 * cm, 2.6 * cm, 2.2 * cm]))
    errs = [w["hll"]["error_pct"] for w in d_wins]
    h0 = d_wins[0]["hll"] if d_wins else {}
    text(story, "hll_res", p=h0.get("precision"), m=h0.get("registers"), rsd=h0.get("relative_sd"),
         mean=f(sum(errs) / len(errs), 3) if errs else "-", mx=f(max(errs), 2) if errs else "-",
         nwin=len(errs), sk=h0.get("sketch_bytes"))

    # ---- 4.5 DGIM
    story.append(P("4.5 DGIM - recent slow trips (5.5)", H2))
    rows = [["run", "k", "exact 1s", "DGIM estimate", "abs error", "error %", "buckets"]]
    for rid in ("default", "R-low", "R-medium", "R-high", "P3"):
        sj = jload(RUNS / rid / "final_summary.json")
        dg = sj.get("dgim") or {}
        for x in dg.get("by_k", []):
            rows.append([rid, f(x["k"], 0), f(x["exact_ones"], 0), f(x["estimate"], 1), f(x["abs_error"], 1),
                         f(x["error_pct"], 2), dg.get("buckets")])
    story.append(table(rows, [2.4 * cm, 1.6 * cm, 2.0 * cm, 2.4 * cm, 2.0 * cm, 2.0 * cm, 1.8 * cm]))
    story.append(P("Table 6 - DGIM over slow_trip_bit (N = 10 000) at the end of each run.", CAPTION))
    text(story, "dgim_res")

    # ---- 4.6 LSH
    story.append(P("4.6 MinHash-LSH on destination sets (Task 3)", H2))
    rows = [["bands x rows", "approx. threshold (1/b)^(1/r)", "mean recall", "mean candidate precision",
             "mean candidate reduction", "total candidates", "total true pairs", "mean LSH ms"]]
    bands_thr = {"20x5": 0.549, "10x10": 0.794, "25x4": 0.447}
    for k, v in (d_sum.get("lsh_by_setting") or {}).items():
        rows.append([k, bands_thr.get(k, "-"), f(v["mean_recall"], 3), f(v["mean_candidate_precision"], 3),
                     f(v["mean_candidate_reduction"], 3), f(v["total_candidates"], 0), f(v["total_true_pairs"], 0),
                     f(v["mean_lsh_latency_ms"], 2)])
    story.append(table(rows, [2.0 * cm, 2.6 * cm, 1.8 * cm, 2.4 * cm, 2.4 * cm, 2.0 * cm, 2.0 * cm, 2.0 * cm]))
    story.append(P(f"Table 7 - LSH settings, default run, averaged over windows with at least one true pair "
                   f"(exact all-pairs baseline: {d_sum.get('mean_exact_latency_ms')} ms per window on average).",
                   CAPTION))
    rows = [["window", "zones (>=20 trips)", "all pairs", "true pairs", "cand. 20x5", "recall 20x5", "cand. 25x4",
             "recall 25x4", "cand. 10x10", "recall 10x10", "LSH ms", "exact ms"]]
    for w in d_wins:
        l = w.get("lsh") or {}
        st = {f"{s['bands']}x{s['rows']}": s for s in l.get("settings", [])}
        if not l.get("active_zones") or l["active_zones"] < 10:
            continue
        rows.append([w["window_start"][5:16].replace("T", " "), l["active_zones"], l["all_pairs"], l["true_pairs"],
                     st["20x5"]["candidates"], f(st["20x5"]["recall"], 2), st["25x4"]["candidates"],
                     f(st["25x4"]["recall"], 2), st["10x10"]["candidates"], f(st["10x10"]["recall"], 2),
                     f(l["lsh_latency_ms"], 2), f(l["exact_latency_ms"], 2)])
    story.append(table(rows, [2.2 * cm, 1.6 * cm, 1.2 * cm, 1.2 * cm, 1.3 * cm, 1.3 * cm, 1.3 * cm, 1.3 * cm,
                              1.4 * cm, 1.4 * cm, 1.3 * cm, 1.3 * cm], font=7))
    story.append(P("Table 8 - per-window LSH results (windows with >= 10 active zones).", CAPTION))
    text(story, "lsh_res")
    lw = RUNS / "default" / "lsh_windows.jsonl"
    if lw.exists():
        ws = [json.loads(x) for x in lw.read_text().splitlines() if x.strip()]
        w = max(ws, key=lambda x: x["true_pairs"])
        rows = [["P", "Q", "zone P", "zone Q", "exact Jaccard", "returned (J >= 0.5)"]]
        for c in w["candidate_pairs"][:14]:
            rows.append([c["p"], c["q"], c["p_name"], c["q_name"], f(c["jaccard"], 3), "yes" if c["similar"] else "no"])
        story.append(KeepTogether([P(f"Candidate pairs of window {w['window_start']} (20x5 banding; "
                                     f"{w['candidates']} candidates, {w['true_pairs']} true pairs; first 14 by "
                                     f"Jaccard, produced by scripts/show_lsh_window.py):", SMALL),
                                   table(rows, [1.0 * cm, 1.0 * cm, 6.0 * cm, 6.0 * cm, 1.8 * cm, 2.2 * cm])]))
        story.append(Spacer(1, 6))

    # ---- 4.7 partitions
    story.append(P("4.7 Partitions and consumers: P1-P4 (7.2)", H2))
    rows = [["run", "partitions", "slots", "produced/s", "Stage 1/s", "Stage 2/s", "p50 ms", "p95 ms",
             "max lag raw", "max lag clean"]]
    for rid in ("P1", "P2", "P3", "P4"):
        r = S.get(rid)
        if r:
            rows.append([rid, r["partitions"], r["parallelism"], f(r["produced_eps"], 0), f(r["stage1_eps"], 0),
                         f(r["stage2_eps"], 0), f(r["latency_p50_ms"], 0), f(r["latency_p95_ms"], 0),
                         f(r["max_lag_raw"], 0), f(r["max_lag_clean"], 0)])
    story.append(table(rows, [1.4 * cm, 1.7 * cm, 1.3 * cm, 1.9 * cm, 1.8 * cm, 1.8 * cm, 1.6 * cm, 1.6 * cm,
                              2.0 * cm, 2.0 * cm]))
    add(story, img(PLOTS / "lag_partitions.png", 14.5, "Figure 3 - consumer lag during the P1-P4 runs."))
    text(story, "partitions", **TEXT["partition_fmt"](S))

    # ---- 4.8 key
    story.append(P("4.8 Partition key: pickup_zone versus no key (7.3)", H2))
    rows = [["run", "key", "msgs p0", "msgs p1", "msgs p2", "msgs p3", "skew (busiest share)", "Stage 1/s",
             "Stage 2/s", "p50 ms", "p95 ms"]]
    for rid in ("K1", "K2"):
        r = S.get(rid)
        pc = jload(RUNS / rid / "partition_counts.json").get("taxi.raw", {})
        if r:
            rows.append([rid, r["partition_key"]] + [f(pc.get(str(i)), 0) for i in range(4)] +
                        [f(r["skew_raw"], 3), f(r["stage1_eps"], 0), f(r["stage2_eps"], 0),
                         f(r["latency_p50_ms"], 0), f(r["latency_p95_ms"], 0)])
    story.append(table(rows, [1.0 * cm, 2.0 * cm, 1.4 * cm, 1.4 * cm, 1.4 * cm, 1.4 * cm, 2.0 * cm, 1.6 * cm,
                              1.6 * cm, 1.4 * cm, 1.4 * cm]))
    text(story, "key", **TEXT["key_fmt"](S))

    # ---- 4.9 rate
    story.append(P("4.9 Producer rate: low / medium / high (7.4)", H2))
    rows = [["Rate", "Produced/s", "Processed/s", "p50 latency", "p95 latency", "max lag", "HLL err.",
             "DGIM err.", "LSH recall"]]
    for rid in ("R-low", "R-medium", "R-high"):
        r = S.get(rid)
        if r:
            rows.append([f"{rid[2:]} ({f(r['requested_rate'], 0)}/s)", f(r["produced_eps"], 0),
                         f(r["stage2_eps"], 0), f"{f(r['latency_p50_ms'], 0)} ms", f"{f(r['latency_p95_ms'], 0)} ms",
                         f"{f(r['max_lag_raw'], 0)} / {f(r['max_lag_clean'], 0)}", f"{f(r['hll_mean_error_pct'], 3)} %",
                         f"{f(r['dgim_error_pct'], 2)} %", f(r["lsh_recall_default"], 3)])
    story.append(table(rows, [2.6 * cm, 1.8 * cm, 1.9 * cm, 1.8 * cm, 1.8 * cm, 2.3 * cm, 1.6 * cm, 1.6 * cm,
                              1.6 * cm]))
    story.append(P("Table 11 - the table of section 7.4 (max lag = taxi.raw / taxi.clean; HLL error = mean over "
                   "windows; DGIM error at k = 5000).", CAPTION))
    add(story, img(PLOTS / "lag_rates.png", 14.5, "Figure 4 - consumer lag at the three input rates."))
    add(story, img(PLOTS / "throughput.png", 14.5, "Figure 5 - produced versus processed throughput per run."))
    text(story, "rates", **TEXT["rate_fmt"](S))

    # ---- 4.10 late
    story.append(P("4.10 Event time and delayed events (8.1)", H2))
    late = d_sum.get("late_events") or {}
    text(story, "late", on=f(late.get("on_time"), 0), inc=f(late.get("late_included"), 0),
         drop=f(late.get("late_dropped"), 0), b=late.get("lateness_buckets_included") or {},
         spark=d_sum.get("spark_rows_dropped_by_watermark"), **TEXT["late_fmt"](RUNS))
    st2 = jload(RUNS / "default" / "stage2_stats.json")
    exs = (st2.get("late") or {}).get("examples") or []
    pick = [e for e in exs if e["class"] == "late_dropped"][:1] + [e for e in exs if e["class"] == "late_included"][:1]
    if pick:
        story.append(Preformatted(json.dumps(pick, indent=1), CODE))

    # ---- 4.11 restart
    story.append(P("4.11 Restart and recovery of Stage 2 (8.2)", H2))
    add(story, img(PLOTS / "lag_restart.png", 14.5, "Figure 6 - lag of taxi.clean while Stage 2 is killed and "
                                                    "restarted (producer keeps running)."))
    text(story, "restart", **TEXT["restart_fmt"](RUNS, S))

    # ============================================================== 5 discussion
    story.append(P("5. Discussion", H1))
    text(story, "discussion")
    story.append(P("6. Limitations and honest notes", H1))
    text(story, "limitations")
    story.append(P("7. Reproducibility and submission checklist", H1))
    rows = [["Checklist item (brief section 14)", "Status / evidence"]] + TEXT["checklist"]
    story.append(table(rows, [8.0 * cm, 9.4 * cm]))

    out = ROOT / "Assignment2_Report.pdf"
    doc = SimpleDocTemplate(str(out), pagesize=A4, leftMargin=1.8 * cm, rightMargin=1.8 * cm, topMargin=1.6 * cm,
                            bottomMargin=1.7 * cm, title="BDA Assignment 2 - Real-Time Taxi Stream Analytics",
                            author="BDA Assignment 2 team")
    doc.build(story, onFirstPage=lambda c, d: None, onLaterPages=on_page)
    print(f"wrote {out}")


if __name__ == "__main__":
    build()
