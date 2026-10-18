"""Draw architecture.png (the pipeline actually implemented)."""
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
BLUE, YELLOW, GREEN, RED, GREY = "#dbe9f6", "#fdf0c2", "#dcefd8", "#f8dcd2", "#555555"


def box(ax, x, y, w, h, title, body, color):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.08",
                                fc=color, ec=GREY, lw=1.2))
    ax.text(x + w / 2, y + h - 0.17, title, ha="center", va="top", fontsize=9.5, weight="bold")
    if body:
        ax.text(x + w / 2, y + h - 0.47, body, ha="center", va="top", fontsize=7.4, linespacing=1.35)


def arrow(ax, x1, y1, x2, y2):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=12, lw=1.2, color=GREY))


def main():
    fig, ax = plt.subplots(figsize=(15, 6.2))
    ax.set_xlim(0, 15)
    ax.set_ylim(0, 6.2)
    ax.axis("off")
    ax.text(7.5, 6.0, "Real-time taxi stream analytics: every stage reads from Kafka and writes back to Kafka",
            ha="center", fontsize=12, weight="bold")

    box(ax, 0.1, 2.3, 1.8, 1.7, "Producer", "parquet replay\nrate-controlled\n2% duplicates\n2% delayed 1-20 s\n"
        "key = pickup_zone", BLUE)
    box(ax, 2.2, 2.6, 1.3, 1.1, "taxi.raw", "4 partitions", YELLOW)
    box(ax, 3.8, 2.15, 2.2, 2.0, "Stage 1 (Spark)", "parse + validate\nroute_key, duration,\nslow_trip_bit\n"
        "Bloom test on event_id\n(mark, never drop)", GREEN)
    box(ax, 6.3, 2.6, 1.3, 1.1, "taxi.clean", "4 partitions", YELLOW)
    box(ax, 8.0, 3.55, 2.6, 2.15, "Stage 2 (Spark)", "A: reservoir k=100/1k/10k, DGIM,\nBloom eval, latency, lag\n"
        "B: 60-min windows: event vs\nentity sampling (diversity)\nC: 60-min windows: HLL++ vs exact",
        GREEN)
    box(ax, 8.0, 0.45, 2.6, 1.8, "Stage 3 (Spark)", "60-min windows + watermark\nS(P) = dropoff zones of P\n"
        "MinHash 100 -> bands 20x5\n(+10x10, 25x4), exact verify", GREEN)
    box(ax, 11.0, 4.05, 1.4, 1.1, "taxi.metrics", "1 partition", YELLOW)
    box(ax, 11.0, 0.85, 1.4, 1.1, "taxi.lsh", "1 partition", YELLOW)
    box(ax, 12.8, 2.3, 2.1, 1.7, "Final consumer", "joins per window\nconsole +\nresults.jsonl\n(+ run summary)", RED)

    arrow(ax, 1.9, 3.15, 2.2, 3.15)
    arrow(ax, 3.5, 3.15, 3.8, 3.15)
    arrow(ax, 6.0, 3.15, 6.3, 3.15)
    arrow(ax, 7.6, 3.3, 8.0, 4.4)
    arrow(ax, 7.6, 3.0, 8.0, 1.5)
    arrow(ax, 10.6, 4.6, 11.0, 4.6)
    arrow(ax, 10.6, 1.4, 11.0, 1.4)
    arrow(ax, 12.4, 4.6, 12.8, 3.6)
    arrow(ax, 12.4, 1.4, 12.8, 2.7)
    ax.text(7.5, 0.1, "Orchestrator (scripts/run_experiment.py): fresh topics per run, starts all stages, "
            "samples consumer lag every second, optional Stage 2 kill/restart, writes experiment_summary.csv",
            ha="center", fontsize=8, style="italic", color=GREY)
    out = ROOT / "architecture.png"
    fig.savefig(out, dpi=150, bbox_inches="tight")
    print(f"wrote {out}")


if __name__ == "__main__":
    sys.exit(main())
