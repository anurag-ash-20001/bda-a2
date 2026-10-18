#!/usr/bin/env bash
# One-command default run on Linux / macOS / WSL.
#   Kafka:  docker compose up -d        (kafka_mode is switched to "external" below)
#   Python: python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt ; Java 17 on PATH
# Extra arguments are passed through:  ./run_all.sh --set producer_rate=5000
set -euo pipefail
cd "$(dirname "$0")"
PY="${PY:-.venv/bin/python}"
if ! "$PY" -c "import pyspark" 2>/dev/null; then
  echo "create the venv first: python3.12 -m venv .venv && .venv/bin/pip install -r requirements.txt"; exit 1
fi
docker compose up -d
"$PY" scripts/run_experiment.py --run-id default --set kafka_mode=external --set work_dir=/tmp/bda-a2 "$@"
"$PY" scripts/analyze_results.py
echo "Results: results/runs/default/results.jsonl results/experiment_summary.csv results/analysis.md"
