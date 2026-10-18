"""Configuration loading shared by every stage.

The orchestrator writes the effective configuration of a run to
<run_dir>/config.json and passes its path in the BDA_CONFIG environment
variable, so all processes of one run see exactly the same parameters.
"""
import json
import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent

TOPIC_RAW = "taxi.raw"
TOPIC_CLEAN = "taxi.clean"
TOPIC_METRICS = "taxi.metrics"
TOPIC_LSH = "taxi.lsh"


def _parse_value(text):
    """Parse a --set override value with YAML rules (numbers, lists, booleans)."""
    return yaml.safe_load(text)


def load_config(path=None, overrides=None):
    path = path or os.environ.get("BDA_CONFIG") or ROOT / "config.yaml"
    path = Path(path)
    with open(path, encoding="utf-8") as f:
        cfg = json.load(f) if path.suffix == ".json" else yaml.safe_load(f)
    for item in overrides or []:
        key, _, value = item.partition("=")
        cfg[key.strip()] = _parse_value(value)
    for key in ("kafka_home", "hadoop_home", "work_dir"):
        cfg[key] = str(Path(os.path.expanduser(cfg[key])))
    if not Path(cfg["data_file"]).is_absolute():
        cfg["data_file"] = str(ROOT / cfg["data_file"])
    assert cfg["lsh_bands"] * cfg["lsh_rows_per_band"] == cfg["lsh_num_hashes"], \
        "lsh_bands * lsh_rows_per_band must equal lsh_num_hashes"
    return cfg


def run_dir(cfg):
    d = ROOT / "results" / "runs" / cfg["run_id"]
    d.mkdir(parents=True, exist_ok=True)
    return d


def checkpoint_dir(cfg, stage):
    return Path(cfg["work_dir"]) / "checkpoints" / cfg["run_id"] / stage
