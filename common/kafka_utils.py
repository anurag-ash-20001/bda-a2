"""Kafka helpers: topic (re)creation, lag measurement, offset bookkeeping."""
import time

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from common.config import TOPIC_CLEAN, TOPIC_LSH, TOPIC_METRICS, TOPIC_RAW


def admin(cfg, quiet=False):
    conf = {"bootstrap.servers": cfg["bootstrap_servers"]}
    if quiet:
        conf["log_level"] = 0  # broker may still be starting: don't print connect errors
    return AdminClient(conf)


def wait_for_broker(cfg, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            admin(cfg, quiet=True).list_topics(timeout=3)
            return True
        except Exception:
            time.sleep(1)
    return False


def recreate_topics(cfg):
    """Delete and recreate the four pipeline topics so each run starts from offset 0."""
    a = admin(cfg)
    topics = [TOPIC_RAW, TOPIC_CLEAN, TOPIC_METRICS, TOPIC_LSH]
    existing = set(a.list_topics(timeout=10).topics)
    to_delete = [t for t in topics if t in existing]
    if to_delete:
        for fut in a.delete_topics(to_delete, operation_timeout=30).values():
            fut.result()
        # deletion is asynchronous on the broker; wait until the names are gone
        deadline = time.time() + 60
        while time.time() < deadline and set(to_delete) & set(a.list_topics(timeout=10).topics):
            time.sleep(0.5)
    p = int(cfg["partitions"])
    mp = int(cfg.get("metrics_partitions", 1))
    new = [NewTopic(TOPIC_RAW, p, 1), NewTopic(TOPIC_CLEAN, p, 1),
           NewTopic(TOPIC_METRICS, mp, 1), NewTopic(TOPIC_LSH, mp, 1)]
    for attempt in range(30):
        try:
            for fut in a.create_topics(new, operation_timeout=30).values():
                fut.result()
            break
        except Exception as e:  # topic still marked for deletion -> retry
            if "already exists" in str(e).lower() or "marked for deletion" in str(e).lower():
                time.sleep(1)
                new = [t for t in new if t.topic not in a.list_topics(timeout=10).topics]
                if not new:
                    break
            else:
                raise
    # metadata propagation
    deadline = time.time() + 30
    while time.time() < deadline:
        md = a.list_topics(timeout=10).topics
        if all(t in md and len(md[t].partitions) > 0 for t in topics):
            return
        time.sleep(0.5)


class OffsetTracker:
    """Measures lag of a topic and commits a stage's processed offsets to a
    consumer group, so external tools (kafka-consumer-groups, the orchestrator's
    lag monitor) can see how far a Spark stage has progressed.

    Spark's Kafka source keeps its offsets in the checkpoint, not in Kafka; these
    commits are for observability only."""

    def __init__(self, cfg, topic, group):
        self.topic = topic
        self.consumer = Consumer({
            "bootstrap.servers": cfg["bootstrap_servers"],
            "group.id": group,
            "enable.auto.commit": False,
        })

    def high_watermarks(self):
        md = self.consumer.list_topics(self.topic, timeout=10)
        out = {}
        for p in md.topics[self.topic].partitions:
            _, high = self.consumer.get_watermark_offsets(TopicPartition(self.topic, p), timeout=10)
            out[p] = high
        return out

    def lag(self, next_offsets):
        """next_offsets: {partition: next offset to read}. Returns total lag."""
        hw = self.high_watermarks()
        return int(sum(max(0, high - next_offsets.get(p, 0)) for p, high in hw.items()))

    def commit(self, next_offsets):
        if next_offsets:
            tps = [TopicPartition(self.topic, p, o) for p, o in next_offsets.items()]
            self.consumer.commit(offsets=tps, asynchronous=False)

    def close(self):
        self.consumer.close()


def group_lag(cfg, topic, group):
    """Lag of a consumer group on a topic from committed offsets (-1 if group unknown)."""
    c = Consumer({"bootstrap.servers": cfg["bootstrap_servers"], "group.id": group,
                  "enable.auto.commit": False})
    try:
        md = c.list_topics(topic, timeout=10)
        if topic not in md.topics or md.topics[topic].error is not None:
            return None
        parts = [TopicPartition(topic, p) for p in md.topics[topic].partitions]
        committed = c.committed(parts, timeout=10)
        total = 0
        per = {}
        for tp in committed:
            _, high = c.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=10)
            pos = tp.offset if tp.offset >= 0 else 0
            per[tp.partition] = high
            total += max(0, high - pos)
        return total, per
    finally:
        c.close()


def partition_counts(cfg, topic):
    c = Consumer({"bootstrap.servers": cfg["bootstrap_servers"], "group.id": "bda-inspect"})
    try:
        md = c.list_topics(topic, timeout=10)
        out = {}
        for p in md.topics[topic].partitions:
            low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            out[p] = high - low
        return out
    finally:
        c.close()
