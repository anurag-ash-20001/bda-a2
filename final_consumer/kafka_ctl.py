"""Start / stop / reset a single-node KRaft Kafka broker natively on Windows/Linux.

    python final_consumer/kafka_ctl.py start     # format storage on first use, start broker
    python final_consumer/kafka_ctl.py stop
    python final_consumer/kafka_ctl.py reset     # stop, wipe all data, start fresh
    python final_consumer/kafka_ctl.py status
    python final_consumer/kafka_ctl.py topics    # (re)create the four pipeline topics

With kafka_mode: external (e.g. docker compose up -d) only `topics`/`status` are used.
"""
import base64
import os
import shutil
import stat
import subprocess
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from producer.pipeline_common import load_config, recreate_topics, wait_for_broker  # noqa: E402


def paths(cfg):
    work = Path(cfg["work_dir"])
    return {
        "data": work / "kafka-data",
        "props": work / "kafka-server.properties",
        "pid": work / "kafka.pid",
        "log": work / "kafka.log",
        "home": Path(cfg["kafka_home"]),
    }


def write_properties(cfg, p):
    p["data"].mkdir(parents=True, exist_ok=True)
    props = f"""process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs={p['data'].as_posix()}
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
delete.topic.enable=true
log.retention.hours=168
auto.create.topics.enable=false
"""
    p["props"].write_text(props, encoding="utf-8")


def java_cmd(p, main, *args):
    home = p["home"]
    cp = str(home / "libs" / "*")
    return ["java", "-Xms512m", "-Xmx1g",
            f"-Dlog4j.configuration=file:{(home / 'config' / 'log4j.properties').as_posix()}",
            f"-Dkafka.logs.dir={(p['data'].parent / 'kafka-logs-app').as_posix()}",
            "-cp", cp, main, *args]


def is_running(p):
    if not p["pid"].exists():
        return False
    pid = p["pid"].read_text().strip()
    if os.name == "nt":
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"], capture_output=True, text=True).stdout
        return pid in out
    try:
        os.kill(int(pid), 0)
        return True
    except OSError:
        return False


def kill_tree(pid):
    if os.name == "nt":
        subprocess.run(["taskkill", "/T", "/F", "/PID", str(pid)], capture_output=True)
    else:
        try:
            os.killpg(os.getpgid(int(pid)), 9)
        except OSError:
            pass


def start(cfg, retry=True):
    """Start the broker. If it dies during start-up (on Windows a crash can leave locked or
    half-renamed metadata segments behind), its data is wiped and it is started once more:
    every run recreates its topics anyway, so the broker's data is disposable."""
    p = paths(cfg)
    if is_running(p):
        print("Kafka already running")
        return wait_for_broker(cfg)
    if not (p["data"] / "meta.properties").exists():
        write_properties(cfg, p)
        cluster_id = base64.urlsafe_b64encode(uuid.uuid4().bytes).rstrip(b"=").decode()
        subprocess.run(java_cmd(p, "kafka.tools.StorageTool", "format", "-t", cluster_id,
                                "-c", str(p["props"])), check=True, capture_output=True)
    write_properties(cfg, p)
    log = open(p["log"], "ab")
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
    proc = subprocess.Popen(java_cmd(p, "kafka.Kafka", str(p["props"])), stdout=log, stderr=log,
                            stdin=subprocess.DEVNULL, creationflags=flags,
                            start_new_session=(os.name != "nt"))
    p["pid"].write_text(str(proc.pid))
    ok = False
    deadline = time.time() + 90
    while time.time() < deadline and proc.poll() is None:
        if wait_for_broker(cfg, timeout=5):
            ok = True
            break
    if not ok and retry:
        print(f"Kafka did not come up (see {p['log']}) -> wiping broker data and retrying")
        stop(cfg)
        wipe(p["data"])
        return start(cfg, retry=False)
    print("Kafka started" if ok else f"Kafka did not come up, see {p['log']}")
    return ok


def stop(cfg):
    p = paths(cfg)
    if p["pid"].exists():
        kill_tree(p["pid"].read_text().strip())
        p["pid"].unlink()
        time.sleep(2)
    print("Kafka stopped")


def wipe(path):
    """Delete a directory tree including read-only files. Kafka marks its metadata snapshots
    (*.checkpoint) read-only; rmtree(ignore_errors=True) silently leaves them behind on Windows,
    and a stale snapshot makes the next broker crash at start-up."""
    def force(func, target, _):
        os.chmod(target, stat.S_IWRITE)
        func(target)
    if path.exists():
        shutil.rmtree(path, onexc=force)


def reset(cfg):
    stop(cfg)
    wipe(paths(cfg)["data"])
    return start(cfg, retry=False)


def fresh_topics(cfg):
    """Give a run empty topics with the configured partition counts.

    Native Kafka on Windows cannot delete topics (the broker fails to rename open
    segment files and shuts down), so in native mode we wipe and restart the whole
    single-node broker instead -- a clean equivalent of delete+recreate."""
    if cfg["kafka_mode"] == "native":
        if not reset(cfg):
            raise RuntimeError("Kafka reset failed")
    recreate_topics(cfg)


def main():
    cfg = load_config()
    cmd = sys.argv[1] if len(sys.argv) > 1 else "status"
    if cmd == "start":
        sys.exit(0 if start(cfg) else 1)
    elif cmd == "stop":
        stop(cfg)
    elif cmd == "reset":
        sys.exit(0 if reset(cfg) else 1)
    elif cmd == "topics":
        fresh_topics(cfg)
        print("topics recreated")
    else:
        print("running" if wait_for_broker(cfg, timeout=5) else "not reachable")


if __name__ == "__main__":
    main()
