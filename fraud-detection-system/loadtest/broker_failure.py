"""Broker failure test: kill Kafka brokers while CDRs are being sent, then check the promise
the pipeline makes: every CDR the API accepted reaches both lakehouse tables, and no Kafka
message is written to a table twice.

Scenarios:
  one  SIGKILL the broker leading the most cdr-events partitions, restart it later.
       With replication factor 3 and min.insync.replicas=2 every partition keeps a leader
       and enough replicas, so ingestion should keep accepting after a short election.
  two  SIGKILL two brokers. Now only one replica is in sync, so no write can be confirmed:
       the API must answer 503 rather than accept a CDR held by a single broker. Each node is
       also a KRaft controller, so the controller quorum is lost too and the cluster cannot
       even elect leaders until the brokers return.

The sender behaves like a client that follows the API's contract: CDRs the API rejects are
sent again until accepted (or the run ends). The checks read the tables through Trino.

Usage (from fraud-detection-system/, with the stack running):
    python loadtest/broker_failure.py --scenario one
    python loadtest/broker_failure.py --scenario two --out two.json
"""
import argparse
import json
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from loadtest import load_templates  # noqa: E402

BROKERS = {1: "kafka_1", 2: "kafka_2", 3: "kafka_3"}
GROUPS_AND_TOPICS = [("fraud-detection", "cdr-events"), ("lakehouse-writer", "cdr-events"),
                     ("lakehouse-fraud-decisions", "fraud-decisions")]


def run(cmd, timeout=60):
    """the command's result; a command that hangs (e.g. while the cluster has no quorum) gives empty output"""
    try:
        return subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
                              timeout=timeout)
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(cmd, None, stdout="", stderr="timed out")


def running_broker():
    """a broker container that is up, to run Kafka's command-line tools in"""
    for container in BROKERS.values():
        if run(["docker", "inspect", "-f", "{{.State.Running}}", container]).stdout.strip() == "true":
            return container
    raise RuntimeError("no broker is running")


def kafka_tool(*args, timeout=60):
    cmd = ["docker", "exec", running_broker(), f"/opt/kafka/bin/{args[0]}",
           "--bootstrap-server", "localhost:9092", *args[1:]]
    return run(cmd, timeout=timeout).stdout


def partition_leaders(topic="cdr-events", timeout=60):
    """{partition: (leader, isr)} from kafka-topics.sh --describe; {} if the cluster cannot answer"""
    leaders = {}
    for line in kafka_tool("kafka-topics.sh", "--describe", "--topic", topic, timeout=timeout).splitlines():
        fields = dict(part.split(": ", 1) for part in line.strip().split("\t") if ": " in part)
        if "Partition" in fields:
            leaders[int(fields["Partition"])] = (int(fields["Leader"]), fields["Isr"])
    return leaders


def under_replicated():
    """partitions missing a replica; None if the cluster cannot answer"""
    out = kafka_tool("kafka-topics.sh", "--describe", "--under-replicated-partitions", timeout=30)
    if not out and not partition_leaders(timeout=30):
        return None
    return len([line for line in out.splitlines() if line.strip()])


def total_lag():
    out = kafka_tool("kafka-consumer-groups.sh", "--describe", "--all-groups", timeout=90)
    lag, seen = 0, set()
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 6 and (parts[0], parts[1]) in GROUPS_AND_TOPICS:
            seen.add((parts[0], parts[1]))
            lag += int(parts[5]) if parts[5].isdigit() else 0
    return lag if len(seen) == len(GROUPS_AND_TOPICS) else None


def post_batch(url, cdrs):
    """transaction ids the API accepted; the rest were rejected"""
    req = urllib.request.Request(f"{url}/ingest/batch", data=json.dumps(cdrs).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return set()
    failed = {e["transaction_id"] for e in body.get("errors", [])}
    return {c["transaction_id"] for c in cdrs} - failed


def trino(sql):
    out = run(["docker", "exec", "trino", "trino", "--catalog", "lakehouse", "--schema", "telecom",
               "--output-format", "CSV_UNQUOTED", "--execute", sql], timeout=180)
    if out.returncode != 0:
        raise RuntimeError(out.stderr)
    return [line for line in out.stdout.splitlines() if line and not line.startswith("WARNING")]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", choices=["one", "two"], default="one")
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--rate", type=int, default=25, help="new CDRs per second")
    parser.add_argument("--duration", type=int, default=90, help="seconds of sending new CDRs")
    parser.add_argument("--kill-at", type=int, default=20)
    parser.add_argument("--restart-at", type=int, default=55)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--drain-timeout", type=int, default=1200)
    parser.add_argument("--out")
    args = parser.parse_args()

    run_id = time.strftime("%H%M%S")
    prefix = f"BF{args.scenario}-{run_id}-"
    templates = load_templates()
    if under_replicated():
        sys.exit("partitions are under-replicated before the test; wait for the cluster to recover")

    leaders = partition_leaders()
    counts = {}
    for leader, _ in leaders.values():
        counts[leader] = counts.get(leader, 0) + 1
    first = max(counts, key=counts.get)
    victims = [first] if args.scenario == "one" else [first, next(b for b in BROKERS if b != first)]
    print(f"Run {prefix}: {args.rate} CDRs/s for {args.duration}s; cdr-events leaders {leaders}; "
          f"will kill broker(s) {victims} at {args.kill_at}s and restart at {args.restart_at}s")

    accepted, pending, timeline, events = set(), [], [], []
    lock = threading.Lock()
    started = time.monotonic()
    stop_sending = threading.Event()

    def sender():
        n = 0
        while not stop_sending.is_set():
            elapsed = time.monotonic() - started
            # new CDRs at the target rate, plus everything rejected so far (the client retries)
            due = int(min(elapsed, args.duration) * args.rate)
            with lock:
                batch = pending[:args.batch_size]
                del pending[:len(batch)]
            while len(batch) < args.batch_size and n < due:
                batch.append(dict(templates[n % len(templates)], transaction_id=f"{prefix}{n}"))
                n += 1
            if not batch:
                if elapsed >= args.duration and not pending:
                    break
                time.sleep(0.05)
                continue
            ok = post_batch(args.url, batch)
            with lock:
                accepted.update(ok)
                pending.extend(c for c in batch if c["transaction_id"] not in ok)
                timeline.append((round(time.monotonic() - started, 1), len(ok), len(batch) - len(ok)))
        return n

    # a daemon thread, so a failure in the main thread cannot leave the process running
    thread = threading.Thread(target=sender, daemon=True)
    thread.start()

    def wait_until(t):
        while time.monotonic() - started < t:
            time.sleep(0.2)

    def event(text):
        events.append((round(time.monotonic() - started, 1), text))
        print(f"t={events[-1][0]}s {text}")

    wait_until(args.kill_at)
    try:
        for broker in victims:
            run(["docker", "kill", BROKERS[broker]])
        event(f"killed {victims}")
        time.sleep(8)
        leaders = partition_leaders(timeout=20)
        event(f"leaders after kill: {leaders or 'unavailable: the cluster cannot answer'}")
        wait_until(args.restart_at)
    finally:
        # whatever happens above, the brokers come back
        for broker in victims:
            run(["docker", "start", BROKERS[broker]])
        event(f"restarted {victims}")

    # stop when the sender has caught up, or give it the time the API needs to recover
    thread.join(timeout=args.duration + 300)
    stop_sending.set()
    thread.join()
    send_seconds = time.monotonic() - started
    print(f"Sending done after {send_seconds:.0f}s: {len(accepted)} accepted, {len(pending)} still rejected")

    # recovery: replicas back in sync, consumers caught up
    replicas_ok_at = None
    while time.monotonic() - started < send_seconds + args.drain_timeout:
        if replicas_ok_at is None and under_replicated() == 0:
            replicas_ok_at = round(time.monotonic() - started, 1)
        lag = total_lag()
        if replicas_ok_at is not None and lag == 0:
            break
        time.sleep(5)
    # the sinks commit every 10 s; give the last batch time to land
    time.sleep(15)
    drained_at = round(time.monotonic() - started, 1)
    print(f"Replicas in sync at t={replicas_ok_at}s, consumers caught up at t~{drained_at}s")

    cdr_ids = set(trino(f"SELECT DISTINCT transaction_id FROM cdr_events WHERE transaction_id LIKE '{prefix}%'"))
    decision_ids = set(trino(f"SELECT DISTINCT transaction_id FROM fraud_decisions "
                             f"WHERE transaction_id LIKE '{prefix}%'"))
    rows, positions = map(int, trino(f"SELECT count(*), count(DISTINCT (kafka_partition, kafka_offset)) "
                                     f"FROM cdr_events WHERE transaction_id LIKE '{prefix}%'")[0].split(","))

    kill_t, restart_t = events[0][0], events[-1][0]
    during = [t for t in timeline if kill_t <= t[0] < restart_t]
    gaps = [b[0] - a[0] for a, b in zip(timeline, timeline[1:])] or [0]
    results = {
        "scenario": args.scenario,
        "killed_brokers": victims,
        "accepted": len(accepted),
        "never_accepted": len(pending),
        "rejections": sum(t[2] for t in timeline),
        "accepted_while_down": sum(t[1] for t in during),
        "rejected_while_down": sum(t[2] for t in during),
        "longest_wait_between_responses_s": round(max(gaps), 1),
        "replicas_in_sync_after_s": replicas_ok_at,
        "lost": len(accepted - cdr_ids),
        "in_cdr_events": len(cdr_ids),
        "cdr_rows": rows,
        "rows_at_duplicate_positions": rows - positions,
        # a client retry after a broker stored the CDR but the API timed out: two messages
        "cdrs_in_kafka_twice": rows - len(cdr_ids),
        "accepted_without_decision": len(accepted - decision_ids),
        "events": events,
    }
    print(json.dumps(results, indent=2))
    if args.out:
        Path(args.out).write_text(json.dumps(results, indent=2))
    return 0 if results["lost"] == 0 and results["rows_at_duplicate_positions"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
