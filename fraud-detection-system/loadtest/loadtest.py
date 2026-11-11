"""Load test: push synthetic CDRs through the pipeline and measure how fast each part keeps up.

1. Send phase: POST batches to /ingest/batch from several threads. Each request returns
   only after Kafka has stored the batch, so this measures confirmed ingest throughput.
2. Drain phase: poll consumer lag for the fraud-detection and lakehouse-writer groups
   until both reach zero, and report how long each took.
3. Writer stats: parse the lakehouse writer's log for batch sizes and Iceberg commit times.

CDRs are copies of the sample dataset with unique transaction ids, so the key
distribution (customer_id) matches the sample data.

Usage (from fraud-detection-system/, with the stack running):
    python loadtest/loadtest.py --records 50000
"""
import argparse
import importlib.util
import json
import re
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GROUPS = ("fraud-detection", "lakehouse-writer")


def load_templates():
    spec = importlib.util.spec_from_file_location("ingest_auto", ROOT / "ingest-auto" / "ingest-auto.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.load_records(str(ROOT / "cdr-ingestion" / "app" / "telecom_fraud_dataset.json"))


def make_batches(templates, records, batch_size, run_id):
    cdrs = [dict(templates[i % len(templates)], transaction_id=f"LT-{run_id}-{i}") for i in range(records)]
    return [cdrs[i:i + batch_size] for i in range(0, len(cdrs), batch_size)]


def post_batch(url, batch):
    req = urllib.request.Request(
        f"{url}/ingest/batch",
        data=json.dumps(batch).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read())
        return time.monotonic() - started, body.get("accepted", 0), body.get("rejected", 0)
    except (urllib.error.URLError, TimeoutError) as e:
        print(f"  batch failed: {e}", file=sys.stderr)
        return time.monotonic() - started, 0, len(batch)


def consumer_lag(kafka_container):
    """total lag per consumer group, from kafka-consumer-groups.sh; {} if the check timed out"""
    try:
        out = subprocess.run(
            ["docker", "exec", kafka_container, "/opt/kafka/bin/kafka-consumer-groups.sh",
             "--bootstrap-server", "localhost:9092", "--describe", "--all-groups"],
            capture_output=True, text=True, timeout=60,
        ).stdout
    except subprocess.TimeoutExpired:
        print("  lag check timed out", file=sys.stderr)
        return {}
    lag = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[0] in GROUPS and parts[1] == "cdr-events":
            current, end = parts[3], parts[4]
            behind = int(end) - int(current) if current.isdigit() else int(end)
            lag[parts[0]] = lag.get(parts[0], 0) + behind
    return lag


def percentile(values, p):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(p / 100 * (len(ordered) - 1))))]


def writer_stats(container, since):
    logs = subprocess.run(["docker", "logs", "--since", since, container],
                          capture_output=True, text=True, encoding="utf-8", errors="replace")
    appends = [(int(n), float(s)) for n, s in
               re.findall(r"Appended (\d+) rows to \S+ in ([\d.]+)s", logs.stdout + logs.stderr)]
    if not appends:
        return None
    rows = [n for n, _ in appends]
    seconds = [s for _, s in appends]
    return {
        "commits": len(appends),
        "rows": sum(rows),
        "rows_per_commit_avg": round(statistics.mean(rows)),
        "commit_seconds_p50": round(statistics.median(seconds), 2),
        "commit_seconds_p95": round(percentile(seconds, 95), 2),
        "commit_seconds_max": round(max(seconds), 2),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--records", type=int, default=50000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--kafka-container", default="kafka_1")
    parser.add_argument("--writer-container", default="lakehouse_writer_1")
    parser.add_argument("--drain-timeout", type=int, default=1800, help="seconds to wait for lag to reach 0")
    parser.add_argument("--out", help="write the results as JSON to this file")
    args = parser.parse_args()

    run_id = time.strftime("%Y%m%d%H%M%S")
    batches = make_batches(load_templates(), args.records, args.batch_size, run_id)
    log_since = time.strftime("%Y-%m-%dT%H:%M:%S")
    start_lag = consumer_lag(args.kafka_container)
    print(f"Run {run_id}: {args.records} CDRs in {len(batches)} batches of {args.batch_size}, "
          f"{args.concurrency} concurrent requests. Lag before start: {start_lag}")

    # send phase
    latencies, accepted, rejected = [], 0, 0
    lock = threading.Lock()
    send_started = time.monotonic()

    def send(batch):
        nonlocal accepted, rejected
        seconds, ok, failed = post_batch(args.url, batch)
        with lock:
            latencies.append(seconds)
            accepted += ok
            rejected += failed

    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        list(pool.map(send, batches))
    send_seconds = time.monotonic() - send_started
    print(f"Sent in {send_seconds:.1f}s: {accepted} accepted, {rejected} rejected, "
          f"{accepted / send_seconds:.0f} CDRs/s")

    # drain phase: time from the start of sending until each group has caught up
    drained, max_lag, last_report = {}, {g: 0 for g in GROUPS}, 0.0
    while len(drained) < len(GROUPS) and time.monotonic() - send_started < args.drain_timeout:
        lag = consumer_lag(args.kafka_container)
        elapsed = time.monotonic() - send_started
        for group in GROUPS:
            max_lag[group] = max(max_lag[group], lag.get(group, 0))
            if group not in drained and lag.get(group) == 0:
                drained[group] = elapsed
                print(f"  {group} caught up after {elapsed:.1f}s")
        if elapsed - last_report >= 30:
            print(f"  t={elapsed:.0f}s lag={lag}")
            last_report = elapsed
        time.sleep(2)

    results = {
        "run_id": run_id,
        "records": args.records,
        "batch_size": args.batch_size,
        "concurrency": args.concurrency,
        "ingest": {
            "accepted": accepted,
            "rejected": rejected,
            "seconds": round(send_seconds, 1),
            "cdrs_per_second": round(accepted / send_seconds),
            "batch_latency_p50": round(statistics.median(latencies), 3),
            "batch_latency_p95": round(percentile(latencies, 95), 3),
            "batch_latency_max": round(max(latencies), 3),
        },
        "consumers": {
            group: {
                "caught_up_after_seconds": round(drained[group], 1) if group in drained else None,
                "cdrs_per_second": round(accepted / drained[group]) if group in drained else None,
                "max_lag": max_lag[group],
            }
            for group in GROUPS
        },
        "lakehouse_writer": writer_stats(args.writer_container, log_since),
    }
    not_drained = [g for g in GROUPS if g not in drained]
    if not_drained:
        results["note"] = f"{not_drained} still lagging after {args.drain_timeout}s"

    print(json.dumps(results, indent=2))
    if args.out:
        Path(args.out).write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
