"""Check the platform on Kubernetes end to end, with a broker deleted mid-stream.

Sends CDRs to cdr-ingestion through a port-forward, deletes a Kafka pod while they flow
(the Strimzi operator recreates it), and then checks inside the cluster that every CDR
the API accepted is in the Iceberg tables, with no Kafka message written twice.

Usage (from fraud-detection-system/, after k8s/up.sh):
    python k8s/verify.py
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

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "loadtest"))
from loadtest import load_templates  # noqa: E402

NAMESPACE = "data-platform"
LOCAL_PORT = 18080

# runs inside a sink pod: rows, distinct Kafka positions and distinct transactions per table
COUNT_ROWS = """
import json, sys
from pyiceberg.expressions import StartsWith
from catalog import load_iceberg_catalog
catalog = load_iceberg_catalog()
result = {}
for name in ("telecom.cdr_events", "telecom.fraud_decisions"):
    rows = catalog.load_table(name).scan(row_filter=StartsWith("transaction_id", sys.argv[1])).to_arrow()
    positions = set(zip(rows["kafka_partition"].to_pylist(), rows["kafka_offset"].to_pylist()))
    result[name] = {"rows": rows.num_rows, "positions": len(positions),
                    "transactions": sorted(set(rows["transaction_id"].to_pylist()))}
print(json.dumps(result))
"""


def kubectl(*args, timeout=120):
    return subprocess.run(["kubectl", "-n", NAMESPACE, *args], capture_output=True, text=True,
                          encoding="utf-8", errors="replace", timeout=timeout)


def post_batch(cdrs):
    req = urllib.request.Request(f"http://localhost:{LOCAL_PORT}/ingest/batch", data=json.dumps(cdrs).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read())
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return set()
    return {c["transaction_id"] for c in cdrs} - {e["transaction_id"] for e in body.get("errors", [])}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rate", type=int, default=20)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--delete-at", type=int, default=20)
    parser.add_argument("--broker-pod", default="platform-dual-role-0")
    parser.add_argument("--timeout", type=int, default=600)
    args = parser.parse_args()

    forward = subprocess.Popen(["kubectl", "-n", NAMESPACE, "port-forward", "svc/cdr-ingestion",
                                f"{LOCAL_PORT}:8080"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(30):
            try:
                urllib.request.urlopen(f"http://localhost:{LOCAL_PORT}/health", timeout=2)
                break
            except Exception:
                time.sleep(1)

        prefix = f"K8S-{time.strftime('%H%M%S')}-"
        templates = load_templates()
        accepted, pending = set(), []
        started = time.monotonic()

        def sender():
            n = 0
            while True:
                elapsed = time.monotonic() - started
                due = int(min(elapsed, args.duration) * args.rate)
                batch, pending[:] = pending[:25], pending[25:]
                while len(batch) < 25 and n < due:
                    batch.append(dict(templates[n % len(templates)], transaction_id=f"{prefix}{n}"))
                    n += 1
                if not batch:
                    if elapsed >= args.duration:
                        return
                    time.sleep(0.05)
                    continue
                ok = post_batch(batch)
                accepted.update(ok)
                pending.extend(c for c in batch if c["transaction_id"] not in ok)

        thread = threading.Thread(target=sender, daemon=True)
        thread.start()

        while time.monotonic() - started < args.delete_at:
            time.sleep(0.2)
        kubectl("delete", "pod", args.broker_pod, "--wait=false")
        deleted_at = time.monotonic()
        print(f"t={deleted_at - started:.1f}s deleted {args.broker_pod}")
        kubectl("wait", f"pod/{args.broker_pod}", "--for=delete", "--timeout=120s")
        kubectl("wait", f"pod/{args.broker_pod}", "--for=condition=Ready", "--timeout=300s", timeout=320)
        print(f"t={time.monotonic() - started:.1f}s {args.broker_pod} recreated by Strimzi and ready "
              f"({time.monotonic() - deleted_at:.0f}s after the delete)")

        thread.join(timeout=args.duration + 300)
        print(f"sent: {len(accepted)} accepted, {len(pending)} still rejected")

        # wait for both tables to hold every accepted CDR
        deadline = time.monotonic() + args.timeout
        while True:
            out = kubectl("exec", "deploy/sink-cdr-events", "--", "python", "-c", COUNT_ROWS, prefix, timeout=180)
            counts = json.loads(out.stdout.strip().splitlines()[-1]) if out.returncode == 0 else None
            if counts and all(accepted <= set(t["transactions"]) for t in counts.values()):
                break
            if time.monotonic() > deadline:
                break
            time.sleep(10)

        cdrs, decisions = counts["telecom.cdr_events"], counts["telecom.fraud_decisions"]
        result = {
            "accepted": len(accepted),
            "lost_from_cdr_events": len(accepted - set(cdrs["transactions"])),
            "without_decision": len(accepted - set(decisions["transactions"])),
            "cdr_rows": cdrs["rows"],
            "rows_at_duplicate_positions": cdrs["rows"] - cdrs["positions"],
            "cdrs_in_kafka_twice": cdrs["rows"] - len(cdrs["transactions"]),
        }
        print(json.dumps(result, indent=2))
        return 0 if result["lost_from_cdr_events"] == 0 and result["rows_at_duplicate_positions"] == 0 else 1
    finally:
        forward.terminate()


if __name__ == "__main__":
    sys.exit(main())
