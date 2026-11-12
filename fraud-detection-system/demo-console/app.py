"""Demo console: one page that shows the pipeline live and drives the demo.

A background thread reads, once a second, what every stage reports:
  - the services' own /metrics endpoints (not Prometheus, which scrapes every 15 s)
  - Kafka's admin API: brokers, partition leaders and in-sync replicas, log-end offsets,
    and the consumer groups' committed offsets
  - Trino, every few seconds: row counts and the latest Iceberg snapshots
and keeps the result for the page, which animates the differences between readings.

The controls send CDRs, follow one CDR through every stage, stop and start the Kafka
broker containers, and run compaction. They need the Docker socket, so this service runs
only under the "demo" Compose profile, and it can only touch the three broker containers
and the maintenance container.
"""
import copy
import io
import json
import logging
import os
import random
import threading
import time
from collections import deque
from datetime import datetime
from pathlib import Path

import docker
import fastavro
import requests
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, Producer, TopicPartition
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from prometheus_client.parser import text_string_to_metric_families

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("demo-console")

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092,kafka-2:9092,kafka-3:9092")
INGESTION_URL = os.getenv("INGESTION_URL", "http://cdr-ingestion:8080")
TRINO_URL = os.getenv("TRINO_URL", "http://trino:8080")
ML_URL = os.getenv("ML_URL", "http://ml-service:8001")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
DATASET = os.getenv("DATASET", "/data/telecom_fraud_dataset.json")

TOPICS = {"cdr-events": 3, "fraud-decisions": 3}
GROUPS = {"fraud-detection": "cdr-events", "lakehouse-writer": "cdr-events",
          "lakehouse-fraud-decisions": "fraud-decisions"}
BROKERS = {1: "kafka_1", 2: "kafka_2", 3: "kafka_3"}
MAINTENANCE_CONTAINER = "lakehouse_maintenance_1"
METRICS = {
    "ingestion": ["http://cdr-ingestion:8080/metrics"],
    "fraud": ["http://ml-service:8001/metrics", "http://ml-service2:8001/metrics"],
    "cdr_sink": ["http://lakehouse-writer:8002/metrics"],
    "decisions_sink": ["http://lakehouse-writer-fraud-decisions:8002/metrics"],
    "maintenance": ["http://lakehouse-maintenance:8003/metrics"],
}
TABLES = {"cdr_events": "cdr-events", "fraud_decisions": "fraud-decisions"}

app = FastAPI(title="Data platform demo console")
docker_client = docker.from_env()
# metadata (brokers, leaders, ISR) and end offsets keep answering while a broker is down;
# the admin client is only used for consumer group offsets
metadata_client = Producer({"bootstrap.servers": KAFKA})
offsets_client = Consumer({"bootstrap.servers": KAFKA, "group.id": "demo-console", "enable.auto.commit": False})
admin = AdminClient({"bootstrap.servers": KAFKA})

state = {"updated": None}
state_lock = threading.Lock()
events = deque(maxlen=60)


def event(kind: str, text: str):
    events.appendleft({"time": datetime.now().strftime("%H:%M:%S"), "kind": kind, "text": text})
    log.info(text)


# --- reading the stages -------------------------------------------------------

def read_metrics(urls):
    """{metric name: {frozenset(labels): value}} summed over the given endpoints"""
    totals = {}
    for url in urls:
        try:
            text = requests.get(url, timeout=1.5).text
        except requests.RequestException:
            continue
        for family in text_string_to_metric_families(text):
            for sample in family.samples:
                key = frozenset((k, v) for k, v in sample.labels.items() if k != "le")
                bucket = totals.setdefault(sample.name, {})
                bucket[key] = bucket.get(key, 0.0) + sample.value
    return totals


def metric(families, name, **labels):
    """sum of a metric's samples whose labels include the given ones"""
    total = 0.0
    for key, value in families.get(name, {}).items():
        if all((k, v) in key for k, v in labels.items()):
            total += value
    return total


def read_kafka(last):
    """brokers, and each partition's leader and in-sync replicas.

    A plain metadata request: it kept answering within milliseconds with a broker down,
    while the admin API's describe calls failed for the whole outage.
    """
    try:
        metadata = metadata_client.list_topics(timeout=3)
    except Exception as e:
        log.warning(f"kafka metadata not available: {e}")
        return dict(last, ok=False)
    partitions = {}
    for name in TOPICS:
        parts = metadata.topics[name].partitions
        partitions[name] = [{"partition": i, "leader": parts[i].leader if parts[i].leader >= 0 else None,
                             "isr": sorted(parts[i].isrs), "replicas": len(parts[i].replicas)}
                            for i in sorted(parts)]
    return {"ok": True, "brokers_alive": sorted(metadata.brokers), "partitions": partitions}


def read_offsets(last):
    """log-end offsets and consumer group offsets.

    Read apart from the metadata: a request to a partition whose leader has just died
    waits for its timeout, and that must not hold back the picture of the brokers.
    Whatever does not answer keeps its last reading.
    """
    ends = {name: dict(last.get("ends", {}).get(name, {})) for name in TOPICS}
    for name, count in TOPICS.items():
        for i in range(count):
            try:
                ends[name][i] = offsets_client.get_watermark_offsets(TopicPartition(name, i), timeout=1)[1]
            except Exception:
                pass
    groups = dict(last.get("groups", {}))
    for group, topic in GROUPS.items():
        try:
            request = [ConsumerGroupTopicPartitions(group, [TopicPartition(topic, i) for i in range(TOPICS[topic])])]
            result = admin.list_consumer_group_offsets(request, request_timeout=2)[group].result(timeout=2.5)
            groups[group] = {tp.partition: max(tp.offset, 0) for tp in result.topic_partitions}
        except Exception:
            continue
    return {"ends": ends, "groups": groups}


def trino(sql: str):
    response = requests.post(f"{TRINO_URL}/v1/statement", data=sql, timeout=10, headers={
        "X-Trino-User": "demo-console", "X-Trino-Catalog": "lakehouse", "X-Trino-Schema": "telecom"})
    body, rows = response.json(), []
    while True:
        rows.extend(body.get("data", []))
        if body.get("error"):
            raise RuntimeError(body["error"].get("message"))
        if "nextUri" not in body:
            return rows
        body = requests.get(body["nextUri"], timeout=10).json()


def read_tables(last):
    tables = dict(last)
    for table, topic in TABLES.items():
        try:
            rows = trino(f"SELECT count(*) FROM {table}")[0][0]
            snapshots = trino(
                f"SELECT committed_at, operation, element_at(summary, 'added-records'), "
                f"element_at(summary, 'kafka.{topic}.offsets') "
                f'FROM "{table}$snapshots" ORDER BY committed_at DESC LIMIT 5')
            tables[table] = {"rows": rows, "snapshots": [
                {"time": str(s[0])[11:19], "operation": s[1], "added": int(s[2]) if s[2] else 0,
                 "offsets": json.loads(s[3]) if s[3] else None} for s in snapshots]}
        except Exception as e:
            log.warning(f"trino read of {table} failed: {e}")
    return tables


def broker_containers():
    status = {}
    for broker, name in BROKERS.items():
        try:
            status[broker] = docker_client.containers.get(name).status
        except docker.errors.DockerException:
            status[broker] = "missing"
    return status


def read_stages():
    m = {stage: read_metrics(urls) for stage, urls in METRICS.items()}
    return {
        "ingested": metric(m["ingestion"], "cdr_ingest_total", result="accepted"),
        "invalid": metric(m["ingestion"], "cdr_ingest_total", result="invalid"),
        "publish_failed": metric(m["ingestion"], "cdr_ingest_total", result="publish_failed"),
        "fraud": metric(m["fraud"], "cdr_processed_total", status="fraud"),
        "clean": metric(m["fraud"], "cdr_processed_total", status="clean"),
        "fraud_errors": metric(m["fraud"], "cdr_processed_total", status="error"),
        "decisions": metric(m["fraud"], "fraud_decisions_published_total"),
        "notifications": metric(m["fraud"], "alert_notifications_total", result="sent"),
        "duplicate_alerts": metric(m["fraud"], "duplicate_alerts_skipped_total"),
        "cdr_buffered": metric(m["cdr_sink"], "lakehouse_buffered_rows"),
        "cdr_appended": metric(m["cdr_sink"], "lakehouse_rows_appended_total"),
        "cdr_dlq": metric(m["cdr_sink"], "lakehouse_dlq_messages_total"),
        "decisions_buffered": metric(m["decisions_sink"], "lakehouse_buffered_rows"),
        "decisions_appended": metric(m["decisions_sink"], "lakehouse_rows_appended_total"),
        "cdr_files": metric(m["maintenance"], "lakehouse_table_data_files", table="telecom.cdr_events"),
        "decisions_files": metric(m["maintenance"], "lakehouse_table_data_files", table="telecom.fraud_decisions"),
    }


def every(seconds, work):
    """run work() in its own thread every `seconds`, so a slow source never stalls the others"""
    def loop():
        while True:
            started = time.monotonic()
            try:
                work()
            except Exception as e:
                log.warning(f"{work.__name__} failed: {e}")
            time.sleep(max(0.0, seconds - (time.monotonic() - started)))
    threading.Thread(target=loop, daemon=True, name=work.__name__).start()


def update(**values):
    with state_lock:
        state.update(values, updated=time.time())


def collect_stages():
    containers = broker_containers()
    with state_lock:
        before = state.get("containers") or {}
    for broker, status in containers.items():
        if broker in before and status != before[broker]:
            if status == "running":
                event("good", f"broker {broker} container started")
            else:
                event("bad", f"broker {broker} container {status}")
    update(stages=read_stages(), containers=containers, traffic=traffic.snapshot(),
           compaction=compaction.snapshot(), events=list(events))


def collect_kafka():
    with state_lock:
        last = state.get("kafka") or {}
    kafka = read_kafka(last)
    if kafka["ok"] and last.get("ok"):
        alive, was = set(kafka["brokers_alive"]), set(last["brokers_alive"] or [])
        for b in sorted(was - alive):
            event("bad", f"broker {b} dropped out of the cluster")
        for b in sorted(alive - was):
            event("good", f"broker {b} is back in the cluster")
        for new, old in zip(kafka["partitions"].get("cdr-events", []), last["partitions"].get("cdr-events", [])):
            if new["leader"] != old["leader"]:
                event("info", f"cdr-events P{new['partition']}: leader broker {old['leader']} -> {new['leader']}")
            if len(new["isr"]) != len(old["isr"]):
                event("bad" if len(new["isr"]) < len(old["isr"]) else "good",
                      f"cdr-events P{new['partition']}: in-sync replicas {len(new['isr'])} of 3 {new['isr']}")
    update(kafka=kafka)


def collect_offsets():
    with state_lock:
        last = state.get("offsets") or {}
    update(offsets=read_offsets(last))


def collect_tables():
    with state_lock:
        last = state.get("tables") or {}
    tables = read_tables(last)
    for table, info in tables.items():
        latest = info["snapshots"][0] if info["snapshots"] else None
        before = last.get(table, {}).get("snapshots") or [None]
        if latest and before[0] and (latest["time"], latest["operation"]) != (before[0]["time"], before[0]["operation"]):
            if latest["operation"] == "append":
                event("data", f"{table}: +{latest['added']} rows committed, offsets {latest['offsets']}")
            else:
                event("info", f"{table}: {latest['operation']} commit (maintenance)")
    update(tables=tables)


# --- the controls -------------------------------------------------------------

def load_templates():
    """the sample dataset flattened into CDRs, as ingest-auto.py does"""
    data = json.loads(Path(DATASET).read_text(encoding="utf-8"))
    records = []
    for c in data["customers"]:
        base = {"customer_id": c["customer_id"], "full_name": c["full_name"],
                "risk_score": round(c["risk_score"] / 100.0, 4), "city": c["contact_info"]["city"],
                "mobile_number": c["telecom_info"]["mobile_number"],
                "sim_serial_number": c["telecom_info"]["sim_id_iccid"],
                "sim_status": c["telecom_info"]["sim_status"].lower(), "imei": c["device_info"]["imei"],
                "fraud_flag": c["fraud_flags"]["flagged_for_fraud"], "fraud_type": c["fraud_flags"]["fraud_type"]}
        for t in c.get("transaction_logs", []):
            records.append({**base, "transaction_id": t["transaction_id"], "time_stamp": t["timestamp"],
                            "type": t["type"], "amount_eur": t["amount_eur"]})
    return records


class Traffic:
    """sends CDRs to the ingestion API at a steady rate, with unique transaction ids.

    Like a real client, it sends a rejected CDR again (same transaction id) with the
    next batch, until the API confirms that Kafka stored it.
    """

    def __init__(self):
        self.templates = load_templates()
        self.run_id = datetime.now().strftime("%H%M%S")
        self.n = 0
        self.rate = 0
        self.sent = self.accepted = self.rejected = self.retried = 0
        self.pending = []
        self.thread = None
        self.lock = threading.Lock()

    def next_cdrs(self, count):
        cdrs = []
        for _ in range(count):
            template = self.templates[self.n % len(self.templates)]
            cdrs.append(dict(template, transaction_id=f"DEMO-{self.run_id}-{self.n}"))
            self.n += 1
        return cdrs

    def post(self, cdrs):
        """the CDRs the API did not accept"""
        try:
            response = requests.post(f"{INGESTION_URL}/ingest/batch", json=cdrs, timeout=30)
            return [cdrs[e["index"]] for e in response.json().get("errors", [])]
        except (requests.RequestException, ValueError, KeyError):
            return cdrs

    def run(self):
        while self.rate > 0:
            started = time.monotonic()
            retries, self.pending = self.pending, []
            cdrs = retries + self.next_cdrs(self.rate)
            failed = self.post(cdrs)
            with self.lock:
                self.sent += len(cdrs)
                self.accepted += len(cdrs) - len(failed)
                self.rejected += len(failed)
                self.retried += len(retries)
            self.pending = failed
            if failed:
                event("bad", f"ingestion rejected {len(failed)} CDRs; sending them again with the next batch")
            elif retries:
                event("good", f"{len(retries)} rejected CDRs sent again and accepted, none lost")
            time.sleep(max(0.0, 1.0 - (time.monotonic() - started)))

    def start(self, rate):
        was_running = self.rate > 0
        self.rate = rate
        if not was_running:
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
        event("info", f"traffic: {rate} CDRs per second")

    def stop(self):
        self.rate = 0
        event("info", "traffic stopped")

    def snapshot(self):
        with self.lock:
            return {"rate": self.rate, "sent": self.sent, "accepted": self.accepted, "rejected": self.rejected,
                    "retried": self.retried, "pending": len(self.pending)}


class Compaction:
    """runs `maintenance.py compact` in the maintenance container"""

    def __init__(self):
        self.running = False
        self.result = None

    def run(self):
        self.running = True
        event("info", "compaction started")
        try:
            container = docker_client.containers.get(MAINTENANCE_CONTAINER)
            output = container.exec_run(["python", "maintenance.py", "compact"], demux=True)
            report = json.loads((output.output[0] or b"{}").decode())
            self.result = {table: {"before": r["before"]["data_files"], "after": r["after"]["data_files"],
                                   "rows_before": r["before"]["rows"], "rows_after": r["after"]["rows"]}
                           for table, r in report.items() if isinstance(r, dict)}
            for table, r in self.result.items():
                event("good", f"{table}: {r['before']} files -> {r['after']}, rows {r['rows_before']} -> "
                              f"{r['rows_after']}")
        except Exception as e:
            self.result = {"error": str(e)}
            event("bad", f"compaction failed: {e}")
        finally:
            self.running = False

    def snapshot(self):
        return {"running": self.running, "result": self.result}


schemas = {}


def decode_avro(value: bytes):
    """a Confluent-framed Avro value: magic byte, 4-byte schema id, then the record"""
    schema_id = int.from_bytes(value[1:5], "big")
    if schema_id not in schemas:
        response = requests.get(f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}", timeout=5)
        schemas[schema_id] = fastavro.parse_schema(json.loads(response.json()["schema"]))
    return fastavro.schemaless_reader(io.BytesIO(value[5:]), schemas[schema_id])


def committing_snapshot(table, topic, partition, offset):
    """the first snapshot whose stored offsets go past the record: the commit that wrote it.
    The sink stores the next offset to read, so that snapshot's offset is above the record's."""
    rows = trino(f"SELECT snapshot_id, committed_at, element_at(summary, 'kafka.{topic}.offsets') "
                 f"FROM \"{table}$snapshots\" WHERE operation = 'append' ORDER BY committed_at DESC LIMIT 50")
    for snapshot_id, committed_at, offsets in reversed(rows):
        if offsets and json.loads(offsets).get(str(partition), -1) > offset:
            return snapshot_id, str(committed_at)[11:19], json.loads(offsets)
    return None


class Trace:
    """sends one CDR and follows it through every stage.

    Each step is confirmed by the system itself, not assumed: the API's answer, the record
    in cdr-events, the decision event in fraud-decisions, the alert in Postgres, and the
    rows in both Iceberg tables with the snapshot that committed them.
    """

    WAITING = {
        "ingest": "sending to the ingestion API",
        "kafka": "looking for it in cdr-events",
        "decision": "waiting for a fraud detection replica to decide",
        "alert": "only fraud gets an alert",
        "cdr_iceberg": "in the sink's buffer until the next commit (500 rows or 10 s)",
        "decision_iceberg": "in the decisions sink's buffer until its next commit",
    }

    def __init__(self):
        self.lock = threading.Lock()
        self.current = None
        self.count = 0

    def snapshot(self):
        with self.lock:
            return copy.deepcopy(self.current)

    @staticmethod
    def pick(suspicious: bool):
        """a suspicious CDR comes from a customer the dataset flags; a normal one from a
        customer whose earlier CDRs were all judged clean, as the lakehouse records"""
        candidates = [t for t in traffic.templates if bool(t["fraud_flag"]) == suspicious]
        if not suspicious:
            try:
                clean = {row[0] for row in trino("SELECT customer_id FROM fraud_decisions "
                                                 "GROUP BY customer_id HAVING NOT bool_or(is_fraud)")}
                candidates = [t for t in candidates if t["customer_id"] in clean] or candidates
            except Exception as e:
                log.warning(f"could not look up clean customers: {e}")
        return random.choice(candidates or traffic.templates)

    def start(self, suspicious: bool):
        template = self.pick(suspicious)
        with self.lock:
            if self.current and not self.current["finished"]:
                return False
            self.count += 1
            cdr = dict(template, transaction_id=f"TRACE-{traffic.run_id}-{self.count}")
            self.current = {"transaction_id": cdr["transaction_id"], "customer_id": cdr["customer_id"],
                            "amount_eur": cdr["amount_eur"], "started": time.time(), "finished": False,
                            "partition": None,
                            "steps": {key: {"status": "waiting", "after": None, "text": text}
                                      for key, text in self.WAITING.items()}}
        event("info", f"following {cdr['transaction_id']} through the pipeline")
        threading.Thread(target=self.run, args=(cdr,), daemon=True).start()
        return True

    def step(self, key, status, text):
        with self.lock:
            self.current["steps"][key] = {"status": status, "text": text,
                                          "after": round(time.time() - self.current["started"], 1)}

    def run(self, cdr):
        try:
            found = self.follow_kafka(cdr)
            if found:
                cdr_position, decision_position, decision = found
                self.follow_alert(cdr["transaction_id"], decision)
                self.follow_iceberg(cdr["transaction_id"], cdr_position, decision_position)
        except Exception as e:
            log.warning(f"trace failed: {e}")
        finally:
            with self.lock:
                for step in self.current["steps"].values():
                    if step["status"] == "waiting":
                        step.update(status="failed", text=f"not seen: {step['text']}")
                self.current["finished"] = True

    def follow_kafka(self, cdr):
        """send the CDR, then find it in cdr-events and its decision in fraud-decisions"""
        tid = cdr["transaction_id"]
        consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "demo-console-trace",
                             "enable.auto.commit": False})
        try:
            # read both topics from their current ends, taken before the CDR is sent
            start = []
            for name, count in TOPICS.items():
                for i in range(count):
                    start.append(TopicPartition(name, i, consumer.get_watermark_offsets(
                        TopicPartition(name, i), timeout=5)[1]))
            consumer.assign(start)

            sent = time.monotonic()
            response = requests.post(f"{INGESTION_URL}/ingest", json=cdr, timeout=30)
            if response.status_code != 202:
                body = response.json()
                self.step("ingest", "failed", f"HTTP {response.status_code}: {body.get('detail') or body}")
                return None
            self.step("ingest", "done", f"valid; encoded with the cdr-events Avro contract; HTTP 202 after "
                                        f"{(time.monotonic() - sent) * 1000:.0f} ms, once two brokers had stored it")

            needle = tid.encode()
            cdr_position = decision_position = decision = None
            deadline = time.monotonic() + 30
            while (cdr_position is None or decision is None) and time.monotonic() < deadline:
                msg = consumer.poll(0.5)
                if msg is None or msg.error() or needle not in msg.value():
                    continue
                if msg.topic() == "cdr-events" and cdr_position is None:
                    cdr_position = (msg.partition(), msg.offset())
                    with self.lock:
                        self.current["partition"] = msg.partition()
                    with state_lock:
                        kafka = state.get("kafka") or {}
                    p = next((p for p in kafka.get("partitions", {}).get("cdr-events", [])
                              if p["partition"] == msg.partition()), {})
                    self.step("kafka", "done", f"cdr-events partition {msg.partition()}, offset {msg.offset()}, "
                                               f"keyed by customer; leader broker {p.get('leader')}, "
                                               f"copies in sync on brokers {p.get('isr')}")
                elif msg.topic() == "fraud-decisions" and decision is None:
                    decision = decode_avro(msg.value())
                    decision_position = (msg.partition(), msg.offset())
                    with self.lock:
                        if self.current["partition"] is None:
                            self.current["partition"] = decision["source_partition"]
                    verdict = f"FRAUD ({decision['fraud_type'] or 'unspecified'})" if decision["is_fraud"] else "clean"
                    confidence = f", confidence {decision['confidence']:.2f}" if decision["confidence"] is not None else ""
                    self.step("decision", "done", f"{verdict} by {decision['method']}{confidence}, decided by "
                                                  f"{decision['detector']}; published to fraud-decisions "
                                                  f"partition {msg.partition()}")
            return (cdr_position, decision_position, decision) if cdr_position and decision else None
        finally:
            consumer.close()

    def follow_alert(self, tid, decision):
        if not decision["is_fraud"]:
            self.step("alert", "skipped", "clean, so no alert")
            return
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            response = requests.get(f"{ML_URL}/alerts/{tid}", timeout=5)
            if response.status_code == 200:
                self.step("alert", "done", f"stored in Postgres (status {response.json().get('status')}); "
                                           f"Alertmanager notified from a background thread")
                return
            time.sleep(1)

    def follow_iceberg(self, tid, cdr_position, decision_position):
        """wait for both rows to be committed, and name the snapshot that committed each"""
        pending = {"cdr_iceberg": ("cdr_events", "cdr-events", cdr_position),
                   "decision_iceberg": ("fraud_decisions", "fraud-decisions", decision_position)}
        deadline = time.monotonic() + 60
        while pending and time.monotonic() < deadline:
            for key, (table, topic, (partition, offset)) in list(pending.items()):
                if not trino(f"SELECT count(*) FROM {table} WHERE transaction_id = '{tid}'")[0][0]:
                    continue
                snapshot = committing_snapshot(table, topic, partition, offset)
                where = (f"committed in snapshot {snapshot[0]} at {snapshot[1]}, which stores {topic} offset "
                         f"{snapshot[2][str(partition)]} for partition {partition}: the next one after this "
                         f"record, so a restart neither skips nor repeats it"
                         if snapshot else "committed in an earlier snapshot")
                self.step(key, "done", f"row in {table} (Parquet in S3); {where}")
                del pending[key]
            time.sleep(2)


traffic = Traffic()
compaction = Compaction()
trace = Trace()


@app.on_event("startup")
def start_collectors():
    every(1.0, collect_stages)
    every(1.0, collect_kafka)
    every(1.0, collect_offsets)
    every(4.0, collect_tables)


@app.get("/")
def page():
    return FileResponse(Path(__file__).parent / "static" / "index.html")


@app.get("/api/state")
def get_state():
    """the readings, with each partition's end offset put next to its leader and ISR"""
    with state_lock:
        result = dict(state)
    result["trace"] = trace.snapshot()
    offsets = result.pop("offsets", None) or {}
    if result.get("kafka"):
        ends = offsets.get("ends", {})
        result["kafka"] = dict(result["kafka"], groups=offsets.get("groups", {}), partitions={
            name: [dict(p, end=ends.get(name, {}).get(p["partition"], 0)) for p in parts]
            for name, parts in result["kafka"].get("partitions", {}).items()})
    return result


@app.post("/api/traffic/start")
def start_traffic(rate: int = 10):
    traffic.start(max(1, min(rate, 100)))
    return traffic.snapshot()


@app.post("/api/traffic/stop")
def stop_traffic():
    traffic.stop()
    return traffic.snapshot()


@app.post("/api/send/{kind}")
def send_one(kind: str):
    if kind not in ("valid", "invalid"):
        raise HTTPException(404)
    cdr = traffic.next_cdrs(1)[0]
    if kind == "invalid":
        cdr["imei"] = "123"
    response = requests.post(f"{INGESTION_URL}/ingest", json=cdr, timeout=30)
    detail = response.json().get("message") or response.json().get("detail")
    event("good" if response.status_code == 202 else "bad",
          f"POST /ingest {cdr['transaction_id']}: HTTP {response.status_code} {detail}")
    return {"status": response.status_code, "detail": detail, "transaction_id": cdr["transaction_id"]}


@app.post("/api/trace/{kind}")
def start_trace(kind: str):
    if kind not in ("normal", "suspicious"):
        raise HTTPException(404)
    if not trace.start(suspicious=kind == "suspicious"):
        raise HTTPException(409, "a CDR is still being followed")
    return trace.snapshot()


@app.post("/api/broker/{broker}/{action}")
def broker_action(broker: int, action: str):
    if broker not in BROKERS or action not in ("kill", "start"):
        raise HTTPException(404)
    container = docker_client.containers.get(BROKERS[broker])
    if action == "kill":
        container.kill()
        event("bad", f"killed broker {broker} (SIGKILL)")
    else:
        container.start()
        event("good", f"started broker {broker}")
    return {"broker": broker, "action": action}


@app.post("/api/compact")
def compact():
    if not compaction.running:
        threading.Thread(target=compaction.run, daemon=True).start()
    return compaction.snapshot()
