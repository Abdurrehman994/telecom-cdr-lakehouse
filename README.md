# Telecom CDR Streaming Lakehouse

[![CI](https://github.com/Abdurrehman994/telecom-cdr-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/Abdurrehman994/telecom-cdr-lakehouse/actions/workflows/ci.yml)

> This repository continues a University of Oulu group project
> ([original repo](https://github.com/FastByte17/DS_Project_2026)) built by
> Nabeel Hussain, Muhammad Abdur Rehman, Christoph Gasche, Marko Tiitto and Talha Pasha.
> Work from November 2026 onward is by Muhammad Abdur Rehman.

Call detail records (CDRs) stream through Kafka into two independent consumers: real-time fraud detection and an Apache Iceberg lakehouse. Every topic has an Avro data contract in a schema registry, and topics land in Iceberg through config-driven sinks: adding a dataset takes a contract and a sink file, not code.

```text
                     Schema Registry (Avro contracts, FULL_TRANSITIVE)
HTTP ─► cdr-ingestion ─► Kafka (cdr-events, keyed by customer_id)
                            ├─► ml-service ×2 ─► Postgres (fraud_alerts) ─► Alertmanager
                            │        └─► Kafka (fraud-decisions)
                            │                └─► sink: fraud_decisions ─┐
                            └─► sink: cdr_events ───────────────────────┴─► Iceberg on S3 (SeaweedFS)
                                                                                  catalog: Lakekeeper (Postgres)
                                                                                  query: Trino, DuckDB
```

## Highlights

- **Data contracts.** Each topic's messages have an Avro schema in a schema registry, with `FULL_TRANSITIVE` compatibility. A breaking change is rejected when contracts are registered, and producers can only send with a registered schema. A shared client library (`libs/dataplatform`) does the encoding for every service.
- **Config-driven lakehouse sinks.** A YAML file per sink names the topic, table, partitioning, batching and maintenance policy; the table's columns come from the contract. New optional contract fields become new columns automatically. A second dataset, fraud decisions, was onboarded with three files and no new code ([how](docs/onboarding-a-dataset.md)).
- **Exactly-once into Iceberg.** Kafka offsets and the topic's id are stored in the same Iceberg snapshot as the data, so a crash, an offset reset or a recreated topic never duplicates or loses rows.
- **Idempotent fraud alerts.** Alerts are keyed by `transaction_id`, so replaying the topic creates no duplicate alerts or notifications.
- **Confirmed ingestion.** The HTTP API answers only after Kafka has stored the CDR, and returns `503` if it could not.
- **Failure handling.** A dead-letter topic for bad messages, retries with backoff when Postgres is down, and consumer rebalancing when a replica stops. Each was tested.
- **OLTP and OLAP side by side.** Postgres for alert case management; Iceberg tables in a Lakekeeper catalog, queried with Trino or DuckDB, for analytics and snapshot time travel.
- **Production-grade catalog.** Lakekeeper keeps catalog state in Postgres and vends short-lived storage credentials, so engines such as Trino hold no storage keys. It replaced a SQLite-backed catalog that failed under concurrent commits; the tables moved over without copying data.
- **Table maintenance.** Hourly compaction, snapshot expiry and orphan file cleanup, safe to run while the writer commits. After a load test it turned 5,081 small files into 301.
- **Observability.** Prometheus metrics from every service, consumer lag and DLQ rate from Kafka, 12 alert rules, and a provisioned Grafana dashboard.
- **Load tested.** About 1,500 CDRs/s confirmed into Kafka on a laptop; the test found and fixed two bottlenecks in fraud detection.
- **Replicated Kafka, tested by killing brokers.** Three nodes, replication factor 3, `min.insync.replicas=2`. With a broker killed mid-stream, writes went on and no accepted CDR was lost or duplicated; with two killed, the API refused writes rather than confirm unreplicated data.

For the design, trade-offs, tested failure scenarios and production roadmap, see [docs/architecture.md](docs/architecture.md). To add a dataset, see [docs/onboarding-a-dataset.md](docs/onboarding-a-dataset.md).

## Table of Contents
- [Prerequisites](#prerequisites)
- [Initial Setup](#initial-setup)
- [Starting the Stack](#starting-the-stack)
- [Accessing Services](#accessing-services)
- [Running Tests](#running-tests)
- [Security Best Practices](#security-best-practices)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

Before you begin, ensure you have the following installed:

- **Git** ([Download](https://git-scm.com/downloads))
- **Docker Desktop** ([Download](https://www.docker.com/products/docker-desktop))
  - Make sure Docker Desktop is running before executing any commands
- **Text Editor**
  - Windows: Notepad, VS Code, or Notepad++
  - Linux/Mac: nano, vim, VS Code

---

## Initial Setup

### 1. Clone the Repository

```bash
git clone https://github.com/Abdurrehman994/telecom-cdr-lakehouse
cd telecom-cdr-lakehouse/fraud-detection-system
```

---

### 2. Environment Variables (optional)

No `.env` file is needed to run the stack locally. `docker-compose.system.yml` has development defaults for every setting:

| Setting | Default |
|---|---|
| Grafana login | `admin` / `admin` |
| Lakehouse S3 credentials | `lakehouse` / `lakehouse-secret` |

To override them, copy `fraud-detection-system/.env.example` to `fraud-detection-system/.env` and change the values. Always override the defaults on any machine other than your own.

---

## Starting the Stack

### 3. Validate Configuration

Before starting, validate your docker-compose file:

```bash
docker compose -f docker-compose.system.yml config
```

If there are no errors, you'll see the parsed configuration.

### 4. Start All Services

```bash
docker compose -f docker-compose.system.yml up -d
```

**Expected output:** NOTE this may differ if your run it the first time.
```
[+] up 9/9
 ✔ Container fraud-detection-alertmanager  Started                                                                                                                                                                                     0.4ss
 ✔ Container fraud-detection-prometheus    Started                                                                                                                                                                                     0.4ss
 ✔ Container fraud-detection-grafana       Started                                                                                                                                                                                     0.5ss
...
```

You can stop the service with:
```bash
docker compose -f docker-compose.system.yml down
```

### 5. Verify Services Are Running

```bash
docker container ps
```

**Expected output:**
```
CONTAINER ID   IMAGE                                  COMMAND                  CREATED          STATUS                     PORTS                                                                                                                                                     NAMES
1ba852b7c0a2   grafana/grafana:latest                 "/run.sh"                5 minutes ago    Up 5 minutes               0.0.0.0:3000->3000/tcp, [::]:3000->3000/tcp                                                                                                               fraud-detection-grafana
1f7805fde75c   prom/prometheus:latest                 "/bin/prometheus --c…"   5 minutes ago    Up 5 minutes               0.0.0.0:9090->9090/tcp, [::]:9090->9090/tcp                                                                                                               fraud-detection-prometheus
51057b47d1ce   prom/alertmanager:latest               "/bin/alertmanager -…"   5 minutes ago    Up 5 minutes               0.0.0.0:9093->9093/tcp, [::]:9093->9093/
...
```

### 6. Add Llama3.2 model to OLlama container

```bash
docker exec -it fraud-detection-ollama ollama pull llama3.2
```

### (Optional) 7. Run a example payload to the service

Go to /fraud-detection-system/ingest-auto/

Run test script:
```bash
python ingest-auto.py
```

---

## Accessing Services

Open your web browser and navigate to:

### Grafana
- **URL**: http://localhost:3000
- **Username**: Value from your `.env` file (default: `admin`)
- **Password**: Value from your `.env` file

**First login steps:**
1. Navigate to http://localhost:3000
2. Enter your credentials
3. Open **Dashboards → CDR Pipeline**: consumer lag, throughput, ingestion results, fraud decisions, Iceberg commit times and table health

### Prometheus
- **URL**: http://localhost:9090
- **Check targets**: http://localhost:9090/targets
- Should show `prometheus` and `node-exporter` as UP

### Alertmanager
- **URL**: http://localhost:9093
- View active alerts and silence rules

### Node Exporter Metrics
- **URL**: http://localhost:9100/metrics
- Raw metrics exported by Node Exporter

### Ingestion Service Swagger
- **URL**: http://localhost:8080/docs
- Run payloads to service

### Schema Registry
- **URL**: http://localhost:8081 (e.g. http://localhost:8081/subjects)
- **Contracts**: `fraud-detection-system/contracts/`, registered by the one-shot `schema-init` job on startup
- **Check a contract change** before starting the stack:
  ```bash
  pip install fraud-detection-system/libs/dataplatform
  python -m dataplatform.contracts check fraud-detection-system/contracts --registry-url http://localhost:8081
  ```

### Kafka
- **Brokers**: three nodes (`kafka`, `kafka-2`, `kafka-3`), replication factor 3, `min.insync.replicas=2`. From the host: `localhost:9094`, `9095`, `9096`; containers use `kafka:9092,kafka-2:9092,kafka-3:9092`
- **Failure test**: `python loadtest/broker_failure.py --scenario one` (or `two`) kills brokers while sending CDRs and checks that nothing accepted was lost
- **Topics**: `cdr-events` (3 partitions, keyed by `customer_id`), `fraud-decisions` (3 partitions, one decision per CDR), and a dead-letter topic for each (`cdr-events-dlq`, `fraud-decisions-dlq`)
- **Consumer group**: `fraud-detection` (`ml-service` and `ml-service2` share the partitions)
- **Check consumer lag**:
  ```bash
  docker exec kafka_1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group fraud-detection
  ```

### Fraud Alerts (Postgres)
- **Database**: `localhost:5432`, database `cdr_database`, table `fraud_alerts` (schema in `fraud-detection-system/postgres/init/`)
- **List open alerts**: http://localhost:8001/alerts?status=open
- **Record a decision**:
  ```bash
  curl -X PATCH http://localhost:8001/alerts/<transaction_id> -H "Content-Type: application/json" -d '{"status": "confirmed", "note": "verified with customer"}'
  ```
- Each transaction has at most one alert, so replaying Kafka never duplicates alerts or notifications

### Lakehouse (Apache Iceberg)
- **Sinks**: one writer per file in `fraud-detection-system/lakehouse/sinks/`. `lakehouse-writer` streams `cdr-events` into `telecom.cdr_events`, and `lakehouse-writer-fraud-decisions` streams `fraud-decisions` into `telecom.fraud_decisions`; both are partitioned by day. Each sink reads its topic as its own consumer group
- **Columns**: one per field of the topic's contract, plus `kafka_partition`, `kafka_offset` and `ingested_at`
- **Storage**: SeaweedFS (S3 API at http://localhost:8333), bucket `warehouse`, Parquet data files
- **Catalog**: Lakekeeper, an Iceberg REST catalog, at http://localhost:8181/catalog (warehouse `lakehouse`); UI at http://localhost:8181/ui
- **Exactly-once**: Kafka offsets are stored in each Iceberg snapshot, and the writer resumes from them after a restart, so replays never duplicate rows
- **Analytics (OLAP)** with Trino (web UI at http://localhost:8085):
  ```bash
  docker exec -it trino trino --catalog lakehouse --schema telecom     # interactive SQL
  docker exec trino trino --catalog lakehouse --schema telecom -f /etc/trino/queries.sql
  ```
  or with DuckDB, including time travel to an earlier snapshot:
  ```bash
  docker compose -f docker-compose.system.yml run --rm --no-deps lakehouse-writer python query.py
  ```
- **Upgrading a stack that still has `iceberg-rest`**: stop the sinks and maintenance, start `lakekeeper-init`, run `python migrate_catalog.py --source-uri http://iceberg-rest:8181` in a lakehouse container, then `docker compose up -d --remove-orphans`. See [docs/architecture.md](docs/architecture.md#lakekeeper-as-the-catalog)
- **Maintenance**: `lakehouse-maintenance` compacts small files, expires old snapshots and deletes unreferenced files every hour, for every sink's table, with the policy in its sink file. To check the tables or run a job now (`--sink cdr_events` limits it to one):
  ```bash
  docker compose -f docker-compose.system.yml run --rm --no-deps lakehouse-writer python maintenance.py stats
  docker compose -f docker-compose.system.yml run --rm --no-deps lakehouse-writer python maintenance.py compact
  ```

#### NOTE
Use the docker compose file `docker-compose.system.yml` as a reference for default ports and credentials.

---

## Running Tests

Each service has unit tests with Kafka, Iceberg and Postgres replaced by fakes, so they run without the stack. GitHub Actions runs them on every push, plus the alert store tests against a real Postgres.

The services pin packages for Python 3.11 and use the platform library in `libs/dataplatform`. From `fraud-detection-system/<service>` (`cdr-ingestion`, `lakehouse` or `ml`):

```bash
pip install -r requirements.txt -r ../requirements-test.txt ../libs/dataplatform
python -m pytest tests
```

Or without a local Python 3.11, from `fraud-detection-system/`:

```bash
docker run --rm -v "$PWD:/src" -w /src/lakehouse python:3.11-slim \
  sh -c "pip install -q -r requirements.txt -r ../requirements-test.txt ../libs/dataplatform && python -m pytest tests"
```

The lakehouse tests include sinks running against a real Iceberg table (SQLite catalog, local files). The library's own tests are in `libs/dataplatform/tests` (`pip install ./libs/dataplatform -r requirements-test.txt`); the ones against a real schema registry run only with `TEST_SCHEMA_REGISTRY_URL` set, e.g. `http://localhost:8081` with the stack up.

The alert store tests in `ml/tests/test_alert_store_postgres.py` run only with `RUN_POSTGRES_TESTS=1` and `POSTGRES_*` pointing at a disposable database, because they truncate `fraud_alerts`.

---

## Security Best Practices

### ⚠️ Critical Security Rules

1. **NEVER commit the `.env` file to Git**
   - It contains sensitive credentials
   - Always keep it in `.gitignore`

2. **Use different credentials for each environment**
   - Development vs Production should have different passwords

---

## Troubleshooting

### Issue: "docker-compose: command not found"

**Solution:**
- Ensure Docker Desktop is running
- Try `docker compose` (without hyphen) for newer Docker versions
- Check Docker is in your PATH

**Linux only:**
```bash
# Install docker-compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

---

### Issue: "Port already in use"

**Check what's using the port:**

**Windows (PowerShell):**
```powershell
# Check port 3000 (Grafana)
Get-NetTCPConnection -LocalPort 3000 -State Listen

# Stop the process
Stop-Process -Id <PID>
```

**Linux/Mac:**
```bash
# Check port 3000
lsof -i :3000

# Kill the process
kill -9 <PID>
```

**Alternative:** Change the port in `docker-compose.system.yml`:
```yaml
ports:
  - "3001:3000"  # Use 3001 instead of 3000
```

---

### Issue: "Cannot connect to Docker daemon"

**Windows:**
1. Open Docker Desktop
2. Wait for it to fully start (whale icon should be steady)
3. Try the command again

**Linux:**
```bash
# Start Docker service
sudo systemctl start docker

# Enable on boot
sudo systemctl enable docker

# Add your user to docker group (to avoid sudo)
sudo usermod -aG docker $USER
# Log out and back in for changes to take effect
```

---

### Issue: Services keep restarting

**View logs to diagnose:**
```bash
# View all logs
docker compose -f docker-compose.system.yml logs

# View specific service logs
docker compose -f docker-compose.system.yml logs grafana
docker compose -f docker-compose.system.yml logs prometheus

# Follow logs in real-time
docker compose -f docker-compose.system.yml logs -f
```

**Common causes:**
- Configuration file syntax errors
- Port conflicts
- Insufficient system resources

---

### Issue: Grafana shows "Bad Gateway"

**Solution:**
1. Wait 30 seconds for Grafana to fully start
2. Check if Prometheus is running:
   ```bash
   docker-compose -f docker-compose.system.yml ps prometheus
   ```
3. Restart Grafana:
   ```bash
   docker-compose -f docker-compose.system.yml restart grafana
   ```

---

## Port Reference

| Service | Port | URL | Purpose |
|---------|------|-----|---------|
| Grafana | 3000 | http://localhost:3000 | Visualization dashboards |
| Prometheus | 9090 | http://localhost:9090 | Metrics collection & queries |
| Node Exporter | 9100 | http://localhost:9100/metrics | System metrics |
| Alertmanager | 9093 | http://localhost:9093 | Alert management |
| Ingestion API | 8080 | http://localhost:8080/docs | Send CDRs |
| Schema Registry | 8081 | http://localhost:8081/subjects | Data contracts |
| Lakekeeper | 8181 | http://localhost:8181/ui | Iceberg REST catalog and its UI |
| Trino | 8085 | http://localhost:8085/ui | SQL over the lakehouse |
| SeaweedFS | 8333 | http://localhost:8333 | S3 API |
| Ingestion API | 8080 | http://localhost:8080/docs | Send CDRs |
| Schema Registry | 8081 | http://localhost:8081/subjects | Data contracts |
| Lakekeeper | 8181 | http://localhost:8181/ui | Iceberg REST catalog and its UI |
| Trino | 8085 | http://localhost:8085/ui | SQL over the lakehouse |
| SeaweedFS | 8333 | http://localhost:8333 | S3 API |

---

## Getting Help

If you encounter issues not covered in this guide:

1. **Check logs**
2. **Verify services**
3. **Check Docker Desktop** is running (Windows/Mac)
4. **Verify ports** are not in use by other applications
5. **Review configuration files** for syntax errors
6. **Consult Docker documentation**: https://docs.docker.com/
7. **Create an issue** in the project repository

---

## License

TBD

## Contributors

[Nabeel Hussain, Muhammad Abdur Rehman, Christoph Gasche, Marko Tiitto, Talha Pasha
]

---

**Last Updated**: November 2026
