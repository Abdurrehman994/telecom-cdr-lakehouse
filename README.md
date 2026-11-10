# Telecom Fraud Detection & Prevention System - Setup Guide

> This repository continues a University of Oulu group project
> ([original repo](https://github.com/FastByte17/DS_Project_2026)) built by
> Nabeel Hussain, Muhammad Abdur Rehman, Christoph Gasche, Marko Tiitto and Talha Pasha.
> Work from November 2026 onward is by Muhammad Abdur Rehman.

CDRs stream through Kafka into real-time fraud detection and an Apache Iceberg lakehouse. For the design, trade-offs and production roadmap, see [docs/architecture.md](docs/architecture.md).

## Table of Contents
- [Prerequisites](#prerequisites)
- [Initial Setup](#initial-setup)
- [Starting the Stack](#starting-the-stack)
- [Accessing Services](#accessing-services)
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

#### Windows (PowerShell or Command Prompt):
```powershell
git clone https://github.com/FastByte17/DS_Project_2026
cd fraud-detection-system
```

#### Linux/Mac (Terminal):
```bash
git clone https://github.com/FastByte17/DS_Project_2026
cd fraud-detection-system
```

---

### 2. Set Up Environment Variables

Create .env file in ´/fraud-detection-system/´ with the example content. 

Feel free to change the credentials.

Example content:

```
# Grafana Credentials
GRAFANA_ADMIN_USER=FOOBAR
GRAFANA_ADMIN_PASSWORD=FOOBAR123

# Alertmanager Email Configuration
ALERTMANAGER_EMAIL=your-email@example.com
ALERTMANAGER_EMAIL_PASSWORD=your-app-password

```


The CDR ingestion and ML services need no `.env` file: their Kafka, service and database settings are set in `docker-compose.system.yml`.

---

## Starting the Stack

### 4. Validate Configuration

Before starting, validate your docker-compose file:

```bash
docker compose -f docker-compose.system.yml config
```

If there are no errors, you'll see the parsed configuration.

### 5. Start All Services

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

### 6. Verify Services Are Running

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

### 7. Add Llama3.2 model to OLlama container

```bash
docker exec -it fraud-detection-ollama ollama pull llama3.2
```

### (Optional) 8. Run a example payload to the service

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
3. You'll see the Grafana home page

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

### Kafka
- **Broker from the host**: `localhost:9094` (containers use `kafka:9092`)
- **Topics**: `cdr-events` (3 partitions, keyed by `customer_id`), `cdr-events-dlq` (messages that failed processing)
- **Consumer group**: `fraud-detection` (`ml-service` and `ml-service2` share the partitions)
- **Check consumer lag**:
  ```bash
  docker exec kafka_1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group fraud-detection
  ```

### Lakehouse (Apache Iceberg)
- **Flow**: `lakehouse-writer` reads `cdr-events` as its own consumer group (`lakehouse-writer`) and appends CDRs in batches to the Iceberg table `telecom.cdr_events`, partitioned by day
- **Storage**: SeaweedFS (S3 API at http://localhost:8333), bucket `warehouse`, Parquet data files
- **Catalog**: Iceberg REST catalog at http://localhost:8181
- **Exactly-once**: Kafka offsets are stored in each Iceberg snapshot, and the writer resumes from them after a restart, so replays never duplicate rows
- **Analytics (OLAP)**: run DuckDB queries, including time travel to an earlier snapshot:
  ```bash
  docker compose -f docker-compose.system.yml run --rm --no-deps lakehouse-writer python query.py
  ```

#### NOTE
Use the docker compose file `docker-compose.system.yml` as a reference for default ports and credentials.

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

**Last Updated**: March 2026