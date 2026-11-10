# CDR Ingestion Service

## Overview

The CDR (Customer Detail Record) Ingestion Service is a FastAPI-based microservice for fraud detection that:

- **Receives** customer transaction records from payment/telecom sources
- **Validates** records with comprehensive field validation
- **Enriches** transaction data with fraud detection flags and risk analysis
- **Publishes** processed CDR data to Kafka for downstream processing (ML Training, Analytics)

## Architecture

```
Transaction Sources → CDR Ingestion Service → Kafka (cdr-events topic) → ML Training/Analytics
                       ├─ Validation
                       ├─ Fraud Detection
                       ├─ Enrichment
                       └─ Risk Scoring
```

## Features

- ✅ **RESTful API** for CDR ingestion via HTTP
- ✅ **Batch Processing** for efficient bulk ingestion
- ✅ **Data Validation** with IMEI, phone number, and SIM status checks
- ✅ **Fraud Detection** with automatic flagging based on risk scores and amounts
- ✅ **Kafka Integration** with an idempotent producer, keyed by `customer_id` so each customer's CDRs stay in order
- ✅ **Health Checks** for service monitoring
- ✅ **Comprehensive Logging** for debugging and monitoring
- ✅ **Docker Support** for containerized deployment

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+ (for local development)

With Docker Compose, all settings come from `../docker-compose.system.yml`. For local development, create a `.env` file in the `/cdr-ingestion` directory.

Example content:
```
# =========================
# Kafka Configuration
# =========================
KAFKA_BOOTSTRAP_SERVERS=localhost:9094
KAFKA_TOPIC=cdr-events

# =========================
# PostgreSQL Configuration
# =========================
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=myuser
POSTGRES_PASSWORD=mypassword
POSTGRES_DB=cdr_database

# =========================
# Service Configuration
# =========================
SERVICE_NAME=SERVICE
SERVICE_VERSION=1.0
DEBUG=False
```

### Running with Docker Compose

Run from the `fraud-detection-system` directory:

```powershell
# Start CDR Ingestion + Kafka (kafka-init creates the topics)
docker compose -f docker-compose.system.yml up -d cdr-ingestion

# Check service status
docker compose -f docker-compose.system.yml ps

# View logs
docker compose -f docker-compose.system.yml logs -f cdr-ingestion
docker compose -f docker-compose.system.yml logs -f kafka
```

### Running Locally (Development)

```powershell
# Install dependencies
pip install -r requirements.txt

# Run the service
fastapi run app/main.py --port 8080

# Or with uvicorn
uvicorn app.main:app --reload --port 8080
```

## API Endpoints

### Health & Status

- **GET** `/` - Root endpoint
- **GET** `/health` - Health check status
- **GET** `/stats` - Service statistics

### CDR Ingestion

- **POST** `/ingest` - Submit a single CDR record
  ```json
  {
    "customer_id": "CUST-001234",
    "full_name": "John Doe",
    "risk_score": 25.5,
    "city": "Helsinki",
    "mobile_number": "+358123456789",
    "sim_serial_number": "SIM1234567890ABC",
    "sim_status": "active",
    "imei": "356938035643809",
    "fraud_flag": false,
    "fraud_type": null,
    "transaction_id": "TXN-20260216-001234",
    "time_stamp": "2026-02-16T10:30:00Z",
    "type": "payment",
    "amount_eur": 150.00
  }
  ```

- **POST** `/ingest/batch` - Submit multiple CDR records
  ```json
  [
    { CDR record 1 },
    { CDR record 2 }
  ]
  ```

### CDR Retrieval

- **GET** `/cdr/{transaction_id}` - Retrieve a CDR record by transaction ID
- **GET** `/cdr_recent/{limit}` - Get recent CDR records

### Interactive API Documentation

- **Swagger UI**: http://localhost:8080/docs
- **ReDoc**: http://localhost:8080/redoc

## CDR Record Schema

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `customer_id` | string | Unique customer identifier (5-50 chars) |
| `full_name` | string | Customer full name |
| `risk_score` | float | Risk score 0-100 |
| `city` | string | Customer city |
| `mobile_number` | string | Mobile number (E.164 format) |
| `sim_serial_number` | string | SIM card serial number (unique) |
| `sim_status` | string | active, inactive, suspended, blocked |
| `imei` | string | Device IMEI (15 digits, unique) |
| `transaction_id` | string | Unique transaction identifier (5-100 chars, unique) |
| `time_stamp` | datetime | Transaction timestamp |
| `type` | string | Transaction type (payment, withdrawal, etc.) |
| `amount_eur` | float | Transaction amount in EUR (≥0) |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| `fraud_flag` | boolean | Whether transaction is flagged as fraudulent (default: false) |
| `fraud_type` | string | Type of fraud if detected (high_risk_profile, suspicious_amount) |

## Enriched CDR Output

After processing, CDRs are enriched with:

```json
{
  ...original fields...,
  "processed_timestamp": "2026-02-16T10:35:00Z",
  "fraud_flag": true,
  "fraud_type": "high_risk_profile"
}
```

### Fraud Detection Rules

- **High-Risk Profile**: Automatically flagged if `risk_score > 70`
- **Suspicious Amount**: Automatically flagged if `amount_eur > 5000`

## Development

### Project Structure

```
cdr-ingestion/
├── app/
│   ├── __init__.py
│   ├── main.py                 # FastAPI application
│   ├── config.py              # Configuration management
│   ├── schemas.py             # Pydantic models
│   ├── processors/
│   │   ├── cdr_validator.py   # Validation logic
│   │   └── cdr_transformer.py # Transformation & enrichment
│   └── services/
│       └── kafka_publisher.py # Kafka integration
├── requirements.txt
├── Dockerfile
├── .env
└── README.md
```

### Adding New Features

1. **New Validators** - Add to `app/processors/cdr_validator.py`
2. **New Transformations** - Add to `app/processors/cdr_transformer.py`
3. **New Endpoints** - Add to `app/main.py`
4. **New Models** - Define in `app/schemas.py`

### Testing

```powershell
# Test via Swagger UI
# http://localhost:8080/docs

# Or using curl
curl -X POST "http://localhost:8080/ingest" `
  -H "Content-Type: application/json" `
  -d '{
    "call_id": "TEST-001",
    "caller_number": "+358123456789",
    "called_number": "+358987654321",
    "call_duration": 300,
    "timestamp": "2026-02-16T10:30:00Z",
    "origin_cell": "Cell001",
    "destination_cell": "Cell002",
    "call_type": "voice",
    "status": "completed"
  }'

# Batch test
curl -X POST "http://localhost:8080/ingest/batch" `
  -H "Content-Type: application/json" `
  -d '[
    { CDR record 1 },
    { CDR record 2 }
  ]'
```

## Monitoring & Logging

### Docker Logs

```powershell
# Live logs
docker compose logs -f cdr-ingestion

# Last 100 lines
docker compose logs --tail 100 cdr-ingestion

# With timestamps
docker compose logs --timestamps cdr-ingestion
```

### Kafka Topic

Read the latest CDRs on the topic, with key and partition:

```powershell
docker exec kafka_1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic cdr-events --from-beginning --max-messages 5 --property print.key=true --property print.partition=true
```

## Performance Considerations

### Single Record Ingestion
- ~5-10ms processing time per CDR
- ~200 CDRs/second throughput with single instance

### Batch Ingestion
- Recommended batch size: 100-1000 records
- Significant performance improvement over single records
- Ideal for bulk data ingestion

### Scaling

For production:

1. **Horizontal Scaling**: Run multiple instances with load balancer
2. **Database**: Add PostgreSQL for persistence and analytics
3. **Caching**: Add Redis for CDR caching and deduplication
4. **Message Broker**: Add Kafka brokers and partitions, with replication factor 3

## Troubleshooting

### Kafka Connection Issues

```
Error: Failed to connect to Kafka

Solutions:
1. Verify the kafka container is healthy: docker compose -f docker-compose.system.yml ps
2. Verify kafka-init exited with code 0 (it creates the cdr-events topic)
3. Check KAFKA_BOOTSTRAP_SERVERS (kafka:9092 in Docker, localhost:9094 from the host)
4. Check Kafka logs: docker compose -f docker-compose.system.yml logs kafka
```

### Service Not Starting

```powershell
# Check container logs
docker compose logs cdr-ingestion

# Verify dependencies
docker compose logs --tail 50 cdr-ingestion

# Rebuild container
docker compose build --no-cache cdr-ingestion
docker compose up cdr-ingestion
```

### High Memory Usage

- Reduce batch size
- Enable debug logging selectively
- Monitor Kafka consumer lag

## Integration with Other Services

### Stream Processing
- Consumes from the cdr-events topic
- Applies streaming transformations
- Publishes to an analytics topic

### ML Training
- Consumes from the cdr-events topic (consumer group `fraud-detection`)
- Extracts features for model training
- Updates fraud detection models

### Monitoring (Prometheus/Grafana)
- Export metrics from CDR service
- Monitor throughput, latency, errors
- Create dashboards for real-time visibility

## Next Steps

1. Deploy with streaming processing service
2. Add Prometheus metrics export
3. Implement CDR persistence (PostgreSQL)
4. Add authentication/authorization
5. Create comprehensive integration tests
6. Performance load testing

## Support & Documentation

For more information:
- Review `app/main.py` for API implementation
- Check `app/processors/` for validation and transformation logic
- See `/docs` endpoint for interactive API documentation

---

**Last Updated**: February 16, 2026  
**Version**: 1.0.0
