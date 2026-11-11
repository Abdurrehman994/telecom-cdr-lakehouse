"""FastAPI application for CDR Ingestion Service"""
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, HTTPException, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from app.config import settings
from app.schemas import CDRRecord, CDRIngestionResponse, HealthResponse
from app.services.kafka_publisher import KafkaPublisher
from app.services.database import DatabaseService
from app.processors.cdr_validator import validate_cdr
from app.processors.cdr_transformer import enrich_cdr


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Prometheus metrics
cdrs_ingested = Counter("cdr_ingest_total", "CDRs received over HTTP, by result", ["result"])
publish_seconds = Histogram(
    "cdr_ingest_publish_seconds", "Wait for Kafka to confirm one request's CDRs",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20),
)


# Global instances
publisher: KafkaPublisher = None
db_service: DatabaseService = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown"""
    # Startup
    global publisher, db_service
    logger.info(f"Starting {settings.service_name} v{settings.service_version}")
    
    # Initialize database
    db_service = DatabaseService()
    if not db_service.init_db():
        logger.warning("Failed to connect to database on startup")
    else:
        logger.info("Database initialized successfully")
    
    # Initialize Kafka publisher
    publisher = KafkaPublisher(topic=settings.kafka_topic)
    if not publisher.init_connection():
        logger.warning("Failed to connect to Kafka on startup")

    app.state.publisher = publisher
    app.state.db_service = db_service
    
    logger.info(f"{settings.service_name} started successfully")
    
    yield
    
    # Shutdown
    logger.info(f"Shutting down {settings.service_name}")
    if publisher:
        publisher.close_connection()
    if db_service:
        db_service.close()
    logger.info(f"{settings.service_name} shut down complete")


# Create FastAPI application
app = FastAPI(
    title=settings.service_name,
    description="CDR (Call Detail Record) Ingestion Service for Telecom Fraud Detection",
    version=settings.service_version,
    lifespan=lifespan
)


def get_publisher() -> KafkaPublisher:
    """Get the Kafka publisher instance"""
    return app.state.publisher


def get_db_service() -> DatabaseService:
    """Get the database service instance"""
    return app.state.db_service


@app.get("/", tags=["Health"])
async def root():
    """Root endpoint"""
    return {
        "message": "CDR Ingestion Service is running",
        "service": settings.service_name,
        "version": settings.service_version
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint"""
    publisher_instance = get_publisher()
    kafka_healthy = publisher_instance.health_check() if publisher_instance else False

    return HealthResponse(
        service=settings.service_name,
        status="healthy" if kafka_healthy else "degraded",
        version=settings.service_version
    )


@app.post(
    "/ingest",
    response_model=CDRIngestionResponse,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["CDR Ingestion"]
)
async def ingest_cdr(record: CDRRecord):
    """Ingest a CDR record - validates, enriches and publishes to Kafka"""
    try:
        # Validate CDR record
        cdr_dict = record.model_dump(exclude_none=True)
        is_valid, validation_msg = validate_cdr(cdr_dict)
        if not is_valid:
            cdrs_ingested.labels("invalid").inc()
            logger.warning(f"CDR validation failed for {record.transaction_id}: {validation_msg}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Validation error: {validation_msg}"
            )
        
        logger.info(f"Validating CDR {record.transaction_id}")
        
        try:
            # Enrich CDR record with additional data
            enriched_cdr = enrich_cdr(record)
            logger.info(f"Enriched CDR {record.transaction_id} with additional data")
        except Exception as e:
            logger.error(f"Error enriching CDR {record.transaction_id}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error enriching CDR: {str(e)}"
            ) from e
        
        # Save to database
        # try:
        #     db_instance = get_db_service()
        #     if db_instance:
        #         db_result = db_instance.save_cdr(enriched_cdr)
        #         if db_result:
        #             logger.info(f"CDR {record.transaction_id} saved to database")
        #         else:
        #             logger.warning(f"Failed to save CDR {record.transaction_id} to database")
        # except Exception as e:
        #     logger.error(f"Database error saving CDR {record.transaction_id}: {str(e)}")
        #     raise HTTPException(
        #         status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        #         detail=f"Database error: {str(e)}"
        #     )
        
        # Publish to Kafka and only answer once the broker has stored the CDR,
        # so a 202 never hides a lost record. The wait blocks, so it runs off the event loop.
        publisher_instance = get_publisher()
        started = time.monotonic()
        error = await run_in_threadpool(publisher_instance.publish, enriched_cdr)
        publish_seconds.observe(time.monotonic() - started)
        if error is not None:
            cdrs_ingested.labels("publish_failed").inc()
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Could not publish CDR to Kafka, retry later: {error}"
            )

        cdrs_ingested.labels("accepted").inc()
        logger.info(f"Successfully ingested CDR {record.transaction_id}")

        return CDRIngestionResponse(
            status="accepted",
            transaction_id=record.transaction_id,
            message=f"CDR record {record.transaction_id} accepted and stored in Kafka"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error processing CDR: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}"
        ) from e


@app.post(
    "/ingest/batch",
    response_model=dict,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["CDR Ingestion"]
)
async def ingest_batch_cdrs(records: list[CDRRecord]):
    """Ingest multiple CDR records in batch"""
    logger.info(f"Received batch of {len(records)} CDR records")
    
    results = {"total": len(records), "accepted": 0, "rejected": 0, "errors": []}
    
    # db_instance = get_db_service()
    publisher_instance = get_publisher()
    to_publish = []

    for idx, record in enumerate(records):
        try:
            cdr_dict = record.model_dump(exclude_none=True)
            is_valid, validation_msg = validate_cdr(cdr_dict)
            
            if not is_valid:
                cdrs_ingested.labels("invalid").inc()
                results["rejected"] += 1
                results["errors"].append({
                    "index": idx,
                    "transaction_id": record.transaction_id,
                    "error": validation_msg
                })
                continue
            
            # Enrich and save
            enriched_cdr = enrich_cdr(record)
            
            # db_saved = False
            # if db_instance:
            #     db_saved = db_instance.save_cdr(enriched_cdr)

            to_publish.append((idx, record.transaction_id, enriched_cdr))

        except Exception as e:
            cdrs_ingested.labels("invalid").inc()
            results["rejected"] += 1
            results["errors"].append({
                "index": idx,
                "transaction_id": record.transaction_id if hasattr(record, 'transaction_id') else "unknown",
                "error": str(e)
            })

    # Send the whole batch, then wait once for all broker confirmations
    started = time.monotonic()
    errors = await run_in_threadpool(
        publisher_instance.publish_batch, [cdr for _, _, cdr in to_publish]
    )
    publish_seconds.observe(time.monotonic() - started)
    for (idx, transaction_id, _), error in zip(to_publish, errors, strict=False):
        if error is None:
            cdrs_ingested.labels("accepted").inc()
            results["accepted"] += 1
        else:
            cdrs_ingested.labels("publish_failed").inc()
            results["rejected"] += 1
            results["errors"].append({
                "index": idx,
                "transaction_id": transaction_id,
                "error": f"Could not publish CDR to Kafka: {error}"
            })

    logger.info(f"Batch processing complete: {results['accepted']} accepted, {results['rejected']} rejected")
    
    return results


@app.get("/metrics", tags=["Monitoring"])
async def metrics():
    """Prometheus metrics"""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/stats", tags=["Monitoring"])
async def get_stats():
    """Get service statistics"""
    db_instance = get_db_service()
    cdr_count = db_instance.get_cdr_count() if db_instance else 0
    
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "kafka_topic": settings.kafka_topic,
        "kafka_bootstrap_servers": settings.kafka_bootstrap_servers,
        "database_host": settings.postgres_host,
        "total_cdr_records": cdr_count,
        "debug_mode": settings.debug
    }


@app.get("/cdr/{transaction_id}", tags=["CDR Retrieval"])
async def get_cdr(transaction_id: str):
    """Retrieve a CDR record by transaction ID"""
    db_instance = get_db_service()
    if not db_instance:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database service unavailable"
        )
    
    cdr = db_instance.get_cdr_by_id(transaction_id)
    if not cdr:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"CDR record {transaction_id} not found"
        )
    
    return cdr


@app.get("/cdr_recent/{limit}", tags=["CDR Retrieval"])
async def get_recent_cdrs(limit: int = 10):
    """Retrieve recent CDR records"""
    if limit > 100:
        limit = 100
    if limit < 1:
        limit = 1
    
    db_instance = get_db_service()
    if not db_instance:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database service unavailable"
        )
    
    return {
        "limit": limit,
        "records": db_instance.get_recent_cdrs(limit)
    }



@app.get("/fraud/dataset", tags=["Fraud Dataset"])
async def get_fraud_dataset():
    """Get telecom fraud dataset from JSON file"""
    try:
        dataset_path = Path(__file__).parent / "telecom_fraud_dataset.json"
        if not dataset_path.exists():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Dataset file not found"
            )
        
        with open(dataset_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info("Fraud dataset retrieved successfully")
        return data
        
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON format in dataset file")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Invalid JSON format in dataset file"
        ) from e
    except Exception as e:
        logger.error(f"Error reading dataset file: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error reading dataset: {str(e)}"
        ) from e


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Custom HTTP exception handler"""
    logger.error(f"HTTP Exception: {exc.status_code} - {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8080,
        log_level="info"
    )
