import os
import sys
from pathlib import Path

# app.config reads these when it is imported; no real Postgres or Kafka is used
os.environ.setdefault("POSTGRES_HOST", "localhost")
os.environ.setdefault("POSTGRES_PORT", "5432")
os.environ.setdefault("POSTGRES_USER", "test")
os.environ.setdefault("POSTGRES_PASSWORD", "test")
os.environ.setdefault("POSTGRES_DB", "test")
os.environ.setdefault("SERVICE_NAME", "cdr-ingestion-test")
os.environ.setdefault("SERVICE_VERSION", "0.0.0")
os.environ.setdefault("DEBUG", "False")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
