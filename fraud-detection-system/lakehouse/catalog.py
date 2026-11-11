"""Connections to the lakehouse: the Iceberg REST catalog and the S3 warehouse."""
import json
import logging
import os
import time
from typing import Dict

from pyarrow import fs
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

logger = logging.getLogger("lakehouse")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://seaweedfs:8333")
S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID", "lakehouse")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY", "lakehouse-secret")
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET", "warehouse")


def s3_filesystem(allow_bucket_creation: bool = False) -> fs.S3FileSystem:
    return fs.S3FileSystem(
        endpoint_override=S3_ENDPOINT,
        access_key=S3_ACCESS_KEY_ID,
        secret_key=S3_SECRET_ACCESS_KEY,
        region=S3_REGION,
        allow_bucket_creation=allow_bucket_creation,
    )


def ensure_bucket():
    """create the warehouse bucket if it does not exist yet"""
    s3 = s3_filesystem(allow_bucket_creation=True)
    if s3.get_file_info(S3_BUCKET).type == fs.FileType.NotFound:
        s3.create_dir(S3_BUCKET)
        logger.info(f"Created bucket {S3_BUCKET}")


def load_iceberg_catalog():
    """connect to the Iceberg REST catalog, retrying until it is up"""
    for attempt in range(30):
        try:
            catalog = load_catalog(
                "lakehouse",
                **{
                    "type": "rest",
                    "uri": os.getenv("ICEBERG_CATALOG_URI", "http://iceberg-rest:8181"),
                    "s3.endpoint": S3_ENDPOINT,
                    "s3.access-key-id": S3_ACCESS_KEY_ID,
                    "s3.secret-access-key": S3_SECRET_ACCESS_KEY,
                    "s3.region": S3_REGION,
                    "s3.path-style-access": "true",
                },
            )
            catalog.list_namespaces()
            return catalog
        except Exception as e:
            logger.warning(f"Iceberg catalog not ready ({e}), retrying in 2s")
            time.sleep(2)
    raise RuntimeError("Could not connect to the Iceberg catalog")


def stored_offsets(table: Table, offsets_property: str) -> Dict[str, int]:
    """the Kafka offsets of the newest snapshot that recorded them.

    Every writer commit records them, and compaction copies them forward. Walking
    back through the history also covers a snapshot made by another tool that did not.
    """
    snapshot = table.current_snapshot()
    while snapshot is not None:
        raw = snapshot.summary.additional_properties.get(offsets_property) if snapshot.summary else None
        if raw:
            return json.loads(raw)
        parent = snapshot.parent_snapshot_id
        snapshot = table.snapshot_by_id(parent) if parent is not None else None
    return {}
