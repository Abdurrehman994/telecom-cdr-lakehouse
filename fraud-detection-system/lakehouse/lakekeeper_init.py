"""One-shot setup of the Lakekeeper catalog: bootstrap it and create the lakehouse warehouse.

Safe to run again: a bootstrapped server and an existing warehouse are left as they are.

The warehouse stores tables in the S3 bucket and lets Lakekeeper vend credentials: it
holds the storage keys, and engines (the sinks, Trino) get short-lived credentials from
SeaweedFS's STS for each table they load, instead of keys of their own.
"""
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request

from catalog import (
    ICEBERG_WAREHOUSE,
    S3_ACCESS_KEY_ID,
    S3_BUCKET,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_ACCESS_KEY,
    ensure_bucket,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("lakekeeper-init")

LAKEKEEPER_URL = os.getenv("LAKEKEEPER_URL", "http://lakekeeper:8181")
STS_ROLE_ARN = os.getenv("S3_STS_ROLE_ARN", "arn:aws:iam::000000000000:role/LakehouseVendedRole")


def request(method: str, path: str, body=None):
    """(status, parsed JSON or None) of a management API call"""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{LAKEKEEPER_URL}{path}", data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, raw.decode(errors="replace")


def warehouse_body() -> dict:
    return {
        "warehouse-name": ICEBERG_WAREHOUSE,
        "storage-profile": {
            "type": "s3",
            "flavor": "s3-compat",
            "bucket": S3_BUCKET,
            "region": S3_REGION,
            "endpoint": S3_ENDPOINT,
            "path-style-access": True,
            # vend short-lived credentials from SeaweedFS's STS; remote signing is
            # disabled because S3-compatible stores do not verify every signed field
            "sts-enabled": True,
            "sts-endpoint": S3_ENDPOINT,
            "sts-role-arn": STS_ROLE_ARN,
            "remote-signing-enabled": False,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": S3_ACCESS_KEY_ID,
            "aws-secret-access-key": S3_SECRET_ACCESS_KEY,
        },
    }


def main() -> int:
    for _ in range(60):
        try:
            status, info = request("GET", "/management/v1/info")
            if status == 200:
                break
        except urllib.error.URLError as e:
            logger.info(f"Lakekeeper not ready ({e.reason}), retrying in 2s")
        time.sleep(2)
    else:
        logger.error("Lakekeeper did not come up")
        return 1

    if not info.get("bootstrapped"):
        status, body = request("POST", "/management/v1/bootstrap", {"accept-terms-of-use": True})
        if status >= 300:
            logger.error(f"Bootstrap failed ({status}): {body}")
            return 1
        logger.info("Bootstrapped Lakekeeper")

    status, body = request("GET", "/management/v1/warehouse")
    if status != 200:
        logger.error(f"Could not list warehouses ({status}): {body}")
        return 1
    if any(w["name"] == ICEBERG_WAREHOUSE for w in body.get("warehouses", [])):
        logger.info(f"Warehouse {ICEBERG_WAREHOUSE} exists")
        return 0

    # Lakekeeper checks that it can write to the storage before creating the warehouse
    ensure_bucket()
    status, body = request("POST", "/management/v1/warehouse", warehouse_body())
    if status >= 300:
        logger.error(f"Creating warehouse {ICEBERG_WAREHOUSE} failed ({status}): {body}")
        return 1
    logger.info(f"Created warehouse {ICEBERG_WAREHOUSE} in s3://{S3_BUCKET} ({body.get('warehouse-id')})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
