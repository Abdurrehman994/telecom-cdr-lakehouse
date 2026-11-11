"""Move tables from one Iceberg REST catalog to another, without copying data.

An Iceberg table is fully described by its metadata file in object storage: schemas,
partitioning, snapshots, and the sinks' Kafka offsets in the snapshot properties. A
catalog only records which metadata file is current. Registering that file in the new
catalog therefore moves the table, history and exactly-once position included.

Stop the sinks and maintenance first, so no commit lands in the old catalog after its
table has been registered in the new one.

Usage (from fraud-detection-system/, with the old catalog still running):
    docker compose -f docker-compose.system.yml run --rm --no-deps lakehouse-writer \\
        python migrate_catalog.py --source-uri http://iceberg-rest:8181
"""
import argparse
import sys

from pyiceberg.catalog import load_catalog

from catalog import S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY, load_iceberg_catalog
from sink_config import load_sinks


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-uri", required=True, help="REST catalog to move the tables from")
    parser.add_argument("--source-warehouse", help="warehouse in the source catalog, if it has several")
    parser.add_argument("--sinks-dir", default="sinks", help="the tables to move are those of these sinks")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    properties = {
        "type": "rest",
        "uri": args.source_uri,
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": S3_ACCESS_KEY_ID,
        "s3.secret-access-key": S3_SECRET_ACCESS_KEY,
        "s3.region": S3_REGION,
        "s3.path-style-access": "true",
    }
    if args.source_warehouse:
        properties["warehouse"] = args.source_warehouse
    source = load_catalog("source", **properties)
    destination = load_iceberg_catalog()

    failed = False
    for sink in load_sinks(args.sinks_dir):
        if not source.table_exists(sink.table):
            print(f"{sink.table}: not in the source catalog, nothing to move")
            continue
        table = source.load_table(sink.table)
        snapshot = table.current_snapshot()
        if destination.table_exists(sink.table):
            current = destination.load_table(sink.table).metadata_location
            state = "already registered" if current == table.metadata_location else "EXISTS with other metadata"
            failed |= current != table.metadata_location
            print(f"{sink.table}: {state} in the destination")
            continue
        print(f"{sink.table}: registering {table.metadata_location} "
              f"(snapshot {snapshot.snapshot_id if snapshot else None})")
        if args.dry_run:
            continue
        destination.create_namespace_if_not_exists(sink.namespace)
        moved = destination.register_table(sink.table, table.metadata_location)
        moved_snapshot = moved.current_snapshot()
        if (moved_snapshot and moved_snapshot.snapshot_id) != (snapshot and snapshot.snapshot_id):
            print(f"{sink.table}: current snapshot differs after registering", file=sys.stderr)
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
