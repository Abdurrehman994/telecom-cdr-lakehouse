"""Iceberg table maintenance: compaction, snapshot expiry and orphan file cleanup.

Streaming appends leave many small files, and every commit adds a snapshot that keeps
old files alive. Three jobs undo that:

compact   Rewrite each day partition that has many small files into one sorted file.
          The rewrite is a single atomic commit that only succeeds if no other commit
          (e.g. the writer's) landed since the files were read, and it carries the
          writer's Kafka offsets forward so exactly-once is unaffected.
expire    Drop old snapshots from the table metadata, always keeping the newest ones.
          Time travel to an expired snapshot is no longer possible.
orphans   Delete files in the table's storage that no snapshot references any more,
          such as the small files replaced by compaction once their snapshots expire.
          Only files older than a minimum age are touched, so files of a commit that
          is still in progress are never deleted.

Usage (from fraud-detection-system/):
    docker compose -f docker-compose.system.yml run --rm --no-deps lakehouse-writer \\
        python maintenance.py all
    ... python maintenance.py stats
    ... python maintenance.py orphans --dry-run
The lakehouse-maintenance service runs `python maintenance.py loop` on a schedule.
"""
import argparse
import json
import logging
import os
import time
from collections import Counter as Tally
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Set, Tuple

from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server
from pyarrow import fs
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.table import Table

from writer import (
    ICEBERG_TABLE,
    OFFSETS_PROPERTY,
    S3_ACCESS_KEY_ID,
    S3_ENDPOINT,
    S3_REGION,
    S3_SECRET_ACCESS_KEY,
    load_iceberg_catalog,
    stored_offsets,
)

logger = logging.getLogger("lakehouse-maintenance")

COMPACT_MIN_FILES = int(os.getenv("COMPACT_MIN_FILES", "5"))
COMPACT_PARTITIONS_PER_COMMIT = int(os.getenv("COMPACT_PARTITIONS_PER_COMMIT", "50"))
SNAPSHOT_RETAIN_LAST = int(os.getenv("SNAPSHOT_RETAIN_LAST", "10"))
SNAPSHOT_MAX_AGE_HOURS = float(os.getenv("SNAPSHOT_MAX_AGE_HOURS", "24"))
ORPHAN_MIN_AGE_MINUTES = float(os.getenv("ORPHAN_MIN_AGE_MINUTES", "1440"))
# a commit takes seconds; anything younger than this may belong to one in progress
ORPHAN_MIN_AGE_FLOOR_MINUTES = 15
MAINTENANCE_INTERVAL_SECONDS = int(os.getenv("MAINTENANCE_INTERVAL_SECONDS", "3600"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8003"))
COMMIT_ATTEMPTS = 5

# own registry, so the writer's metrics (imported with it) are not exported from here
registry = CollectorRegistry()
table_files = Gauge("lakehouse_table_data_files", "Data files in the current snapshot", registry=registry)
table_rows = Gauge("lakehouse_table_rows", "Rows in the current snapshot", registry=registry)
table_bytes = Gauge("lakehouse_table_bytes", "Size of the data files in the current snapshot", registry=registry)
table_snapshots = Gauge("lakehouse_table_snapshots", "Snapshots in the table metadata", registry=registry)
last_success = Gauge("lakehouse_maintenance_last_success_timestamp_seconds",
                     "Unix time a maintenance task last succeeded", ["task"], registry=registry)
failures = Counter("lakehouse_maintenance_failures_total", "Failed maintenance runs", ["task"], registry=registry)
files_removed = Counter("lakehouse_compaction_files_removed_total", "Small files replaced by compaction",
                        registry=registry)
files_written = Counter("lakehouse_compaction_files_written_total", "Files written by compaction", registry=registry)
snapshots_expired = Counter("lakehouse_snapshots_expired_total", "Snapshots expired", registry=registry)
orphans_deleted = Counter("lakehouse_orphan_files_deleted_total", "Unreferenced files deleted", registry=registry)
orphan_bytes = Counter("lakehouse_orphan_bytes_deleted_total", "Bytes of unreferenced files deleted",
                       registry=registry)

EPOCH = date(1970, 1, 1)


# --- pure helpers (unit tested) ---

def day_number(value) -> int:
    """a day partition value as days since 1970-01-01 (PyIceberg may return int or date)"""
    return (value - EPOCH).days if isinstance(value, date) else int(value)


def partitions_to_compact(files: Iterable[Dict], min_files: int) -> List[int]:
    """day partitions (days since epoch) holding at least min_files data files"""
    counts = Tally(
        day_number(f["partition"]["event_day"])
        for f in files
        if f["content"] == 0 and f["partition"].get("event_day") is not None
    )
    return sorted(day for day, n in counts.items() if n >= min_files)


def chunks(items: List, size: int) -> List[List]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def snapshots_to_expire(snapshots: Iterable[Tuple[int, int]], current_id: int,
                        retain_last: int, older_than_ms: int) -> List[int]:
    """ids of snapshots to expire, given (snapshot_id, timestamp_ms) pairs.

    The newest retain_last snapshots and the current one are always kept.
    """
    ordered = sorted(snapshots, key=lambda s: s[1], reverse=True)
    keep = {snapshot_id for snapshot_id, _ in ordered[:retain_last]} | {current_id}
    return [snapshot_id for snapshot_id, ts in ordered if snapshot_id not in keep and ts < older_than_ms]


def strip_scheme(path: str) -> str:
    """s3://bucket/key -> bucket/key, the form pyarrow's S3 filesystem lists"""
    return path.split("://", 1)[1] if "://" in path else path


def orphan_files(listing: Iterable[Tuple[str, datetime, int]], referenced: Set[str],
                 cutoff: datetime) -> List[Tuple[str, int]]:
    """(path, size) of listed files that nothing references and that are older than cutoff"""
    orphans = []
    for path, modified, size in listing:
        modified = modified if modified.tzinfo else modified.replace(tzinfo=timezone.utc)
        if path not in referenced and modified < cutoff:
            orphans.append((path, size))
    return orphans


# --- jobs ---

def s3_filesystem() -> fs.S3FileSystem:
    return fs.S3FileSystem(
        endpoint_override=S3_ENDPOINT,
        access_key=S3_ACCESS_KEY_ID,
        secret_key=S3_SECRET_ACCESS_KEY,
        region=S3_REGION,
    )


def table_stats(table: Table) -> Dict:
    files = table.inspect.files().to_pylist() if table.current_snapshot() else []
    data_files = [f for f in files if f["content"] == 0]
    stats = {
        "snapshots": len(table.snapshots()),
        "data_files": len(data_files),
        "rows": sum(f["record_count"] for f in data_files),
        "bytes": sum(f["file_size_in_bytes"] for f in data_files),
    }
    table_snapshots.set(stats["snapshots"])
    table_files.set(stats["data_files"])
    table_rows.set(stats["rows"])
    table_bytes.set(stats["bytes"])
    return stats


def compaction_ranges(days: List[int], partitions_per_commit: int) -> List[Tuple[int, int]]:
    """contiguous (first_day, last_day) ranges, each covering up to partitions_per_commit of the days.

    One commit rewrites one range. A range filter is cheap to plan, while a filter of many
    OR'd days is not (20 days over 5,000 files took 31 s). Days inside a range that did not
    need compaction are rewritten too, so nothing in the range is deleted without being rewritten.
    """
    return [(group[0], group[-1]) for group in chunks(days, partitions_per_commit)]


def range_filter(first_day: int, last_day: int):
    """rows whose time_stamp falls between the two UTC days, inclusive"""
    start = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(days=first_day)
    end = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(days=last_day + 1)
    return And(GreaterThanOrEqual("time_stamp", start.isoformat()), LessThan("time_stamp", end.isoformat()))


def compact_range(catalog, first_day: int, last_day: int) -> Tuple[int, int]:
    """rewrite the day partitions first_day..last_day in one commit; returns (files removed, files written)"""
    # Load once and do not refresh before committing: the commit is checked against the
    # snapshot that was read, so a concurrent append makes it fail instead of being lost.
    table = catalog.load_table(ICEBERG_TABLE)
    files = [f for f in table.inspect.files().to_pylist()
             if f["content"] == 0 and f["partition"].get("event_day") is not None
             and first_day <= day_number(f["partition"]["event_day"]) <= last_day]
    expected_rows = sum(f["record_count"] for f in files)

    row_filter = range_filter(first_day, last_day)
    data = table.scan(row_filter=row_filter).to_arrow()
    if data.num_rows != expected_rows:
        raise RuntimeError(f"read {data.num_rows} rows but the files hold {expected_rows}; not compacting")

    offsets = stored_offsets(table)
    properties = {"lakehouse.maintenance": "compaction"}
    if offsets:
        properties[OFFSETS_PROPERTY] = json.dumps(offsets)
    # Delete the range's files and add the rewritten rows in one commit. The filter
    # covers whole days, so whole files are dropped. (dynamic_partition_overwrite would
    # be simpler, but PyIceberg only supports it for identity partitions.)
    # Sorted files let date-filtered queries skip more data.
    table.overwrite(data.sort_by("time_stamp"), overwrite_filter=row_filter, snapshot_properties=properties)

    summary = table.current_snapshot().summary.additional_properties
    written = int(summary.get("added-data-files", 0))
    logger.info(f"Compacted days {first_day}-{last_day}: {len(files)} files -> {written}")
    return len(files), written


def compact(catalog, min_files: int = COMPACT_MIN_FILES,
            partitions_per_commit: int = COMPACT_PARTITIONS_PER_COMMIT) -> Dict:
    table = catalog.load_table(ICEBERG_TABLE)
    if table.current_snapshot() is None:
        return {"partitions": 0, "files_removed": 0, "files_written": 0}
    days = partitions_to_compact(table.inspect.files().to_pylist(), min_files)
    removed = written = 0
    for first_day, last_day in compaction_ranges(days, partitions_per_commit):
        for attempt in range(1, COMMIT_ATTEMPTS + 1):
            try:
                r, w = compact_range(catalog, first_day, last_day)
                removed, written = removed + r, written + w
                files_removed.inc(r)
                files_written.inc(w)
                break
            except CommitFailedException as e:
                if attempt == COMMIT_ATTEMPTS:
                    raise
                logger.warning(f"Compaction lost a commit race, retrying ({attempt}/{COMMIT_ATTEMPTS}): {e}")
    result = {"partitions": len(days), "files_removed": removed, "files_written": written}
    logger.info(f"Compaction: {result}")
    return result


def expire(catalog, retain_last: int = SNAPSHOT_RETAIN_LAST, max_age_hours: float = SNAPSHOT_MAX_AGE_HOURS) -> Dict:
    table = catalog.load_table(ICEBERG_TABLE)
    current = table.current_snapshot()
    if current is None:
        return {"expired": 0}
    older_than_ms = int((time.time() - max_age_hours * 3600) * 1000)
    ids = snapshots_to_expire(((s.snapshot_id, s.timestamp_ms) for s in table.snapshots()),
                              current.snapshot_id, retain_last, older_than_ms)
    if ids:
        table.maintenance.expire_snapshots().by_ids(ids).commit()
        snapshots_expired.inc(len(ids))
    result = {"expired": len(ids), "remaining": len(table.snapshots())}
    logger.info(f"Snapshot expiry: {result}")
    return result


def referenced_files(table: Table) -> Set[str]:
    """every file the table metadata still points to: metadata, manifests and data"""
    io = table.io
    refs = {table.metadata_location}
    refs.update(entry.metadata_file for entry in table.metadata.metadata_log)
    seen_manifests = set()
    for snapshot in table.snapshots():
        refs.add(snapshot.manifest_list)
        for manifest in snapshot.manifests(io):
            refs.add(manifest.manifest_path)
            if manifest.manifest_path in seen_manifests:
                continue
            seen_manifests.add(manifest.manifest_path)
            for entry in manifest.fetch_manifest_entry(io, discard_deleted=False):
                refs.add(entry.data_file.file_path)
    return {strip_scheme(p) for p in refs}


def remove_orphans(catalog, min_age_minutes: float = ORPHAN_MIN_AGE_MINUTES, dry_run: bool = False) -> Dict:
    if min_age_minutes < ORPHAN_MIN_AGE_FLOOR_MINUTES:
        raise ValueError(f"min age must be at least {ORPHAN_MIN_AGE_FLOOR_MINUTES} minutes, "
                         "or files of a commit in progress could be deleted")
    table = catalog.load_table(ICEBERG_TABLE)
    s3 = s3_filesystem()
    # list first, then read references: a file committed in between is referenced and kept
    listing = [(info.path, info.mtime, info.size)
               for info in s3.get_file_info(fs.FileSelector(strip_scheme(table.location()), recursive=True))
               if info.type == fs.FileType.File]
    table.refresh()
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=min_age_minutes)
    orphans = orphan_files(listing, referenced_files(table), cutoff)

    if not dry_run:
        for path, size in orphans:
            s3.delete_file(path)
            orphans_deleted.inc()
            orphan_bytes.inc(size)
    result = {"listed": len(listing), "orphans": len(orphans),
              "orphan_bytes": sum(size for _, size in orphans), "deleted": not dry_run}
    logger.info(f"Orphan file cleanup: {result}")
    return result


def run_all(catalog, min_files: int = COMPACT_MIN_FILES, retain_last: int = SNAPSHOT_RETAIN_LAST,
            max_age_hours: float = SNAPSHOT_MAX_AGE_HOURS,
            orphan_min_age_minutes: float = ORPHAN_MIN_AGE_MINUTES) -> Dict:
    """run every task, recording success or failure for each; later tasks still run"""
    if not catalog.table_exists(ICEBERG_TABLE):
        # on a fresh stack the writer creates the table with its first CDRs
        logger.info(f"{ICEBERG_TABLE} does not exist yet; nothing to maintain")
        return {}
    jobs = (
        ("compact", lambda: compact(catalog, min_files=min_files)),
        ("expire", lambda: expire(catalog, retain_last=retain_last, max_age_hours=max_age_hours)),
        ("orphans", lambda: remove_orphans(catalog, min_age_minutes=orphan_min_age_minutes)),
    )
    results = {}
    for task, job in jobs:
        try:
            results[task] = job()
            last_success.labels(task).set_to_current_time()
        except Exception as e:
            failures.labels(task).inc()
            logger.exception(f"Maintenance task {task} failed")
            results[task] = {"error": str(e)}
    results["stats"] = table_stats(catalog.load_table(ICEBERG_TABLE))
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("task", choices=["compact", "expire", "orphans", "all", "stats", "loop"])
    parser.add_argument("--min-files", type=int, default=COMPACT_MIN_FILES)
    parser.add_argument("--retain-last", type=int, default=SNAPSHOT_RETAIN_LAST)
    parser.add_argument("--max-age-hours", type=float, default=SNAPSHOT_MAX_AGE_HOURS)
    parser.add_argument("--orphan-min-age-minutes", type=float, default=ORPHAN_MIN_AGE_MINUTES)
    parser.add_argument("--dry-run", action="store_true", help="orphans: report, do not delete")
    args = parser.parse_args()

    catalog = load_iceberg_catalog()
    if args.task == "loop":
        start_http_server(METRICS_PORT, registry=registry)
        logger.info(f"Running maintenance every {MAINTENANCE_INTERVAL_SECONDS}s, metrics on :{METRICS_PORT}")
        # measure the table right away, so a restart does not show 0 files until the first run ends
        try:
            if catalog.table_exists(ICEBERG_TABLE):
                table_stats(catalog.load_table(ICEBERG_TABLE))
        except Exception as e:
            logger.warning(f"Could not read table stats: {e}")
        while True:
            run_all(catalog)
            # refresh the table gauges between runs, so the dashboard shows files piling up
            deadline = time.monotonic() + MAINTENANCE_INTERVAL_SECONDS
            while time.monotonic() < deadline:
                time.sleep(min(60, max(0.0, deadline - time.monotonic())))
                try:
                    if catalog.table_exists(ICEBERG_TABLE):
                        table_stats(catalog.load_table(ICEBERG_TABLE))
                except Exception as e:
                    logger.warning(f"Could not read table stats: {e}")

    table = catalog.load_table(ICEBERG_TABLE)
    before = table_stats(table)
    if args.task == "compact":
        result = compact(catalog, min_files=args.min_files)
    elif args.task == "expire":
        result = expire(catalog, retain_last=args.retain_last, max_age_hours=args.max_age_hours)
    elif args.task == "orphans":
        result = remove_orphans(catalog, min_age_minutes=args.orphan_min_age_minutes, dry_run=args.dry_run)
    elif args.task == "all":
        result = run_all(catalog, min_files=args.min_files, retain_last=args.retain_last,
                         max_age_hours=args.max_age_hours, orphan_min_age_minutes=args.orphan_min_age_minutes)
    else:
        result = {}
    print(json.dumps({"before": before, "result": result,
                      "after": table_stats(catalog.load_table(ICEBERG_TABLE))}, indent=2, default=str))


if __name__ == "__main__":
    main()
