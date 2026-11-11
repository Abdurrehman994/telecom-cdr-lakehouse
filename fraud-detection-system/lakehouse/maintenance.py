"""Iceberg table maintenance: compaction, snapshot expiry and orphan file cleanup.

Streaming appends leave many small files, and every commit adds a snapshot that keeps
old files alive. Three jobs undo that, for every sink in sinks/, each with the policy in
its sink file:

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
    ... python maintenance.py orphans --dry-run --sink cdr_events
The lakehouse-maintenance service runs `python maintenance.py loop` on a schedule.
"""
import argparse
import json
import logging
import os
import time
from collections import Counter as Tally
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Set, Tuple

from prometheus_client import Counter, Gauge, start_http_server
from pyarrow import fs
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.table import Table
from pyiceberg.types import DateType

from catalog import load_iceberg_catalog, s3_filesystem, stored_offsets
from sink_config import SinkConfig, load_sinks

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("lakehouse-maintenance")

SINKS_DIR = os.getenv("SINKS_DIR", "sinks")
COMPACT_PARTITIONS_PER_COMMIT = int(os.getenv("COMPACT_PARTITIONS_PER_COMMIT", "50"))
# a commit takes seconds; anything younger than this may belong to one in progress
ORPHAN_MIN_AGE_FLOOR_MINUTES = 15
MAINTENANCE_INTERVAL_SECONDS = int(os.getenv("MAINTENANCE_INTERVAL_SECONDS", "3600"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "8003"))
COMMIT_ATTEMPTS = 5

table_files = Gauge("lakehouse_table_data_files", "Data files in the current snapshot", ["table"])
table_rows = Gauge("lakehouse_table_rows", "Rows in the current snapshot", ["table"])
table_bytes = Gauge("lakehouse_table_bytes", "Size of the data files in the current snapshot", ["table"])
table_snapshots = Gauge("lakehouse_table_snapshots", "Snapshots in the table metadata", ["table"])
last_success = Gauge("lakehouse_maintenance_last_success_timestamp_seconds",
                     "Unix time a maintenance task last succeeded", ["table", "task"])
failures = Counter("lakehouse_maintenance_failures_total", "Failed maintenance runs", ["table", "task"])
files_removed = Counter("lakehouse_compaction_files_removed_total", "Small files replaced by compaction", ["table"])
files_written = Counter("lakehouse_compaction_files_written_total", "Files written by compaction", ["table"])
snapshots_expired = Counter("lakehouse_snapshots_expired_total", "Snapshots expired", ["table"])
orphans_deleted = Counter("lakehouse_orphan_files_deleted_total", "Unreferenced files deleted", ["table"])
orphan_bytes = Counter("lakehouse_orphan_bytes_deleted_total", "Bytes of unreferenced files deleted", ["table"])

EPOCH = date(1970, 1, 1)


# --- pure helpers (unit tested) ---

def day_number(value) -> int:
    """a day partition value as days since 1970-01-01 (PyIceberg may return int or date)"""
    return (value - EPOCH).days if isinstance(value, date) else int(value)


def partitions_to_compact(files: Iterable[Dict], min_files: int, partition: str) -> List[int]:
    """day partitions (days since epoch) holding at least min_files data files"""
    counts = Tally(
        day_number(f["partition"][partition])
        for f in files
        if f["content"] == 0 and f["partition"].get(partition) is not None
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


def compaction_ranges(days: List[int], partitions_per_commit: int) -> List[Tuple[int, int]]:
    """contiguous (first_day, last_day) ranges, each covering up to partitions_per_commit of the days.

    One commit rewrites one range. A range filter is cheap to plan, while a filter of many
    OR'd days is not (20 days over 5,000 files took 31 s). Days inside a range that did not
    need compaction are rewritten too, so nothing in the range is deleted without being rewritten.
    """
    return [(group[0], group[-1]) for group in chunks(days, partitions_per_commit)]


def range_filter(column: str, first_day: int, last_day: int, is_date: bool = False):
    """rows whose column falls between the two UTC days, inclusive"""
    start = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(days=first_day)
    end = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(days=last_day + 1)
    if is_date:
        start, end = start.date(), end.date()
    return And(GreaterThanOrEqual(column, start.isoformat()), LessThan(column, end.isoformat()))


# --- jobs ---

def table_stats(table: Table, name: str) -> Dict:
    files = table.inspect.files().to_pylist() if table.current_snapshot() else []
    data_files = [f for f in files if f["content"] == 0]
    stats = {
        "snapshots": len(table.snapshots()),
        "data_files": len(data_files),
        "rows": sum(f["record_count"] for f in data_files),
        "bytes": sum(f["file_size_in_bytes"] for f in data_files),
    }
    table_snapshots.labels(name).set(stats["snapshots"])
    table_files.labels(name).set(stats["data_files"])
    table_rows.labels(name).set(stats["rows"])
    table_bytes.labels(name).set(stats["bytes"])
    return stats


def compact_range(catalog, sink: SinkConfig, first_day: int, last_day: int) -> Tuple[int, int]:
    """rewrite the day partitions first_day..last_day in one commit; returns (files removed, files written)"""
    # Load once and do not refresh before committing: the commit is checked against the
    # snapshot that was read, so a concurrent append makes it fail instead of being lost.
    table = catalog.load_table(sink.table)
    partition = sink.compaction_partition
    files = [f for f in table.inspect.files().to_pylist()
             if f["content"] == 0 and f["partition"].get(partition.name) is not None
             and first_day <= day_number(f["partition"][partition.name]) <= last_day]
    expected_rows = sum(f["record_count"] for f in files)

    is_date = isinstance(table.schema().find_field(partition.column).field_type, DateType)
    row_filter = range_filter(partition.column, first_day, last_day, is_date)
    data = table.scan(row_filter=row_filter).to_arrow()
    if data.num_rows != expected_rows:
        raise RuntimeError(f"read {data.num_rows} rows but the files hold {expected_rows}; not compacting")

    offsets = stored_offsets(table, sink.offsets_property)
    properties = {"lakehouse.maintenance": "compaction"}
    if offsets:
        properties[sink.offsets_property] = json.dumps(offsets)
    # Delete the range's files and add the rewritten rows in one commit. The filter
    # covers whole days, so whole files are dropped. (dynamic_partition_overwrite would
    # be simpler, but PyIceberg only supports it for identity partitions.)
    # Sorted files let filtered queries skip more data.
    if sink.sort_by:
        data = data.sort_by(sink.sort_by)
    table.overwrite(data, overwrite_filter=row_filter, snapshot_properties=properties)

    summary = table.current_snapshot().summary.additional_properties
    written = int(summary.get("added-data-files", 0))
    logger.info(f"Compacted {sink.table} days {first_day}-{last_day}: {len(files)} files -> {written}")
    return len(files), written


def compact(catalog, sink: SinkConfig, min_files: Optional[int] = None,
            partitions_per_commit: int = COMPACT_PARTITIONS_PER_COMMIT) -> Dict:
    min_files = sink.maintenance.compact_min_files if min_files is None else min_files
    table = catalog.load_table(sink.table)
    if min_files <= 0 or sink.compaction_partition is None or table.current_snapshot() is None:
        return {"partitions": 0, "files_removed": 0, "files_written": 0}
    days = partitions_to_compact(table.inspect.files().to_pylist(), min_files, sink.compaction_partition.name)
    removed = written = 0
    for first_day, last_day in compaction_ranges(days, partitions_per_commit):
        for attempt in range(1, COMMIT_ATTEMPTS + 1):
            try:
                r, w = compact_range(catalog, sink, first_day, last_day)
                removed, written = removed + r, written + w
                files_removed.labels(sink.table).inc(r)
                files_written.labels(sink.table).inc(w)
                break
            except CommitFailedException as e:
                if attempt == COMMIT_ATTEMPTS:
                    raise
                logger.warning(f"Compaction lost a commit race, retrying ({attempt}/{COMMIT_ATTEMPTS}): {e}")
    result = {"partitions": len(days), "files_removed": removed, "files_written": written}
    logger.info(f"Compaction of {sink.table}: {result}")
    return result


def expire(catalog, sink: SinkConfig, retain_last: Optional[int] = None,
           max_age_hours: Optional[float] = None) -> Dict:
    policy = sink.maintenance
    retain_last = policy.snapshot_retain_last if retain_last is None else retain_last
    max_age_hours = policy.snapshot_max_age_hours if max_age_hours is None else max_age_hours
    table = catalog.load_table(sink.table)
    current = table.current_snapshot()
    if current is None:
        return {"expired": 0}
    older_than_ms = int((time.time() - max_age_hours * 3600) * 1000)
    ids = snapshots_to_expire(((s.snapshot_id, s.timestamp_ms) for s in table.snapshots()),
                              current.snapshot_id, retain_last, older_than_ms)
    if ids:
        table.maintenance.expire_snapshots().by_ids(ids).commit()
        snapshots_expired.labels(sink.table).inc(len(ids))
    result = {"expired": len(ids), "remaining": len(table.snapshots())}
    logger.info(f"Snapshot expiry of {sink.table}: {result}")
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


def remove_orphans(catalog, sink: SinkConfig, min_age_minutes: Optional[float] = None,
                   dry_run: bool = False) -> Dict:
    if min_age_minutes is None:
        min_age_minutes = sink.maintenance.orphan_min_age_minutes
    if min_age_minutes < ORPHAN_MIN_AGE_FLOOR_MINUTES:
        raise ValueError(f"min age must be at least {ORPHAN_MIN_AGE_FLOOR_MINUTES} minutes, "
                         "or files of a commit in progress could be deleted")
    table = catalog.load_table(sink.table)
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
            orphans_deleted.labels(sink.table).inc()
            orphan_bytes.labels(sink.table).inc(size)
    result = {"listed": len(listing), "orphans": len(orphans),
              "orphan_bytes": sum(size for _, size in orphans), "deleted": not dry_run}
    logger.info(f"Orphan file cleanup of {sink.table}: {result}")
    return result


def run_all(catalog, sink: SinkConfig, min_files: Optional[int] = None, retain_last: Optional[int] = None,
            max_age_hours: Optional[float] = None, orphan_min_age_minutes: Optional[float] = None) -> Dict:
    """run every task on one table, recording success or failure for each; later tasks still run"""
    if not catalog.table_exists(sink.table):
        # on a fresh stack the sink's writer creates the table when it starts
        logger.info(f"{sink.table} does not exist yet; nothing to maintain")
        return {}
    jobs = (
        ("compact", lambda: compact(catalog, sink, min_files=min_files)),
        ("expire", lambda: expire(catalog, sink, retain_last=retain_last, max_age_hours=max_age_hours)),
        ("orphans", lambda: remove_orphans(catalog, sink, min_age_minutes=orphan_min_age_minutes)),
    )
    results = {}
    for task, job in jobs:
        try:
            results[task] = job()
            last_success.labels(sink.table, task).set_to_current_time()
        except Exception as e:
            failures.labels(sink.table, task).inc()
            logger.exception(f"Maintenance task {task} failed for {sink.table}")
            results[task] = {"error": str(e)}
    results["stats"] = table_stats(catalog.load_table(sink.table), sink.table)
    return results


def refresh_stats(catalog, sinks: List[SinkConfig]):
    """measure every table, so the dashboard shows files piling up between runs"""
    for sink in sinks:
        try:
            if catalog.table_exists(sink.table):
                table_stats(catalog.load_table(sink.table), sink.table)
        except Exception as e:
            logger.warning(f"Could not read stats of {sink.table}: {e}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("task", choices=["compact", "expire", "orphans", "all", "stats", "loop"])
    parser.add_argument("--sink", help="name of one sink; default: every sink in $SINKS_DIR")
    parser.add_argument("--min-files", type=int, help="override the sink's compact_min_files")
    parser.add_argument("--retain-last", type=int, help="override the sink's snapshot_retain_last")
    parser.add_argument("--max-age-hours", type=float, help="override the sink's snapshot_max_age_hours")
    parser.add_argument("--orphan-min-age-minutes", type=float, help="override the sink's orphan_min_age_minutes")
    parser.add_argument("--dry-run", action="store_true", help="orphans: report, do not delete")
    args = parser.parse_args()

    sinks = load_sinks(SINKS_DIR)
    if args.sink:
        sinks = [s for s in sinks if s.name == args.sink]
        if not sinks:
            parser.error(f"no sink named {args.sink} in {SINKS_DIR}")

    catalog = load_iceberg_catalog()
    if args.task == "loop":
        start_http_server(METRICS_PORT)
        logger.info(f"Maintaining {[s.table for s in sinks]} every {MAINTENANCE_INTERVAL_SECONDS}s, "
                    f"metrics on :{METRICS_PORT}")
        # measure the tables right away, so a restart does not show 0 files until the first run ends
        refresh_stats(catalog, sinks)
        while True:
            for sink in sinks:
                run_all(catalog, sink)
            deadline = time.monotonic() + MAINTENANCE_INTERVAL_SECONDS
            while time.monotonic() < deadline:
                time.sleep(min(60, max(0.0, deadline - time.monotonic())))
                refresh_stats(catalog, sinks)

    report = {}
    for sink in sinks:
        if not catalog.table_exists(sink.table):
            report[sink.table] = "does not exist yet"
            continue
        before = table_stats(catalog.load_table(sink.table), sink.table)
        if args.task == "compact":
            result = compact(catalog, sink, min_files=args.min_files)
        elif args.task == "expire":
            result = expire(catalog, sink, retain_last=args.retain_last, max_age_hours=args.max_age_hours)
        elif args.task == "orphans":
            result = remove_orphans(catalog, sink, min_age_minutes=args.orphan_min_age_minutes, dry_run=args.dry_run)
        elif args.task == "all":
            result = run_all(catalog, sink, min_files=args.min_files, retain_last=args.retain_last,
                             max_age_hours=args.max_age_hours, orphan_min_age_minutes=args.orphan_min_age_minutes)
        else:
            result = {}
        report[sink.table] = {"before": before, "result": result,
                              "after": table_stats(catalog.load_table(sink.table), sink.table)}
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
