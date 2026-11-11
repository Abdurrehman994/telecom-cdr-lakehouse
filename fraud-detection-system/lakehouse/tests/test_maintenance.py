"""Selection rules of the table maintenance jobs."""
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from maintenance import (  # noqa: E402
    chunks,
    compaction_ranges,
    day_number,
    orphan_files,
    partitions_to_compact,
    range_filter,
    snapshots_to_expire,
    strip_scheme,
)


def data_file(day, content=0):
    return {"content": content, "partition": {"event_day": day}}


def test_only_partitions_with_enough_files_are_compacted():
    files = [data_file(100)] * 5 + [data_file(101)] * 4 + [data_file(102)] * 9

    assert partitions_to_compact(files, min_files=5) == [100, 102]


def test_compaction_ignores_delete_files_and_unpartitioned_rows():
    files = [data_file(100, content=1)] * 6 + [data_file(None)] * 6

    assert partitions_to_compact(files, min_files=5) == []


def test_day_partitions_are_read_as_ints_or_dates():
    assert day_number(date(1970, 1, 11)) == 10
    assert day_number(10) == 10
    assert partitions_to_compact([data_file(date(2026, 1, 1))] * 5, min_files=5) == [20454]


def test_partitions_are_compacted_in_bounded_groups():
    assert chunks(list(range(5)), 2) == [[0, 1], [2, 3], [4]]


def test_each_commit_covers_one_contiguous_range_of_days():
    # days 12 and 13 are inside the second range, so they are rewritten with it
    assert compaction_ranges([3, 4, 10, 14, 20], partitions_per_commit=2) == [(3, 4), (10, 14), (20, 20)]


def test_range_filter_covers_whole_utc_days():
    expr = range_filter(20454, 20455)  # 2026-01-01 .. 2026-01-02

    assert "2026-01-01T00:00:00+00:00" in repr(expr)
    assert "2026-01-03T00:00:00+00:00" in repr(expr)


def test_newest_snapshots_and_the_current_one_are_never_expired():
    now = 1_000_000
    snapshots = [(sid, now - sid * 1000) for sid in range(1, 8)]  # 1 is newest, 7 oldest

    expired = snapshots_to_expire(snapshots, current_id=1, retain_last=3, older_than_ms=now)

    assert sorted(expired) == [4, 5, 6, 7]


def test_recent_snapshots_are_kept_even_beyond_retain_last():
    now = 1_000_000
    snapshots = [(1, now), (2, now - 10), (3, now - 5_000)]

    assert snapshots_to_expire(snapshots, current_id=1, retain_last=1, older_than_ms=now - 1_000) == [3]


def test_current_snapshot_is_kept_even_if_it_is_old():
    snapshots = [(1, 10), (2, 20)]  # 2 is newest, but 1 is current (e.g. after a rollback)

    assert snapshots_to_expire(snapshots, current_id=1, retain_last=0, older_than_ms=100) == [2]


def test_only_old_unreferenced_files_are_orphans():
    now = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
    old, new = now - timedelta(hours=2), now - timedelta(minutes=1)
    listing = [
        ("warehouse/t/data/a.parquet", old, 100),   # referenced
        ("warehouse/t/data/b.parquet", old, 200),   # orphan
        ("warehouse/t/data/c.parquet", new, 300),   # unreferenced but may be an in-flight commit
        ("warehouse/t/metadata/v1.json", old.replace(tzinfo=None), 50),  # orphan, naive time
    ]
    referenced = {strip_scheme("s3://warehouse/t/data/a.parquet")}

    orphans = orphan_files(listing, referenced, cutoff=now - timedelta(minutes=15))

    assert orphans == [("warehouse/t/data/b.parquet", 200), ("warehouse/t/metadata/v1.json", 50)]
