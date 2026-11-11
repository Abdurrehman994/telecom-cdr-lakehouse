"""Sink definitions: which Kafka topic lands in which Iceberg table, and how.

Each file in sinks/ defines one sink. The table's columns are not listed here: they come
from the topic's data contract in the schema registry. Example:

    name: cdr_events
    source:
      topic: cdr-events
      consumer_group: lakehouse-writer
      dead_letter_topic: cdr-events-dlq
    table:
      identifier: telecom.cdr_events
      partition_by:
        - {column: time_stamp, transform: day, name: event_day}
      sort_by: time_stamp
    flush:
      max_records: 500
      interval_seconds: 10
    maintenance:
      compact_min_files: 5
      snapshot_retain_last: 10
      snapshot_max_age_hours: 24
      orphan_min_age_minutes: 1440

Unknown keys are rejected, so a typo fails at startup instead of being ignored.
"""
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

TRANSFORM_PATTERN = re.compile(r"^(identity|year|month|day|hour|bucket\[\d+\]|truncate\[\d+\])$")
# a compaction rewrites whole days, so it needs a day partition to select them by
COMPACTION_TRANSFORM = "day"


class SinkConfigError(ValueError):
    pass


@dataclass(frozen=True)
class PartitionField:
    column: str
    transform: str
    name: str


@dataclass(frozen=True)
class MaintenancePolicy:
    # compact a day partition once it holds this many files; 0 turns compaction off
    compact_min_files: int = 5
    snapshot_retain_last: int = 10
    snapshot_max_age_hours: float = 24
    orphan_min_age_minutes: float = 1440


@dataclass(frozen=True)
class SinkConfig:
    name: str
    topic: str
    consumer_group: str
    dead_letter_topic: str
    table: str
    partition_by: Tuple[PartitionField, ...] = ()
    sort_by: Optional[str] = None
    flush_max_records: int = 500
    flush_interval_seconds: float = 10
    maintenance: MaintenancePolicy = field(default_factory=MaintenancePolicy)

    @property
    def namespace(self) -> str:
        return self.table.rsplit(".", 1)[0]

    @property
    def offsets_property(self) -> str:
        """snapshot property that holds the next Kafka offset to read, per partition"""
        return f"kafka.{self.topic}.offsets"

    @property
    def topic_id_property(self) -> str:
        """snapshot property that holds the id of the topic those offsets belong to"""
        return f"kafka.{self.topic}.topic-id"

    @property
    def compaction_partition(self) -> Optional[PartitionField]:
        """the day partition that compaction rewrites by, if the table has one"""
        return next((p for p in self.partition_by if p.transform == COMPACTION_TRANSFORM), None)


def _section(raw: Dict, key: str, allowed: set, required: set = frozenset(), where: str = "") -> Dict:
    value = raw.get(key) or {}
    if not isinstance(value, dict):
        raise SinkConfigError(f"{where}{key} must be a mapping")
    unknown = set(value) - allowed
    if unknown:
        raise SinkConfigError(f"{where}{key}: unknown keys {sorted(unknown)}")
    missing = set(required) - set(value)
    if missing:
        raise SinkConfigError(f"{where}{key}: missing {sorted(missing)}")
    return value


def parse_sink(raw: Any, where: str = "") -> SinkConfig:
    if not isinstance(raw, dict):
        raise SinkConfigError(f"{where}a sink must be a mapping")
    unknown = set(raw) - {"name", "source", "table", "flush", "maintenance"}
    if unknown:
        raise SinkConfigError(f"{where}unknown keys {sorted(unknown)}")
    if not raw.get("name"):
        raise SinkConfigError(f"{where}missing name")

    source = _section(raw, "source", {"topic", "consumer_group", "dead_letter_topic"},
                      {"topic", "consumer_group", "dead_letter_topic"}, where)
    table = _section(raw, "table", {"identifier", "partition_by", "sort_by"}, {"identifier"}, where)
    flush = _section(raw, "flush", {"max_records", "interval_seconds"}, where=where)
    maintenance = _section(raw, "maintenance", set(MaintenancePolicy.__dataclass_fields__), where=where)

    if "." not in table["identifier"]:
        raise SinkConfigError(f"{where}table.identifier must be namespace.table, got {table['identifier']}")

    partitions = []
    for entry in table.get("partition_by") or []:
        if not isinstance(entry, dict) or set(entry) - {"column", "transform", "name"} or "column" not in entry:
            raise SinkConfigError(f"{where}partition_by entries need column, transform and optionally name")
        transform = str(entry.get("transform", "identity"))
        if not TRANSFORM_PATTERN.match(transform):
            raise SinkConfigError(f"{where}unknown partition transform {transform}")
        default_name = entry["column"] if transform == "identity" else f"{entry['column']}_{transform.split('[')[0]}"
        partitions.append(PartitionField(entry["column"], transform, entry.get("name", default_name)))
    if len({p.name for p in partitions}) < len(partitions):
        raise SinkConfigError(f"{where}partition names must be unique")

    policy = MaintenancePolicy(**maintenance)
    sink = SinkConfig(
        name=str(raw["name"]),
        topic=source["topic"],
        consumer_group=source["consumer_group"],
        dead_letter_topic=source["dead_letter_topic"],
        table=table["identifier"],
        partition_by=tuple(partitions),
        sort_by=table.get("sort_by"),
        flush_max_records=int(flush.get("max_records", 500)),
        flush_interval_seconds=float(flush.get("interval_seconds", 10)),
        maintenance=policy,
    )
    if sink.flush_max_records < 1 or sink.flush_interval_seconds <= 0:
        raise SinkConfigError(f"{where}flush.max_records and flush.interval_seconds must be positive")
    if policy.compact_min_files > 0 and sink.compaction_partition is None:
        raise SinkConfigError(f"{where}compaction needs a partition with transform {COMPACTION_TRANSFORM}; "
                              "add one or set maintenance.compact_min_files to 0")
    return sink


def load_sink(path: str) -> SinkConfig:
    with open(path, encoding="utf-8") as f:
        return parse_sink(yaml.safe_load(f), where=f"{path}: ")


def load_sinks(directory: str) -> List[SinkConfig]:
    """every sink in the directory; names, tables and consumer groups must be unique"""
    sinks = [load_sink(str(p)) for p in sorted(Path(directory).glob("*.yaml"))]
    for attr in ("name", "table", "consumer_group"):
        values = [getattr(s, attr) for s in sinks]
        duplicates = {v for v in values if values.count(v) > 1}
        if duplicates:
            raise SinkConfigError(f"{directory}: more than one sink uses {attr} {sorted(duplicates)}")
    return sinks
