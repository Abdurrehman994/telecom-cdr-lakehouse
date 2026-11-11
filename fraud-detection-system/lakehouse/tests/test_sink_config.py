"""Validation of sink files."""
import pytest
import yaml

from conftest import LAKEHOUSE
from sink_config import SinkConfigError, load_sinks, parse_sink

VALID = {
    "name": "events",
    "source": {"topic": "events", "consumer_group": "lakehouse-events", "dead_letter_topic": "events-dlq"},
    "table": {"identifier": "ns.events", "partition_by": [{"column": "at", "transform": "day"}]},
}


def with_changes(section=None, **changes):
    raw = yaml.safe_load(yaml.safe_dump(VALID))  # deep copy
    target = raw[section] if section else raw
    for key, value in changes.items():
        if value is None:
            target.pop(key, None)
        else:
            target[key] = value
    return raw


def test_the_repository_sinks_are_valid():
    sinks = load_sinks(str(LAKEHOUSE / "sinks"))

    assert [s.table for s in sinks] == ["telecom.cdr_events", "telecom.fraud_decisions"]
    assert sinks[0].offsets_property == "kafka.cdr-events.offsets"
    assert sinks[0].compaction_partition.name == "event_day"


def test_defaults_fill_what_a_sink_leaves_out():
    sink = parse_sink(VALID)

    assert sink.flush_max_records == 500 and sink.maintenance.snapshot_retain_last == 10
    assert sink.partition_by[0].name == "at_day"
    assert sink.namespace == "ns"


@pytest.mark.parametrize("raw, message", [
    (with_changes(tabel={"identifier": "ns.x"}), "unknown keys"),
    (with_changes("source", topik="events"), "unknown keys"),
    (with_changes("source", dead_letter_topic=None), "missing"),
    (with_changes("table", identifier="events"), "namespace.table"),
    (with_changes("table", partition_by=[{"column": "at", "transform": "weekly"}]), "unknown partition transform"),
    (with_changes("table", partition_by=[{"column": "at", "transform": "bucket[16]"}]), "compaction needs"),
    (with_changes(flush={"max_records": 0}), "must be positive"),
    (with_changes(maintenance={"compact_min_file": 5}), "unknown keys"),
])
def test_mistakes_fail_at_startup_instead_of_being_ignored(raw, message):
    with pytest.raises(SinkConfigError, match=message):
        parse_sink(raw)


def test_compaction_can_be_turned_off_for_tables_without_a_day_partition():
    raw = with_changes("table", partition_by=[{"column": "region", "transform": "identity"}])
    raw["maintenance"] = {"compact_min_files": 0}

    assert parse_sink(raw).compaction_partition is None


def test_two_sinks_cannot_share_a_consumer_group(tmp_path):
    for name, table in (("a", "ns.a"), ("b", "ns.b")):
        raw = with_changes(name=name)
        raw["table"]["identifier"] = table
        (tmp_path / f"{name}.yaml").write_text(yaml.safe_dump(raw))

    with pytest.raises(SinkConfigError, match="consumer_group"):
        load_sinks(str(tmp_path))
