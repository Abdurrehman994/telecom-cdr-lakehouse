"""Iceberg schemas from Avro contracts, and converting message values to column values."""
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
)

from conftest import contract_json
from sink_config import PartitionField, SinkConfigError
from table_schema import RowBuilder, UnsupportedSchema, iceberg_schema, partition_spec


def record(*fields):
    return {"type": "record", "name": "Event", "fields": list(fields)}


def test_avro_types_map_to_iceberg_types():
    schema = iceberg_schema(record(
        {"name": "id", "type": "string"},
        {"name": "n", "type": "int"},
        {"name": "big", "type": "long"},
        {"name": "ok", "type": "boolean"},
        {"name": "x", "type": ["null", "double"], "default": None},
        {"name": "at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "local", "type": {"type": "long", "logicalType": "local-timestamp-millis"}},
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
        {"name": "kind", "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B"]}},
    ))

    types = {f.name: f.field_type for f in schema.fields}
    assert types == {
        "id": StringType(), "n": IntegerType(), "big": LongType(), "ok": BooleanType(), "x": DoubleType(),
        "at": TimestamptzType(), "local": TimestampType(), "day": DateType(), "price": DecimalType(10, 2),
        "kind": StringType(),
        "kafka_partition": IntegerType(), "kafka_offset": LongType(), "ingested_at": TimestamptzType(),
    }
    # optional in Iceberg, so the contract can retire a field without breaking the table
    assert not any(f.required for f in schema.fields)


@pytest.mark.parametrize("field, message", [
    ({"name": "tags", "type": {"type": "array", "items": "string"}}, "not supported"),
    ({"name": "either", "type": ["string", "long"]}, "only unions of null"),
    ({"name": "kafka_offset", "type": "long"}, "reserved"),
])
def test_unsupported_contracts_are_rejected_with_the_field_named(field, message):
    with pytest.raises(UnsupportedSchema, match=message):
        iceberg_schema(record(field))


def test_partition_columns_must_exist_and_fit_the_transform():
    schema = iceberg_schema(contract_json("cdr-events"))

    assert partition_spec(schema, [PartitionField("time_stamp", "day", "event_day")]).fields[0].name == "event_day"
    with pytest.raises(SinkConfigError, match="not in the contract"):
        partition_spec(schema, [PartitionField("event_time", "day", "d")])
    with pytest.raises(SinkConfigError, match="does not apply"):
        partition_spec(schema, [PartitionField("city", "day", "d")])


def test_rows_take_json_values_and_record_lineage():
    builder = RowBuilder(iceberg_schema(record(
        {"name": "at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "amount", "type": "double"},
        {"name": "n", "type": "long"},
        {"name": "ok", "type": "boolean"},
        {"name": "price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
        {"name": "note", "type": ["null", "string"], "default": None},
    )))
    now = datetime(2026, 3, 1, tzinfo=timezone.utc)

    row = builder.build({"at": "2026-03-01T08:00:00", "day": "2026-03-01", "amount": 12, "n": "5",
                         "ok": "true", "price": "9.99"}, partition=2, offset=41, ingested_at=now)

    assert row == {"at": datetime(2026, 3, 1, 8, tzinfo=timezone.utc), "day": date(2026, 3, 1), "amount": 12.0,
                   "n": 5, "ok": True, "price": Decimal("9.99"), "note": None,
                   "kafka_partition": 2, "kafka_offset": 41, "ingested_at": now}


@pytest.mark.parametrize("value", [{"amount": "lots"}, {"n": 1.5}, {"ok": "maybe"}, {"at": 12345}])
def test_values_that_do_not_fit_their_column_raise(value):
    builder = RowBuilder(iceberg_schema(record(
        {"name": "amount", "type": "double"}, {"name": "n", "type": "long"}, {"name": "ok", "type": "boolean"},
        {"name": "at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
    )))

    with pytest.raises((ValueError, TypeError)):
        builder.build(value, partition=0, offset=0, ingested_at=datetime.now(timezone.utc))
