"""Iceberg tables derived from Avro data contracts.

A sink's table has one column per field of the topic's contract, plus lineage columns
that record where in Kafka each row came from. All columns are optional in Iceberg: the
contract already decides which fields a message must have, and optional columns can be
added and retired as the contract evolves.

Supported Avro types: the primitives, enums (as strings), the logical types timestamp,
local-timestamp, date, decimal and uuid, and unions of null with one of these. Nested
records, arrays and maps are rejected for now.
"""
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from pyiceberg.partitioning import PartitionField as IcebergPartitionField
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    TruncateTransform,
    YearTransform,
)
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

from sink_config import PartitionField, SinkConfigError

# where in Kafka each row came from
LINEAGE_FIELDS: Tuple[Tuple[str, IcebergType, str], ...] = (
    ("kafka_partition", IntegerType(), "Kafka partition the row was read from"),
    ("kafka_offset", LongType(), "Kafka offset the row was read from"),
    ("ingested_at", TimestamptzType(), "When the sink read the row from Kafka"),
)
LINEAGE_NAMES = {name for name, _, _ in LINEAGE_FIELDS}

PRIMITIVES = {
    "string": StringType(),
    "boolean": BooleanType(),
    "int": IntegerType(),
    "long": LongType(),
    "float": FloatType(),
    "double": DoubleType(),
    "bytes": BinaryType(),
}


class UnsupportedSchema(ValueError):
    pass


def iceberg_type(avro_type: Any, where: str) -> IcebergType:
    """the Iceberg type for an Avro field type"""
    if isinstance(avro_type, list):
        non_null = [t for t in avro_type if t != "null"]
        if len(non_null) != 1:
            raise UnsupportedSchema(f"{where}: only unions of null and one other type are supported")
        return iceberg_type(non_null[0], where)
    if isinstance(avro_type, dict):
        logical = avro_type.get("logicalType")
        if logical in ("timestamp-micros", "timestamp-millis"):
            return TimestamptzType()
        if logical in ("local-timestamp-micros", "local-timestamp-millis"):
            return TimestampType()
        if logical == "date":
            return DateType()
        if logical == "decimal":
            return DecimalType(avro_type["precision"], avro_type.get("scale", 0))
        if logical == "uuid":
            return StringType()
        if avro_type.get("type") == "enum":
            return StringType()
        return iceberg_type(avro_type.get("type"), where)
    if avro_type in PRIMITIVES:
        return PRIMITIVES[avro_type]
    raise UnsupportedSchema(f"{where}: Avro type {avro_type!r} is not supported yet")


def iceberg_schema(avro_schema: Dict) -> Schema:
    """the table schema for a contract: its fields, then the lineage columns"""
    name = avro_schema.get("name", "?")
    if avro_schema.get("type") != "record":
        raise UnsupportedSchema(f"{name}: a contract must be a record")
    fields = []
    for i, f in enumerate(avro_schema["fields"], start=1):
        if f["name"] in LINEAGE_NAMES:
            raise UnsupportedSchema(f"{name}.{f['name']}: the name is reserved for a lineage column")
        fields.append(NestedField(i, f["name"], iceberg_type(f["type"], f"{name}.{f['name']}"),
                                  required=False, doc=f.get("doc")))
    for j, (lineage_name, field_type, doc) in enumerate(LINEAGE_FIELDS, start=len(fields) + 1):
        fields.append(NestedField(j, lineage_name, field_type, required=False, doc=doc))
    return Schema(*fields)


def parse_transform(transform: str) -> Transform:
    if transform.startswith("bucket["):
        return BucketTransform(int(transform[7:-1]))
    if transform.startswith("truncate["):
        return TruncateTransform(int(transform[9:-1]))
    return {
        "identity": IdentityTransform(),
        "year": YearTransform(),
        "month": MonthTransform(),
        "day": DayTransform(),
        "hour": HourTransform(),
    }[transform]


def partition_spec(schema: Schema, partition_by: Iterable[PartitionField]) -> PartitionSpec:
    fields = []
    for i, p in enumerate(partition_by):
        try:
            source = schema.find_field(p.column)
        except ValueError:
            raise SinkConfigError(f"partition column {p.column} is not in the contract") from None
        transform = parse_transform(p.transform)
        if not transform.can_transform(source.field_type):
            raise SinkConfigError(f"partition transform {p.transform} does not apply to "
                                  f"{p.column} ({source.field_type})")
        fields.append(IcebergPartitionField(source_id=source.field_id, field_id=1000 + i,
                                            transform=transform, name=p.name))
    return PartitionSpec(*fields)


def describe_spec(schema: Schema, spec: PartitionSpec) -> List[Tuple[str, str, str]]:
    """(column, transform, name) per partition field, to compare specs across schemas"""
    return [(schema.find_column_name(f.source_id), str(f.transform), f.name) for f in spec.fields]


def sync_table_schema(table: Table, avro_schema: Dict) -> List[str]:
    """add the contract's new fields to the table; returns the columns added or widened.

    Columns are matched by name. A field removed from the contract keeps its column, which
    is null for new rows. Type changes other than safe widening (int to long, float to
    double) raise; the registry's compatibility rule normally stops them first.
    """
    desired = iceberg_schema(avro_schema)
    current = table.schema()
    current_types = {f.name: f.field_type for f in current.fields}
    changed = [f.name for f in desired.fields
               if f.name not in current_types or current_types[f.name] != f.field_type]
    if changed:
        with table.update_schema() as update:
            update.union_by_name(desired)
    return changed


# --- converting message values to column values ---

def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    """parse an ISO timestamp; values without a timezone are treated as UTC"""
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _to_timestamptz(value):
    if isinstance(value, str):
        return parse_timestamp(value)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    raise TypeError(f"expected a timestamp, got {value!r}")


def _to_timestamp(value):
    parsed = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(parsed, datetime):
        raise TypeError(f"expected a timestamp, got {value!r}")
    return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed


def _to_date(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _to_int(value):
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"expected an integer, got {value!r}")
    return int(value)


def _to_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ("true", "false"):
        return value.lower() == "true"
    raise TypeError(f"expected a boolean, got {value!r}")


def _to_bytes(value):
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    raise TypeError(f"expected bytes, got {value!r}")


def converter(field_type: IcebergType) -> Callable[[Any], Any]:
    """turns a decoded message value into the column's Python type.

    Avro values already have the right type; JSON values (from before a topic had a
    contract) arrive as strings and plain numbers.
    """
    if isinstance(field_type, TimestamptzType):
        return _to_timestamptz
    if isinstance(field_type, TimestampType):
        return _to_timestamp
    if isinstance(field_type, DateType):
        return _to_date
    if isinstance(field_type, (DoubleType, FloatType)):
        return float
    if isinstance(field_type, (IntegerType, LongType)):
        return _to_int
    if isinstance(field_type, BooleanType):
        return _to_bool
    if isinstance(field_type, DecimalType):
        return lambda v: v if isinstance(v, Decimal) else Decimal(str(v))
    if isinstance(field_type, BinaryType):
        return _to_bytes
    return str


class RowBuilder:
    """maps decoded messages to rows of a table schema"""

    def __init__(self, schema: Schema):
        self._fields = [(f.name, converter(f.field_type)) for f in schema.fields if f.name not in LINEAGE_NAMES]

    def build(self, record: Dict[str, Any], partition: int, offset: int, ingested_at: datetime) -> Dict[str, Any]:
        """raises ValueError or TypeError if a value does not fit its column"""
        row = {}
        for name, convert in self._fields:
            value = record.get(name)
            row[name] = None if value is None else convert(value)
        row["kafka_partition"] = partition
        row["kafka_offset"] = offset
        row["ingested_at"] = ingested_at
        return row
