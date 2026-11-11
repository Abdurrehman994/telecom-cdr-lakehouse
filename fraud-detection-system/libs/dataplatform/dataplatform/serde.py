"""Avro messages through the schema registry.

Messages use the Confluent wire format, so any Kafka tool that speaks it can read them:
one zero byte, the schema id as a 4-byte big-endian integer, then the Avro-encoded record.

The value schema of topic T is registered under the subject "T-value". Producers never
register schemas: the contracts job does (python -m dataplatform.contracts apply), and the
registry checks compatibility there. A producer can only send with a schema that is
already registered, so a change that would break consumers cannot reach the topic.
"""
import io
import json
import os
import struct
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import fastavro
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError
from fastavro.validation import ValidationError, validate

MAGIC_BYTE = 0
HEADER = struct.Struct(">bI")


def subject_for(topic: str) -> str:
    """the registry subject that holds a topic's value schema"""
    return f"{topic}-value"


def registry_client(url: Optional[str] = None) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": url or os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")})


class InvalidMessage(ValueError):
    """The message can never be decoded (not Avro, unknown schema, corrupt body).

    Consumers send it to their dead-letter topic. Other errors, such as the registry being
    unreachable, are temporary and must not be: retrying the message later will work.
    """


class ContractViolation(ValueError):
    """A record does not match the schema it is being sent with."""


@dataclass(frozen=True)
class Decoded:
    value: Dict[str, Any]
    # None for a legacy JSON message
    schema_id: Optional[int]


class Encoder:
    """Encodes records for one topic with a schema that must be registered for it."""

    def __init__(self, registry: SchemaRegistryClient, topic: str, schema_str: str):
        self.registry = registry
        self.topic = topic
        self.schema_str = schema_str
        self._parsed = fastavro.parse_schema(json.loads(schema_str))
        self._schema_id: Optional[int] = None

    @classmethod
    def from_file(cls, registry: SchemaRegistryClient, topic: str, path: str) -> "Encoder":
        with open(path, encoding="utf-8") as f:
            return cls(registry, topic, f.read())

    @property
    def schema_id(self) -> int:
        """id of this schema in the registry; raises SchemaRegistryError if it is not registered"""
        if self._schema_id is None:
            registered = self.registry.lookup_schema(
                subject_for(self.topic), Schema(self.schema_str, "AVRO"), normalize_schemas=True
            )
            if registered.schema_id is None:
                raise SchemaRegistryError(404, 40403, f"no id for the {self.topic} schema")
            self._schema_id = registered.schema_id
        return self._schema_id

    def encode(self, record: Dict[str, Any]) -> bytes:
        try:
            validate(record, self._parsed, raise_errors=True)
        except ValidationError as e:
            raise ContractViolation(f"record does not match the {self.topic} contract: {e}") from e
        buf = io.BytesIO()
        buf.write(HEADER.pack(MAGIC_BYTE, self.schema_id))
        fastavro.schemaless_writer(buf, self._parsed, record)
        return buf.getvalue()


class Decoder:
    """Decodes messages with the schema they were written with, fetched from the registry by id.

    allow_json accepts plain JSON objects too, for topics that still hold messages from
    before they had a contract.
    """

    def __init__(self, registry: SchemaRegistryClient, allow_json: bool = False):
        self.registry = registry
        self.allow_json = allow_json
        self._schemas: Dict[int, Tuple[Dict, Any]] = {}

    def writer_schema(self, schema_id: int) -> Dict:
        """the Avro schema (as parsed JSON) registered under schema_id"""
        return self._schema(schema_id)[0]

    def _schema(self, schema_id: int) -> Tuple[Dict, Any]:
        if schema_id not in self._schemas:
            try:
                schema_str = self.registry.get_schema(schema_id).schema_str
            except SchemaRegistryError as e:
                if e.http_status_code == 404:
                    raise InvalidMessage(f"schema id {schema_id} is not in the registry") from e
                raise
            if schema_str is None:
                raise InvalidMessage(f"schema id {schema_id} has no schema in the registry")
            raw = json.loads(schema_str)
            self._schemas[schema_id] = (raw, fastavro.parse_schema(raw))
        return self._schemas[schema_id]

    def decode(self, data: Optional[bytes]) -> Decoded:
        if not data:
            raise InvalidMessage("empty message")
        if data[0] == MAGIC_BYTE:
            if len(data) < HEADER.size:
                raise InvalidMessage("message is shorter than the Avro header")
            _, schema_id = HEADER.unpack_from(data)
            _, parsed = self._schema(schema_id)
            try:
                value = fastavro.schemaless_reader(io.BytesIO(data[HEADER.size:]), parsed, None)
            except Exception as e:
                raise InvalidMessage(f"corrupt Avro body for schema id {schema_id}: {e!r}") from e
            if not isinstance(value, dict):
                raise InvalidMessage(f"schema id {schema_id} is not a record schema")
            return Decoded(value, schema_id)
        if self.allow_json and data[:1] == b"{":
            try:
                return Decoded(json.loads(data), None)
            except ValueError as e:
                raise InvalidMessage(f"invalid JSON: {e}") from e
        raise InvalidMessage("not an Avro message from the schema registry")
