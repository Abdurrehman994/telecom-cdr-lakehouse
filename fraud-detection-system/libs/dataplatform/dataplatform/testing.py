"""Test doubles for code that uses the platform library.

InMemoryRegistry stands in for SchemaRegistryClient, so producers and consumers can be
tested without a running schema registry.
"""
import json
from types import SimpleNamespace

from confluent_kafka.schema_registry.error import SchemaRegistryError


def canonical(schema_str: str) -> str:
    return json.dumps(json.loads(schema_str), sort_keys=True)


class InMemoryRegistry:
    """the parts of SchemaRegistryClient the library uses, kept in memory"""

    def __init__(self, incompatible=()):
        # subject -> list of schema ids, oldest first
        self.subjects = {}
        self.schemas = {}
        self.compatibility = {}
        # schema strings register_schema rejects, as the registry does for a breaking change
        self.incompatible = {canonical(s) for s in incompatible}
        self.lookups = 0

    def _id_for(self, schema_str):
        key = canonical(schema_str)
        for schema_id, stored in self.schemas.items():
            if canonical(stored) == key:
                return schema_id
        return None

    def register_schema(self, subject, schema, normalize_schemas=False):
        if canonical(schema.schema_str) in self.incompatible:
            raise SchemaRegistryError(409, 409, "Schema being registered is incompatible with an earlier schema")
        schema_id = self._id_for(schema.schema_str)
        if schema_id is None:
            schema_id = len(self.schemas) + 1
            self.schemas[schema_id] = schema.schema_str
        versions = self.subjects.setdefault(subject, [])
        if schema_id not in versions:
            versions.append(schema_id)
        return schema_id

    def lookup_schema(self, subject, schema, normalize_schemas=False):
        self.lookups += 1
        schema_id = self._id_for(schema.schema_str)
        if schema_id is None or schema_id not in self.subjects.get(subject, []):
            raise SchemaRegistryError(404, 40403, "Schema not found")
        return SimpleNamespace(schema_id=schema_id, version=self.subjects[subject].index(schema_id) + 1)

    def get_schema(self, schema_id):
        if schema_id not in self.schemas:
            raise SchemaRegistryError(404, 40403, "Schema not found")
        return SimpleNamespace(schema_str=self.schemas[schema_id])

    def get_latest_version(self, subject):
        if not self.subjects.get(subject):
            raise SchemaRegistryError(404, 40401, "Subject not found")
        schema_id = self.subjects[subject][-1]
        return SimpleNamespace(schema_id=schema_id, version=len(self.subjects[subject]),
                               schema=SimpleNamespace(schema_str=self.schemas[schema_id]))

    def get_subjects(self):
        return list(self.subjects)

    def get_versions(self, subject):
        return list(range(1, len(self.subjects.get(subject, [])) + 1))

    def set_compatibility(self, subject_name=None, level=None):
        self.compatibility[subject_name] = level

    def test_compatibility(self, subject_name, schema, version="latest"):
        return canonical(schema.schema_str) not in self.incompatible


def registry_with(contracts):
    """an InMemoryRegistry with {topic: schema_str} registered, in order"""
    from confluent_kafka.schema_registry import Schema
    from dataplatform.serde import subject_for

    registry = InMemoryRegistry()
    for topic, schema_str in contracts.items():
        registry.register_schema(subject_for(topic), Schema(schema_str, "AVRO"))
    return registry
