"""Register data contracts in the schema registry.

A contracts directory holds contracts.yaml and one Avro schema per topic:

    contracts:
      - topic: cdr-events
        schema: cdr-events-value.avsc
        compatibility: FULL_TRANSITIVE
        owner: ingestion

apply  Set each subject's compatibility level and register its schema. The registry
       rejects a schema that breaks the compatibility rule; the job then exits non-zero
       and nothing that depends on it starts.
check  Report what apply would do, without changing the registry.

Usage:
    python -m dataplatform.contracts apply contracts/
    python -m dataplatform.contracts check contracts/
"""
import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List

import fastavro
import yaml
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from dataplatform.serde import registry_client, subject_for

COMPATIBILITY_LEVELS = {
    "BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE",
    "FULL", "FULL_TRANSITIVE", "NONE",
}


class ContractError(Exception):
    pass


@dataclass(frozen=True)
class Contract:
    topic: str
    schema_str: str
    compatibility: str
    owner: str

    @property
    def subject(self) -> str:
        return subject_for(self.topic)


def load_contracts(directory: str) -> List[Contract]:
    """read and validate contracts.yaml and the schemas it names"""
    root = Path(directory)
    with open(root / "contracts.yaml", encoding="utf-8") as f:
        entries = (yaml.safe_load(f) or {}).get("contracts") or []
    contracts, topics = [], set()
    for entry in entries:
        missing = {"topic", "schema", "compatibility", "owner"} - set(entry)
        if missing:
            raise ContractError(f"contract {entry} is missing {sorted(missing)}")
        if entry["topic"] in topics:
            raise ContractError(f"topic {entry['topic']} has more than one contract")
        if entry["compatibility"] not in COMPATIBILITY_LEVELS:
            raise ContractError(f"{entry['topic']}: unknown compatibility {entry['compatibility']}")
        schema_str = (root / entry["schema"]).read_text(encoding="utf-8")
        try:
            schema = json.loads(schema_str)
            fastavro.parse_schema(schema)
        except Exception as e:
            raise ContractError(f"{entry['schema']} is not a valid Avro schema: {e}") from e
        if schema.get("type") != "record":
            raise ContractError(f"{entry['schema']}: a topic's value schema must be a record")
        topics.add(entry["topic"])
        contracts.append(Contract(entry["topic"], schema_str, entry["compatibility"], entry["owner"]))
    return contracts


def is_registered(registry: SchemaRegistryClient, contract: Contract) -> bool:
    try:
        registry.lookup_schema(contract.subject, Schema(contract.schema_str, "AVRO"), normalize_schemas=True)
        return True
    except SchemaRegistryError as e:
        if e.http_status_code == 404:
            return False
        raise


def check(registry: SchemaRegistryClient, contract: Contract) -> str:
    """what apply would do with this contract; raises ContractError if it would be rejected"""
    if contract.subject not in registry.get_subjects():
        return "new subject"
    if is_registered(registry, contract):
        return "unchanged"
    schema = Schema(contract.schema_str, "AVRO")
    # transitive levels compare with every registered version, the others with the latest only
    # (the registry tests with the subject's current level, so a level change in the same
    # edit is only checked by apply)
    versions = registry.get_versions(contract.subject) if contract.compatibility.endswith("_TRANSITIVE") else ["latest"]
    for version in versions:
        if not registry.test_compatibility(contract.subject, schema, version=version):
            raise ContractError(f"{contract.subject}: not {contract.compatibility} compatible "
                                f"with registered version {version}")
    return "new version"


def apply(registry: SchemaRegistryClient, contract: Contract) -> str:
    """set the compatibility level and register the schema; raises ContractError if rejected"""
    registry.set_compatibility(contract.subject, contract.compatibility)
    try:
        registered_id = registry.register_schema(contract.subject, Schema(contract.schema_str, "AVRO"),
                                                 normalize_schemas=True)
    except SchemaRegistryError as e:
        if e.http_status_code == 409:
            raise ContractError(f"{contract.subject}: rejected, not {contract.compatibility} compatible "
                                f"with the registered versions ({e.error_message})") from e
        raise
    version = registry.lookup_schema(contract.subject, Schema(contract.schema_str, "AVRO"),
                                     normalize_schemas=True).version
    return f"schema id {registered_id}, version {version}"


def wait_for_registry(registry: SchemaRegistryClient, timeout_seconds: float):
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            registry.get_subjects()
            return
        except Exception as e:
            if time.monotonic() >= deadline:
                raise ContractError(f"schema registry is not reachable: {e}") from e
            time.sleep(2)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["apply", "check"])
    parser.add_argument("directory", help="directory with contracts.yaml")
    parser.add_argument("--registry-url", help="defaults to $SCHEMA_REGISTRY_URL")
    parser.add_argument("--wait", type=float, default=60, help="seconds to wait for the registry")
    args = parser.parse_args(argv)

    try:
        contracts = load_contracts(args.directory)
        registry = registry_client(args.registry_url)
        wait_for_registry(registry, args.wait)
    except ContractError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    failed = False
    for contract in contracts:
        try:
            result = apply(registry, contract) if args.command == "apply" else check(registry, contract)
            print(f"{contract.subject} ({contract.compatibility}, owner {contract.owner}): {result}")
        except ContractError as e:
            failed = True
            print(f"error: {e}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
