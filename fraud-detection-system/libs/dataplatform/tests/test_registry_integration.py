"""The library against a real schema registry.

Runs only when TEST_SCHEMA_REGISTRY_URL is set, e.g. with the stack up:
    TEST_SCHEMA_REGISTRY_URL=http://localhost:8081 python -m pytest tests
Each test uses its own topic names, so it does not touch the stack's subjects.
"""
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from dataplatform import Decoder, Encoder, registry_client
from dataplatform.contracts import Contract, ContractError, apply, check, load_contracts, main

URL = os.getenv("TEST_SCHEMA_REGISTRY_URL")
pytestmark = pytest.mark.skipif(not URL, reason="needs TEST_SCHEMA_REGISTRY_URL")

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
V1 = {"type": "record", "name": "Reading", "namespace": "test", "fields": [
    {"name": "sensor", "type": "string"},
    {"name": "value", "type": "double"},
    {"name": "at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
]}


def contract(topic, schema, compatibility="FULL_TRANSITIVE"):
    return Contract(topic, json.dumps(schema, indent=2), compatibility, "tests")


def with_field(schema, field):
    return dict(schema, fields=schema["fields"] + [field])


@pytest.fixture
def registry():
    return registry_client(URL)


@pytest.fixture
def topic():
    return f"test-{uuid.uuid4().hex[:12]}"


def test_compatible_changes_are_registered_and_breaking_ones_rejected(registry, topic):
    assert apply(registry, contract(topic, V1)).endswith("version 1")
    v2 = with_field(V1, {"name": "unit", "type": ["null", "string"], "default": None})
    assert check(registry, contract(topic, v2)) == "new version"
    assert apply(registry, contract(topic, v2)).endswith("version 2")

    # a new required field: old data has no value for it
    required = with_field(v2, {"name": "site", "type": "string"})
    # a type change
    retyped = dict(v2, fields=[dict(f, type="string") if f["name"] == "value" else f for f in v2["fields"]])
    for breaking in (required, retyped):
        with pytest.raises(ContractError):
            check(registry, contract(topic, breaking))
        with pytest.raises(ContractError, match="rejected"):
            apply(registry, contract(topic, breaking))

    assert registry.get_versions(f"{topic}-value") == [1, 2]
    assert registry.get_compatibility(f"{topic}-value") == "FULL_TRANSITIVE"


def test_reformatted_schema_is_the_same_contract(registry, topic):
    """whitespace and key order do not create a new version"""
    apply(registry, contract(topic, V1))

    assert check(registry, Contract(topic, json.dumps(V1, separators=(",", ":")), "FULL_TRANSITIVE", "t")) == "unchanged"


def test_round_trip_through_the_registry(registry, topic):
    apply(registry, contract(topic, V1))
    at = datetime(2026, 11, 11, 9, 30, tzinfo=timezone.utc)

    data = Encoder(registry, topic, json.dumps(V1)).encode({"sensor": "s1", "value": 2.5, "at": at})
    decoded = Decoder(registry_client(URL)).decode(data)

    assert decoded.value == {"sensor": "s1", "value": 2.5, "at": at}


def test_the_repository_contracts_apply_cleanly(tmp_path, capsys):
    """apply twice to fresh subjects: the second run changes nothing"""
    suffix = uuid.uuid4().hex[:8]
    entries = []
    for c in load_contracts(str(CONTRACTS)):
        name = f"{c.topic}-{suffix}"
        (tmp_path / f"{name}.avsc").write_text(c.schema_str)
        entries.append({"topic": name, "schema": f"{name}.avsc", "compatibility": c.compatibility, "owner": c.owner})
    (tmp_path / "contracts.yaml").write_text(json.dumps({"contracts": entries}))

    assert main(["apply", str(tmp_path), "--registry-url", URL]) == 0
    assert main(["check", str(tmp_path), "--registry-url", URL]) == 0
    assert capsys.readouterr().out.count("unchanged") == len(entries)
