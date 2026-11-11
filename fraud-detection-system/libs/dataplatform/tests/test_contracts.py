"""The contracts job: validating contract files and registering them."""
import json
from pathlib import Path

import pytest

from dataplatform import contracts
from dataplatform.contracts import ContractError, apply, check, load_contracts
from dataplatform.testing import InMemoryRegistry

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"

RECORD = {"type": "record", "name": "Event", "fields": [{"name": "id", "type": "string"}]}


def write_contracts(tmp_path, entries, schemas):
    (tmp_path / "contracts.yaml").write_text(json.dumps({"contracts": entries}))
    for name, schema in schemas.items():
        (tmp_path / name).write_text(schema if isinstance(schema, str) else json.dumps(schema))
    return str(tmp_path)


def entry(**overrides):
    return dict({"topic": "events", "schema": "events.avsc", "compatibility": "FULL_TRANSITIVE",
                 "owner": "team-a"}, **overrides)


def test_the_repository_contracts_are_valid():
    loaded = load_contracts(str(CONTRACTS))

    assert {c.subject for c in loaded} == {"cdr-events-value", "fraud-decisions-value"}
    assert all(c.compatibility == "FULL_TRANSITIVE" for c in loaded)


@pytest.mark.parametrize("bad_entry, schema, message", [
    ({"topic": "events", "schema": "events.avsc", "compatibility": "FULL"}, RECORD, "missing"),
    (entry(compatibility="SOMETIMES"), RECORD, "unknown compatibility"),
    (entry(), "{not json", "not a valid Avro schema"),
    (entry(), {"type": "record", "name": "E", "fields": [{"name": "x", "type": "nope"}]}, "not a valid Avro schema"),
    (entry(), {"type": "string"}, "must be a record"),
])
def test_invalid_contracts_are_rejected_before_reaching_the_registry(tmp_path, bad_entry, schema, message):
    directory = write_contracts(tmp_path, [bad_entry], {"events.avsc": schema})
    with pytest.raises(ContractError, match=message):
        load_contracts(directory)


def test_a_topic_has_one_contract(tmp_path):
    directory = write_contracts(tmp_path, [entry(), entry()], {"events.avsc": RECORD})

    with pytest.raises(ContractError, match="more than one contract"):
        load_contracts(directory)


def test_apply_sets_compatibility_and_registers(tmp_path):
    registry = InMemoryRegistry()
    contract = load_contracts(write_contracts(tmp_path, [entry()], {"events.avsc": RECORD}))[0]

    assert check(registry, contract) == "new subject"
    assert apply(registry, contract) == "schema id 1, version 1"
    assert registry.compatibility == {"events-value": "FULL_TRANSITIVE"}
    assert check(registry, contract) == "unchanged"


def test_a_breaking_change_is_rejected(tmp_path):
    breaking = dict(RECORD, fields=[{"name": "id", "type": "long"}])
    registry = InMemoryRegistry(incompatible=[json.dumps(breaking)])
    apply(registry, load_contracts(write_contracts(tmp_path, [entry()], {"events.avsc": RECORD}))[0])
    changed = load_contracts(write_contracts(tmp_path, [entry()], {"events.avsc": breaking}))[0]

    with pytest.raises(ContractError, match="not FULL_TRANSITIVE compatible"):
        check(registry, changed)
    with pytest.raises(ContractError, match="rejected"):
        apply(registry, changed)
    assert registry.get_versions("events-value") == [1]


def test_main_exits_non_zero_when_a_contract_is_rejected(tmp_path, monkeypatch, capsys):
    breaking = dict(RECORD, fields=[{"name": "id", "type": "long"}])
    registry = InMemoryRegistry(incompatible=[json.dumps(breaking)])
    monkeypatch.setattr(contracts, "registry_client", lambda url=None: registry)
    write_contracts(tmp_path, [entry()], {"events.avsc": RECORD})
    assert contracts.main(["apply", str(tmp_path)]) == 0

    write_contracts(tmp_path, [entry()], {"events.avsc": breaking})

    assert contracts.main(["apply", str(tmp_path)]) == 1
    assert "rejected" in capsys.readouterr().err
