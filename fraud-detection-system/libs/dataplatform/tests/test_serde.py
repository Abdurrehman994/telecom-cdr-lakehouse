"""Avro encoding and decoding through the schema registry."""
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError

from dataplatform import ContractViolation, Decoder, Encoder, InvalidMessage, subject_for
from dataplatform.testing import InMemoryRegistry

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
CDR_SCHEMA = (CONTRACTS / "cdr-events-value.avsc").read_text()

CDR = {
    "transaction_id": "TXN-1", "customer_id": "CUST-1", "full_name": "A B", "city": "Oulu",
    "mobile_number": "+358401234567", "sim_serial_number": "SIM1", "sim_status": "active",
    "imei": "356938035643809", "risk_score": 12.5, "fraud_flag": False, "type": "call",
    "amount_eur": 3.2, "time_stamp": datetime(2026, 1, 1, 12, tzinfo=timezone.utc),
    "processed_timestamp": datetime(2026, 1, 1, 12, 0, 1, tzinfo=timezone.utc),
}


def registered(schema_str=CDR_SCHEMA, topic="cdr-events"):
    registry = InMemoryRegistry()
    registry.register_schema(subject_for(topic), Schema(schema_str, "AVRO"))
    return registry


def test_round_trip_keeps_types_and_fills_defaults():
    registry = registered()
    data = Encoder(registry, "cdr-events", CDR_SCHEMA).encode(CDR)

    decoded = Decoder(registry).decode(data)

    assert data[0] == 0 and int.from_bytes(data[1:5], "big") == decoded.schema_id == 1
    assert decoded.value["time_stamp"] == CDR["time_stamp"]
    assert decoded.value["amount_eur"] == 3.2
    # optional field left out by the producer
    assert decoded.value["fraud_type"] is None


def test_producer_cannot_send_with_an_unregistered_schema():
    """the contract is registered by the contracts job, never by a producer"""
    with pytest.raises(SchemaRegistryError):
        Encoder(InMemoryRegistry(), "cdr-events", CDR_SCHEMA).encode(CDR)


def test_record_that_breaks_the_contract_is_rejected_before_sending():
    encoder = Encoder(registered(), "cdr-events", CDR_SCHEMA)

    with pytest.raises(ContractViolation):
        encoder.encode({k: v for k, v in CDR.items() if k != "amount_eur"})
    with pytest.raises(ContractViolation):
        encoder.encode(dict(CDR, risk_score="high"))


def test_schema_id_is_looked_up_once():
    registry = registered()
    encoder = Encoder(registry, "cdr-events", CDR_SCHEMA)

    encoder.encode(CDR)
    encoder.encode(CDR)

    assert registry.lookups == 1


def test_messages_are_read_with_the_schema_they_were_written_with():
    """a consumer that knows nothing about v2 still reads its new field"""
    v2 = json.loads(CDR_SCHEMA)
    v2["fields"].append({"name": "roaming", "type": ["null", "boolean"], "default": None})
    v2_str = json.dumps(v2)
    registry = registered()
    registry.register_schema("cdr-events-value", Schema(v2_str, "AVRO"))

    decoded = Decoder(registry).decode(Encoder(registry, "cdr-events", v2_str).encode(dict(CDR, roaming=True)))

    assert decoded.schema_id == 2
    assert decoded.value["roaming"] is True
    assert Decoder(registry).writer_schema(2)["fields"][-1]["name"] == "roaming"


def test_json_is_accepted_only_when_allowed():
    message = b'{"transaction_id": "TXN-1"}'

    assert Decoder(InMemoryRegistry(), allow_json=True).decode(message).value == {"transaction_id": "TXN-1"}
    assert Decoder(InMemoryRegistry(), allow_json=True).decode(message).schema_id is None
    with pytest.raises(InvalidMessage):
        Decoder(InMemoryRegistry()).decode(message)


@pytest.mark.parametrize("data", [
    b"",
    b"not json",
    b"\x00\x00",                       # shorter than the header
    b"\x00\x00\x00\x00\x09rest",       # schema id 9 is not registered
    b"\x00\x00\x00\x00\x01\xff",       # body cut short
    b'{"broken": ',
])
def test_messages_that_can_never_be_decoded_are_invalid(data):
    with pytest.raises(InvalidMessage):
        Decoder(registered(), allow_json=True).decode(data)


def test_registry_outage_is_not_an_invalid_message():
    """a temporary outage must not send good messages to a dead-letter topic"""
    class Down(InMemoryRegistry):
        def get_schema(self, schema_id):
            raise SchemaRegistryError(503, 50300, "unavailable")

    data = Encoder(registered(), "cdr-events", CDR_SCHEMA).encode(CDR)

    with pytest.raises(SchemaRegistryError) as raised:
        Decoder(Down()).decode(data)
    assert not isinstance(raised.value, InvalidMessage)
