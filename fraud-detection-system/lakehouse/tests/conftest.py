import json
import sys
from pathlib import Path

LAKEHOUSE = Path(__file__).resolve().parents[1]
CONTRACTS = LAKEHOUSE.parent / "contracts"

sys.path.insert(0, str(LAKEHOUSE))


def contract(topic: str) -> str:
    """the schema string of a topic's contract in contracts/"""
    return (CONTRACTS / f"{topic}-value.avsc").read_text(encoding="utf-8")


def contract_json(topic: str) -> dict:
    return json.loads(contract(topic))


class FakeMessage:
    """a consumed Kafka message"""

    def __init__(self, value: bytes, partition: int = 0, offset: int = 0):
        self._value, self._partition, self._offset = value, partition, offset

    def value(self):
        return self._value

    def key(self):
        return b"key"

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return None


class FakeProducer:
    def __init__(self):
        self.produced = []

    def produce(self, topic, key, value, headers):
        self.produced.append({"topic": topic, "value": value, "headers": headers})

    def flush(self, timeout):
        return 0
