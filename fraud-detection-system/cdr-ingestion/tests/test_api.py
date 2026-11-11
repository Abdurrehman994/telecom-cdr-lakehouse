"""The HTTP API only accepts a CDR once Kafka has stored it."""
import pytest
from fastapi.testclient import TestClient

from app.main import app

VALID_CDR = {
    "customer_id": "CUST-001234",
    "full_name": "John Doe",
    "risk_score": 25.5,
    "city": "Helsinki",
    "mobile_number": "+358123456789",
    "sim_serial_number": "SIM1234567890",
    "sim_status": "active",
    "imei": "356938035643809",
    "fraud_flag": False,
    "transaction_id": "TXN-0001",
    "time_stamp": "2026-02-16T10:30:00Z",
    "type": "payment",
    "amount_eur": 150.0,
}


class FakePublisher:
    """Fails delivery for the transaction ids in failing_ids."""

    def __init__(self, failing_ids=()):
        self.failing_ids = set(failing_ids)
        self.published = []

    def _error_for(self, cdr):
        return "Local: Message timed out" if cdr["transaction_id"] in self.failing_ids else None

    def publish(self, cdr):
        self.published.append(cdr)
        return self._error_for(cdr)

    def publish_batch(self, cdrs):
        self.published.extend(cdrs)
        return [self._error_for(cdr) for cdr in cdrs]


@pytest.fixture
def client():
    # not used as a context manager, so the lifespan (real Kafka and Postgres) does not run
    return TestClient(app)


def cdr(transaction_id, **overrides):
    return {**VALID_CDR, "transaction_id": transaction_id, **overrides}


def test_ingest_returns_202_after_kafka_confirms(client):
    app.state.publisher = FakePublisher()

    response = client.post("/ingest", json=VALID_CDR)

    assert response.status_code == 202
    assert response.json()["transaction_id"] == "TXN-0001"
    assert app.state.publisher.published[0]["transaction_id"] == "TXN-0001"


def test_ingest_returns_503_when_kafka_does_not_confirm(client):
    app.state.publisher = FakePublisher(failing_ids={"TXN-0001"})

    response = client.post("/ingest", json=VALID_CDR)

    assert response.status_code == 503
    assert "retry" in response.json()["detail"]


def test_ingest_rejects_invalid_cdr_without_publishing(client):
    app.state.publisher = FakePublisher()

    response = client.post("/ingest", json=cdr("TXN-0002", imei="123"))

    assert response.status_code == 400
    assert app.state.publisher.published == []


def test_batch_reports_validation_and_delivery_failures_per_record(client):
    app.state.publisher = FakePublisher(failing_ids={"TXN-B"})

    response = client.post("/ingest/batch", json=[
        cdr("TXN-A"),
        cdr("TXN-B"),                    # Kafka does not confirm it
        cdr("TXN-C", imei="not-an-imei"),  # fails validation, never published
    ])

    body = response.json()
    assert body["accepted"] == 1
    assert body["rejected"] == 2
    assert {e["transaction_id"] for e in body["errors"]} == {"TXN-B", "TXN-C"}
    assert [c["transaction_id"] for c in app.state.publisher.published] == ["TXN-A", "TXN-B"]
