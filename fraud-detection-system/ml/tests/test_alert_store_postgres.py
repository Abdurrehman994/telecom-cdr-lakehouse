"""AlertStore against a real Postgres: inserts are idempotent per transaction_id.

Runs only when RUN_POSTGRES_TESTS=1, with POSTGRES_* pointing at a disposable database.
"""
import os
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.getenv("RUN_POSTGRES_TESTS") != "1", reason="needs a Postgres database")

SCHEMA = Path(__file__).resolve().parents[2] / "postgres" / "init" / "001_fraud_alerts.sql"

CDR = {"transaction_id": "T-PG-1", "customer_id": "CUST-1", "mobile_number": "+358401234567",
       "amount_eur": 99.5, "time_stamp": "2026-01-01T12:00:00Z"}
RESULT = {"fraud_type": "SIM_SWAP", "method": "rules", "confidence": 1.0, "explanation": "flagged"}


@pytest.fixture
def store():
    from ml_service import AlertStore

    s = AlertStore()
    s.pool.open(wait=True, timeout=10)
    with s.pool.connection() as conn:
        conn.execute(SCHEMA.read_text())
        conn.execute("TRUNCATE fraud_alerts")
    yield s
    s.close()


def test_second_insert_of_the_same_transaction_is_a_no_op(store):
    assert store.save(CDR, RESULT, partition=0, offset=1) is True
    assert store.save(CDR, {**RESULT, "fraud_type": "OTHER"}, partition=0, offset=1) is False

    alerts = store.list(status=None, limit=10)
    assert len(alerts) == 1
    # the first decision is kept
    assert alerts[0]["fraud_type"] == "SIM_SWAP"


def test_analyst_decision_updates_status_and_keeps_the_note(store):
    store.save(CDR, RESULT, partition=0, offset=1)

    updated = store.update_status("T-PG-1", "confirmed", "verified with customer")
    assert updated["status"] == "confirmed"
    assert updated["analyst_note"] == "verified with customer"

    # no note: the earlier one stays
    assert store.update_status("T-PG-1", "dismissed", None)["analyst_note"] == "verified with customer"
    assert store.list(status="open", limit=10) == []
    assert store.update_status("UNKNOWN", "confirmed", None) is None
