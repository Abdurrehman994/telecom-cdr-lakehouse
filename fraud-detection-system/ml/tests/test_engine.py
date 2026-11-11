"""Throughput safeguards in the detection engine: LLM circuit breaker and training cadence."""
import requests

import ml_service
from ml_service import FraudDetectionEngine

CDR = {"transaction_id": "T1", "risk_score": 0.5, "amount_eur": 2000, "sim_status": "active"}


def test_llm_failure_opens_the_circuit_so_later_cdrs_skip_the_llm(monkeypatch):
    """Regression: with Ollama unreachable every uncertain CDR waited ~4 s for the same
    failure, and fraud detection fell to about 0.5 CDRs/s."""
    calls = []

    def unreachable(*args, **kwargs):
        calls.append(kwargs.get("timeout"))
        raise requests.ConnectionError("Failed to resolve 'ollama'")

    monkeypatch.setattr(ml_service.requests, "post", unreachable)
    engine = FraudDetectionEngine()

    first = engine.llm_validate(CDR, ml_score=0.6)
    second = engine.llm_validate(CDR, ml_score=0.4)

    assert len(calls) == 1
    assert calls[0][0] <= 2  # short connect timeout
    assert first["method"] == second["method"] == "scoring_fallback"
    assert first["is_fraud"] is True and second["is_fraud"] is False


def test_llm_is_tried_again_after_the_retry_window(monkeypatch):
    calls = []

    def unreachable(*args, **kwargs):
        calls.append(1)
        raise requests.ConnectionError("down")

    monkeypatch.setattr(ml_service.requests, "post", unreachable)
    engine = FraudDetectionEngine()
    engine.llm_validate(CDR, ml_score=0.6)

    engine.llm_unavailable_until = 0.0  # the window has passed
    engine.llm_validate(CDR, ml_score=0.6)

    assert len(calls) == 2


def test_model_retrains_once_per_batch_not_on_every_cdr(monkeypatch):
    """Regression: the training window never empties, so checking its length
    retrained the random forest on every CDR after the first 50."""
    engine = FraudDetectionEngine()
    trainings = []
    monkeypatch.setattr(engine, "_train_batch", lambda: trainings.append(len(engine.training_buffer)))

    for i in range(175):
        engine._buffer_and_train({**CDR, "transaction_id": f"T{i}", "fraud_flag": i % 2 == 0})

    assert trainings == [50, 100, 150]
