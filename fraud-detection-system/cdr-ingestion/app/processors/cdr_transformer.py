"""CDR transformation and enrichment logic"""
from datetime import datetime, timezone
from typing import Dict, Any
from app.schemas import CDRRecord


def calculate_fraud_probability(risk_score: float, amount: float) -> bool:
    """Determine if transaction should be flagged based on risk score and amount"""
    # Flag if risk score is high (>70) or amount is unusually high (>5000 EUR)
    return risk_score > 70 or amount > 5000


def enrich_cdr(record: CDRRecord) -> Dict[str, Any]:
    """Enrich CDR record with additional data"""
    enriched = record.model_dump(exclude_none=True)
    enriched["processed_timestamp"] = datetime.now(timezone.utc)
    
    # Auto-flag fraud if certain conditions are met
    if not enriched.get("fraud_flag"):
        enriched["fraud_flag"] = calculate_fraud_probability(
            enriched.get("risk_score", 0),
            enriched.get("amount_eur", 0)
        )
        # Determine fraud type if flagged
        if enriched["fraud_flag"]:
            if enriched.get("risk_score", 0) > 70:
                enriched["fraud_type"] = "high_risk_profile"
            elif enriched.get("amount_eur", 0) > 5000:
                enriched["fraud_type"] = "suspicious_amount"
    
    return enriched
