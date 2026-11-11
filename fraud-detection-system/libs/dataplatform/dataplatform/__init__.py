"""Client library for the data platform.

Every service that produces or consumes a platform topic uses it, so they all
agree on how messages are encoded and how data contracts are enforced.
"""
from dataplatform.serde import (
    ContractViolation,
    Decoded,
    Decoder,
    Encoder,
    InvalidMessage,
    registry_client,
    subject_for,
)

__all__ = ["ContractViolation", "Decoded", "Decoder", "Encoder", "InvalidMessage", "registry_client", "subject_for"]
