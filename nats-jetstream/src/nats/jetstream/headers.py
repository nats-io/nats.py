"""JetStream protocol header names and value constants."""

from __future__ import annotations

from typing import Final

# Atomic Batch Publishing (ADR-50)
NATS_BATCH_ID: Final[str] = "Nats-Batch-Id"
"""Atomic batch publish: batch id (max 64 chars)."""

NATS_BATCH_SEQUENCE: Final[str] = "Nats-Batch-Sequence"
"""Atomic batch publish: per-message sequence within the batch."""

NATS_BATCH_COMMIT: Final[str] = "Nats-Batch-Commit"
"""Atomic batch publish: commit marker. ``"1"`` to commit and store the
final message; ``"eob"`` to commit without storing it (end-of-batch)."""

NATS_BATCH_COMMIT_FINAL: Final[str] = "1"
"""Value for :data:`NATS_BATCH_COMMIT`: commit and store the final message."""

NATS_BATCH_COMMIT_EOB: Final[str] = "eob"
"""Value for :data:`NATS_BATCH_COMMIT`: commit without storing the final
message (end-of-batch). Server is case-sensitive on this string."""

# API-level guards
NATS_REQUIRED_API_LEVEL: Final[str] = "Nats-Required-Api-Level"
"""Minimum JetStream API level the publishing client requires; the server
rejects the message (and the enclosing batch, if any) when its own level is
below the value set here."""

__all__ = [
    "NATS_BATCH_ID",
    "NATS_BATCH_SEQUENCE",
    "NATS_BATCH_COMMIT",
    "NATS_BATCH_COMMIT_FINAL",
    "NATS_BATCH_COMMIT_EOB",
    "NATS_REQUIRED_API_LEVEL",
]
