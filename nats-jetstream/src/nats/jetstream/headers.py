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

# Message schedules (ADR-51). Set on the publish to define when and where the
# server will produce messages on a recurring or one-off basis.
NATS_SCHEDULE: Final[str] = "Nats-Schedule"
"""Schedule expression. Cron form (6 fields), an ``@every <duration>`` /
``@at <RFC3339>`` shorthand, or one of the predefined constants below.
Requires :class:`StreamConfig.allow_msg_schedules`."""

NATS_SCHEDULE_TARGET: Final[str] = "Nats-Schedule-Target"
"""Subject the schedule will publish to when it fires."""

NATS_SCHEDULE_SOURCE: Final[str] = "Nats-Schedule-Source"
"""Subject the schedule samples from. The latest message on this subject
is republished to the target on each firing."""

NATS_SCHEDULE_TTL: Final[str] = "Nats-Schedule-TTL"
"""TTL applied to messages produced by the schedule. Server default
``"5m"``; ``"never"`` disables TTL on generated messages. Requires
:class:`StreamConfig.allow_msg_ttl`."""

NATS_SCHEDULE_TIME_ZONE: Final[str] = "Nats-Schedule-Time-Zone"
"""IANA time-zone name (e.g. ``"America/New_York"``) for cron schedule
expressions."""

NATS_SCHEDULE_ROLLUP: Final[str] = "Nats-Schedule-Rollup"
"""Auto-applies a rollup on the schedule target. Currently only
:data:`NATS_SCHEDULE_ROLLUP_SUB` is valid."""

NATS_SCHEDULE_ROLLUP_SUB: Final[str] = "sub"
"""Value for :data:`NATS_SCHEDULE_ROLLUP`: rollup the schedule's target
subject."""

# Server-set headers on schedule-produced messages. Read-only — do not set
# these from a publishing client.
NATS_SCHEDULER: Final[str] = "Nats-Scheduler"
"""On generated messages: the subject holding the schedule definition."""

NATS_SCHEDULE_NEXT: Final[str] = "Nats-Schedule-Next"
"""On generated messages: timestamp of the next firing, or ``"purge"`` for
one-off schedules that have completed."""

# Predefined schedule expressions for use as a :data:`NATS_SCHEDULE` value.
NATS_SCHEDULE_YEARLY: Final[str] = "@yearly"
"""Run once a year at midnight Jan 1."""

NATS_SCHEDULE_MONTHLY: Final[str] = "@monthly"
"""Run once a month at midnight on the 1st."""

NATS_SCHEDULE_WEEKLY: Final[str] = "@weekly"
"""Run once a week at midnight on Sunday."""

NATS_SCHEDULE_DAILY: Final[str] = "@daily"
"""Run once a day at midnight."""

NATS_SCHEDULE_HOURLY: Final[str] = "@hourly"
"""Run once an hour at the top of the hour."""

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
    "NATS_SCHEDULE",
    "NATS_SCHEDULE_TARGET",
    "NATS_SCHEDULE_SOURCE",
    "NATS_SCHEDULE_TTL",
    "NATS_SCHEDULE_TIME_ZONE",
    "NATS_SCHEDULE_ROLLUP",
    "NATS_SCHEDULE_ROLLUP_SUB",
    "NATS_SCHEDULER",
    "NATS_SCHEDULE_NEXT",
    "NATS_SCHEDULE_YEARLY",
    "NATS_SCHEDULE_MONTHLY",
    "NATS_SCHEDULE_WEEKLY",
    "NATS_SCHEDULE_DAILY",
    "NATS_SCHEDULE_HOURLY",
    "NATS_REQUIRED_API_LEVEL",
]
