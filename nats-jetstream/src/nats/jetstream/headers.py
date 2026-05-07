"""JetStream protocol header names and value constants."""

from __future__ import annotations

# Message schedules (ADR-51). Set on the publish to define when and where the
# server will produce messages on a recurring or one-off basis.
NATS_SCHEDULE = "Nats-Schedule"
"""Schedule expression. Cron form (6 fields), an ``@every <duration>`` /
``@at <RFC3339>`` shorthand, or one of the predefined constants below.
Requires :class:`StreamConfig.allow_msg_schedules`."""

NATS_SCHEDULE_TARGET = "Nats-Schedule-Target"
"""Subject the schedule will publish to when it fires."""

NATS_SCHEDULE_SOURCE = "Nats-Schedule-Source"
"""Subject the schedule samples from. The latest message on this subject
is republished to the target on each firing."""

NATS_SCHEDULE_TTL = "Nats-Schedule-TTL"
"""TTL applied to messages produced by the schedule. Server default
``"5m"``; ``"never"`` disables TTL on generated messages. Requires
:class:`StreamConfig.allow_msg_ttl`."""

NATS_SCHEDULE_TIME_ZONE = "Nats-Schedule-Time-Zone"
"""IANA time-zone name (e.g. ``"America/New_York"``) for cron schedule
expressions."""

NATS_SCHEDULE_ROLLUP = "Nats-Schedule-Rollup"
"""Auto-applies a rollup on the schedule target. Currently only
:data:`NATS_SCHEDULE_ROLLUP_SUB` is valid."""

NATS_SCHEDULE_ROLLUP_SUB = "sub"
"""Value for :data:`NATS_SCHEDULE_ROLLUP`: rollup the schedule's target
subject."""

# Server-set headers on schedule-produced messages. Read-only — do not set
# these from a publishing client.
NATS_SCHEDULER = "Nats-Scheduler"
"""On generated messages: the subject holding the schedule definition."""

NATS_SCHEDULE_NEXT = "Nats-Schedule-Next"
"""On generated messages: timestamp of the next firing, or ``"purge"`` for
one-off schedules that have completed."""

# Predefined schedule expressions for use as a :data:`NATS_SCHEDULE` value.
NATS_SCHEDULE_YEARLY = "@yearly"
"""Run once a year at midnight Jan 1."""

NATS_SCHEDULE_MONTHLY = "@monthly"
"""Run once a month at midnight on the 1st."""

NATS_SCHEDULE_WEEKLY = "@weekly"
"""Run once a week at midnight on Sunday."""

NATS_SCHEDULE_DAILY = "@daily"
"""Run once a day at midnight."""

NATS_SCHEDULE_HOURLY = "@hourly"
"""Run once an hour at the top of the hour."""
