from dataclasses import dataclass
from datetime import datetime, timezone


def parse_datetime(value: str) -> datetime:
    # Remove Z UTC marker
    if value.endswith("Z"):
        value = value[:-1]
    # Check precision
    if "." in value:
        base, microseconds = value.split(".")
        # We can keep up to 999999 microseconds
        value = base + "." + microseconds[:6]
    else:
        value = value + ".0"
    return datetime.strptime(value,
                             "%Y-%m-%dT%H:%M:%S.%f").astimezone(timezone.utc)


@dataclass
class JetStreamResponse:
    type: str
