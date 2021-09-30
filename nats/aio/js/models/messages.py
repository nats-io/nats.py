from __future__ import annotations

from base64 import b64decode
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional


@dataclass
class Message:
    """A message read from a stream

    References:
        * Fetching a message from a stream by sequence - [NATS Docs](https://docs.nats.io/jetstream/nats_api_reference#fetching-from-a-stream-by-sequence)
    """

    subject: str
    seq: int
    time: datetime

    data: Optional[bytes] = None
    hdrs: Optional[Dict[str, str]] = None

    def __post_init__(self):
        if isinstance(self.time, str):
            try:
                self.time = datetime.strptime(
                    self.time[:-2], "%Y-%m-%dT%H:%M:%S.%f"
                ).astimezone(timezone.utc)
            except ValueError:
                self.time = datetime.strptime(
                    self.time, "%Y-%m-%dT%H:%M:%S.%f"
                ).astimezone(timezone.utc)

        if isinstance(self.data, str):
            self.data = b64decode(self.data)
