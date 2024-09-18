from __future__ import annotations

from asyncio import Future
from typing import Dict, Any, Protocol, Optional
from dataclasses import dataclass


@dataclass
class PublishAck:
    """
    Represents the response of publishing a message to JetStream.
    """

    stream: str
    """
    The stream name the message was published to.
    """

    sequence: int
    """
    The stream sequence number of the message.
    """

    domain: Optional[str] = None
    """
    The domain the message was published to.
    """

    duplicate: Optional[bool] = None
    """
    Indicates whether the message was a duplicate.
    """

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PublishAck:
        return cls(
            stream=data["stream"],
            sequence=data["seq"],
            domain=data.get("domain"),
            duplicate=data.get("duplicate"),
        )

class Publisher(Protocol):
    """
    A protocol for publishing messages to a stream.
    """

    async def publish(self, subject: str, payload: bytes) -> PublishAck:
        """
        Publishes a message with the given payload on the given subject.
        """
        ...

    async def publish_async(
        self,
        subject: str,
        payload: bytes = b'',
        wait_stall: Optional[float] = None,
      ) -> Future[PublishAck]:
        """
        Publishes a message with the given payload on the given subject without waiting for a server acknowledgement.
        """
        ...
