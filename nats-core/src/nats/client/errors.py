"""NATS client error classes."""

from __future__ import annotations

__all__ = ["StatusError", "NoRespondersError", "SlowConsumerError"]


class StatusError(Exception):
    """Base class for NATS status-related errors."""

    status: str
    description: str
    subject: str | None

    def __init__(self, status: str, description: str, subject: str | None = None) -> None:
        """Initialize StatusError.

        Args:
            status: The error status code
            description: Human-readable error description
            subject: The subject that caused the error (optional)
        """
        self.status = status
        self.description = description
        self.subject = subject
        super().__init__(f"{status}: {description}")

    @classmethod
    def from_status(cls, status: str, description: str, *, subject: str | None = None) -> StatusError:
        """Create appropriate StatusError subclass based on status code.

        Args:
            status: The error status code
            description: Human-readable error description
            subject: The subject that caused the error (optional)

        Returns:
            Appropriate StatusError subclass instance
        """
        match status:
            case "503":
                return NoRespondersError(status, description, subject)
            case _:
                return cls(status, description, subject)


class NoRespondersError(StatusError):
    """Error raised when no responders are available (503)."""

    def __init__(self, status: str, description: str, subject: str | None = None) -> None:
        """Initialize NoRespondersError.

        Args:
            status: The error status code
            description: Human-readable error description
            subject: The subject that caused the error (optional)
        """
        super().__init__(status, description, subject)


class SlowConsumerError(Exception):
    """Error raised when a subscription cannot keep up with message flow.

    This occurs when the subscription's pending message queue exceeds
    the configured limits (pending_msgs_limit or pending_bytes_limit).
    Messages will be dropped to prevent memory exhaustion.
    """

    subject: str
    sid: str
    pending_messages: int
    pending_bytes: int

    def __init__(self, subject: str, sid: str, pending_messages: int, pending_bytes: int) -> None:
        """Initialize SlowConsumerError.

        Args:
            subject: The subscription subject
            sid: The subscription ID
            pending_messages: Number of pending messages in queue
            pending_bytes: Number of pending bytes in queue
        """
        self.subject = subject
        self.sid = sid
        self.pending_messages = pending_messages
        self.pending_bytes = pending_bytes
        super().__init__(
            f"Slow consumer on subject '{subject}': {pending_messages} pending messages, {pending_bytes} pending bytes"
        )
