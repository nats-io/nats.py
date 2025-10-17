"""NATS client error classes."""

from __future__ import annotations


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
