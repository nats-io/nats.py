"""NATS client error classes."""

from __future__ import annotations


class StatusError(Exception):
    """Base class for NATS status-related errors."""

    def __init__(
        self,
        status: str,
        description: str,
        subject: str | None = None
    ) -> None:
        """Initialize StatusError.

        Args:
            status: The error status code or name
            description: Human-readable error description
            subject: The subject that caused the error (optional)
        """
        self.status = status
        self.description = description
        self.subject = subject
        super().__init__(f"{status}: {description}")

    @classmethod
    def from_status(
        cls,
        status: str,
        description: str,
        *,
        subject: str | None = None
    ) -> StatusError:
        """Create appropriate StatusError subclass based on status.

        Args:
            status: The error status code or name
            description: Human-readable error description
            subject: The subject that caused the error (optional)

        Returns:
            Appropriate StatusError subclass instance
        """
        # Map common status codes to specific error classes
        status_lower = status.lower()
        match status_lower:
            case "400" | "bad request" | "bad_request":
                return BadRequestError(status, description, subject)
            case "503" | "no responders" | "no_responders":
                return NoRespondersError(status, description, subject)
            case _:
                # Default to base StatusError for unknown status codes
                return cls(status, description, subject)


class BadRequestError(StatusError):
    """Error raised for bad request status (400)."""

    def __init__(
        self,
        status: str,
        description: str,
        subject: str | None = None
    ) -> None:
        """Initialize BadRequestError.

        Args:
            status: The error status code or name
            description: Human-readable error description
            subject: The subject that caused the error (optional)
        """
        super().__init__(status, description, subject)


class NoRespondersError(StatusError):
    """Error raised when no responders are available (503)."""

    def __init__(
        self,
        status: str,
        description: str,
        subject: str | None = None
    ) -> None:
        """Initialize NoRespondersError.

        Args:
            status: The error status code or name
            description: Human-readable error description
            subject: The subject that caused the error (optional)
        """
        super().__init__(status, description, subject)
