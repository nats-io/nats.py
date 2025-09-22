"""NATS message types and utilities."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Headers:
    """NATS message headers."""

    _headers: dict[str, list[str]]

    def __init__(self, headers: dict[str, str | list[str]]) -> None:
        self._headers = {}
        for key, value in headers.items():
            if isinstance(value, str):
                self._headers[key] = [value]
            elif isinstance(value, list):
                if not all(isinstance(v, str) for v in value):
                    msg = "All items in header value list must be strings"
                    raise ValueError(msg)
                self._headers[key] = value
            else:
                msg = "Header values must be strings or lists of strings"
                raise TypeError(msg)

    def get(self, key: str) -> str | None:
        """Get a header value. If multiple values exist, returns the first one.

        Args:
            key: The header name

        Returns:
            The first header value or None if the header doesn't exist
        """
        values = self._headers.get(key)
        if values is None or len(values) == 0:
            return None
        return values[0]

    def get_all(self, key: str) -> list[str]:
        """Get all values for a header.

        Args:
            key: The header name

        Returns:
            A list of all values for the header. Returns an empty list if the header doesn't exist.
        """
        return self._headers.get(key, [])

    def items(self):
        """Get all header items as key-value pairs.

        Returns:
            An iterable of (key, value_list) pairs.
        """
        return self._headers.items()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Headers):
            return NotImplemented
        return self._headers == other._headers


@dataclass
class Status:
    """NATS message status information.

    Attributes:
        code: The status code (e.g., "503")
        description: Human-readable description (e.g., "No Responders")
    """

    code: str
    description: str | None = None

    @property
    def is_error(self) -> bool:
        """Check if this status represents an error.

        Returns:
            True if the status code is not "200"
        """
        return self.code != "200"

    def __str__(self) -> str:
        """String representation of the status."""
        if self.description:
            return f"{self.code}: {self.description}"
        return self.code


@dataclass
class Message:
    """A NATS message.

    Attributes:
        subject: The subject the message was published to
        data: The message payload as bytes
        reply_to: Optional reply subject for request-reply messaging
        headers: Optional message headers
        status: Optional NATS status information
    """

    subject: str
    data: bytes
    reply_to: str | None = None
    headers: Headers | None = None
    status: Status | None = None

    @property
    def has_status(self) -> bool:
        """Check if this message has a NATS status.

        Returns:
            True if the message has status information
        """
        return self.status is not None

    @property
    def is_error_status(self) -> bool:
        """Check if this message has an error status.

        Returns:
            True if the message has a non-200 status code
        """
        return self.status is not None and self.status.is_error
