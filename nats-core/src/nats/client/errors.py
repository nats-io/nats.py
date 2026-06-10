"""NATS client error classes."""

from __future__ import annotations

__all__ = [
    "AuthenticationExpiredError",
    "AuthorizationViolationError",
    "InvalidSubjectError",
    "MaxConnectionsExceededError",
    "MaxPayloadError",
    "MaxPayloadServerError",
    "NoRespondersError",
    "ParserViolationError",
    "PermissionsViolationError",
    "SecureConnectionRequiredError",
    "ServerError",
    "SlowConsumerError",
    "StaleConnectionError",
    "StatusError",
    "server_error_from_message",
]


class MaxPayloadError(ValueError):
    """Raised when a published payload exceeds the server's ``max_payload``.

    The client checks this locally against the ``max_payload`` value advertised
    by the server in its INFO message, so callers see a clear error before the
    frame is written to the wire instead of an opaque server-side disconnect.
    """

    size: int
    max_payload: int

    def __init__(self, size: int, max_payload: int) -> None:
        self.size = size
        self.max_payload = max_payload
        super().__init__(f"payload of {size} bytes exceeds server max_payload of {max_payload} bytes")


class ServerError(Exception):
    """Base class for ``-ERR`` messages reported by the server.

    The ``message`` attribute holds the raw, unquoted string the server sent
    so callers can fall back to substring inspection if they need a category
    that does not yet have its own subclass.
    """

    message: str

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class AuthorizationViolationError(ServerError):
    """Server rejected the connection or operation due to invalid credentials."""


class AuthenticationExpiredError(ServerError):
    """Server reports the client's credentials are no longer valid (JWT expiry)."""


class PermissionsViolationError(ServerError):
    """Server denied a publish or subscribe based on account permissions."""


class StaleConnectionError(ServerError):
    """Server is closing the connection and the client should reconnect."""


class MaxConnectionsExceededError(ServerError):
    """Server is at its configured connection limit."""


class MaxPayloadServerError(ServerError):
    """Server rejected a frame whose payload exceeded ``max_payload``.

    This is the server-reported variant; :class:`MaxPayloadError` is raised
    locally by :meth:`Client.publish` when the client can detect the overrun
    before the frame is written to the wire.
    """


class InvalidSubjectError(ServerError):
    """Server rejected a subject as malformed."""


class ParserViolationError(ServerError):
    """Server failed to parse a protocol frame sent by the client."""


class SecureConnectionRequiredError(ServerError):
    """Client requested TLS but the server does not offer it.

    Also raised by the server (``-ERR 'Secure Connection - TLS Required'``)
    when a non-TLS client connects to a TLS-only server.
    """

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message or "secure connection required but server does not offer TLS")


# Ordered most-specific to least-specific. Each entry maps a case-insensitive
# substring of the server's ``-ERR`` text to the typed exception class. The
# server's exact wording has drifted across releases (e.g. trailing punctuation,
# subject suffixes), so substring matching keeps the table robust without
# encoding every minor variation.
_SERVER_ERROR_PATTERNS: tuple[tuple[str, type[ServerError]], ...] = (
    ("authentication expired", AuthenticationExpiredError),
    ("authentication timeout", AuthorizationViolationError),
    ("authorization violation", AuthorizationViolationError),
    ("permissions violation", PermissionsViolationError),
    ("stale connection", StaleConnectionError),
    ("maximum connections exceeded", MaxConnectionsExceededError),
    ("maximum payload violation", MaxPayloadServerError),
    ("invalid subject", InvalidSubjectError),
    ("parser error", ParserViolationError),
    ("secure connection - tls required", SecureConnectionRequiredError),
    ("tls required", SecureConnectionRequiredError),
)


def server_error_from_message(message: str) -> ServerError:
    """Map a raw ``-ERR`` payload to the most specific :class:`ServerError`.

    Matching is case-insensitive substring matching against a table ordered
    from most-specific to least-specific. Unknown messages fall back to a
    bare :class:`ServerError` so callers can still inspect the raw text.
    """
    lowered = message.lower()
    for needle, cls in _SERVER_ERROR_PATTERNS:
        if needle in lowered:
            return cls(message)
    return ServerError(message)


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
    the configured limits (max_pending_messages or max_pending_bytes).
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
