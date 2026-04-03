"""JetStream errors."""

from __future__ import annotations


# JetStream error code constants (matching Go's JSErrCode* constants)
class ErrorCode:
    """JetStream API error codes from the server."""

    CONSUMER_NOT_FOUND = 10014
    MAXIMUM_CONSUMERS_LIMIT = 10026
    MESSAGE_NOT_FOUND = 10037
    JETSTREAM_NOT_ENABLED_FOR_ACCOUNT = 10039
    STREAM_NAME_IN_USE = 10058
    STREAM_NOT_FOUND = 10059
    JETSTREAM_NOT_ENABLED = 10076


class JetStreamError(Exception):
    """Base JetStream error.

    Attributes:
        code: API status code (400, 404, 503, etc.) or None for client-side errors
        error_code: JetStream-specific error code (10003, 10014, 10059, etc.) or None
        description: Human-readable error description
    """

    def __init__(
        self, message: str, code: int | None = None, error_code: int | None = None, description: str | None = None
    ):
        super().__init__(message)
        self.code = code  # API status code
        self.error_code = error_code  # JetStream-specific error code
        self.description = description


# Specific error types (subclasses of JetStreamError for user-friendly error handling)


class StreamNotFoundError(JetStreamError):
    """Stream not found (error code 10059)."""

    pass


class StreamNameAlreadyInUseError(JetStreamError):
    """Stream name already in use (error code 10058)."""

    pass


class ConsumerNotFoundError(JetStreamError):
    """Consumer not found (error code 10014)."""

    pass


class JetStreamNotEnabledError(JetStreamError):
    """JetStream not enabled (error code 10076)."""

    pass


class JetStreamNotEnabledForAccountError(JetStreamError):
    """JetStream not enabled for account (error code 10039)."""

    pass


class ConsumerDeletedError(JetStreamError):
    """Error raised when a consumer has been deleted."""

    pass


class MaximumConsumersLimitError(JetStreamError):
    """Maximum consumers limit reached (error code 10026)."""

    pass


class MessageNotFoundError(JetStreamError):
    """Message not found (error code 10037)."""

    pass
