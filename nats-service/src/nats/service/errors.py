"""NATS service errors."""

from __future__ import annotations


class ServiceError(Exception):
    """Raised inside an endpoint handler to send a structured error response.

    The framework converts this exception into an empty reply carrying the
    ``Nats-Service-Error`` and ``Nats-Service-Error-Code`` headers.
    """

    def __init__(self, code: int, description: str) -> None:
        super().__init__(f"{code}: {description}")
        self.code = code
        self.description = description
