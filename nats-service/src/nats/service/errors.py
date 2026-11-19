"""NATS service errors."""

from __future__ import annotations


class ServiceError(Exception):
    """Raised inside an endpoint handler to send a structured error response.

    The framework converts this exception into an empty reply carrying the
    ``Nats-Service-Error`` and ``Nats-Service-Error-Code`` headers.

    Raises:
        ValueError: If ``description`` is empty.
    """

    def __init__(self, code: int, description: str) -> None:
        if not description:
            raise ValueError("description cannot be empty")
        super().__init__(f"{code}: {description}")
        self.code = code
        self.description = description
