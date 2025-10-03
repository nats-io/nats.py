from __future__ import annotations

from abc import ABC, abstractmethod
from collections import UserDict
from typing import Dict, Optional


class StatsInterface(ABC):
    """
    Abstract base class defining the interface for NATS client statistics tracking.
    """

    @abstractmethod
    def message_received(
        self, subject: str, payload_size: int, headers: Optional[Dict[str, str]] = None
    ) -> None:
        """Record an incoming message with its payload size."""
        pass

    @abstractmethod
    def message_sent(
        self, subject: str, payload_size: int, headers: Optional[Dict[str, str]] = None
    ) -> None:
        """Record an outgoing message with its payload size."""
        pass

    @abstractmethod
    def client_reconnected(self) -> None:
        """Record a client reconnection."""
        pass

    @abstractmethod
    def error_received(self) -> None:
        """Record a server error."""
        pass


class ClientStats(StatsInterface, UserDict):
    """
    ClientStats tracks NATS client connection statistics and acts as a dict
    for backward compatibility while providing structured methods for updates.
    """

    def __init__(self) -> None:
        super().__init__(
            {
                "in_msgs": 0,
                "out_msgs": 0,
                "in_bytes": 0,
                "out_bytes": 0,
                "reconnects": 0,
                "errors_received": 0,
            }
        )

    def message_received(
        self, subject: str, payload_size: int, headers: Optional[Dict[str, str]] = None
    ) -> None:
        """Record an incoming message with its payload size."""
        self.data["in_msgs"] += 1
        self.data["in_bytes"] += payload_size

    def message_sent(
        self, subject: str, payload_size: int, headers: Optional[Dict[str, str]] = None
    ) -> None:
        """Record an outgoing message with its payload size."""
        self.data["out_msgs"] += 1
        self.data["out_bytes"] += payload_size

    def client_reconnected(self) -> None:
        """Record a client reconnection."""
        self.data["reconnects"] += 1

    def error_received(self) -> None:
        """Record a server error."""
        self.data["errors_received"] += 1

