from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Replica:
    """Peer info."""

    name: str
    current: bool
    active: float
    offline: Optional[bool] = False
    lag: Optional[int] = None


@dataclass
class Cluster:
    """Cluster info."""

    name: Optional[str] = None
    leader: Optional[str] = None
    replicas: Optional[List[Replica]] = None
