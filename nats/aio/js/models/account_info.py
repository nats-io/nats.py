from dataclasses import dataclass
from typing import Optional

from .base import JetStreamResponse


@dataclass
class Limits:
    """Account limits

    References:
        * Multi-tenancy & Resource Mgmt, NATS Docs - https://docs.nats.io/jetstream/resource_management
    """

    max_memory: int
    max_storage: int
    max_streams: int
    max_consumers: int


@dataclass
class Api:
    """API stats"""

    total: int
    errors: int


@dataclass
class AccountInfo(JetStreamResponse):
    """Account information

    References:
        * Account Information, NATS Docs - https://docs.nats.io/jetstream/administration/account#account-information
    """

    memory: int
    storage: int
    streams: int
    consumers: int
    limits: Limits
    api: Api
    domain: Optional[str] = None

    def __post_init__(self):
        if isinstance(self.limits, dict):
            self.limits = Limits(**self.limits)
        if isinstance(self.api, dict):
            self.api = Api(**self.api)
