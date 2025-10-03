from dataclasses import dataclass, field
from typing import List, Optional

from nats.contrib.flatten_model import FlatteningModel
from nats.contrib.types import ConnectionType, Limits, Permissions


class UserPermissionsLimits(FlatteningModel):
    permissions: Permissions
    limits: Limits
    bearer_token: Optional[bool]
    allowed_connection_types: Optional[List[ConnectionType]]

    def __init__(
        self,
        permissions: Permissions,
        limits: Limits,
        bearer_token: Optional[bool] = None,
        allowed_connection_types: Optional[List[ConnectionType]] = None,
    ):
        self.permissions = permissions
        self.limits = limits
        self.bearer_token = bearer_token
        self.allowed_connection_types = allowed_connection_types

    class Meta:
        unflatten_fields = [
            ('allowed_connection_types', 'allowed_connection_types')
        ]
