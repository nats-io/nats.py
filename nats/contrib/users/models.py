from dataclasses import dataclass, field
from typing import List, Optional

from nats.contrib.claims.generic import GenericFields
from nats.contrib.flatten_model import FlatteningModel
from nats.contrib.types import (
    ConnectionType,
    Limits,
    Permissions,
    Types,
    UserLimits,
)
from nats.contrib.users.limits import UserPermissionsLimits


class User(FlatteningModel):
    permissions_limits: UserPermissionsLimits
    issuer_account: Optional[str]

    generic_fields: GenericFields

    def __init__(
        self,
        permissions_limits: UserPermissionsLimits,
        issuer_account: Optional[str] = None,
        generic_fields: Optional[GenericFields] = None,
    ):
        self.permissions_limits: UserPermissionsLimits = permissions_limits
        self.issuer_account: Optional[str] = issuer_account
        self.generic_fields: GenericFields = generic_fields if generic_fields else GenericFields(
            type=Types.User, version=2
        )
