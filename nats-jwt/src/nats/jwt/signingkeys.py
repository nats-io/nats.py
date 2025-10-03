from dataclasses import dataclass
from typing import List, Literal, Union

from nats.jwt.users.limits import UserPermissionsLimits


@dataclass
class SigningKey:
    kind: Literal["user_scope"]
    key: str
    role: str
    template: UserPermissionsLimits
    description: str


SigningKeys = List[Union[str, SigningKey]]
