from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Union

from nats.jwt.accounts.limits import AccountLimits, NatsLimits
from nats.jwt.flatten_model import FlatteningModel


class Types(str, Enum):
    Operator = "operator"
    Account = "account"
    User = "user"
    Activation = "activation"
    AuthorizationResponse = "authorization_response"


class ConnectionType(str, Enum):
    STANDARD = "STANDARD"
    WEBSOCKET = "WEBSOCKET"
    LEAFNODE = "LEAFNODE"
    LEAFNODE_WS = "LEAFNODE_WS"
    MQTT = "MQTT"
    MQTT_WS = "MQTT_WS"
    IN_PROCESS = "IN_PROCESS"


@dataclass
class Info:
    description: Optional[str] = None
    info_url: Optional[str] = None


@dataclass
class TimeRange:
    start: Optional[str]
    end: Optional[str]


@dataclass
class UserLimits:
    src: Optional[List[str]] = None
    times: Optional[List[TimeRange]] = None
    locale: Optional[str] = None


# Inherit AccountLimits as well
# @dataclass
# class Limits(UserLimits, NatsLimits):
#     pass
Limits = Union[UserLimits, NatsLimits, AccountLimits]


@dataclass
class ResponsePermissions:
    max: int
    ttl: int


@dataclass
class Permission:
    allow: Optional[List[str]]
    deny: Optional[List[str]]


class Permissions(FlatteningModel):
    pub: Optional[Union[Permission, Dict[str, List[str]]]]
    sub: Optional[Union[Permission, Dict[str, List[str]]]]
    resp: Optional[ResponsePermissions]

    def __init__(
        self,
        pub: Optional[Union[Permission, Dict[str, List[str]]]] = None,
        sub: Optional[Union[Permission, Dict[str, List[str]]]] = None,
        resp: Optional[ResponsePermissions] = None,
    ):
        self.pub = pub if pub else {}
        self.sub = sub if sub else {}
        self.resp = resp if resp else None

    class Meta:
        unflatten_fields = [('pub', 'pub'), ('sub', 'sub'), ('resp', 'resp')]
