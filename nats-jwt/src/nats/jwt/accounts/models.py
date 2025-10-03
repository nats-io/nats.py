from dataclasses import dataclass, field, fields
from typing import Dict, List, Optional, Union

from nats.jwt.accounts.limits import OperatorLimits
from nats.jwt.claims.generic import GenericFields
from nats.jwt.exports import Exports
from nats.jwt.flatten_model import FlatteningModel
from nats.jwt.imports import Imports
from nats.jwt.signingkeys import SigningKeys
from nats.jwt.types import Info, Permissions, Types


@dataclass
class WeightedMapping:
    subject: str
    weight: Optional[
        int] = None  # uint8 is mapped to int, with Optional for omitempty
    cluster: Optional[str] = None


@dataclass
class ExternalAuthorization:
    auth_users: Optional[List[str]]
    allowed_accounts: Optional[List[str]]
    xkey: Optional[str]


@dataclass
class MsgTrace:
    # Destination is the subject the server will send message traces to
    # if the inbound message contains the "traceparent" header and has
    # its sampled field indicating that the trace should be triggered.
    dest: Optional[str]  # `json:"dest,omitempty"`

    # Sampling is used to set the probability sampling, that is, the
    # server will get a random number between 1 and 100 and trigger
    # the trace if the number is lower than this Sampling value.
    # The valid range is [1..100]. If the value is not set Validate()
    # will set the value to 100.
    sampling: Optional[int]  # `json:"sampling,omitempty"`


class Account(FlatteningModel):
    imports: Optional[Imports]  # `json:"imports,omitempty"`
    exports: Optional[Exports]  # `json:"exports,omitempty"`
    limits: Optional[OperatorLimits]  # `json:"limits,omitempty"`
    signing_keys: Optional[SigningKeys]  # `json:"signing_keys,omitempty"`
    revocations: Optional[Dict[str, int]]  # `json:"revocations,omitempty"`
    default_permissions: Optional[Permissions
                                  ]  # `json:"default_permissions,omitempty"`
    mappings: Optional[Dict[str,
                            WeightedMapping]]  # `json:"mappings,omitempty"`
    authorization: Optional[ExternalAuthorization
                            ]  # `json:"authorization,omitempty"`
    trace: Optional[MsgTrace]  # `json:"trace,omitempty"`
    cluster_traffic: Optional[str]  # `json:"cluster_traffic,omitempty"`

    info: Info = field(default_factory=Info)
    generic_fields: GenericFields = field(default_factory=GenericFields)

    def __init__(
        self,
        imports: Optional[Imports] = None,
        exports: Optional[Exports] = None,
        limits: Optional[OperatorLimits] = None,
        signing_keys: Optional[SigningKeys] = None,
        revocations: Optional[Dict[str, int]] = None,
        default_permissions: Optional[Permissions] = None,
        mappings: Optional[Dict[str, WeightedMapping]] = None,
        authorization: Optional[ExternalAuthorization] = None,
        trace: Optional[MsgTrace] = None,
        cluster_traffic: Optional[str] = None,
        info: Optional[Info] = None,
        generic_fields: Optional[GenericFields] = None
    ):
        self.imports = imports
        self.exports = exports
        self.limits = limits
        self.signing_keys = signing_keys
        self.revocations = revocations
        self.default_permissions = default_permissions
        self.mappings = mappings
        self.authorization = authorization
        self.trace = trace
        self.cluster_traffic = cluster_traffic

        self.info = info if info else Info()
        self.generic_fields = generic_fields if generic_fields else GenericFields(
            type=Types.Account, version=2
        )

    class Meta:
        unflatten_fields = [
            ("imports", "imports"),
            ("exports", "exports"),
            ("limits", "limits"),
            ("signing_keys", "signing_keys"),
            ("revocations", "revocations"),
            ("default_permissions", "default_permissions"),
            ("mappings", "mappings"),
            ("authorization", "authorization"),
            ("trace", "trace"),
            ("cluster_traffic", "cluster_traffic"),
        ]
