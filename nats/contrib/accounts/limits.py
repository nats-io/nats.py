from dataclasses import dataclass
from typing import Dict, Optional

from nats.contrib.flatten_model import FlatteningModel


@dataclass
class NatsLimits:
    data: Optional[int] = None
    payload: Optional[int] = None
    subs: Optional[int] = None


class AccountLimits(FlatteningModel):
    imports: Optional[
        int]  # `json:"imports,omitempty"`         // Max number of imports
    exports: Optional[
        int]  # `json:"exports,omitempty"`         // Max number of exports
    wildcards: Optional[
        bool
    ]  #  `json:"wildcards,omitempty"`       // Are wildcards allowed in exports
    disallow_bearer: Optional[
        bool
    ]  #  `json:"disallow_bearer,omitempty"` // User JWT can't be bearer token
    conn: Optional[
        int
    ]  # `json:"conn,omitempty"`            // Max number of active connections
    leaf: Optional[
        int
    ]  # `json:"leaf,omitempty"`            // Max number of active leaf node connections

    def __init__(
        self,
        imports: Optional[int] = None,
        exports: Optional[int] = None,
        wildcards: Optional[bool] = None,
        disallow_bearer: Optional[bool] = None,
        conn: Optional[int] = None,
        leaf: Optional[int] = None,
    ):
        self.imports = imports
        self.exports = exports
        self.wildcards = wildcards
        self.disallow_bearer = disallow_bearer
        self.conn = conn
        self.leaf = leaf


class JetStreamLimits(FlatteningModel):
    mem_storage: Optional[int] = None
    disk_storage: Optional[int] = None
    streams: Optional[int] = None
    consumer: Optional[int] = None
    mem_max_stream_bytes: Optional[int] = None
    disk_max_stream_bytes: Optional[int] = None
    max_bytes_required: Optional[bool] = None
    max_ack_pending: Optional[int] = None

    def __init__(
        self,
        mem_storage: Optional[int] = None,
        disk_storage: Optional[int] = None,
        streams: Optional[int] = None,
        consumer: Optional[int] = None,
        mem_max_stream_bytes: Optional[int] = None,
        disk_max_stream_bytes: Optional[int] = None,
        max_bytes_required: Optional[bool] = None,
        max_ack_pending: Optional[int] = None,
    ):
        self.mem_storage = mem_storage
        self.disk_storage = disk_storage
        self.streams = streams
        self.consumer = consumer
        self.mem_max_stream_bytes = mem_max_stream_bytes
        self.disk_max_stream_bytes = disk_max_stream_bytes
        self.max_bytes_required = max_bytes_required
        self.max_ack_pending = max_ack_pending


JetStreamTieredLimits = Dict[str, JetStreamLimits]


class OperatorLimits(FlatteningModel):
    nats_limits: NatsLimits
    account_limits: AccountLimits
    jetstream_limits: Optional[JetStreamLimits]
    tiered_limits: Optional[JetStreamTieredLimits
                            ]  # `json:"tiered_limits,omitempty"`

    def __init__(
        self,
        nats_limits: NatsLimits,
        account_limits: AccountLimits,
        jetstream_limits: Optional[JetStreamLimits] = None,
        tiered_limits: Optional[JetStreamTieredLimits] = None,
    ):
        self.nats_limits = nats_limits
        self.account_limits = account_limits
        self.jetstream_limits = jetstream_limits
        self.tiered_limits = tiered_limits

    class META:
        unflattened_fields = [('tiered_limits', 'tiered_limits')]
