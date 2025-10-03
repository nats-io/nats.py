from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional

from nats.jwt.flatten_model import FlatteningModel
from nats.jwt.types import Info


@dataclass
class ServiceLatency:
    sampling: int
    results: str


@dataclass
class Export(FlatteningModel):
    name: str
    subject: str
    type: Literal["stream", "service"]
    token_req: Optional[bool] = None
    revocations: Optional[Dict[str, int]] = None
    response_type: Optional[Literal["Singleton", "Stream", "Chunked"]] = None
    response_threshold: Optional[int] = None
    service_latency: Optional[ServiceLatency] = None
    account_token_position: Optional[int] = None
    advertise: Optional[bool] = None
    allow_trace: Optional[bool] = None

    info: Optional[Info] = field(default_factory=Info)

    class Meta:
        unflatten_fields = [("service_latency", "service_latency")]


Exports = List[Export]
