from dataclasses import dataclass
from typing import List, Literal, Optional


@dataclass
class Import:
    name: str
    subject: str
    type: Literal["stream", "service"]
    account: str
    token: Optional[str]
    to: Optional[str]
    local_subject: Optional[str]
    share: Optional[bool]
    allow_trace: Optional[bool]


Imports = List[Import]
