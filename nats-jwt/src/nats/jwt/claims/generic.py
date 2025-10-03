from typing import List, Optional

from nats.jwt.flatten_model import FlatteningModel
from nats.jwt.types import Types


class GenericFields(FlatteningModel):
    tags: Optional[List[str]]
    type: Optional[Types]
    version: Optional[int]

    def __init__(
        self,
        tags: Optional[List[str]] = None,
        type: Optional[Types] = None,
        version: Optional[int] = None,
    ):
        self.tags = tags
        self.type = type
        self.version = version
