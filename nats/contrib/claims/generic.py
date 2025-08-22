from typing import List, Optional

from nats.contrib.flatten_model import FlatteningModel
from nats.contrib.types import Types


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
