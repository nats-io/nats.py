from typing import Optional
from dataclasses import dataclass, field

@dataclass
class Error(Exception):
    """
   	Represents an error that happens when using JetStream.
	"""
    code: Optional[int] = field(metadata={"json": "code"})
    error_code: Optional[int] = field(metadata={"json": "err_code"})
    description: Optional[str] = field(metadata={"json": "description"})

class JetStreamNotEnabledError(Error):
    pass

class JetStreamNotEnabledForAccountError(Error):
    pass

class InvalidAckError(Error):
    pass

class NoStreamResponseError(Error):
    pass
