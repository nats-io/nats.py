import json

from typing import Dict, Optional

from nats.aio.client import Client
from nats.jetstream.api import PubAck

class Publisher:
    def __init__(self, client: Client, timeout: float = 1):
        self._client = client
        self._timeout = timeout

    async def publish(
        self,
        subject: str,
        payload: bytes = b'',
        timeout: Optional[float] = None,
        stream: Optional[str] = None,
        headers: Optional[Dict] = None
    ) -> PubAck:
        """
        publish emits a new message to JetStream.
        """
        try:
            msg = await self._client.request(
                subject,
                payload,
                timeout=timeout or self._timeout,
                headers=headers,
            )
        except NoRespondersError:
            raise NoStreamResponseError

        data = json.loads(msg.data)
        if 'error' in data:
            raise Error(data['error'])

        return PubAck.from_dict(data)

__all__ = ["Publisher", "PubAck"]
