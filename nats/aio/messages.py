from typing import TYPE_CHECKING, Dict, Optional

from nats.aio.errors import NatsError

if TYPE_CHECKING:
    from nats.aio.client import Client


class Msg:
    """
    Msg represents a message delivered by NATS.
    """
    __slots__ = ('subject', 'reply', 'data', 'sid', '_client', 'headers')

    def __init__(
        self,
        subject: str = '',
        reply: str = '',
        data: bytes = b'',
        sid: int = 0,
        client: Optional["Client"] = None,
        headers: Dict[str, str] = None,
    ) -> None:
        self.subject = subject
        self.reply = reply
        self.data = data
        self.sid = sid
        self._client = client
        self.headers = headers

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}: subject='{self.subject}' reply='{self.reply}' data='{self.data[:10].decode()}...'>"

    async def respond(self, data: bytes) -> None:
        if not self.reply:
            raise NatsError('no reply subject available')
        if not self._client:
            raise NatsError('client not set')

        await self._client.publish(self.reply, data, headers=self.headers)
