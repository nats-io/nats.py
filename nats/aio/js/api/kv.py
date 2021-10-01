from datetime import datetime
from typing import TYPE_CHECKING, Dict, List, Optional

from nats.aio.errors import JetStreamAPIError
from nats.aio.js.models.consumers import AckPolicy, DeliverPolicy, ReplayPolicy
from nats.aio.js.models.messages import Message
from nats.aio.js.models.streams import (
    PubAck, Retention, Storage, StreamDeleteResponse, StreamInfoResponse,
    StreamMsgGetResponse
)
from nats.aio.messages import Msg

if TYPE_CHECKING:
    from nats.aio.js.client import JetStream  # pragma: no cover


class KeyValueAPI:
    def __init__(self, js: "JetStream") -> None:
        self._js = js

    async def add(
        self,
        name: str,
        history: int = -1,
        ttl: int = 0,
        max_bucket_size: int = -1,
        max_value_size: int = -1,
        duplicate_window: int = 0,
        replicas: int = 1,
        timeout: float = 1,
    ) -> StreamInfoResponse:
        """Add a new KV store (bucket).

        Args:
            * `name`: Name of the bucket.
            * `history`: How many historic values to keep per key.
            * `ttl`: How long to keep values for, expressed in nanoseconds.
            * `max_bucket_size`: Maximum size for the bucket in bytes.
            * `max_value_size`: Maximum size for any single value in bytes.
            * `duplicate_window`: The time window to track duplicate messages for, expressed in nanoseconds.
            * `replicas`: How many replicas of the data to store.
            * `timeout`: timeout to wait before raising a TimeoutError.
        """
        result = await self._js.stream.add(
            # The main write bucket must be called KV_<Bucket Name>
            f"KV_{name}",
            # The ingest subjects must be $KV.<Bucket Name>.>
            subjects=[f"$KV.{name}.>"],
            # Ack is always enabled
            no_ack=False,
            # Number of consumers is always illimited
            max_consumers=-1,
            # Limit is always "limits"
            retention=Retention.limits,
            # Storage is always "file"
            storage=Storage.file,
            # User can still configure replicas
            num_replicas=replicas,
            # Duplicate window must be same as max_age when max_age is less than 2 minutes
            duplicate_window=ttl if ttl < 60 * 5 * 1e9 else duplicate_window,
            # Key TTL is managed using the max_age key
            max_age=ttl,
            # Maximum value sizes can be capped using max_msg_size,
            max_msg_size=max_value_size,
            # Overall bucket size can be limited using max_bytes
            max_bytes=max_bucket_size,
            # The bucket history is achieved by setting max_msgs_per_subject to the desired history level
            max_msgs_per_subject=history,
            timeout=timeout,
        )
        return result

    async def rm(
        self,
        name: str,
        timeout: float = 1,
    ) -> StreamDeleteResponse:
        """Delete a KV store.

        Be careful, this will permanently delete a whole KV store.

        Args:
            * `name`: Name of the KV store (bucket) to remove
            * `timeout`: timeout to wait before raising a TimeoutError.
        """
        res = await self._js.stream.delete(f"KV_{name}", timeout=timeout)
        return res

    async def get(
        self,
        name: str,
        key: str,
        timeout: float = 1,
    ) -> Message:
        """Fetch a key from a KV store bucket"""
        result: StreamMsgGetResponse = await self._js.stream.msg_get(
            f"KV_{name}",
            last_by_subj=f"$KV.{name}.{key}",
            timeout=timeout,
        )
        if result.message.hdrs and result.message.hdrs.get("KV-Operation",
                                                           None) == "DEL":
            raise JetStreamAPIError(code="404", description="unknown key")
        return result.message

    async def put(
        self,
        name: str,
        key: str,
        value: bytes,
        timeout: float = 1,
        headers: Optional[Dict[str, str]] = None,
    ) -> PubAck:
        """Put a new value in a KV Store (bucket) under given key.

        This method can be used to both add a new key/value pair or update an existing key value
        """
        # Publish the message and return an acknowledgement
        return await self._js.stream.publish(
            f"$KV.{name}.{key}",
            payload=value,
            timeout=timeout,
            headers=headers
        )

    async def delete(
        self,
        name: str,
        key: str,
        timeout: float = 1,
    ) -> PubAck:
        """Delete a value from KV Store under given key"""
        headers = {"KV-Operation": "DEL"}
        return await self._js.stream.publish(
            f"$KV.{name}.{key}", payload=b"", timeout=timeout, headers=headers
        )

    async def history(
        self,
        name: str,
        key: str,
        timeout: float = 1,
    ) -> List[Msg]:
        """Get history of values under given key"""
        # Create a consumer without durable name
        _now = int(datetime.utcnow().timestamp() * 1000)
        _stream = f"KV_{name}"
        _subject = f"$KV.{name}.{key}"
        _delivery_subject = f"HISTORY.{_subject}.{_now}"
        versions: List[Msg] = []

        last_msg: Message = await self.get("demo", key, timeout=timeout)

        sub = await self._js._nc.subscribe(
            _delivery_subject, max_msgs=last_msg.seq
        )

        await self._js.consumer.add(
            _stream,
            filter_subject=_subject,
            ack_policy=AckPolicy.explicit,
            deliver_subject=_delivery_subject,
            deliver_policy=DeliverPolicy.all,
            replay_policy=ReplayPolicy.instant,
            timeout=timeout,
        )

        async for msg in sub.messages:
            versions.append(msg)
            await msg.ack()

        return versions
