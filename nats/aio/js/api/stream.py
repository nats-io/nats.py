import json
from base64 import b64decode
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from nats.aio.js.models.streams import (
    Discard, Mirror, PubAck, Retention, Source, Storage, StreamCreateRequest,
    StreamDeleteResponse, StreamInfoRequest, StreamInfoResponse,
    StreamListRequest, StreamListResponse, StreamMsgDeleteRequest,
    StreamMsgDeleteResponse, StreamMsgGetRequest, StreamMsgGetResponse,
    StreamNamesRequest, StreamNamesResponse, StreamPurgeRequest,
    StreamPurgeResponse, StreamUpdateRequest
)

if TYPE_CHECKING:
    from nats.aio.js.client import JetStream  # pragma: no cover


class StreamAPI:
    def __init__(self, js: "JetStream") -> None:
        self._js = js

    async def list(
        self, offset: int = 0, timeout: float = 1
    ) -> StreamListResponse:
        options = StreamListRequest(offset=offset)
        return await self._js._request(
            "STREAM.LIST",
            asdict(options),
            StreamListResponse,
            timeout=timeout
        )

    async def names(
        self,
        offset: int = 0,
        subject: Optional[str] = None,
        timeout: float = 1
    ) -> StreamNamesResponse:
        options = StreamNamesRequest(offset=offset, subject=subject)
        return await self._js._request(
            "STREAM.NAMES",
            asdict(options),
            StreamNamesResponse,
            timeout=timeout
        )

    async def add(
        self,
        name: str,
        subjects: Optional[List[str]] = None,
        retention: Retention = Retention.limits,
        max_consumers: int = -1,
        max_msgs: int = -1,
        max_msgs_per_subject: int = -1,
        max_bytes: int = -1,
        max_age: int = 0,
        max_msg_size: int = -1,
        storage: Storage = Storage.file,
        num_replicas: int = 1,
        duplicate_window: Optional[int] = 0,
        no_ack: Optional[bool] = False,
        mirror: Optional[Mirror] = None,
        sources: Optional[List[Source]] = None,
        timeout: float = 1,
        **kwargs: Any,
    ) -> StreamInfoResponse:
        """Create a new stream.

        Args:
            * `subjects`: A list of subjects to consume, supports wildcards. Must be empty when a mirror is configured. May be empty when sources are configured.
            * `retention`: How messages are retained in the Stream, once this is exceeded old messages are removed.
            * `max_consumers`: How many Consumers can be defined for a given Stream. -1 for unlimited.
            * `max_msgs`: How many messages may be in a Stream, oldest messages will be removed if the Stream exceeds this size. -1 for unlimited.
            * `max_msgs_per_subject`: For wildcard streams ensure that for every unique subject this many messages are kept - a per subject retention limit
            * `max_bytes`: How big the Stream may be, when the combined stream size exceeds this old messages are removed. -1 for unlimited.
            * `max_age`: Maximum age of any message in the stream, expressed in nanoseconds. 0 for unlimited.
            * `max_msg_size`: The largest message that will be accepted by the Stream. -1 for unlimited.
            * `storage`: The storage backend to use for the Stream ('file' or 'memory').
            * `num_replicas`: How many replicas to keep for each message.
            * `mirror`: Maintains a 1:1 mirror of another stream with name matching this argument.  When a mirror is configured subjects and sources must be empty.
            * `sources`: List of Stream names to replicate into this Stream.
            * `timeout`: timeout to wait before raising a TimeoutError.

        Returns:
            A StreamInfoResponse

        References:
            * Streams - [NATS Docs](https://docs.nats.io/jetstream/concepts/streams)
            * Stream, NATS API Reference - [NATS Docs](https://docs.nats.io/jetstream/nats_api_reference#streams)
            * `io.nats.jetstream.api.v1.stream_create_response` (JSON Schema): <https://github.com/nats-io/jsm.go/blob/v0.0.24/schemas/jetstream/api/v1/stream_create_request.json>
            * `io.nats.jetstream.api.v1.stream_create_request` (JSON Schema): <https://github.com/nats-io/jsm.go/blob/v0.0.24/schemas/jetstream/api/v1/stream_create_response.json>
        """
        options = StreamCreateRequest(
            name=name,
            subjects=subjects,
            retention=retention,
            max_consumers=max_consumers,
            max_msgs=max_msgs,
            max_msgs_per_subject=max_msgs_per_subject,
            max_bytes=max_bytes,
            max_msg_size=max_msg_size,
            max_age=max_age,
            storage=storage,
            num_replicas=num_replicas,
            mirror=mirror,
            sources=sources,
            duplicate_window=duplicate_window,
            no_ack=no_ack,
            **kwargs,
        )
        await self._js._request(
            f"STREAM.CREATE.{name}",
            asdict(options),
            StreamInfoResponse,
            timeout=timeout,
        )
        return await self.info(name)

    async def info(
        self,
        name: str,
        deleted_details: bool = False,
        timeout: float = 1,
    ) -> StreamInfoResponse:
        """Get info about a specific stream.

        Args:
            name: stream name. Argument is positional only.
            deleted_details: When set to True, response will include details about deleted messages.
            timeout: seconds to wait before raising a TimeoutError.

        Returns:
            An StreamInfoResponse
        """
        options = StreamInfoRequest(deleted_details=deleted_details)
        return await self._js._request(
            f"STREAM.INFO.{name}",
            asdict(options),
            StreamInfoResponse,
            timeout=timeout,
        )

    async def update(
        self,
        name: str,
        subjects: Optional[List[str]] = None,
        discard: Optional[Discard] = None,
        max_msgs: Optional[int] = None,
        max_msgs_per_subject: Optional[int] = None,
        max_bytes: Optional[int] = None,
        max_age: Optional[int] = None,
        num_replicas: Optional[int] = None,
        timeout: float = 1,
        **kwargs: Any,
    ) -> StreamInfoResponse:
        """Update an existing stream by its name.

        Args:
            * `subjects`: A list of subjects to consume, supports wildcards. Must be empty when a mirror is configured. May be empty when sources are configured.
            * `max_msgs`: How many messages may be in a Stream, oldest messages will be removed if the Stream exceeds this size. -1 for unlimited.
            * `max_msgs_per_subject`: For wildcard streams ensure that for every unique subject this many messages are kept - a per subject retention limit
            * `max_bytes`: How big the Stream may be, when the combined stream size exceeds this old messages are removed. -1 for unlimited.
            * `max_age`: Maximum age of any message in the stream, expressed in nanoseconds. 0 for unlimited.
            * `max_msg_size`: The largest message that will be accepted by the Stream. -1 for unlimited.
            * `storage`: The storage backend to use for the Stream ('file' or 'memory').
            * `num_replicas`: How many replicas to keep for each message.
            * `timeout`: timeout to wait before raising a TimeoutError.

        Returns:
            An StreamUpdateResponse

        References:
            * Streams - [NATS Docs](https://docs.nats.io/jetstream/concepts/streams)
            * Stream, NATS API Reference - [NATS Docs](https://docs.nats.io/jetstream/nats_api_reference#streams)
            * `io.nats.jetstream.api.v1.stream_update_response` (JSON Schema): <https://github.com/nats-io/jsm.go/blob/v0.0.24/schemas/jetstream/api/v1/stream_update_response.json>
            * `io.nats.jetstream.api.v1.stream_update_request` (JSON Schema): <https://github.com/nats-io/jsm.go/blob/v0.0.24/schemas/jetstream/api/v1/stream_update_request.json>
        """
        current_config = (await self.info(name, False)).config
        new_config: Dict[str, Any] = {}
        new_config["name"] = name
        if subjects is not None:
            new_config["subjects"] = subjects
        if discard is not None:
            new_config["discard"] = discard
        elif current_config.discard is not None:
            kwargs.pop("discard", None)
            try:
                new_config["discard"] = current_config.discard.value
            except AttributeError:
                new_config["discard"] = current_config.discard
        if max_msgs is not None:
            new_config["max_msgs"] = max_msgs
        if max_msgs_per_subject is not None:
            new_config["max_msgs_per_subject"] = max_msgs_per_subject
        if max_bytes is not None:
            new_config["max_bytes"] = max_bytes
        if max_age is not None:
            new_config["max_age"] = max_age
        if num_replicas is not None:
            new_config["num_replicas"] = num_replicas
        options = StreamUpdateRequest(
            **{
                **asdict(current_config),
                **kwargs,
                **new_config
            }
        )
        return await self._js._request(
            f"STREAM.UPDATE.{name}",
            {
                key: value
                for key, value in asdict(options).items() if value is not None
            },
            StreamInfoResponse,
            timeout=timeout,
        )

    async def delete(
        self,
        name: str,
        timeout: float = 1,
    ) -> StreamDeleteResponse:
        """Delete a stream by its name.

        Args:
            * `name`: Name of the stream
            * `timeout`: timeout to wait before raising a TimeoutError.

        Returns:
            A StreamDeleteResponse
        """
        return await self._js._request(
            f"STREAM.DELETE.{name}",
            None,
            StreamDeleteResponse,
            timeout=timeout,
        )

    async def purge(
        self,
        name: str,
        filter: Optional[str] = None,
        seq: Optional[int] = None,
        keep: Optional[int] = None,
        timeout: float = 1,
    ) -> StreamPurgeResponse:
        """Purge messages from a stream.

        Args:
            * `name`: Name of the stream
            * `filter`: Restrict purging to messages that match this subject.
            * `seq`: Purge all messages up to but not including the message with this sequence. Can be combined with subject filter but not the keep option.
            * `keep`: Ensures this many messages are present after the purge. Can be combined with the subject filter but not the sequence.
            * `timeout`: timeout to wait before raising a TimeoutError.

        Returns:
            A StreamPurgeResponse
        """
        options = StreamPurgeRequest(filter=filter, seq=seq, keep=keep)
        return await self._js._request(
            f"STREAM.PURGE.{name}",
            asdict(options),
            StreamPurgeResponse,
            timeout=timeout,
        )

    async def msg_get(
        self,
        name: str,
        seq: Optional[int] = None,
        last_by_subj: Optional[str] = None,
        timeout: float = 1,
    ) -> StreamMsgGetResponse:
        """Get a message from a stream by sequence.

        Args:
            * `name`: Name of the stream.
            * `seq`: Stream sequence number of the message to get.
            * `last_by_subj`: A subject from which last message will be get.
            * `timeout`: timeout to wait before raising a TimeoutError.

        Returns:
               An IoNatsJetstreamApiV1StreamMsgGetResponse or an IoNatsJetstreamApiV1ErrorResponse.
        """
        options = StreamMsgGetRequest(seq=seq, last_by_subj=last_by_subj)
        res = await self._js._request(
            f"STREAM.MSG.GET.{name}",
            asdict(options),
            StreamMsgGetResponse,
            timeout=timeout,
        )
        if isinstance(res.message.hdrs, str):
            bytes_hdrs = b64decode(res.message.hdrs)
            res.message.hdrs = self._js._nc._headers_parser.parse(bytes_hdrs)
        return res

    async def msg_delete(
        self,
        name: str,
        seq: int,
        no_erase: Optional[bool] = None,
        timeout: float = 1,
    ) -> StreamMsgDeleteResponse:
        """Delete a message from a stream.

        Args:
            * `name`: Name of the stream
            * `seq`: Stream sequence number of the message to delete.
            * `no_erase`: Default will securely remove a message and rewrite the data with random data, set this to true to only remove the message
            * `timeout`: timeout to wait before raising a TimeoutError.

        Returns:
            An IoNatsJetstreamApiV1StreamMsgDeleteResponse or an IoNatsJetstreamApiV1ErrorResponse.
        """
        options = StreamMsgDeleteRequest(seq=seq, no_erase=no_erase)
        return await self._js._request(
            f"STREAM.MSG.DELETE.{name}",
            asdict(options),
            StreamMsgDeleteResponse,
            timeout=timeout,
        )

    async def publish(
        self,
        subject: str,
        payload: bytes = b"",
        headers: Optional[Dict[str, str]] = None,
        timeout: float = 1,
    ) -> PubAck:
        """Publish a message to an NATS subject and wait for stream acknowledgement.

        Args:
            * `subject`: subject to publish message to
            * `payload`: content of the message in bytes
            * `timeout`: optional timeout in seconds
        """
        res = await self._js._nc.request(
            subject, payload, timeout=timeout, headers=headers
        )
        return PubAck(**json.loads(res.data))